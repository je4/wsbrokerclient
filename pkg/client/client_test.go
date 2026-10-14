package client_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/wsbrokerclient/pkg/client"
	"github.com/je4/wsbrokerclient/pkg/event"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

type mockBrokerServer struct {
	mu           sync.Mutex
	connections  map[string]*websocket.Conn
	groups       map[string][]string // channel -> []clientName
	receivedEvts []*event.Event
	authBearer   string
	rejectCount  atomic.Int32
}

func newMockBrokerServer(bearer string) *mockBrokerServer {
	return &mockBrokerServer{
		connections: make(map[string]*websocket.Conn),
		groups:      make(map[string][]string),
		authBearer:  bearer,
	}
}

func (s *mockBrokerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.authBearer != "" {
		auth := r.Header.Get("Authorization")
		token := r.URL.Query().Get("token")
		if !strings.HasPrefix(auth, "Bearer "+s.authBearer) && token != s.authBearer {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	if s.rejectCount.Load() > 0 {
		s.rejectCount.Add(-1)
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}

	opts := &websocket.AcceptOptions{
		OriginPatterns: []string{"*"},
	}
	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		return
	}

	pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	clientName := "unknown"
	if len(pathParts) >= 2 && pathParts[0] == "ws" {
		clientName = pathParts[1]
	}

	s.mu.Lock()
	s.connections[clientName] = conn
	for _, ch := range r.URL.Query()["channel"] {
		s.groups[ch] = append(s.groups[ch], clientName)
	}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.connections, clientName)
		for ch, members := range s.groups {
			var newMembers []string
			for _, m := range members {
				if m != clientName {
					newMembers = append(newMembers, m)
				}
			}
			s.groups[ch] = newMembers
		}
		s.mu.Unlock()
		_ = conn.Close(websocket.StatusNormalClosure, "server closing")
	}()

	for {
		var evt event.Event
		if err := wsjson.Read(r.Context(), conn, &evt); err != nil {
			break
		}

		s.mu.Lock()
		s.receivedEvts = append(s.receivedEvts, &evt)

		switch evt.Type {
		case event.TypeAttach:
			ch, _ := evt.GetStringData()
			if ch == "" {
				ch = evt.Target
			}
			if ch != "" {
				s.groups[ch] = append(s.groups[ch], clientName)
			}
		case event.TypeDetach:
			ch, _ := evt.GetStringData()
			if ch == "" {
				ch = evt.Target
			}
			if ch != "" {
				var newMembers []string
				for _, m := range s.groups[ch] {
					if m != clientName {
						newMembers = append(newMembers, m)
					}
				}
				s.groups[ch] = newMembers
			}
		case event.TypePing:
			pongEvt, _ := event.NewPongEvent("mockbroker", clientName, evt.Data)
			_ = wsjson.Write(r.Context(), conn, pongEvt)
		case event.TypeMessage:
			// Direct or Channel fan-out
			if targetConn, exists := s.connections[evt.Target]; exists {
				_ = wsjson.Write(r.Context(), targetConn, &evt)
			} else if members, isGroup := s.groups[evt.Target]; isGroup {
				for _, m := range members {
					if memberConn, ok := s.connections[m]; ok {
						_ = wsjson.Write(r.Context(), memberConn, &evt)
					}
				}
			}
		}
		s.mu.Unlock()
	}
}

func TestClientConnectAndPublish(t *testing.T) {
	mockServer := newMockBrokerServer("test-secret-token")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	receivedCh := make(chan *event.Event, 10)

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("subscriber-1"),
		client.WithBearerToken("test-secret-token"),
		client.WithInitialChannels("alerts"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	cli.OnChannel("alerts", func(evt *event.Event) {
		receivedCh <- evt
	})

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect client: %v", err)
	}

	if !cli.IsConnected() {
		t.Fatalf("expected client to be connected")
	}

	// Create a second publisher client
	pubCli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("publisher-1"),
		client.WithBearerToken("test-secret-token"),
	)
	if err != nil {
		t.Fatalf("failed to create publisher client: %v", err)
	}
	defer pubCli.Close()

	if err := pubCli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect publisher client: %v", err)
	}

	// Broadcast to channel alerts
	testMsg := map[string]string{"msg": "fire alarm"}
	if err := pubCli.Broadcast(ctx, "alerts", testMsg); err != nil {
		t.Fatalf("failed to broadcast: %v", err)
	}

	select {
	case evt := <-receivedCh:
		if evt.Target != "alerts" {
			t.Errorf("expected target alerts, got %s", evt.Target)
		}
		var data map[string]string
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal received event data: %v", err)
		}
		if data["msg"] != "fire alarm" {
			t.Errorf("expected 'fire alarm', got '%s'", data["msg"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for event on alerts channel")
	}
}

func TestStreamSubscription(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("sub-stream-client"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	sub, err := cli.Subscribe(ctx, "telemetry")
	if err != nil {
		t.Fatalf("failed to subscribe to telemetry: %v", err)
	}
	defer sub.Unsubscribe()

	// Publish message to telemetry
	if err := cli.Broadcast(ctx, "telemetry", "metric-42"); err != nil {
		t.Fatalf("failed to broadcast to telemetry: %v", err)
	}

	select {
	case evt := <-sub.Events():
		str, err := evt.GetStringData()
		if err != nil {
			t.Fatalf("failed to get string data: %v", err)
		}
		if str != "metric-42" {
			t.Errorf("expected metric-42, got %s", str)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for stream subscription event")
	}
}

func TestDirectMessagingAndHandlers(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("alice"),
	)
	if err != nil {
		t.Fatalf("failed to create client alice: %v", err)
	}
	defer cli1.Close()

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("bob"),
	)
	if err != nil {
		t.Fatalf("failed to create client bob: %v", err)
	}
	defer cli2.Close()

	directCh := make(chan *event.Event, 10)
	cli2.OnDirect(func(evt *event.Event) {
		directCh <- evt
	})

	if err := cli1.ConnectSync(ctx); err != nil {
		t.Fatalf("alice connect failed: %v", err)
	}
	if err := cli2.ConnectSync(ctx); err != nil {
		t.Fatalf("bob connect failed: %v", err)
	}

	// Alice sends direct message to Bob
	if err := cli1.SendDirect(ctx, "bob", "secret rendezvous"); err != nil {
		t.Fatalf("failed to send direct message: %v", err)
	}

	select {
	case evt := <-directCh:
		if evt.Source != "alice" || evt.Target != "bob" {
			t.Errorf("unexpected event source/target: %s -> %s", evt.Source, evt.Target)
		}
		str, _ := evt.GetStringData()
		if str != "secret rendezvous" {
			t.Errorf("expected 'secret rendezvous', got '%s'", str)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for direct message")
	}
}

func TestDynamicAttachAndDetach(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("dynamic-client"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	if err := cli.Attach(ctx, "logs", "metrics"); err != nil {
		t.Fatalf("attach failed: %v", err)
	}

	channels := cli.Channels()
	if len(channels) != 2 {
		t.Fatalf("expected 2 channels, got %d: %v", len(channels), channels)
	}

	if err := cli.Detach(ctx, "logs"); err != nil {
		t.Fatalf("detach failed: %v", err)
	}

	channels = cli.Channels()
	if len(channels) != 1 || channels[0] != "metrics" {
		t.Fatalf("expected only metrics channel, got: %v", channels)
	}
}

func TestAutoReconnectionAndChannelResubscription(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reconnectCalled := make(chan struct{}, 1)

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("reconnecting-client"),
		client.WithInitialChannels("ch1"),
		client.WithReconnectStrategy(100*time.Millisecond, 500*time.Millisecond, 1.2, 5),
		client.WithAutoReconnect(true),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	cli.OnReconnect(func() {
		select {
		case reconnectCalled <- struct{}{}:
		default:
		}
	})

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("connect failed: %v", err)
	}

	// Dynamically attach ch2
	if err := cli.Attach(ctx, "ch2"); err != nil {
		t.Fatalf("attach ch2 failed: %v", err)
	}

	// Force disconnect from server side
	mockServer.mu.Lock()
	if conn, ok := mockServer.connections["reconnecting-client"]; ok {
		_ = conn.Close(websocket.StatusAbnormalClosure, "forced server drop")
	}
	mockServer.mu.Unlock()

	// Wait for reconnect callback
	select {
	case <-reconnectCalled:
		// Reconnected!
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for auto-reconnection")
	}

	// Verify client state is Connected
	if !cli.IsConnected() {
		t.Errorf("expected client state to be connected, got %s", cli.GetState())
	}

	// Verify server re-received subscriptions for ch1 and ch2
	time.Sleep(200 * time.Millisecond)
	mockServer.mu.Lock()
	defer mockServer.mu.Unlock()

	ch1Members := mockServer.groups["ch1"]
	ch2Members := mockServer.groups["ch2"]

	hasCh1 := false
	for _, m := range ch1Members {
		if m == "reconnecting-client" {
			hasCh1 = true
			break
		}
	}
	hasCh2 := false
	for _, m := range ch2Members {
		if m == "reconnecting-client" {
			hasCh2 = true
			break
		}
	}

	if !hasCh1 || !hasCh2 {
		t.Errorf("expected server to have re-registered ch1 and ch2, got ch1=%v, ch2=%v", ch1Members, ch2Members)
	}
}

func TestTLSConnection(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewTLSServer(mockServer)
	defer ts.Close()

	// Use test server CA pool
	certPool := x509.NewCertPool()
	certPool.AddCert(ts.Certificate())

	tlsConfig := &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: false,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("tls-client"),
		client.WithTLSConfig(tlsConfig),
	)
	if err != nil {
		t.Fatalf("failed to create TLS client: %v", err)
	}
	defer cli.Close()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect over TLS: %v", err)
	}

	if !cli.IsConnected() {
		t.Fatal("expected TLS client to be connected")
	}

	// Ping test
	if err := cli.Ping(ctx); err != nil {
		t.Fatalf("failed to ping over TLS: %v", err)
	}
}

func TestMTLSConnection(t *testing.T) {
	serverLoaderConfig := &loader.Config{
		Type: "DEV",
	}
	serverTLSConfig, serverCloser, err := loader.CreateServerLoader(true, serverLoaderConfig, nil, nil)
	if err != nil {
		t.Fatalf("failed to create server TLS loader: %v", err)
	}
	defer serverCloser.Close()

	mockServer := newMockBrokerServer("")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	tlsLn := tls.NewListener(ln, serverTLSConfig)
	httpServer := &http.Server{Handler: mockServer}
	go func() {
		_ = httpServer.Serve(tlsLn)
	}()
	defer httpServer.Close()

	serverURL := fmt.Sprintf("https://%s", ln.Addr().String())

	// 1. Client configured with mTLS client certificate via TLSCertLoader
	clientLoaderConfig := &loader.Config{
		Type: "DEV",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(serverURL),
		client.WithClientName("mtls-client"),
		client.WithTLSCertLoader(clientLoaderConfig),
	)
	if err != nil {
		t.Fatalf("failed to create mTLS client: %v", err)
	}
	defer cli.Close()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect over mTLS with certloader: %v", err)
	}

	if !cli.IsConnected() {
		t.Fatal("expected mTLS client to be connected")
	}

	if err := cli.Ping(ctx); err != nil {
		t.Fatalf("failed to ping over mTLS: %v", err)
	}

	// 2. Client configured with mTLS client certificate directly via TLSConfig
	clientTLSConfig, clientCloser, err := loader.CreateClientLoader(clientLoaderConfig, nil)
	if err != nil {
		t.Fatalf("failed to create client TLS config: %v", err)
	}
	defer clientCloser.Close()

	cliDirect, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(serverURL),
		client.WithClientName("mtls-direct-client"),
		client.WithTLSConfig(clientTLSConfig),
	)
	if err != nil {
		t.Fatalf("failed to create direct mTLS client: %v", err)
	}
	defer cliDirect.Close()

	if err := cliDirect.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect over direct mTLS: %v", err)
	}

	if !cliDirect.IsConnected() {
		t.Fatal("expected direct mTLS client to be connected")
	}

	// 3. Client WITHOUT client cert should fail against mTLS server
	cliNoCert, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(serverURL),
		client.WithClientName("no-cert-client"),
		client.WithTLSConfig(&tls.Config{
			InsecureSkipVerify: true,
		}),
	)
	if err != nil {
		t.Fatalf("failed to create client without cert: %v", err)
	}
	defer cliNoCert.Close()

	if err := cliNoCert.ConnectSync(ctx); err == nil {
		t.Fatal("expected client without client cert to fail connecting to mTLS server requiring client cert")
	}
}

func TestConnectionModeParsingAndMethods(t *testing.T) {
	// Parse valid modes
	for _, raw := range []string{"rw", "RW", "readwrite", "ReadWrite"} {
		m, err := client.ParseConnectionMode(raw)
		if err != nil {
			t.Fatalf("unexpected error parsing %s: %v", raw, err)
		}
		if m != client.ModeRW {
			t.Errorf("expected ModeRW, got %v", m)
		}
		if !m.CanRead() || !m.CanWrite() {
			t.Errorf("ModeRW should allow read and write")
		}
	}

	for _, raw := range []string{"ro", "RO", "readonly", "ReadOnly"} {
		m, err := client.ParseConnectionMode(raw)
		if err != nil {
			t.Fatalf("unexpected error parsing %s: %v", raw, err)
		}
		if m != client.ModeRO {
			t.Errorf("expected ModeRO, got %v", m)
		}
		if !m.CanRead() || m.CanWrite() {
			t.Errorf("ModeRO should allow read but not write")
		}
	}

	for _, raw := range []string{"wo", "WO", "writeonly", "WriteOnly", "w", "W"} {
		m, err := client.ParseConnectionMode(raw)
		if err != nil {
			t.Fatalf("unexpected error parsing %s: %v", raw, err)
		}
		if m != client.ModeWO {
			t.Errorf("expected ModeWO, got %v", m)
		}
		if m.CanRead() || !m.CanWrite() {
			t.Errorf("ModeWO should allow write but not read")
		}
	}

	// Invalid mode
	if _, err := client.ParseConnectionMode("invalid"); err == nil {
		t.Error("expected error for invalid mode, got nil")
	}
}

func TestBuildURLWithModesAndChannels(t *testing.T) {
	var requestedPath string
	var requestedQuery string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		requestedQuery = r.URL.RawQuery
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Default ModeRW path /ws/:name/rw
	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("client-1"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli1.Close()
	_ = cli1.ConnectSync(ctx)
	if requestedPath != "/ws/client-1/rw" {
		t.Errorf("expected path /ws/client-1/rw, got %s", requestedPath)
	}

	// 2. Explicit ModeRO
	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("client-ro"),
		client.WithMode(client.ModeRO),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli2.Close()
	if cli2.Mode() != client.ModeRO {
		t.Errorf("expected ModeRO, got %v", cli2.Mode())
	}
	_ = cli2.ConnectSync(ctx)
	if requestedPath != "/ws/client-ro/ro" {
		t.Errorf("expected path /ws/client-ro/ro, got %s", requestedPath)
	}

	// 3. Explicit ModeWO
	cli3, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("client-wo"),
		client.WithMode(client.ModeWO),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli3.Close()
	if cli3.Mode() != client.ModeWO {
		t.Errorf("expected ModeWO, got %v", cli3.Mode())
	}
	_ = cli3.ConnectSync(ctx)
	if requestedPath != "/ws/client-wo/wo" {
		t.Errorf("expected path /ws/client-wo/wo, got %s", requestedPath)
	}

	// 4. Channel route without mode in endpoint URL: /ws/channel/alerts -> /ws/channel/alerts/rw
	cli4, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL+"/ws/channel/alerts"),
		client.WithClientName("subscriber-1"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli4.Close()
	_ = cli4.ConnectSync(ctx)
	if requestedPath != "/ws/channel/alerts/rw" {
		t.Errorf("expected path /ws/channel/alerts/rw, got %s", requestedPath)
	}
	if !strings.Contains(requestedQuery, "name=subscriber-1") {
		t.Errorf("expected query to contain name=subscriber-1, got %s", requestedQuery)
	}

	// 5. Channel route with explicit mode in endpoint URL: /ws/channel/alerts/ro
	cli5, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL+"/ws/channel/alerts/ro"),
		client.WithClientName("subscriber-ro"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli5.Close()
	_ = cli5.ConnectSync(ctx)
	if requestedPath != "/ws/channel/alerts/ro" {
		t.Errorf("expected path /ws/channel/alerts/ro, got %s", requestedPath)
	}
}
