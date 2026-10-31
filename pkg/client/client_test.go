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
	receivedEvts []*event.RawEvent
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
	conn.SetReadLimit(16 * 1024 * 1024)

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
		var evt event.RawEvent
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
			pongData := any(evt.Data)
			if len(evt.Data) == 0 {
				pongData = "pong"
			}
			pongEvt, _ := event.NewRawEvent(event.TypePong, "mockbroker", clientName, pongData)
			pongEvt.RefID = evt.ID
			_ = wsjson.Write(r.Context(), conn, pongEvt)
		case event.TypeChannelInfoQuery:
			var targetChannel string
			_ = evt.UnmarshalData(&targetChannel)
			if targetChannel == "" {
				targetChannel = evt.Target
			}
			if targetChannel == "error-channel" {
				errEvt, _ := event.NewRawEvent(event.TypeChannelInfoError, "mockbroker", clientName, "channel access forbidden")
				errEvt.RefID = evt.ID
				_ = wsjson.Write(r.Context(), conn, errEvt)
			} else if targetChannel == "silent-channel" {
				// Don't respond to trigger timeout in test
			} else {
				members := s.groups[targetChannel]
				if members == nil {
					members = []string{}
				}
				info := event.ChannelInfo{
					Name:        targetChannel,
					Subscribers: len(members),
					Clients:     members,
				}
				respEvt, _ := event.NewRawEvent(event.TypeChannelInfoResponse, "mockbroker", clientName, info)
				respEvt.RefID = evt.ID
				_ = wsjson.Write(r.Context(), conn, respEvt)
			}
		default:
			if evt.ID == "" {
				evt.ID = event.GenerateEventID()
			}
			// Direct or Channel fan-out
			if targetConn, exists := s.connections[evt.Target]; exists {
				_ = wsjson.Write(r.Context(), targetConn, &evt)
			} else if members, isGroup := s.groups[evt.Target]; isGroup {
				evt.Channel = evt.Target
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

	receivedCh := make(chan *event.RawEvent, 10)

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

	cli.OnChannel("alerts", func(evt *event.RawEvent) {
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
		if evt.Channel != "alerts" {
			t.Errorf("expected channel alerts, got %s", evt.Channel)
		}
		if evt.ID == "" {
			t.Errorf("expected non-empty event ID")
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

	directCh := make(chan *event.RawEvent, 10)
	cli2.OnDirect(func(evt *event.RawEvent) {
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
	case <-time.After(10 * time.Second):
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

func TestClient_LargePayload(t *testing.T) {
	server := httptest.NewServer(newMockBrokerServer(""))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sender, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("sender"),
	)
	if err != nil {
		t.Fatalf("failed to create sender: %v", err)
	}
	defer sender.Close()
	if err := sender.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect sender: %v", err)
	}

	receiver, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("receiver"),
	)
	if err != nil {
		t.Fatalf("failed to create receiver: %v", err)
	}
	defer receiver.Close()

	received := make(chan *event.RawEvent, 1)
	receiver.OnEvent(func(evt *event.RawEvent) {
		if evt.Target == "receiver" {
			received <- evt
		}
	})

	if err := receiver.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect receiver: %v", err)
	}

	// Create a payload larger than 32 KiB (e.g. 64 KiB screenshot / file message)
	largeData := make([]byte, 64*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	fileMsg := event.NewFileMessage("test_screenshot.jpg", "image/jpeg", largeData)
	evt, err := event.NewRawEvent(event.TypeFile, "sender", "receiver", fileMsg)
	if err != nil {
		t.Fatalf("failed to create file event: %v", err)
	}

	if err := sender.Send(ctx, evt); err != nil {
		t.Fatalf("failed to send large event: %v", err)
	}

	select {
	case recEvt := <-received:
		data, err := recEvt.GetData()
		if err != nil {
			t.Fatalf("failed to parse received file message: %v", err)
		}
		fileContent, ok := data.(*event.FileMessage)
		if !ok {
			t.Fatalf("expected *event.FileMessage, got %T", data)
		}
		if len(fileContent.Data) != len(largeData) {
			t.Fatalf("expected payload size %d, got %d", len(largeData), len(fileContent.Data))
		}
		if fileContent.Filename != "test_screenshot.jpg" {
			t.Errorf("expected filename test_screenshot.jpg, got %s", fileContent.Filename)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for large event")
	}
}

func TestClient_QueryChannelInfo(t *testing.T) {
	server := httptest.NewServer(newMockBrokerServer(""))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Connect client 1 with initial channel "room1"
	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("client1"),
		client.WithInitialChannels("room1"),
	)
	if err != nil {
		t.Fatalf("failed to create client1: %v", err)
	}
	defer cli1.Close()
	if err := cli1.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect client1: %v", err)
	}

	// Connect client 2 with initial channel "room1"
	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("client2"),
		client.WithInitialChannels("room1"),
	)
	if err != nil {
		t.Fatalf("failed to create client2: %v", err)
	}
	defer cli2.Close()
	if err := cli2.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect client2: %v", err)
	}

	// 1. Successful channel info query
	info, err := cli1.QueryChannelInfo(ctx, "room1")
	if err != nil {
		t.Fatalf("QueryChannelInfo failed: %v", err)
	}
	if info.Name != "room1" {
		t.Errorf("expected channel name 'room1', got %s", info.Name)
	}
	if info.Subscribers != 2 {
		t.Errorf("expected 2 subscribers, got %d", info.Subscribers)
	}
	if len(info.Clients) != 2 {
		t.Errorf("expected 2 clients, got %v", info.Clients)
	}

	// 2. Convenience alias method ChannelInfo
	infoAlias, err := cli1.ChannelInfo(ctx, "room1")
	if err != nil {
		t.Fatalf("ChannelInfo alias failed: %v", err)
	}
	if infoAlias.Subscribers != 2 {
		t.Errorf("expected 2 subscribers from alias, got %d", infoAlias.Subscribers)
	}

	// 3. Query non-existent / empty channel
	emptyInfo, err := cli1.QueryChannelInfo(ctx, "nonexistent-room")
	if err != nil {
		t.Fatalf("query non-existent channel failed: %v", err)
	}
	if emptyInfo.Name != "nonexistent-room" {
		t.Errorf("expected name 'nonexistent-room', got %s", emptyInfo.Name)
	}
	if emptyInfo.Subscribers != 0 {
		t.Errorf("expected 0 subscribers, got %d", emptyInfo.Subscribers)
	}
	if len(emptyInfo.Clients) != 0 {
		t.Errorf("expected 0 clients, got %v", emptyInfo.Clients)
	}

	// 4. Empty channel name validation
	if _, err := cli1.QueryChannelInfo(ctx, ""); err == nil {
		t.Error("expected error for empty channel name, got nil")
	}
	if _, err := cli1.QueryChannelInfo(ctx, "   "); err == nil {
		t.Error("expected error for whitespace channel name, got nil")
	}

	// 5. Server error handling
	_, err = cli1.QueryChannelInfo(ctx, "error-channel")
	if err == nil {
		t.Error("expected error for error-channel, got nil")
	} else if !strings.Contains(err.Error(), "channel access forbidden") {
		t.Errorf("expected error message to contain 'channel access forbidden', got: %v", err)
	}

	// 6. Context timeout handling
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer timeoutCancel()
	_, err = cli1.QueryChannelInfo(timeoutCtx, "silent-channel")
	if err == nil {
		t.Error("expected timeout error for silent-channel, got nil")
	}

	// 7. Disconnected client error
	cliDisconnected, err := client.New(client.DefaultConfig(),
		client.WithEndpoint("ws://invalid-host:12345"),
		client.WithClientName("disconn-client"),
	)
	if err != nil {
		t.Fatalf("failed to create disconnected client: %v", err)
	}
	defer cliDisconnected.Close()

	if _, err := cliDisconnected.QueryChannelInfo(ctx, "room1"); err == nil {
		t.Error("expected error when querying on disconnected client, got nil")
	} else if !strings.Contains(err.Error(), "client is not connected") {
		t.Errorf("expected 'client is not connected' error, got %v", err)
	}
}

func TestClient_RequestReply(t *testing.T) {
	server := httptest.NewServer(newMockBrokerServer(""))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("requester"),
	)
	if err != nil {
		t.Fatalf("failed to create requester: %v", err)
	}
	defer cli1.Close()
	if err := cli1.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect requester: %v", err)
	}

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(server.URL),
		client.WithClientName("responder"),
	)
	if err != nil {
		t.Fatalf("failed to create responder: %v", err)
	}
	defer cli2.Close()
	if err := cli2.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect responder: %v", err)
	}

	// Responder listens for direct messages and responds using Reply
	cli2.OnDirect(func(evt *event.RawEvent) {
		var reqMsg string
		_ = evt.UnmarshalData(&reqMsg)

		_ = cli2.Reply(context.Background(), evt, fmt.Sprintf("echo:%s", reqMsg))
	})

	// 1. Successful request with auto-generated ID
	reqEvt, err := event.NewMessageEvent("", "responder", "hello-world")
	if err != nil {
		t.Fatalf("failed to create request event: %v", err)
	}

	resp, err := cli1.Request(ctx, reqEvt)
	if err != nil {
		t.Fatalf("Request failed: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if resp.RefID == "" || resp.RefID != reqEvt.ID {
		t.Errorf("expected RefID == %s, got %s", reqEvt.ID, resp.RefID)
	}
	var respMsg string
	if err := resp.UnmarshalData(&respMsg); err != nil {
		t.Fatalf("failed to unmarshal response data: %v", err)
	}
	if respMsg != "echo:hello-world" {
		t.Errorf("expected 'echo:hello-world', got '%s'", respMsg)
	}

	// 2. Request with explicit session
	customSession := "custom-session-12345"
	reqEvt2, _ := event.NewMessageEvent("", "responder", "custom-session-test")
	reqEvt2.Session = customSession

	resp2, err := cli1.Request(ctx, reqEvt2)
	if err != nil {
		t.Fatalf("Request with explicit session failed: %v", err)
	}
	if resp2.Session != customSession {
		t.Errorf("expected session '%s', got '%s'", customSession, resp2.Session)
	}

	// 3. Request timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer timeoutCancel()
	reqEvtSilent, _ := event.NewMessageEvent("", "non-existent-client", "will-timeout")
	_, err = cli1.Request(timeoutCtx, reqEvtSilent)
	if err == nil {
		t.Error("expected timeout error for non-existent target, got nil")
	}

	// 4. GenerateSessionID verification
	sessID, err := client.GenerateSessionID()
	if err != nil {
		t.Fatalf("GenerateSessionID failed: %v", err)
	}
	if len(sessID) != 32 { // 16 bytes hex-encoded = 32 chars
		t.Errorf("expected 32 character hex session ID, got %d chars: %s", len(sessID), sessID)
	}
}

func TestClient_RequestN(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("requester2"),
	)
	if err != nil {
		t.Fatalf("failed to create requester2: %v", err)
	}
	defer cli1.Close()
	if err := cli1.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect requester2: %v", err)
	}

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("responder2"),
	)
	if err != nil {
		t.Fatalf("failed to create responder2: %v", err)
	}
	defer cli2.Close()
	if err := cli2.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect responder2: %v", err)
	}

	// Responder sends multiple responses for any direct event
	cli2.OnDirect(func(evt *event.RawEvent) {
		var reqMsg string
		_ = evt.UnmarshalData(&reqMsg)

		for i := 1; i <= 3; i++ {
			respEvt, _ := event.NewMessageEvent("responder2", evt.Source, fmt.Sprintf("%s-part-%d", reqMsg, i))
			respEvt.RefID = evt.ID
			_ = cli2.Send(context.Background(), respEvt)
		}
	})

	// 1. Successful RequestN collecting 3 responses
	reqEvt, err := event.NewMessageEvent("", "responder2", "multi-part")
	if err != nil {
		t.Fatalf("failed to create request event: %v", err)
	}
	originalID := "some-prefilled-id"
	reqEvt.ID = originalID

	respCh := make(chan *event.RawEvent, 5)
	err = cli1.RequestN(ctx, reqEvt, 3, respCh)
	if err != nil {
		t.Fatalf("RequestN failed: %v", err)
	}

	// Verify ID was preserved
	if reqEvt.ID != originalID {
		t.Errorf("expected ID '%s', got '%s'", originalID, reqEvt.ID)
	}

	if len(respCh) != 3 {
		t.Fatalf("expected 3 responses in channel, got %d", len(respCh))
	}

	for i := 1; i <= 3; i++ {
		resp := <-respCh
		if resp.RefID != reqEvt.ID {
			t.Errorf("expected RefID == %s, got %s", reqEvt.ID, resp.RefID)
		}
		var msg string
		_ = resp.UnmarshalData(&msg)
		expected := fmt.Sprintf("multi-part-part-%d", i)
		if msg != expected {
			t.Errorf("expected msg '%s', got '%s'", expected, msg)
		}
	}

	// 2. Timeout when expecting more responses than sent
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer timeoutCancel()

	timeoutReq, _ := event.NewMessageEvent("", "responder2", "expect-too-many")
	timeoutCh := make(chan *event.RawEvent, 10)
	err = cli1.RequestN(timeoutCtx, timeoutReq, 5, timeoutCh)
	if err == nil {
		t.Error("expected timeout error when expecting 5 results from responder that sends 3, got nil")
	}
	// Verify that the 3 responses sent before timeout were still pushed into timeoutCh
	if len(timeoutCh) != 3 {
		t.Errorf("expected 3 responses pushed before timeout, got %d", len(timeoutCh))
	}

	// 3. Validation errors
	if err := cli1.RequestN(ctx, nil, 1, respCh); err == nil {
		t.Error("expected error for nil event, got nil")
	}
	if err := cli1.RequestN(ctx, reqEvt, 1, nil); err == nil {
		t.Error("expected error for nil channel, got nil")
	}
}

func TestClient_Request1(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("requester1"),
	)
	if err != nil {
		t.Fatalf("failed to create requester1: %v", err)
	}
	defer cli1.Close()
	if err := cli1.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect requester1: %v", err)
	}

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("responder1"),
	)
	if err != nil {
		t.Fatalf("failed to create responder1: %v", err)
	}
	defer cli2.Close()
	if err := cli2.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect responder1: %v", err)
	}

	cli2.OnDirect(func(evt *event.RawEvent) {
		var reqMsg string
		_ = evt.UnmarshalData(&reqMsg)

		respEvt, _ := event.NewMessageEvent("responder1", evt.Source, fmt.Sprintf("echo:%s", reqMsg))
		respEvt.RefID = evt.ID
		_ = cli2.Send(context.Background(), respEvt)
	})

	// 1. Successful Request1
	reqEvt, err := event.NewMessageEvent("", "responder1", "hello-req1")
	if err != nil {
		t.Fatalf("failed to create request event: %v", err)
	}
	originalID := "some-old-id"
	reqEvt.ID = originalID

	resp, err := cli1.Request1(ctx, reqEvt)
	if err != nil {
		t.Fatalf("Request1 failed: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
	if reqEvt.ID != originalID {
		t.Errorf("expected preserved ID '%s', got '%s'", originalID, reqEvt.ID)
	}
	if resp.RefID != reqEvt.ID {
		t.Errorf("expected RefID == %s, got %s", reqEvt.ID, resp.RefID)
	}
	var respMsg string
	if err := resp.UnmarshalData(&respMsg); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}
	if respMsg != "echo:hello-req1" {
		t.Errorf("expected 'echo:hello-req1', got '%s'", respMsg)
	}

	// 2. Timeout
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer timeoutCancel()

	timeoutReq, _ := event.NewMessageEvent("", "non-existent", "timeout-test")
	_, err = cli1.Request1(timeoutCtx, timeoutReq)
	if err == nil {
		t.Error("expected timeout error, got nil")
	}

	// 3. Validation error (nil event)
	if _, err := cli1.Request1(ctx, nil); err == nil {
		t.Error("expected error for nil event, got nil")
	}
}

func TestClient_Request0(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("requester0"),
	)
	if err != nil {
		t.Fatalf("failed to create requester0: %v", err)
	}
	defer cli1.Close()
	if err := cli1.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect requester0: %v", err)
	}

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("responder0"),
	)
	if err != nil {
		t.Fatalf("failed to create responder0: %v", err)
	}
	defer cli2.Close()
	if err := cli2.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect responder0: %v", err)
	}

	received := make(chan *event.RawEvent, 1)
	cli2.OnDirect(func(evt *event.RawEvent) {
		received <- evt
	})

	// 1. Successful Request0
	reqEvt, err := event.NewMessageEvent("", "responder0", "hello-req0")
	if err != nil {
		t.Fatalf("failed to create request event: %v", err)
	}
	originalID := "some-old-id"
	reqEvt.ID = originalID

	if err := cli1.Request0(ctx, reqEvt); err != nil {
		t.Fatalf("Request0 failed: %v", err)
	}

	if reqEvt.ID != originalID {
		t.Errorf("expected preserved ID '%s', got '%s'", originalID, reqEvt.ID)
	}

	select {
	case evt := <-received:
		if evt.ID != reqEvt.ID {
			t.Errorf("expected received event ID %s, got %s", reqEvt.ID, evt.ID)
		}
		var msg string
		_ = evt.UnmarshalData(&msg)
		if msg != "hello-req0" {
			t.Errorf("expected msg 'hello-req0', got '%s'", msg)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for event on responder")
	}

	// 2. Validation error (nil event)
	if err := cli1.Request0(ctx, nil); err == nil {
		t.Error("expected error for nil event, got nil")
	}
}

func TestClient_ChannelAndIDRouting(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("receiver-client"),
		client.WithInitialChannels("sports"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect client: %v", err)
	}

	chanCh := make(chan *event.RawEvent, 5)
	cli.OnChannel("sports", func(evt *event.RawEvent) {
		chanCh <- evt
	})

	sub, err := cli.Subscribe(ctx, "sports")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// 1. Send broadcast from publisher
	pubCli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("publisher"),
	)
	if err != nil {
		t.Fatalf("failed to create publisher: %v", err)
	}
	defer pubCli.Close()
	if err := pubCli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect publisher: %v", err)
	}

	if err := pubCli.Broadcast(ctx, "sports", "goal scored"); err != nil {
		t.Fatalf("failed to broadcast: %v", err)
	}

	// Verify channel listener received event with ID and Channel
	select {
	case evt := <-chanCh:
		if evt.Channel != "sports" {
			t.Errorf("expected Channel 'sports', got %q", evt.Channel)
		}
		if evt.ID == "" {
			t.Errorf("expected non-empty event ID")
		}
		str, _ := evt.GetStringData()
		if str != "goal scored" {
			t.Errorf("expected 'goal scored', got %q", str)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for OnChannel listener")
	}

	// Verify stream subscription received event with ID and Channel
	select {
	case evt := <-sub.Events():
		if evt.Channel != "sports" {
			t.Errorf("expected subscription Channel 'sports', got %q", evt.Channel)
		}
		if evt.ID == "" {
			t.Errorf("expected non-empty event ID")
		}
		str, _ := evt.GetStringData()
		if str != "goal scored" {
			t.Errorf("expected 'goal scored', got %q", str)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Subscription events channel")
	}
}

func TestClient_ReplyAndRefIDCorrelation(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliA, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("client-a"),
	)
	if err != nil {
		t.Fatalf("failed to create client-a: %v", err)
	}
	defer cliA.Close()
	if err := cliA.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect client-a: %v", err)
	}

	cliB, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("client-b"),
	)
	if err != nil {
		t.Fatalf("failed to create client-b: %v", err)
	}
	defer cliB.Close()
	if err := cliB.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect client-b: %v", err)
	}

	replyReceived := make(chan *event.RawEvent, 1)
	cliA.OnDirect(func(evt *event.RawEvent) {
		replyReceived <- evt
	})

	cliB.OnDirect(func(evt *event.RawEvent) {
		// Reply back to client-a
		_ = cliB.Reply(ctx, evt, "response-to-alice")
	})

	reqEvt, _ := event.NewMessageEvent("client-a", "client-b", "hello-bob")
	reqEvt.ID = "msg-12345"
	if err := cliA.Send(ctx, reqEvt); err != nil {
		t.Fatalf("failed to send initial message: %v", err)
	}

	select {
	case reply := <-replyReceived:
		if reply.RefID != "msg-12345" {
			t.Errorf("expected reply RefID 'msg-12345', got %q", reply.RefID)
		}
		if reply.Source != "client-b" {
			t.Errorf("expected reply Source 'client-b', got %q", reply.Source)
		}
		if reply.Target != "client-a" {
			t.Errorf("expected reply Target 'client-a', got %q", reply.Target)
		}
		str, _ := reply.GetStringData()
		if str != "response-to-alice" {
			t.Errorf("expected 'response-to-alice', got %q", str)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for reply")
	}
}

func TestClient_AutomaticPongRefID(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("pong-test-client"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()
	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect client: %v", err)
	}

	// Wait briefly for connection registration on server
	time.Sleep(100 * time.Millisecond)

	mockServer.mu.Lock()
	conn := mockServer.connections["pong-test-client"]
	mockServer.mu.Unlock()
	if conn == nil {
		t.Fatal("expected mockServer connection for client")
	}

	// Send Ping from mock server to client with specific ID
	pingEvt, _ := event.NewRawEvent(event.TypePing, "mockbroker", "pong-test-client", "ping-payload")
	pingEvt.ID = "ping-id-98765"
	if err := wsjson.Write(ctx, conn, pingEvt); err != nil {
		t.Fatalf("failed to write ping: %v", err)
	}

	// Wait for Pong response from client
	deadline := time.Now().Add(3 * time.Second)
	var foundPong *event.RawEvent
	for time.Now().Before(deadline) {
		mockServer.mu.Lock()
		for _, e := range mockServer.receivedEvts {
			if e.Type == event.TypePong && e.Source == "pong-test-client" {
				foundPong = e
				break
			}
		}
		mockServer.mu.Unlock()
		if foundPong != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if foundPong == nil {
		t.Fatal("never received automatic pong response from client")
	}
	if foundPong.RefID != "ping-id-98765" {
		t.Errorf("expected Pong RefID 'ping-id-98765', got %q", foundPong.RefID)
	}
}

type rpcReq struct {
	Command string `json:"command"`
}

type rpcResp struct {
	Status string `json:"status"`
	Code   int    `json:"code"`
}

func TestClient_RequestTypedAndPayload(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("typed-caller"),
	)
	if err != nil {
		t.Fatalf("failed to create caller: %v", err)
	}
	defer cli1.Close()
	if err := cli1.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect caller: %v", err)
	}

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("typed-callee"),
	)
	if err != nil {
		t.Fatalf("failed to create callee: %v", err)
	}
	defer cli2.Close()
	if err := cli2.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect callee: %v", err)
	}

	cli2.OnDirect(func(evt *event.RawEvent) {
		var req rpcReq
		_ = evt.UnmarshalData(&req)
		respPayload := rpcResp{
			Status: "ok:" + req.Command,
			Code:   200,
		}
		_ = cli2.Reply(context.Background(), evt, respPayload)
	})

	// 1. Test RequestTyped
	rawReq, err := event.NewMessageEvent("typed-caller", "typed-callee", rpcReq{Command: "get_config"})
	if err != nil {
		t.Fatalf("NewMessageEvent failed: %v", err)
	}
	typedResp, err := client.RequestTyped[rpcResp](ctx, cli1, rawReq)
	if err != nil {
		t.Fatalf("RequestTyped failed: %v", err)
	}
	if typedResp.Data.Status != "ok:get_config" || typedResp.Data.Code != 200 {
		t.Errorf("unexpected typed response payload: %+v", typedResp.Data)
	}
	if typedResp.RefID != rawReq.ID {
		t.Errorf("expected correlated RefID %s, got %s", rawReq.ID, typedResp.RefID)
	}

	// 2. Test RequestPayload
	typedResp2, err := client.RequestPayload[rpcResp, rpcReq](ctx, cli1, "typed-callee", event.TypeMessage, rpcReq{Command: "restart"})
	if err != nil {
		t.Fatalf("RequestPayload failed: %v", err)
	}
	if typedResp2.Data.Status != "ok:restart" || typedResp2.Data.Code != 200 {
		t.Errorf("unexpected RequestPayload response: %+v", typedResp2.Data)
	}
}

func TestClient_NormalizeIncomingEvent(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Client attached to single channel
	cliSingleChan, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("single-chan-client"),
		client.WithInitialChannels("single-room"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cliSingleChan.Close()
	if err := cliSingleChan.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	getConn := func(name string) *websocket.Conn {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mockServer.mu.Lock()
			conn := mockServer.connections[name]
			mockServer.mu.Unlock()
			if conn != nil {
				return conn
			}
			time.Sleep(10 * time.Millisecond)
		}
		return nil
	}

	connSingle := getConn("single-chan-client")
	if connSingle == nil {
		t.Fatal("expected mockServer connection for single-chan-client")
	}

	// Case A: Event with Target="" and Channel="my-chan" -> Target normalized to "my-chan"
	var receivedA atomic.Pointer[event.RawEvent]
	cliSingleChan.OnEvent(func(evt *event.RawEvent) {
		if evt.ID == "evt-norm-a" {
			receivedA.Store(evt)
		}
	})

	evtA := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:      "evt-norm-a",
			Type:    event.TypeMessage,
			Source:  "sender",
			Channel: "custom-chan",
		},
		Data: []byte(`"msg-a"`),
	}
	_ = wsjson.Write(ctx, connSingle, evtA)

	// Case B: Event with Target="" and Channel="" -> Normalized to single channel "single-room"
	var receivedB atomic.Pointer[event.RawEvent]
	cliSingleChan.OnEvent(func(evt *event.RawEvent) {
		if evt.ID == "evt-norm-b" {
			receivedB.Store(evt)
		}
	})

	evtB := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:     "evt-norm-b",
			Type:   event.TypeMessage,
			Source: "sender",
		},
		Data: []byte(`"msg-b"`),
	}
	_ = wsjson.Write(ctx, connSingle, evtB)

	// Case C: Event with Channel="" and Target="single-room" -> Channel normalized to "single-room"
	var receivedC atomic.Pointer[event.RawEvent]
	cliSingleChan.OnEvent(func(evt *event.RawEvent) {
		if evt.ID == "evt-norm-c" {
			receivedC.Store(evt)
		}
	})

	evtC := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:     "evt-norm-c",
			Type:   event.TypeMessage,
			Source: "sender",
			Target: "single-room",
		},
		Data: []byte(`"msg-c"`),
	}
	_ = wsjson.Write(ctx, connSingle, evtC)

	// 2. Client with no channels: Target="" and Channel="" -> Target normalized to clientName
	cliNoChan, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("no-chan-client"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cliNoChan.Close()
	if err := cliNoChan.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	connNoChan := getConn("no-chan-client")
	if connNoChan == nil {
		t.Fatal("expected mockServer connection for no-chan-client")
	}

	var receivedD atomic.Pointer[event.RawEvent]
	cliNoChan.OnEvent(func(evt *event.RawEvent) {
		if evt.ID == "evt-norm-d" {
			receivedD.Store(evt)
		}
	})

	evtD := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:     "evt-norm-d",
			Type:   event.TypeMessage,
			Source: "sender",
		},
		Data: []byte(`"msg-d"`),
	}
	_ = wsjson.Write(ctx, connNoChan, evtD)

	// Wait and verify
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if receivedA.Load() != nil && receivedB.Load() != nil && receivedC.Load() != nil && receivedD.Load() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	resA := receivedA.Load()
	if resA == nil {
		t.Fatal("never received evtA")
	}
	if resA.Target != "custom-chan" || resA.Channel != "custom-chan" {
		t.Errorf("resA expected Target 'custom-chan', got Target=%q Channel=%q", resA.Target, resA.Channel)
	}

	resB := receivedB.Load()
	if resB == nil {
		t.Fatal("never received evtB")
	}
	if resB.Target != "single-room" || resB.Channel != "single-room" {
		t.Errorf("resB expected Target 'single-room' and Channel 'single-room', got Target=%q Channel=%q", resB.Target, resB.Channel)
	}

	resC := receivedC.Load()
	if resC == nil {
		t.Fatal("never received evtC")
	}
	if resC.Channel != "single-room" || resC.Target != "single-room" {
		t.Errorf("resC expected Target 'single-room' and Channel 'single-room', got Target=%q Channel=%q", resC.Target, resC.Channel)
	}

	resD := receivedD.Load()
	if resD == nil {
		t.Fatal("never received evtD")
	}
	if resD.Target != "no-chan-client" {
		t.Errorf("resD expected Target 'no-chan-client', got %q", resD.Target)
	}

	// 3. Client with multiple channels: Target="" and Channel="" -> Target normalized to clientName
	cliMultiChan, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("multi-chan-client"),
		client.WithInitialChannels("room1", "room2"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cliMultiChan.Close()
	if err := cliMultiChan.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	connMulti := getConn("multi-chan-client")
	if connMulti == nil {
		t.Fatal("expected mockServer connection for multi-chan-client")
	}

	var receivedE atomic.Pointer[event.RawEvent]
	cliMultiChan.OnEvent(func(evt *event.RawEvent) {
		if evt.ID == "evt-norm-e" {
			receivedE.Store(evt)
		}
	})

	evtE := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:     "evt-norm-e",
			Type:   event.TypeMessage,
			Source: "sender",
		},
		Data: []byte(`"msg-e"`),
	}
	_ = wsjson.Write(ctx, connMulti, evtE)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if receivedE.Load() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	resE := receivedE.Load()
	if resE == nil {
		t.Fatal("never received evtE")
	}
	if resE.Target != "multi-chan-client" {
		t.Errorf("resE expected Target 'multi-chan-client', got %q", resE.Target)
	}
}

func TestClient_ListenerIsolation_ChannelVsDirect(t *testing.T) {
	mockServer := newMockBrokerServer("")
	ts := httptest.NewServer(mockServer)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("listener-isolation-client"),
		client.WithInitialChannels("broadcast-room"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()
	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var conn *websocket.Conn
	for time.Now().Before(deadline) {
		mockServer.mu.Lock()
		conn = mockServer.connections["listener-isolation-client"]
		mockServer.mu.Unlock()
		if conn != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if conn == nil {
		t.Fatal("mock server connection not found")
	}

	var directCount atomic.Int32
	var channelCount atomic.Int32
	var globalCount atomic.Int32

	cli.OnDirect(func(evt *event.RawEvent) {
		directCount.Add(1)
	})
	cli.OnChannel("broadcast-room", func(evt *event.RawEvent) {
		channelCount.Add(1)
	})
	cli.OnEvent(func(evt *event.RawEvent) {
		globalCount.Add(1)
	})

	// 1. Send channel broadcast message (Channel="broadcast-room", Target="")
	broadcastEvt := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:      "broadcast-1",
			Type:    event.TypeMessage,
			Source:  "publisher",
			Channel: "broadcast-room",
		},
		Data: []byte(`"hello room"`),
	}
	if err := wsjson.Write(ctx, conn, broadcastEvt); err != nil {
		t.Fatalf("failed to write broadcast event: %v", err)
	}

	// Wait briefly to process broadcast
	time.Sleep(100 * time.Millisecond)

	if channelCount.Load() != 1 {
		t.Errorf("expected channelCount 1 for broadcast, got %d", channelCount.Load())
	}
	if directCount.Load() != 0 {
		t.Errorf("expected directCount 0 for broadcast, got %d (channel broadcast triggered OnDirect!)", directCount.Load())
	}
	if globalCount.Load() != 1 {
		t.Errorf("expected globalCount 1 for broadcast, got %d", globalCount.Load())
	}

	// 2. Send direct point-to-point message (Target="listener-isolation-client", Channel="")
	directEvt := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:     "direct-1",
			Type:   event.TypeMessage,
			Source: "caller",
			Target: "listener-isolation-client",
		},
		Data: []byte(`"hello direct"`),
	}
	if err := wsjson.Write(ctx, conn, directEvt); err != nil {
		t.Fatalf("failed to write direct event: %v", err)
	}

	// Wait briefly to process direct message
	time.Sleep(100 * time.Millisecond)

	if directCount.Load() != 1 {
		t.Errorf("expected directCount 1 for direct message, got %d", directCount.Load())
	}
	if channelCount.Load() != 1 {
		t.Errorf("expected channelCount to remain 1, got %d (direct message triggered OnChannel!)", channelCount.Load())
	}
	if globalCount.Load() != 2 {
		t.Errorf("expected globalCount 2, got %d", globalCount.Load())
	}
}

func TestClient_CreateReply_SetsClientSource(t *testing.T) {
	cli, err := client.New(client.DefaultConfig(),
		client.WithClientName("responding-client"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	// 1. Direct incoming message
	directEvt := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:     "msg-1",
			Type:   event.TypeMessage,
			Source: "caller-alice",
			Target: "responding-client",
		},
		Data: []byte(`"hello"`),
	}

	reply, err := cli.CreateReply(directEvt, event.TypePong, "pong-response")
	if err != nil {
		t.Fatalf("CreateReply failed: %v", err)
	}
	if reply.Source != "responding-client" {
		t.Errorf("expected reply Source to be 'responding-client', got %q", reply.Source)
	}
	if reply.Target != "caller-alice" {
		t.Errorf("expected reply Target to be 'caller-alice', got %q", reply.Target)
	}
	if reply.RefID != "msg-1" {
		t.Errorf("expected reply RefID to be 'msg-1', got %q", reply.RefID)
	}

	// 2. Channel broadcast incoming message
	broadcastEvt := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:      "broadcast-2",
			Type:    event.TypeMessage,
			Source:  "caller-bob",
			Channel: "room-lobby",
			Target:  "room-lobby",
		},
		Data: []byte(`"broadcast announcement"`),
	}

	chanReply, err := cli.CreateReply(broadcastEvt, event.TypeMessage, "broadcast acknowledgement")
	if err != nil {
		t.Fatalf("CreateReply on broadcast failed: %v", err)
	}
	if chanReply.Source != "responding-client" {
		t.Errorf("expected chanReply Source to be 'responding-client', got %q", chanReply.Source)
	}
	if chanReply.Target != "caller-bob" {
		t.Errorf("expected chanReply Target to be 'caller-bob', got %q", chanReply.Target)
	}
	if chanReply.Channel != "room-lobby" {
		t.Errorf("expected chanReply Channel to be 'room-lobby', got %q", chanReply.Channel)
	}
}
