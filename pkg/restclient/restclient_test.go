package restclient_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/je4/wsbrokerclient/pkg/restclient"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

func TestRESTClientEndpoints(t *testing.T) {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("pong"))
	})

	mux.HandleFunc("/api/v1/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		auth := r.Header.Get("Authorization")
		if auth != "Bearer secret-123" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		var req restclient.PublishRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp := restclient.PublishResponse{
			Target:      req.Target,
			Type:        req.Type,
			Sent:        true,
			Subscribers: 2,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/api/v1/channel/alerts/publish", func(w http.ResponseWriter, r *http.Request) {
		resp := restclient.PublishResponse{
			Target:      "alerts",
			Type:        "message",
			Sent:        true,
			Subscribers: 5,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/api/v1/channels", func(w http.ResponseWriter, r *http.Request) {
		channels := []restclient.ChannelInfo{
			{Name: "alerts", Subscribers: 3, Clients: []string{"c1", "c2", "c3"}},
			{Name: "news", Subscribers: 1, Clients: []string{"c1"}},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(channels)
	})

	mux.HandleFunc("/api/v1/clients", func(w http.ResponseWriter, r *http.Request) {
		clients := []string{"c1", "c2", "c3"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(clients)
	})

	mux.HandleFunc("/api/v1/channel/alerts/clients", func(w http.ResponseWriter, r *http.Request) {
		clients := []string{"c1", "c2", "c3"}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(clients)
	})

	mux.HandleFunc("/api/v1/client/c1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := restclient.New(restclient.Config{
		BaseURL: ts.URL,
		Bearer:  "secret-123",
	})
	if err != nil {
		t.Fatalf("failed to create restclient: %v", err)
	}
	defer client.Close()

	// 1. Ping
	pong, err := client.Ping(ctx)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	if pong != "pong" {
		t.Errorf("expected 'pong', got '%s'", pong)
	}

	// 2. Publish Event
	evt, err := event.NewMessageEvent("test-source", "alerts", map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("failed to create event: %v", err)
	}
	pubResp, err := client.Publish(ctx, evt)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	if !pubResp.Sent || pubResp.Subscribers != 2 {
		t.Errorf("unexpected publish response: %+v", pubResp)
	}

	// 3. Publish to Channel
	chResp, err := client.PublishToChannel(ctx, "alerts", "test-source", "hello alerts")
	if err != nil {
		t.Fatalf("PublishToChannel failed: %v", err)
	}
	if chResp.Subscribers != 5 {
		t.Errorf("expected 5 subscribers, got %d", chResp.Subscribers)
	}

	// 4. Get Channels
	channels, err := client.GetChannels(ctx)
	if err != nil {
		t.Fatalf("GetChannels failed: %v", err)
	}
	if len(channels) != 2 || channels[0].Name != "alerts" {
		t.Errorf("unexpected channels: %+v", channels)
	}

	// 5. Get Clients
	clients, err := client.GetClients(ctx)
	if err != nil {
		t.Fatalf("GetClients failed: %v", err)
	}
	if len(clients) != 3 {
		t.Errorf("expected 3 clients, got %d", len(clients))
	}

	// 6. Get Channel Clients
	chClients, err := client.GetChannelClients(ctx, "alerts")
	if err != nil {
		t.Fatalf("GetChannelClients failed: %v", err)
	}
	if len(chClients) != 3 {
		t.Errorf("expected 3 channel clients, got %d", len(chClients))
	}

	// 7. Disconnect Client
	if err := client.DisconnectClient(ctx, "c1"); err != nil {
		t.Fatalf("DisconnectClient failed: %v", err)
	}
}

func TestRESTClientMTLS(t *testing.T) {
	serverLoaderConfig := &loader.Config{
		Type: "DEV",
	}
	serverTLSConfig, serverCloser, err := loader.CreateServerLoader(true, serverLoaderConfig, nil, nil)
	if err != nil {
		t.Fatalf("failed to create server TLS loader: %v", err)
	}
	defer serverCloser.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("pong"))
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	tlsLn := tls.NewListener(ln, serverTLSConfig)
	httpServer := &http.Server{Handler: mux}
	go func() {
		_ = httpServer.Serve(tlsLn)
	}()
	defer httpServer.Close()

	serverURL := fmt.Sprintf("https://%s", ln.Addr().String())

	// 1. RESTClient with TLSCertLoader
	clientLoaderConfig := &loader.Config{
		Type: "DEV",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := restclient.New(restclient.Config{
		BaseURL:       serverURL,
		TLSCertLoader: clientLoaderConfig,
	})
	if err != nil {
		t.Fatalf("failed to create mTLS rest client: %v", err)
	}
	defer cli.Close()

	pong, err := cli.Ping(ctx)
	if err != nil {
		t.Fatalf("mTLS Ping failed: %v", err)
	}
	if pong != "pong" {
		t.Errorf("expected 'pong', got '%s'", pong)
	}

	// 2. Client WITHOUT client cert should fail against mTLS server
	cliNoCert, err := restclient.New(restclient.Config{
		BaseURL: serverURL,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	})
	if err != nil {
		t.Fatalf("failed to create rest client without cert: %v", err)
	}
	defer cliNoCert.Close()

	if _, err := cliNoCert.Ping(ctx); err == nil {
		t.Fatal("expected request without client certificate to fail on mTLS server")
	}
}
