package wschat

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

func createDummyCertFile(t *testing.T) func() {
	certDir := "certs"
	certPath := filepath.Join(certDir, "minivault.ca.pem")
	if _, err := os.Stat(certPath); err == nil {
		return func() {}
	}
	if err := os.MkdirAll(certDir, 0755); err != nil {
		t.Fatalf("failed to create cert dir: %v", err)
	}
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Dummy CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create dummy cert: %v", err)
	}
	certPem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
	if err := os.WriteFile(certPath, certPem, 0644); err != nil {
		t.Fatalf("failed to write dummy cert: %v", err)
	}
	return func() {
		_ = os.Remove(certPath)
		_ = os.Remove(certDir)
	}
}

func TestLoadConfig(t *testing.T) {
	cleanup := createDummyCertFile(t)
	defer cleanup()

	// 1. Test embedded config fallback
	conf, err := LoadConfig("")
	if err != nil {
		t.Fatalf("failed to load embedded config: %v", err)
	}
	t.Logf("ClientTLS: Type=%q Vault=%+v", conf.ClientTLS.Type, conf.ClientTLS.Vault)
	if conf.Name == "" {
		t.Errorf("expected non-empty name from embedded config, got %s", conf.Name)
	}
	if len(conf.Channels) == 0 {
		t.Errorf("expected channels in embedded config, got none")
	}

	// 2. Test non-existent file
	_, err = LoadConfig("non_existent_file.toml")
	if err == nil {
		t.Errorf("expected error for non-existent config file, got nil")
	}
}

func TestMessageStore(t *testing.T) {
	channels := []string{"all", "dev"}
	store := NewMessageStore("alice", channels, 5)

	if store.GetClientName() != "alice" {
		t.Errorf("expected client name alice, got %s", store.GetClientName())
	}
	if store.GetActiveChannel() != "all" {
		t.Errorf("expected active channel all, got %s", store.GetActiveChannel())
	}
	if len(store.GetChannels()) != 2 {
		t.Errorf("expected 2 channels, got %d", len(store.GetChannels()))
	}

	// Add messages
	for i := 1; i <= 7; i++ {
		store.AddMessage(ChatMessage{
			ID:      fmt.Sprintf("msg-%d", i),
			Sender:  "bob",
			Channel: "all",
			Content: fmt.Sprintf("hello %d", i),
		})
	}

	msgs := store.GetMessages("all")
	if len(msgs) != 5 {
		t.Errorf("expected pruned 5 messages, got %d", len(msgs))
	}
	if msgs[0].Content != "hello 3" {
		t.Errorf("expected first message to be 'hello 3', got %s", msgs[0].Content)
	}
	if msgs[len(msgs)-1].Content != "hello 7" {
		t.Errorf("expected last message to be 'hello 7', got %s", msgs[len(msgs)-1].Content)
	}

	// Test IsSelf marking and RefID
	store.AddMessage(ChatMessage{
		ID:      "self-1",
		RefID:   "parent-msg-99",
		Sender:  "alice",
		Channel: "all",
		Content: "my message",
	})
	msgs = store.GetMessages("all")
	lastMsg := msgs[len(msgs)-1]
	if !lastMsg.IsSelf {
		t.Errorf("expected message from alice to have IsSelf=true")
	}
	if lastMsg.RefID != "parent-msg-99" {
		t.Errorf("expected RefID 'parent-msg-99', got %q", lastMsg.RefID)
	}

	// Test channel switching and adding
	store.AddChannel("general")
	store.SetActiveChannel("general")
	if store.GetActiveChannel() != "general" {
		t.Errorf("expected active channel general, got %s", store.GetActiveChannel())
	}

	// Test status
	store.SetStatus("Connected")
	if store.GetStatus() != "Connected" {
		t.Errorf("expected status Connected, got %s", store.GetStatus())
	}

	stats := store.GetStats()
	if stats.ActiveChannel != "general" || stats.Status != "Connected" {
		t.Errorf("unexpected stats: %+v", stats)
	}

	// Concurrent access test
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ch := fmt.Sprintf("chan-%d", id%3)
			store.AddMessage(ChatMessage{
				Sender:  "user",
				Channel: ch,
				Content: "concurrent test",
			})
			_ = store.GetMessages(ch)
			_ = store.GetChannels()
			_ = store.GetStats()
		}(i)
	}
	wg.Wait()
}

func TestHTTPServerAndHandlers(t *testing.T) {
	conf := &WSChatConfig{
		Name:     "testuser",
		WSBroker: "https://localhost:8765",
		Channels: []string{"all", "intern"},
		WebTLS: loader.Config{
			Type: "DEV",
		},
	}

	store := NewMessageStore(conf.Name, conf.Channels, 100)
	store.AddMessage(ChatMessage{
		Sender:    "system",
		Channel:   "all",
		Content:   "Welcome to #all",
		Timestamp: time.Now(),
	})

	server, err := NewServer(conf, store, nil, nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		_ = server.Close()
	}()

	if server.Port() <= 0 {
		t.Errorf("expected non-zero port, got %d", server.Port())
	}
	if !strings.HasPrefix(server.URL(), "https://localhost:") {
		t.Errorf("expected https url, got %s", server.URL())
	}

	// HTTP client that trusts dev self-signed cert
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
		Timeout: 5 * time.Second,
	}

	baseURL := server.URL()

	// 1. GET / (Index page)
	resp, err := client.Get(baseURL + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / returned %d, want 200", resp.StatusCode)
	}
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "WSChat") {
		t.Errorf("GET / response missing WSChat title")
	}
	if !strings.Contains(bodyStr, "testuser") {
		t.Errorf("GET / response missing client name testuser")
	}

	// 2. GET /chat/messages
	resp, err = client.Get(baseURL + "/chat/messages?channel=all")
	if err != nil {
		t.Fatalf("GET /chat/messages failed: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /chat/messages returned %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "Welcome to #all") {
		t.Errorf("GET /chat/messages missing seeded message")
	}

	// 3. POST /chat/send
	formData := url.Values{
		"channel": {"all"},
		"message": {"Hello from test!"},
	}
	resp, err = client.PostForm(baseURL+"/chat/send", formData)
	if err != nil {
		t.Fatalf("POST /chat/send failed: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /chat/send returned %d, want 200", resp.StatusCode)
	}

	// 4. POST /chat/channel (Switch channel)
	switchData := url.Values{
		"channel": {"intern"},
	}
	resp, err = client.PostForm(baseURL+"/chat/channel", switchData)
	if err != nil {
		t.Fatalf("POST /chat/channel failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /chat/channel returned %d, want 200", resp.StatusCode)
	}
	if store.GetActiveChannel() != "intern" {
		t.Errorf("expected active channel 'intern', got %s", store.GetActiveChannel())
	}

	// 5. GET /chat/channels
	resp, err = client.Get(baseURL + "/chat/channels")
	if err != nil {
		t.Fatalf("GET /chat/channels failed: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /chat/channels returned %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "intern") {
		t.Errorf("GET /chat/channels missing 'intern'")
	}

	// 6. GET /chat/status
	resp, err = client.Get(baseURL + "/chat/status")
	if err != nil {
		t.Fatalf("GET /chat/status failed: %v", err)
	}
	body, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET /chat/status returned %d, want 200", resp.StatusCode)
	}
	if !strings.Contains(string(body), "Disconnected") {
		t.Errorf("GET /chat/status expected Disconnected initial status")
	}

	// 7. POST /chat/join
	joinData := url.Values{
		"channel": {"random"},
	}
	resp, err = client.PostForm(baseURL+"/chat/join", joinData)
	if err != nil {
		t.Fatalf("POST /chat/join failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("POST /chat/join returned %d, want 200", resp.StatusCode)
	}
	if store.GetActiveChannel() != "random" {
		t.Errorf("expected active channel 'random', got %s", store.GetActiveChannel())
	}
}

func TestServerGracefulStop(t *testing.T) {
	conf := &WSChatConfig{
		Name:     "testuser",
		WSBroker: "https://localhost:8765",
		Channels: []string{"all"},
		WebTLS: loader.Config{
			Type: "DEV",
		},
	}

	store := NewMessageStore(conf.Name, conf.Channels, 100)
	server, err := NewServer(conf, store, nil, nil)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := server.GracefulStop(ctx); err != nil {
		t.Errorf("GracefulStop returned error: %v", err)
	}
}
