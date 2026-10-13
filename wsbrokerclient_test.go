package wsbrokerclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/wsbrokerclient"
)

func TestTopLevelClientIntegration(t *testing.T) {
	var mu sync.Mutex
	conns := make(map[string]*websocket.Conn)
	groups := make(map[string][]string)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			return
		}
		pathParts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		clientName := "anon"
		if len(pathParts) >= 2 {
			clientName = pathParts[1]
		}

		mu.Lock()
		conns[clientName] = conn
		for _, ch := range r.URL.Query()["channel"] {
			groups[ch] = append(groups[ch], clientName)
		}
		mu.Unlock()

		defer func() {
			mu.Lock()
			delete(conns, clientName)
			mu.Unlock()
			_ = conn.Close(websocket.StatusNormalClosure, "server close")
		}()

		for {
			var evt wsbrokerclient.Event
			if err := wsjson.Read(r.Context(), conn, &evt); err != nil {
				break
			}
			mu.Lock()
			if members, ok := groups[evt.Target]; ok {
				for _, m := range members {
					if c, exists := conns[m]; exists {
						_ = wsjson.Write(r.Context(), c, &evt)
					}
				}
			} else if targetConn, ok := conns[evt.Target]; ok {
				_ = wsjson.Write(r.Context(), targetConn, &evt)
			}
			mu.Unlock()
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithClientName("integration-client"),
		wsbrokerclient.WithInitialChannels("sensors"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	sub, err := cli.Subscribe(ctx, "sensors")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	payload := map[string]float64{"temperature": 23.4}
	if err := cli.Broadcast(ctx, "sensors", payload); err != nil {
		t.Fatalf("broadcast failed: %v", err)
	}

	select {
	case evt := <-sub.Events():
		var data map[string]float64
		if err := evt.UnmarshalData(&data); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if data["temperature"] != 23.4 {
			t.Errorf("expected 23.4, got %v", data["temperature"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for sensors event")
	}
}

func TestTopLevelModeConfiguration(t *testing.T) {
	m, err := wsbrokerclient.ParseConnectionMode("readonly")
	if err != nil {
		t.Fatalf("failed to parse mode: %v", err)
	}
	if m != wsbrokerclient.ModeRO {
		t.Errorf("expected ModeRO, got %v", m)
	}

	cli, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint("ws://localhost:8080"),
		wsbrokerclient.WithMode(wsbrokerclient.ModeWO),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	if cli.Mode() != wsbrokerclient.ModeWO {
		t.Errorf("expected ModeWO, got %v", cli.Mode())
	}
}
