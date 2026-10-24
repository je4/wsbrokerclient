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

func TestTopLevelChannelInfoIntegration(t *testing.T) {
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
			for ch, members := range groups {
				var updated []string
				for _, m := range members {
					if m != clientName {
						updated = append(updated, m)
					}
				}
				groups[ch] = updated
			}
			mu.Unlock()
			_ = conn.Close(websocket.StatusNormalClosure, "server close")
		}()

		for {
			var evt wsbrokerclient.Event
			if err := wsjson.Read(r.Context(), conn, &evt); err != nil {
				break
			}
			mu.Lock()
			switch evt.Type {
			case wsbrokerclient.TypeChannelInfoQuery:
				var ch string
				_ = evt.UnmarshalData(&ch)
				if ch == "" {
					ch = evt.Target
				}
				members := groups[ch]
				if members == nil {
					members = []string{}
				}
				info := wsbrokerclient.ChannelInfo{
					Name:        ch,
					Subscribers: len(members),
					Clients:     members,
				}
				respEvt, _ := wsbrokerclient.NewChannelInfoResponseEvent(clientName, info, "wsbroker", evt.ID)
				_ = wsjson.Write(r.Context(), conn, respEvt)
			default:
				if members, ok := groups[evt.Target]; ok {
					for _, m := range members {
						if c, exists := conns[m]; exists {
							_ = wsjson.Write(r.Context(), c, &evt)
						}
					}
				} else if targetConn, ok := conns[evt.Target]; ok {
					_ = wsjson.Write(r.Context(), targetConn, &evt)
				}
			}
			mu.Unlock()
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cliA, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithClientName("clientA"),
		wsbrokerclient.WithInitialChannels("chat-room"),
	)
	if err != nil {
		t.Fatalf("failed to create clientA: %v", err)
	}
	defer cliA.Close()

	cliB, err := wsbrokerclient.New(wsbrokerclient.DefaultConfig(),
		wsbrokerclient.WithEndpoint(server.URL),
		wsbrokerclient.WithClientName("clientB"),
		wsbrokerclient.WithInitialChannels("chat-room"),
	)
	if err != nil {
		t.Fatalf("failed to create clientB: %v", err)
	}
	defer cliB.Close()

	if err := cliA.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect clientA: %v", err)
	}
	if err := cliB.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect clientB: %v", err)
	}

	// Test QueryChannelInfo
	info, err := cliA.QueryChannelInfo(ctx, "chat-room")
	if err != nil {
		t.Fatalf("QueryChannelInfo failed: %v", err)
	}
	if info.Name != "chat-room" {
		t.Errorf("expected channel name 'chat-room', got '%s'", info.Name)
	}
	if info.Subscribers != 2 {
		t.Errorf("expected 2 subscribers, got %d", info.Subscribers)
	}
	if len(info.Clients) != 2 {
		t.Errorf("expected 2 clients, got %v", info.Clients)
	}

	// Test ChannelInfo alias
	infoAlias, err := cliB.ChannelInfo(ctx, "chat-room")
	if err != nil {
		t.Fatalf("ChannelInfo alias failed: %v", err)
	}
	if infoAlias.Subscribers != 2 {
		t.Errorf("expected 2 subscribers, got %d", infoAlias.Subscribers)
	}

	// Test top-level constructor helpers
	queryEvt, err := wsbrokerclient.NewChannelInfoQueryEvent("clientA", "chat-room")
	if err != nil || queryEvt.Type != wsbrokerclient.TypeChannelInfoQuery {
		t.Fatalf("NewChannelInfoQueryEvent failed: %v", err)
	}

	respEvt, err := wsbrokerclient.NewChannelInfoResponseEvent("clientA", *info)
	if err != nil || respEvt.Type != wsbrokerclient.TypeChannelInfoResponse {
		t.Fatalf("NewChannelInfoResponseEvent failed: %v", err)
	}

	errEvt, err := wsbrokerclient.NewChannelInfoErrorEvent("clientA", "failed")
	if err != nil || errEvt.Type != wsbrokerclient.TypeChannelInfoError {
		t.Fatalf("NewChannelInfoErrorEvent failed: %v", err)
	}
}

func TestTopLevelReplyAndRefID(t *testing.T) {
	orig, err := wsbrokerclient.NewMessageEvent("sender-client", "receiver-client", "original payload")
	if err != nil {
		t.Fatalf("failed to create original message: %v", err)
	}
	orig.ID = "original-id-123"

	reply, err := wsbrokerclient.NewReplyEvent(orig, wsbrokerclient.TypeMessage, "reply payload")
	if err != nil {
		t.Fatalf("NewReplyEvent failed: %v", err)
	}
	if reply.RefID != "original-id-123" {
		t.Errorf("expected RefID 'original-id-123', got %q", reply.RefID)
	}
	if reply.Target != "sender-client" {
		t.Errorf("expected Target 'sender-client', got %q", reply.Target)
	}
	if reply.ID == "" || reply.ID == orig.ID {
		t.Errorf("expected unique new event ID, got %q", reply.ID)
	}
}
