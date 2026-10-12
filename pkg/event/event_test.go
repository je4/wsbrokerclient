package event_test

import (
	"encoding/json"
	"testing"

	"github.com/je4/wsbrokerclient/pkg/event"
)

type customPayload struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
}

func TestEventCreationAndSerialization(t *testing.T) {
	// 1. NewMessageEvent with struct
	payload := customPayload{ID: 42, Title: "Test"}
	evt, err := event.NewMessageEvent("client-1", "channel-alerts", payload)
	if err != nil {
		t.Fatalf("failed to create message event: %v", err)
	}

	if evt.GetType() != event.TypeMessage {
		t.Errorf("expected type %s, got %s", event.TypeMessage, evt.GetType())
	}
	if evt.GetSource() != "client-1" {
		t.Errorf("expected source client-1, got %s", evt.GetSource())
	}
	if evt.GetTarget() != "channel-alerts" {
		t.Errorf("expected target channel-alerts, got %s", evt.GetTarget())
	}

	var decoded customPayload
	if err := evt.UnmarshalData(&decoded); err != nil {
		t.Fatalf("failed to unmarshal struct data: %v", err)
	}
	if decoded.ID != 42 || decoded.Title != "Test" {
		t.Errorf("unexpected unmarshaled struct: %+v", decoded)
	}

	// 2. NewMessageEvent with string
	strEvt, err := event.NewMessageEvent("client-1", "channel-news", "breaking news")
	if err != nil {
		t.Fatalf("failed to create string message event: %v", err)
	}
	strData, err := strEvt.GetStringData()
	if err != nil {
		t.Fatalf("failed to get string data: %v", err)
	}
	if strData != "breaking news" {
		t.Errorf("expected 'breaking news', got '%s'", strData)
	}

	// 3. NewAttachEvent
	attachEvt, err := event.NewAttachEvent("client-1", "metrics")
	if err != nil {
		t.Fatalf("failed to create attach event: %v", err)
	}
	if attachEvt.GetType() != event.TypeAttach {
		t.Errorf("expected type %s, got %s", event.TypeAttach, attachEvt.GetType())
	}
	if attachEvt.GetTarget() != "metrics" {
		t.Errorf("expected target metrics, got %s", attachEvt.GetTarget())
	}

	// 4. NewDetachEvent
	detachEvt, err := event.NewDetachEvent("client-1", "metrics")
	if err != nil {
		t.Fatalf("failed to create detach event: %v", err)
	}
	if detachEvt.GetType() != event.TypeDetach {
		t.Errorf("expected type %s, got %s", event.TypeDetach, detachEvt.GetType())
	}

	// 5. NewPingEvent & NewPongEvent
	pingEvt, err := event.NewPingEvent("client-1")
	if err != nil {
		t.Fatalf("failed to create ping event: %v", err)
	}
	if pingEvt.GetType() != event.TypePing {
		t.Errorf("expected type %s, got %s", event.TypePing, pingEvt.GetType())
	}

	pongEvt, err := event.NewPongEvent("server", "client-1")
	if err != nil {
		t.Fatalf("failed to create pong event: %v", err)
	}
	if pongEvt.GetType() != event.TypePong {
		t.Errorf("expected type %s, got %s", event.TypePong, pongEvt.GetType())
	}

	// 6. GenericStringMessage
	gsm := event.NewGenericStringMessage(event.TypeMessage, "hello generic")
	evtFromData, err := event.NewEvent(gsm, "alerts", "token123")
	if err != nil {
		t.Fatalf("failed to create event from DataInterface: %v", err)
	}
	if evtFromData.GetToken() != "token123" {
		t.Errorf("expected token token123, got %s", evtFromData.GetToken())
	}

	// 7. JSON Marshal & Unmarshal of Event Envelope
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}
	var roundtrip event.Event
	if err := json.Unmarshal(b, &roundtrip); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}
	if roundtrip.Type != evt.Type || roundtrip.Source != evt.Source || roundtrip.Target != evt.Target {
		t.Errorf("roundtrip mismatch: %+v vs %+v", roundtrip, evt)
	}
}
