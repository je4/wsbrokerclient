package event

import (
	"time"
)

// WSMessage represents a logged or formatted WebSocket message event.
// It is used for telemetry logging, debugging consoles, and message history ringbuffers.
type WSMessage struct {
	Timestamp time.Time `json:"timestamp"`
	Direction string    `json:"direction"` // "in" (received) or "out" (sent)
	ID        string    `json:"id,omitempty"`
	RefID     string    `json:"refID,omitempty"`
	Type      string    `json:"type"` // "message", "file", "ping", "pong", "attach", etc.
	Source    string    `json:"source"`
	Target    string    `json:"target,omitempty"`
	Channel   string    `json:"channel,omitempty"`
	Session   string    `json:"session,omitempty"`
	Data      string    `json:"data,omitempty"`
}

// NewWSMessage converts a RawEvent into a WSMessage log entry.
func NewWSMessage(direction string, evt *RawEvent) WSMessage {
	if evt == nil {
		return WSMessage{Timestamp: time.Now().UTC(), Direction: direction}
	}
	dataStr, _ := evt.GetStringData()
	return WSMessage{
		Timestamp: time.Now().UTC(),
		Direction: direction,
		ID:        evt.ID,
		RefID:     evt.RefID,
		Type:      string(evt.Type),
		Source:    evt.Source,
		Target:    evt.Target,
		Channel:   evt.Channel,
		Session:   evt.Session,
		Data:      dataStr,
	}
}
