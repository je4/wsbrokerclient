// Package event defines the event envelope data structures, event types,
// and serialization helpers used by wsbroker clients and servers.
package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"time"

	"emperror.dev/errors"
)

// GenerateEventID generates a cryptographically secure, random 16-byte (32-char hex) unique event identifier.
func GenerateEventID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// Fallback to nanosecond timestamp if crypto/rand fails
		return fmt.Sprintf("evt-%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Event represents a standardized message envelope transmitted across WebSockets and REST APIs.
type Event struct {
	ID      string         `json:"id,omitempty"`
	RefID   string         `json:"ref_id,omitempty"`
	Type    EventType      `json:"type"`
	Source  string         `json:"source"`
	Target  string         `json:"target"`
	Channel string         `json:"channel,omitempty"`
	Token   string         `json:"token,omitempty"`
	Data    jsontext.Value `json:"data"`
}

// String returns a human-readable summary of the Event.
func (e *Event) String() string {
	if e == nil {
		return "<nil event>"
	}
	targetStr := e.Target
	if e.Channel != "" {
		targetStr = fmt.Sprintf("%s (channel: %s)", e.Target, e.Channel)
	}
	base := fmt.Sprintf("%s [%s -> %s]", e.Type, e.Source, targetStr)

	if e.ID != "" && e.RefID != "" {
		return fmt.Sprintf("%s (id: %s, ref: %s)", base, e.ID, e.RefID)
	}
	if e.ID != "" {
		return fmt.Sprintf("%s (%s)", base, e.ID)
	}
	if e.RefID != "" {
		return fmt.Sprintf("%s (ref: %s)", base, e.RefID)
	}
	return base
}

// GetID returns the event identifier.
func (e *Event) GetID() string {
	if e == nil {
		return ""
	}
	return e.ID
}

// SetID sets the event identifier.
func (e *Event) SetID(id string) {
	if e != nil {
		e.ID = id
	}
}

// GetRefID returns the reference event identifier.
func (e *Event) GetRefID() string {
	if e == nil {
		return ""
	}
	return e.RefID
}

// SetRefID sets the reference event identifier.
func (e *Event) SetRefID(id string) {
	if e != nil {
		e.RefID = id
	}
}

// GetChannel returns the delivery channel name, if dispatched over a channel.
func (e *Event) GetChannel() string {
	if e == nil {
		return ""
	}
	return e.Channel
}

// SetChannel sets the delivery channel name.
func (e *Event) SetChannel(channel string) {
	if e != nil {
		e.Channel = channel
	}
}

// GetType returns the event type.
func (e *Event) GetType() EventType {
	if e == nil {
		return ""
	}
	return e.Type
}

// GetSource returns the sender identifier.
func (e *Event) GetSource() string {
	if e == nil {
		return ""
	}
	return e.Source
}

// GetTarget returns the recipient (channel name or client identifier).
func (e *Event) GetTarget() string {
	if e == nil {
		return ""
	}
	return e.Target
}

// GetToken returns the optional authentication/authorization token.
func (e *Event) GetToken() string {
	if e == nil {
		return ""
	}
	return e.Token
}

// GetData unmarshals the JSON payload into either a primitive string, raw bytes (for NTP events), or generic JSON structure.
func (e *Event) GetData() (any, error) {
	if e == nil || len(e.Data) == 0 {
		return nil, nil
	}
	switch e.Type {
	case TypeNTPQuery, TypeNTPResponse, TypeNTPError:
		var raw []byte
		if err := json.Unmarshal(e.Data, &raw); err == nil {
			return raw, nil
		}
	case TypeFile:
		var fm FileMessage
		if err := json.Unmarshal(e.Data, &fm); err == nil {
			return &fm, nil
		}
	case TypeChannelInfoResponse:
		var ci ChannelInfo
		if err := json.Unmarshal(e.Data, &ci); err == nil {
			return &ci, nil
		}
	}
	var msg string
	if err := json.Unmarshal(e.Data, &msg); err == nil {
		return msg, nil
	}
	var raw any
	if err := json.Unmarshal(e.Data, &raw); err != nil {
		return nil, errors.Wrapf(err, "cannot unmarshal event data: %v", string(e.Data))
	}
	return raw, nil
}

// UnmarshalData unmarshals the Event's JSON payload into the target value pointer.
func (e *Event) UnmarshalData(v any) error {
	if e == nil || len(e.Data) == 0 {
		return errors.New("empty event data")
	}
	if err := json.Unmarshal(e.Data, v); err != nil {
		return errors.Wrapf(err, "failed to unmarshal event data into %T", v)
	}
	return nil
}

// GetStringData attempts to extract the payload as a string.
// If the payload is a raw string or JSON-encoded string, it returns that string.
func (e *Event) GetStringData() (string, error) {
	if e == nil || len(e.Data) == 0 {
		return "", nil
	}
	var str string
	if err := json.Unmarshal(e.Data, &str); err == nil {
		return str, nil
	}
	return string(e.Data), nil
}

// CreateReply creates a response Event correlating to the original event e.
// The created event sets:
// - ID: a newly generated unique identifier
// - RefID: the ID of event e
// - Target: e.Source (or e.Channel if e.Source is empty)
// - Channel: e.Channel (if present)
// - Token: e.Token (if present)
// - Type: replyType
// - Data: JSON-marshaled payload of data
func (e *Event) CreateReply(replyType EventType, data any) (*Event, error) {
	if e == nil {
		return nil, errors.New("cannot create reply from nil event")
	}
	raw, err := marshalPayload(data)
	if err != nil {
		return nil, err
	}
	target := e.Source
	if target == "" {
		target = e.Channel
	}
	if target == "" {
		target = e.Target
	}
	reply := &Event{
		ID:      GenerateEventID(),
		RefID:   e.ID,
		Type:    replyType,
		Target:  target,
		Channel: e.Channel,
		Token:   e.Token,
		Data:    raw,
	}
	return reply, nil
}

func marshalPayload(data any) (jsontext.Value, error) {
	if data == nil {
		return nil, nil
	}
	switch v := data.(type) {
	case jsontext.Value:
		return v, nil
	case []byte:
		return jsontext.Value(v), nil
	case string:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal string data: %v", v)
		}
		return b, nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
		}
		return b, nil
	}
}
