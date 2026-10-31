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

// BaseEvent encapsulates all transport, routing, and correlation metadata.
type BaseEvent struct {
	ID        string    `json:"id,omitempty"`
	RefID     string    `json:"refId,omitempty"`
	Type      EventType `json:"type"`
	Source    string    `json:"source"`
	Target    string    `json:"target,omitempty"`
	Channel   string    `json:"channel,omitempty"`
	Session   string    `json:"session,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// Event couples the transport metadata with a strongly-typed payload.
type Event[T any] struct {
	BaseEvent
	Data T `json:"data"`
}

// RawEvent represents an event envelope with unparsed JSON payload.
type RawEvent = Event[jsontext.Value]

// String returns a human-readable summary of the BaseEvent.
func (e *BaseEvent) String() string {
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
func (e *BaseEvent) GetID() string {
	if e == nil {
		return ""
	}
	return e.ID
}

// SetID sets the event identifier.
func (e *BaseEvent) SetID(id string) {
	if e != nil {
		e.ID = id
	}
}

// GetRefID returns the reference event identifier.
func (e *BaseEvent) GetRefID() string {
	if e == nil {
		return ""
	}
	return e.RefID
}

// SetRefID sets the reference event identifier.
func (e *BaseEvent) SetRefID(id string) {
	if e != nil {
		e.RefID = id
	}
}

// GetChannel returns the delivery channel name, if dispatched over a channel.
func (e *BaseEvent) GetChannel() string {
	if e == nil {
		return ""
	}
	return e.Channel
}

// SetChannel sets the delivery channel name.
func (e *BaseEvent) SetChannel(channel string) {
	if e != nil {
		e.Channel = channel
	}
}

// GetType returns the event type.
func (e *BaseEvent) GetType() EventType {
	if e == nil {
		return ""
	}
	return e.Type
}

// GetSource returns the sender identifier.
func (e *BaseEvent) GetSource() string {
	if e == nil {
		return ""
	}
	return e.Source
}

// GetTarget returns the recipient (channel name or client identifier).
func (e *BaseEvent) GetTarget() string {
	if e == nil {
		return ""
	}
	if e.Target != "" {
		return e.Target
	}
	return e.Channel
}

// GetSession returns the optional session/correlation identifier.
func (e *BaseEvent) GetSession() string {
	if e == nil {
		return ""
	}
	return e.Session
}

// SetSession sets the optional session/correlation identifier.
func (e *BaseEvent) SetSession(session string) {
	if e != nil {
		e.Session = session
	}
}

// GetTimestamp returns the event timestamp.
func (e *BaseEvent) GetTimestamp() time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.Timestamp
}

// SetTimestamp sets the event timestamp.
func (e *BaseEvent) SetTimestamp(t time.Time) {
	if e != nil {
		e.Timestamp = t
	}
}

// String returns a human-readable summary of the Event.
func (e *Event[T]) String() string {
	if e == nil {
		return "<nil event>"
	}
	return e.BaseEvent.String()
}

// GetID returns the event identifier.
func (e *Event[T]) GetID() string {
	if e == nil {
		return ""
	}
	return e.BaseEvent.GetID()
}

// SetID sets the event identifier.
func (e *Event[T]) SetID(id string) {
	if e != nil {
		e.BaseEvent.SetID(id)
	}
}

// GetRefID returns the reference event identifier.
func (e *Event[T]) GetRefID() string {
	if e == nil {
		return ""
	}
	return e.BaseEvent.GetRefID()
}

// SetRefID sets the reference event identifier.
func (e *Event[T]) SetRefID(id string) {
	if e != nil {
		e.BaseEvent.SetRefID(id)
	}
}

// GetChannel returns the delivery channel name, if dispatched over a channel.
func (e *Event[T]) GetChannel() string {
	if e == nil {
		return ""
	}
	return e.BaseEvent.GetChannel()
}

// SetChannel sets the delivery channel name.
func (e *Event[T]) SetChannel(channel string) {
	if e != nil {
		e.BaseEvent.SetChannel(channel)
	}
}

// GetType returns the event type.
func (e *Event[T]) GetType() EventType {
	if e == nil {
		return ""
	}
	return e.BaseEvent.GetType()
}

// GetSource returns the sender identifier.
func (e *Event[T]) GetSource() string {
	if e == nil {
		return ""
	}
	return e.BaseEvent.GetSource()
}

// GetTarget returns the recipient (channel name or client identifier).
func (e *Event[T]) GetTarget() string {
	if e == nil {
		return ""
	}
	return e.BaseEvent.GetTarget()
}

// GetSession returns the optional session/correlation identifier.
func (e *Event[T]) GetSession() string {
	if e == nil {
		return ""
	}
	return e.BaseEvent.GetSession()
}

// SetSession sets the optional session/correlation identifier.
func (e *Event[T]) SetSession(session string) {
	if e != nil {
		e.BaseEvent.SetSession(session)
	}
}

// GetTimestamp returns the event timestamp.
func (e *Event[T]) GetTimestamp() time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.BaseEvent.GetTimestamp()
}

// SetTimestamp sets the event timestamp.
func (e *Event[T]) SetTimestamp(t time.Time) {
	if e != nil {
		e.BaseEvent.SetTimestamp(t)
	}
}

// CreateReply creates a response RawEvent correlating to event e.
func (e *Event[T]) CreateReply(replyType EventType, data any) (*RawEvent, error) {
	if e == nil {
		return nil, errors.New("cannot create reply from nil event")
	}
	return e.BaseEvent.CreateReply(replyType, data)
}

// GetData unmarshals the JSON payload into either a registered concrete type,
// a primitive string, or a generic JSON map/slice structure.
func (e *Event[T]) GetData() (any, error) {
	if e == nil {
		return nil, nil
	}
	switch d := any(e.Data).(type) {
	case jsontext.Value:
		if len(d) == 0 {
			return nil, nil
		}
		if unmarshaler, ok := GetPayloadUnmarshaler(e.Type); ok {
			return unmarshaler(d)
		}
		var msg string
		if err := json.Unmarshal(d, &msg); err == nil {
			return msg, nil
		}
		var raw any
		if err := json.Unmarshal(d, &raw); err != nil {
			return nil, errors.Wrapf(err, "cannot unmarshal event data: %v", string(d))
		}
		return raw, nil
	default:
		return e.Data, nil
	}
}

// UnmarshalData unmarshals the Event's JSON payload into the target value pointer.
func (e *Event[T]) UnmarshalData(v any) error {
	if e == nil {
		return errors.New("empty event data")
	}
	switch d := any(e.Data).(type) {
	case jsontext.Value:
		if len(d) == 0 {
			return errors.New("empty event data")
		}
		if err := json.Unmarshal(d, v); err != nil {
			return errors.Wrapf(err, "failed to unmarshal event data into %T", v)
		}
		return nil
	case []byte:
		if len(d) == 0 {
			return errors.New("empty event data")
		}
		if err := json.Unmarshal(d, v); err != nil {
			return errors.Wrapf(err, "failed to unmarshal event data into %T", v)
		}
		return nil
	case nil:
		return errors.New("empty event data")
	default:
		b, err := json.Marshal(e.Data)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal event data of type %T", e.Data)
		}
		if err := json.Unmarshal(b, v); err != nil {
			return errors.Wrapf(err, "failed to unmarshal event data into %T", v)
		}
		return nil
	}
}

// GetStringData attempts to extract the payload as a string.
// If the payload is a raw string or JSON-encoded string, it returns that string.
func (e *Event[T]) GetStringData() (string, error) {
	if e == nil {
		return "", nil
	}
	switch d := any(e.Data).(type) {
	case jsontext.Value:
		if len(d) == 0 {
			return "", nil
		}
		var str string
		if err := json.Unmarshal(d, &str); err == nil {
			return str, nil
		}
		return string(d), nil
	case []byte:
		if len(d) == 0 {
			return "", nil
		}
		var str string
		if err := json.Unmarshal(d, &str); err == nil {
			return str, nil
		}
		return string(d), nil
	case string:
		return d, nil
	case fmt.Stringer:
		return d.String(), nil
	case nil:
		return "", nil
	default:
		b, err := json.Marshal(e.Data)
		if err != nil {
			return "", errors.Wrapf(err, "cannot marshal event data: %v", e.Data)
		}
		var str string
		if err := json.Unmarshal(b, &str); err == nil {
			return str, nil
		}
		return string(b), nil
	}
}

// CreateReply creates a response RawEvent correlating to the original event e.
// The created event sets:
// - ID: a newly generated unique identifier
// - RefID: the ID of event e
// - Source: e.Target (or e.Channel if e.Target is empty)
// - Target: e.Source (or e.Channel if e.Source is empty)
// - Channel: e.Channel (if present)
// - Session: e.Session (if present)
// - Type: replyType
// - Data: JSON-marshaled payload of data
func (e *BaseEvent) CreateReply(replyType EventType, data any) (*RawEvent, error) {
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
	source := e.Target
	if source == "" {
		source = e.Channel
	}
	reply := &RawEvent{
		BaseEvent: BaseEvent{
			ID:      GenerateEventID(),
			RefID:   e.ID,
			Type:    replyType,
			Source:  source,
			Target:  target,
			Channel: e.Channel,
			Session: e.Session,
		},
		Data: raw,
	}
	return reply, nil
}

func marshalPayload(data any) (jsontext.Value, error) {
	if data == nil {
		return nil, nil
	}
	if raw, ok := data.(jsontext.Value); ok {
		return raw, nil
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
	}
	return jsontext.Value(b), nil
}
