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

// EventType represents the category of event sent across WebSocket channels.
type EventType string

const (
	// TypeAttach is sent by a client to subscribe to one or more channels.
	TypeAttach EventType = "attach"
	// TypeDetach is sent by a client to unsubscribe from one or more channels.
	TypeDetach EventType = "detach"
	// TypeMessage represents a standard message carrying a data payload.
	TypeMessage EventType = "message"
	// TypeStringMessage is an alias for TypeMessage for string payloads.
	TypeStringMessage EventType = "message"
	// TypeFile represents a binary file payload with attached metadata.
	TypeFile EventType = "file"
	// TypePing represents a ping heartbeat or health-check event.
	TypePing EventType = "ping"
	// TypePong represents a pong heartbeat response event.
	TypePong EventType = "pong"
	// TypeNTPQuery is sent by a client to query NTP time via raw 48-byte UDP packet.
	TypeNTPQuery EventType = "ntp-query"
	// TypeNTPResponse is sent by the server with the raw NTP response packet.
	TypeNTPResponse EventType = "ntp-response"
	// TypeNTPError is sent by the server if NTP querying fails.
	TypeNTPError EventType = "ntp-error"
	// TypeChannelInfoQuery is sent by a client to query metadata and subscriber members of a channel.
	TypeChannelInfoQuery EventType = "channel-info-query"
	// TypeChannelInfoResponse is sent by the server with the ChannelInfo data.
	TypeChannelInfoResponse EventType = "channel-info-response"
	// TypeChannelInfoError is sent by the server if channel-info querying fails or is unauthorized.
	TypeChannelInfoError EventType = "channel-info-error"
)

// DataInterface provides a contract for typed data objects that can supply their event type and string representation.
type DataInterface interface {
	String() string
	Type() EventType
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

// NewEvent constructs a new Event instance by JSON-serializing the given DataInterface.
func NewEvent(data DataInterface, target string, token string) (*Event, error) {
	if data == nil {
		return nil, errors.New("event data cannot be nil")
	}
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   data.Type(),
		Target: target,
		Token:  token,
		Data:   jsonStr,
	}, nil
}

// NewMessageEvent creates an Event of type TypeMessage carrying arbitrary data as JSON payload.
func NewMessageEvent(source, target string, data any) (*Event, error) {
	raw, err := marshalPayload(data)
	if err != nil {
		return nil, err
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeMessage,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
}

// NewFileEvent creates an Event of type TypeFile carrying a FileMessage payload.
func NewFileEvent(source, target string, file *FileMessage, token ...string) (*Event, error) {
	if file == nil {
		return nil, errors.New("file message cannot be nil")
	}
	var tok string
	if len(token) > 0 {
		tok = token[0]
	}
	raw, err := json.Marshal(file)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal file event data: %v", file.Filename)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeFile,
		Source: source,
		Target: target,
		Token:  tok,
		Data:   raw,
	}, nil
}

// NewAttachEvent creates an Event of type TypeAttach to subscribe to a channel.
func NewAttachEvent(source, channel string) (*Event, error) {
	raw, err := json.Marshal(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal attach channel: %s", channel)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeAttach,
		Source: source,
		Target: channel,
		Data:   raw,
	}, nil
}

// NewDetachEvent creates an Event of type TypeDetach to unsubscribe from a channel.
func NewDetachEvent(source, channel string) (*Event, error) {
	raw, err := json.Marshal(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal detach channel: %s", channel)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeDetach,
		Source: source,
		Target: channel,
		Data:   raw,
	}, nil
}

// NewPingEvent creates a ping heartbeat Event.
func NewPingEvent(source string, data ...any) (*Event, error) {
	var raw jsontext.Value
	if len(data) > 0 && data[0] != nil {
		var err error
		raw, err = marshalPayload(data[0])
		if err != nil {
			return nil, err
		}
	} else {
		raw = jsontext.Value(`"ping"`)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypePing,
		Source: source,
		Data:   raw,
	}, nil
}

// NewPongEvent creates a pong heartbeat response Event.
// If an optional RefID string is passed as a subsequent argument, it is assigned to RefID.
func NewPongEvent(source, target string, data ...any) (*Event, error) {
	var raw jsontext.Value
	if len(data) > 0 && data[0] != nil {
		var err error
		raw, err = marshalPayload(data[0])
		if err != nil {
			return nil, err
		}
	} else {
		raw = jsontext.Value(`"pong"`)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypePong,
		Source: source,
		Target: target,
		Data:   raw,
	}
	if len(data) > 1 {
		if refStr, ok := data[1].(string); ok {
			evt.RefID = refStr
		}
	}
	return evt, nil
}

// NewNTPQueryEvent creates an Event of type TypeNTPQuery with raw NTP packet bytes.
func NewNTPQueryEvent(source string, data []byte) (*Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP query data")
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeNTPQuery,
		Source: source,
		Data:   raw,
	}, nil
}

// NewNTPResponseEvent creates an Event of type TypeNTPResponse with raw NTP response bytes.
// An optional refID can be passed to correlate with the query event ID.
func NewNTPResponseEvent(source, target string, data []byte, refID ...string) (*Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP response data")
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeNTPResponse,
		Source: source,
		Target: target,
		Data:   raw,
	}
	if len(refID) > 0 {
		evt.RefID = refID[0]
	}
	return evt, nil
}

// NewNTPErrorEvent creates an Event of type TypeNTPError with an error message string.
// An optional refID can be passed to correlate with the query event ID.
func NewNTPErrorEvent(source, target, errMsg string, refID ...string) (*Event, error) {
	raw, err := json.Marshal(errMsg)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP error message: %s", errMsg)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeNTPError,
		Source: source,
		Target: target,
		Data:   raw,
	}
	if len(refID) > 0 {
		evt.RefID = refID[0]
	}
	return evt, nil
}

// NewChannelInfoQueryEvent creates an Event of type TypeChannelInfoQuery for querying channel metadata and members.
// The target channel name can be passed as target (and is also stored as string data payload).
func NewChannelInfoQueryEvent(source, target string) (*Event, error) {
	raw, err := json.Marshal(target)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info query target: %s", target)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeChannelInfoQuery,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
}

// NewChannelInfoResponseEvent creates an Event of type TypeChannelInfoResponse containing ChannelInfo.
// Optional source and refID strings can be provided.
func NewChannelInfoResponseEvent(target string, info ChannelInfo, source ...string) (*Event, error) {
	src := "wsbroker"
	if len(source) > 0 && source[0] != "" {
		src = source[0]
	}
	raw, err := json.Marshal(info)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info: %v", info)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeChannelInfoResponse,
		Source: src,
		Target: target,
		Data:   raw,
	}
	if len(source) > 1 {
		evt.RefID = source[1]
	}
	return evt, nil
}

// NewChannelInfoErrorEvent creates an Event of type TypeChannelInfoError with an error message string.
// Optional source and refID strings can be provided.
func NewChannelInfoErrorEvent(target, errMsg string, source ...string) (*Event, error) {
	src := "wsbroker"
	if len(source) > 0 && source[0] != "" {
		src = source[0]
	}
	raw, err := json.Marshal(errMsg)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info error message: %s", errMsg)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeChannelInfoError,
		Source: src,
		Target: target,
		Data:   raw,
	}
	if len(source) > 1 {
		evt.RefID = source[1]
	}
	return evt, nil
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

// NewReplyEvent creates a response Event correlating to original with the given replyType and data.
func NewReplyEvent(original *Event, replyType EventType, data any) (*Event, error) {
	if original == nil {
		return nil, errors.New("original event cannot be nil")
	}
	return original.CreateReply(replyType, data)
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

// GenericStringMessage represents a string payload attached to an event type.
type GenericStringMessage struct {
	Type_ EventType `json:"type"`
	Msg   string    `json:"msg"`
}

// NewGenericStringMessage wraps a simple string message into a GenericStringMessage implementing DataInterface.
func NewGenericStringMessage(t EventType, msg string) DataInterface {
	return &GenericStringMessage{
		Type_: t,
		Msg:   msg,
	}
}

// MarshalJSON encodes the inner string message to JSON.
func (m *GenericStringMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(m.Msg)
}

// UnmarshalJSON decodes JSON data into the inner string message.
func (m *GenericStringMessage) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &m.Msg)
}

// String returns the inner string message.
func (m *GenericStringMessage) String() string {
	if m == nil {
		return ""
	}
	return m.Msg
}

// Type returns the EventType of the generic message.
func (m *GenericStringMessage) Type() EventType {
	if m == nil {
		return ""
	}
	return m.Type_
}

var _ DataInterface = (*GenericStringMessage)(nil)

// FileMessage represents a binary file payload with metadata.
type FileMessage struct {
	Filename string `json:"filename"`
	MimeType string `json:"mime_type"`
	Size     int64  `json:"size"`
	Data     []byte `json:"data"`
}

// NewFileMessage creates a new FileMessage with the provided metadata and binary content.
// If size <= 0, it is automatically derived from the length of the data slice.
func NewFileMessage(filename, mimeType string, data []byte, size ...int64) *FileMessage {
	dataSize := int64(len(data))
	if len(size) > 0 && size[0] > 0 {
		dataSize = size[0]
	}
	return &FileMessage{
		Filename: filename,
		MimeType: mimeType,
		Size:     dataSize,
		Data:     data,
	}
}

// String returns a human-readable representation of the file metadata.
func (f *FileMessage) String() string {
	if f == nil {
		return "<nil file>"
	}
	return fmt.Sprintf("File: %s (%s, %d bytes)", f.Filename, f.MimeType, f.Size)
}

// Type returns the EventType for file messages.
func (f *FileMessage) Type() EventType {
	return TypeFile
}

var _ DataInterface = (*FileMessage)(nil)

// ChannelInfo describes a channel, its total subscriber count, and its connected client members.
type ChannelInfo struct {
	Name        string   `json:"name"`
	Subscribers int      `json:"subscribers"`
	Clients     []string `json:"clients"`
}

// String returns a human-readable representation of ChannelInfo.
func (c *ChannelInfo) String() string {
	if c == nil {
		return "<nil channel info>"
	}
	return fmt.Sprintf("Channel: %s (%d subscribers)", c.Name, c.Subscribers)
}

// Type returns the EventType for ChannelInfo response messages.
func (c *ChannelInfo) Type() EventType {
	return TypeChannelInfoResponse
}

var _ DataInterface = (*ChannelInfo)(nil)

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
	Token     string    `json:"token,omitempty"`
	Data      string    `json:"data,omitempty"`
}

// NewWSMessage converts an Event into a WSMessage log entry.
func NewWSMessage(direction string, evt *Event) WSMessage {
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
		Token:     evt.Token,
		Data:      dataStr,
	}
}
