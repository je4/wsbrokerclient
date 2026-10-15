// Package event defines the event envelope data structures, event types,
// and serialization helpers used by wsbroker clients and servers.
package event

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"

	"emperror.dev/errors"
)

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
)

// DataInterface provides a contract for typed data objects that can supply their event type and string representation.
type DataInterface interface {
	String() string
	Type() EventType
}

// Event represents a standardized message envelope transmitted across WebSockets and REST APIs.
type Event struct {
	Type   EventType      `json:"type"`
	Source string         `json:"source"`
	Target string         `json:"target"`
	Token  string         `json:"token,omitempty"`
	Data   jsontext.Value `json:"data"`
}

// String returns a human-readable summary of the Event.
func (e *Event) String() string {
	if e == nil {
		return "<nil event>"
	}
	return fmt.Sprintf("%s [%s -> %s]", e.Type, e.Source, e.Target)
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
func (e *Event) GetData() (interface{}, error) {
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
		Type:   TypePing,
		Source: source,
		Data:   raw,
	}, nil
}

// NewPongEvent creates a pong heartbeat response Event.
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
	return &Event{
		Type:   TypePong,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
}

// NewNTPQueryEvent creates an Event of type TypeNTPQuery with raw NTP packet bytes.
func NewNTPQueryEvent(source string, data []byte) (*Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP query data")
	}
	return &Event{
		Type:   TypeNTPQuery,
		Source: source,
		Data:   raw,
	}, nil
}

// NewNTPResponseEvent creates an Event of type TypeNTPResponse with raw NTP response bytes.
func NewNTPResponseEvent(source, target string, data []byte) (*Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP response data")
	}
	return &Event{
		Type:   TypeNTPResponse,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
}

// NewNTPErrorEvent creates an Event of type TypeNTPError with an error message string.
func NewNTPErrorEvent(source, target, errMsg string) (*Event, error) {
	raw, err := json.Marshal(errMsg)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP error message: %s", errMsg)
	}
	return &Event{
		Type:   TypeNTPError,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
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
