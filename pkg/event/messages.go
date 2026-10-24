package event

import (
	"encoding/json/v2"
)

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
