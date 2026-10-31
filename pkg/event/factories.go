package event

import (
	"encoding/json/v2"

	"emperror.dev/errors"
)

// ToTyped converts a RawEvent into a strongly-typed Event[T] by unmarshaling its JSON payload into T.
// If expectedTypes are specified, it verifies that raw.Type matches one of them before unmarshaling.
// If T implements DataInterface, it verifies that raw.Type matches the DataInterface's Type().
func ToTyped[T any](raw *RawEvent, expectedTypes ...EventType) (*Event[T], error) {
	if raw == nil {
		return nil, errors.New("cannot convert nil raw event to typed event")
	}

	// 1. Check against explicitly expected EventTypes if provided
	if len(expectedTypes) > 0 {
		matched := false
		for _, et := range expectedTypes {
			if raw.Type == et {
				matched = true
				break
			}
		}
		if !matched {
			return nil, errors.Errorf("event type mismatch: expected %v, but got %q", expectedTypes, raw.Type)
		}
	}

	// 2. Check DataInterface compatibility (e.g. FileMessage, etc.)
	var zero T
	if di, ok := any(zero).(DataInterface); ok {
		if expectedType := di.Type(); expectedType != "" && expectedType != raw.Type {
			return nil, errors.Errorf("event type mismatch: payload %T expects type %q, got %q", zero, expectedType, raw.Type)
		}
	} else if di, ok := any(&zero).(DataInterface); ok {
		if expectedType := di.Type(); expectedType != "" && expectedType != raw.Type {
			return nil, errors.Errorf("event type mismatch: payload %T expects type %q, got %q", zero, expectedType, raw.Type)
		}
	}

	var data T
	if len(raw.Data) > 0 {
		if err := json.Unmarshal(raw.Data, &data); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal raw event payload into %T", data)
		}
	}
	return &Event[T]{
		BaseEvent: raw.BaseEvent,
		Data:      data,
	}, nil
}

// ToRaw converts a typed Event[T] into a RawEvent by JSON-marshaling its payload into jsontext.Value.
func ToRaw[T any](evt *Event[T]) (*RawEvent, error) {
	if evt == nil {
		return nil, errors.New("cannot convert nil typed event to raw event")
	}
	rawBytes, err := marshalPayload(evt.Data)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal typed event payload of type %T", evt.Data)
	}
	return &RawEvent{
		BaseEvent: evt.BaseEvent,
		Data:      rawBytes,
	}, nil
}

// NewEventTyped constructs a strongly-typed Event[T] with generated ID and metadata.
func NewEventTyped[T any](eventType EventType, source, target string, data T) (*Event[T], error) {
	return &Event[T]{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   eventType,
			Source: source,
			Target: target,
		},
		Data: data,
	}, nil
}

// NewRawEvent constructs a new RawEvent with JSON-marshaled data payload.
func NewRawEvent(eventType EventType, source, target string, data any) (*RawEvent, error) {
	raw, err := marshalPayload(data)
	if err != nil {
		return nil, err
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   eventType,
			Source: source,
			Target: target,
		},
		Data: raw,
	}, nil
}

// NewEvent constructs a new RawEvent instance by JSON-serializing the given DataInterface.
func NewEvent(data DataInterface, target string, session ...string) (*RawEvent, error) {
	if data == nil {
		return nil, errors.New("event data cannot be nil")
	}
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
	}
	var sess string
	if len(session) > 0 {
		sess = session[0]
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:      GenerateEventID(),
			Type:    data.Type(),
			Target:  target,
			Session: sess,
		},
		Data: jsonStr,
	}, nil
}

// NewMessageEvent creates a RawEvent of type TypeMessage carrying arbitrary data as JSON payload.
func NewMessageEvent(source, target string, data any) (*RawEvent, error) {
	return NewRawEvent(TypeMessage, source, target, data)
}

// NewReplyEvent creates a response RawEvent correlating to original with the given replyType and data.
// An optional source string can be provided to explicitly set the sender identity.
func NewReplyEvent(original *RawEvent, replyType EventType, data any, source ...string) (*RawEvent, error) {
	if original == nil {
		return nil, errors.New("original event cannot be nil")
	}
	return original.CreateReply(replyType, data, source...)
}
