package event

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"sync"

	"emperror.dev/errors"
)

// PayloadUnmarshaler defines a factory function that decodes raw JSON payload into a concrete type.
type PayloadUnmarshaler func(data jsontext.Value) (any, error)

var payloadRegistry sync.Map

// RegisterPayloadFactory registers a custom unmarshaler function for the given EventType.
// It panics if eventType is empty, factory is nil, or if the eventType is already registered.
func RegisterPayloadFactory(eventType EventType, factory PayloadUnmarshaler) {
	if eventType == "" {
		panic("event: RegisterPayloadFactory called with empty eventType")
	}
	if factory == nil {
		panic("event: RegisterPayloadFactory called with nil factory")
	}
	if _, loaded := payloadRegistry.LoadOrStore(eventType, factory); loaded {
		panic(fmt.Sprintf("event: duplicate payload registration for event type %q", eventType))
	}
}

// RegisterPayload associates an EventType with a concrete payload type T.
// When an event with this EventType is decoded dynamically via GetData() or UnmarshalDynamic(),
// the JSON payload will be unmarshaled into an instance of T.
// It panics if eventType is empty or already registered.
func RegisterPayload[T any](eventType EventType) {
	RegisterPayloadFactory(eventType, func(data jsontext.Value) (any, error) {
		if len(data) == 0 {
			return nil, nil
		}
		var val T
		if err := json.Unmarshal(data, &val); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal payload into %T", val)
		}
		return val, nil
	})
}

// GetPayloadUnmarshaler returns the registered unmarshaler for the specified EventType, if present.
func GetPayloadUnmarshaler(eventType EventType) (PayloadUnmarshaler, bool) {
	val, ok := payloadRegistry.Load(eventType)
	if !ok {
		return nil, false
	}
	unmarshaler, ok := val.(PayloadUnmarshaler)
	return unmarshaler, ok
}

// UnmarshalDynamic unmarshals a RawEvent's data payload according to the type registry.
// If the event's type is registered, it returns the concrete type.
// If unregistered, it falls back to string or a generic JSON map/slice structure.
func UnmarshalDynamic(raw *RawEvent) (any, error) {
	if raw == nil {
		return nil, nil
	}
	return raw.GetData()
}

func init() {
	RegisterPayload[[]byte](TypeNTPQuery)
	RegisterPayload[[]byte](TypeNTPResponse)
	RegisterPayload[string](TypeNTPError)
	RegisterPayload[*FileMessage](TypeFile)
	RegisterPayload[*ChannelInfo](TypeChannelInfoResponse)
}
