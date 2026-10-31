package event_test

import (
	"encoding/json/jsontext"
	"testing"
	"time"

	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/stretchr/testify/require"
)

type TestCustomPayload struct {
	EPC       string    `json:"epc"`
	RSSI      float64   `json:"rssi"`
	ReadCount int       `json:"readCount"`
	Timestamp time.Time `json:"timestamp"`
}

func TestRegisterPayload_CustomStruct(t *testing.T) {
	const customType event.EventType = "rfid/tag_read"

	event.RegisterPayload[*TestCustomPayload](customType)

	unmarshaler, ok := event.GetPayloadUnmarshaler(customType)
	require.True(t, ok)
	require.NotNil(t, unmarshaler)

	now := time.Now().UTC().Truncate(time.Millisecond)
	payload := &TestCustomPayload{
		EPC:       "E2801160600002048E123456",
		RSSI:      -55.4,
		ReadCount: 42,
		Timestamp: now,
	}

	evt, err := event.NewEventTyped(customType, "rfid-reader", "target", payload)
	require.NoError(t, err)
	rawEvt, err := event.ToRaw(evt)
	require.NoError(t, err)

	// Test GetData()
	data, err := rawEvt.GetData()
	require.NoError(t, err)
	require.IsType(t, &TestCustomPayload{}, data)

	decoded := data.(*TestCustomPayload)
	require.Equal(t, payload.EPC, decoded.EPC)
	require.Equal(t, payload.RSSI, decoded.RSSI)
	require.Equal(t, payload.ReadCount, decoded.ReadCount)
	require.True(t, payload.Timestamp.Equal(decoded.Timestamp))

	// Test UnmarshalDynamic
	dynData, err := event.UnmarshalDynamic(rawEvt)
	require.NoError(t, err)
	require.Equal(t, decoded, dynData)
}

func TestRegisterPayload_ValueStruct(t *testing.T) {
	const customValueType event.EventType = "custom/value_payload"

	event.RegisterPayload[TestCustomPayload](customValueType)

	evt, err := event.NewEventTyped(customValueType, "sender", "target", TestCustomPayload{
		EPC: "12345",
	})
	require.NoError(t, err)
	rawEvt, err := event.ToRaw(evt)
	require.NoError(t, err)

	data, err := rawEvt.GetData()
	require.NoError(t, err)
	require.IsType(t, TestCustomPayload{}, data)
	require.Equal(t, "12345", data.(TestCustomPayload).EPC)
}

func TestRegisterPayload_Factory(t *testing.T) {
	const factoryType event.EventType = "custom/factory_type"

	event.RegisterPayloadFactory(factoryType, func(data jsontext.Value) (any, error) {
		return "transformed: " + string(data), nil
	})

	rawEvt := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			Type: factoryType,
		},
		Data: jsontext.Value(`{"hello":"world"}`),
	}

	data, err := rawEvt.GetData()
	require.NoError(t, err)
	require.Equal(t, `transformed: {"hello":"world"}`, data)
}

func TestRegisterPayload_FallbackUnregistered(t *testing.T) {
	const unregType event.EventType = "unregistered/type"

	// String fallback
	rawEvtStr := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			Type: unregType,
		},
		Data: jsontext.Value(`"a simple string message"`),
	}

	dataStr, err := rawEvtStr.GetData()
	require.NoError(t, err)
	require.Equal(t, "a simple string message", dataStr)

	// JSON map fallback
	rawEvtObj := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			Type: unregType,
		},
		Data: jsontext.Value(`{"key": "value", "count": 10}`),
	}

	dataObj, err := rawEvtObj.GetData()
	require.NoError(t, err)
	m, ok := dataObj.(map[string]any)
	require.True(t, ok)
	require.Equal(t, "value", m["key"])
	require.Equal(t, float64(10), m["count"])
}

func TestRegisterPayload_EmptyAndNil(t *testing.T) {
	var nilEvt *event.RawEvent
	data, err := nilEvt.GetData()
	require.NoError(t, err)
	require.Nil(t, data)

	dynData, err := event.UnmarshalDynamic(nilEvt)
	require.NoError(t, err)
	require.Nil(t, dynData)

	emptyDataEvt := &event.RawEvent{
		BaseEvent: event.BaseEvent{Type: "some/type"},
		Data:      nil,
	}
	data, err = emptyDataEvt.GetData()
	require.NoError(t, err)
	require.Nil(t, data)
}

func TestRegisterPayload_DuplicateAndValidationPanics(t *testing.T) {
	const dupType event.EventType = "custom/duplicate_test"

	// 1. Initial registration succeeds
	require.NotPanics(t, func() {
		event.RegisterPayload[TestCustomPayload](dupType)
	})

	// 2. Duplicate RegisterPayload panics
	require.Panics(t, func() {
		event.RegisterPayload[TestCustomPayload](dupType)
	})

	// 3. Duplicate RegisterPayloadFactory panics
	require.Panics(t, func() {
		event.RegisterPayloadFactory(dupType, func(data jsontext.Value) (any, error) {
			return nil, nil
		})
	})

	// 4. Empty event type panics
	require.Panics(t, func() {
		event.RegisterPayload[TestCustomPayload]("")
	})
	require.Panics(t, func() {
		event.RegisterPayloadFactory("", func(data jsontext.Value) (any, error) {
			return nil, nil
		})
	})

	// 5. Nil factory panics
	require.Panics(t, func() {
		event.RegisterPayloadFactory("custom/nil_factory", nil)
	})
}

func TestToTyped_TypeValidation(t *testing.T) {
	const customType event.EventType = "rfid/validation_test"
	event.RegisterPayload[*TestCustomPayload](customType)

	// 1. Valid matching conversion
	payload := &TestCustomPayload{EPC: "E2801160", RSSI: -44.0}
	typedEvt, err := event.NewEventTyped(customType, "source", "target", payload)
	require.NoError(t, err)
	rawEvt, err := event.ToRaw(typedEvt)
	require.NoError(t, err)

	decodedPtr, err := event.ToTyped[*TestCustomPayload](rawEvt)
	require.NoError(t, err)
	require.Equal(t, payload.EPC, decodedPtr.Data.EPC)

	decodedVal, err := event.ToTyped[TestCustomPayload](rawEvt)
	require.NoError(t, err)
	require.Equal(t, payload.EPC, decodedVal.Data.EPC)

	// 2. Explicit expectedTypes match vs mismatch
	decodedWithMatchingType, err := event.ToTyped[TestCustomPayload](rawEvt, customType)
	require.NoError(t, err)
	require.Equal(t, payload.EPC, decodedWithMatchingType.Data.EPC)

	_, err = event.ToTyped[TestCustomPayload](rawEvt, "different/event_type")
	require.Error(t, err)
	require.Contains(t, err.Error(), "event type mismatch: expected [different/event_type]")

	// 3. Type mismatch with DataInterface (FileMessage used for non-file event)
	_, err = event.ToTyped[event.FileMessage](rawEvt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "event type mismatch")

	_, err = event.ToTyped[*event.FileMessage](rawEvt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "event type mismatch")

	// 4. FileMessage used on valid FileEvent
	fileEvt, err := event.NewRawEvent(event.TypeFile, "source", "target", &event.FileMessage{Filename: "test.txt"})
	require.NoError(t, err)

	decodedFile, err := event.ToTyped[*event.FileMessage](fileEvt)
	require.NoError(t, err)
	require.Equal(t, "test.txt", decodedFile.Data.Filename)

	// 5. Any / jsontext.Value generic decoding is allowed
	anyEvt, err := event.ToTyped[any](rawEvt)
	require.NoError(t, err)
	require.NotNil(t, anyEvt)

	rawTextEvt, err := event.ToTyped[jsontext.Value](rawEvt)
	require.NoError(t, err)
	require.NotEmpty(t, rawTextEvt.Data)
}
