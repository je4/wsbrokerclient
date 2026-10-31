package client_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/je4/wsbrokerclient/pkg/client"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/stretchr/testify/require"
)

type TestRFIDData struct {
	EPC  string  `json:"epc"`
	RSSI float64 `json:"rssi"`
}

func TestTypedListeners_OnTyped(t *testing.T) {
	server := newMockBrokerServer("")
	ts := httptest.NewServer(server)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(wsURL),
		client.WithClientName("sender"),
	)
	require.NoError(t, err)
	defer cli1.Close()
	require.NoError(t, cli1.ConnectSync(ctx))

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(wsURL),
		client.WithClientName("receiver"),
	)
	require.NoError(t, err)
	defer cli2.Close()
	require.NoError(t, cli2.ConnectSync(ctx))

	const eventType event.EventType = "rfid/tag_read"

	receivedCh := make(chan *event.Event[TestRFIDData], 1)
	errCh := make(chan error, 1)

	unsub := client.OnTyped[TestRFIDData](cli2, eventType, func(evt *event.Event[TestRFIDData]) {
		receivedCh <- evt
	}, func(raw *event.RawEvent, err error) {
		errCh <- err
	})
	defer unsub()

	// 1. Send valid typed event
	rfidPayload := TestRFIDData{
		EPC:  "300833B2DDD9014000000000",
		RSSI: -62.5,
	}
	typedEvt, err := event.NewEventTyped(eventType, "sender", "receiver", rfidPayload)
	require.NoError(t, err)
	rawEvt, err := event.ToRaw(typedEvt)
	require.NoError(t, err)

	require.NoError(t, cli1.Send(ctx, rawEvt))

	select {
	case evt := <-receivedCh:
		require.Equal(t, rfidPayload.EPC, evt.Data.EPC)
		require.Equal(t, rfidPayload.RSSI, evt.Data.RSSI)
		require.Equal(t, "sender", evt.Source)
		require.Equal(t, "receiver", evt.Target)
	case <-ctx.Done():
		t.Fatal("timed out waiting for typed event")
	}

	// 2. Test error handler with invalid payload
	badEvt := &event.RawEvent{
		BaseEvent: event.BaseEvent{
			ID:     event.GenerateEventID(),
			Type:   eventType,
			Source: "sender",
			Target: "receiver",
		},
		Data: []byte(`"not-a-json-object"`),
	}
	require.NoError(t, cli1.Send(ctx, badEvt))

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for unmarshal error callback")
	}

	// 3. Test unsubscribe
	unsub()

	require.NoError(t, cli1.Send(ctx, rawEvt))
	select {
	case <-receivedCh:
		t.Fatal("received event after unsubscribe")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}
}

func TestTypedListeners_OnChannelTyped(t *testing.T) {
	server := newMockBrokerServer("")
	ts := httptest.NewServer(server)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const testChan = "rfid-live"

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(wsURL),
		client.WithClientName("publisher"),
	)
	require.NoError(t, err)
	defer cli1.Close()
	require.NoError(t, cli1.ConnectSync(ctx))

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(wsURL),
		client.WithClientName("subscriber"),
		client.WithInitialChannels(testChan),
	)
	require.NoError(t, err)
	defer cli2.Close()
	require.NoError(t, cli2.ConnectSync(ctx))

	received := make(chan *event.Event[TestRFIDData], 1)
	unsub := client.OnChannelTyped[TestRFIDData](cli2, testChan, func(evt *event.Event[TestRFIDData]) {
		received <- evt
	})
	defer unsub()

	typedEvt, err := event.NewEventTyped("rfid/scan", "publisher", testChan, TestRFIDData{
		EPC:  "E20000172211012518905322",
		RSSI: -45.0,
	})
	typedEvt.Channel = testChan
	require.NoError(t, err)

	rawEvt, err := event.ToRaw(typedEvt)
	require.NoError(t, err)

	require.NoError(t, cli1.Send(ctx, rawEvt))

	select {
	case evt := <-received:
		require.Equal(t, "E20000172211012518905322", evt.Data.EPC)
		require.Equal(t, -45.0, evt.Data.RSSI)
	case <-ctx.Done():
		t.Fatal("timed out waiting for channel typed event")
	}
}

func TestTypedListeners_SubscribeTyped(t *testing.T) {
	server := newMockBrokerServer("")
	ts := httptest.NewServer(server)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const streamChan = "rfid-stream"

	cli1, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(wsURL),
		client.WithClientName("stream-pub"),
	)
	require.NoError(t, err)
	defer cli1.Close()
	require.NoError(t, cli1.ConnectSync(ctx))

	cli2, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(wsURL),
		client.WithClientName("stream-sub"),
		client.WithInitialChannels(streamChan),
	)
	require.NoError(t, err)
	defer cli2.Close()
	require.NoError(t, cli2.ConnectSync(ctx))

	sub, err := client.SubscribeTyped[TestRFIDData](ctx, cli2, streamChan)
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.Equal(t, streamChan, sub.Channel())

	// Publish 3 events
	for i := 1; i <= 3; i++ {
		typedEvt, err := event.NewEventTyped("rfid/stream_item", "stream-pub", streamChan, TestRFIDData{
			EPC:  "TAG-000",
			RSSI: float64(i * -10),
		})
		typedEvt.Channel = streamChan
		require.NoError(t, err)
		rawEvt, err := event.ToRaw(typedEvt)
		require.NoError(t, err)
		require.NoError(t, cli1.Send(ctx, rawEvt))
	}

	var count int
	for range 3 {
		select {
		case evt, ok := <-sub.Events():
			require.True(t, ok)
			require.Equal(t, "TAG-000", evt.Data.EPC)
			count++
		case <-ctx.Done():
			t.Fatal("timed out waiting for stream events")
		}
	}
	require.Equal(t, 3, count)

	// Test unsubscribe
	require.NoError(t, sub.Unsubscribe())
}
