// Package client provides a robust, mTLS-capable WebSocket client for WSBroker
// with automatic reconnection, dynamic channel subscription tracking, and event routing.
package client

import (
	"context"
	"net"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/beevik/ntp"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// ConnectionMode specifies the access permission mode of a WebSocket connection.
type ConnectionMode string

const (
	// ModeRW allows both reading (subscribing to events) and writing (publishing events).
	ModeRW ConnectionMode = "rw"
	// ModeRO allows only reading (subscribing to events), rejecting publish attempts.
	ModeRO ConnectionMode = "ro"
	// ModeWO allows only writing (publishing events), bypassing all outgoing broadcast deliveries.
	ModeWO ConnectionMode = "wo"
)

// ParseConnectionMode parses and validates a string into a ConnectionMode.
func ParseConnectionMode(s string) (ConnectionMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "rw", "readwrite":
		return ModeRW, nil
	case "ro", "readonly":
		return ModeRO, nil
	case "wo", "w", "writeonly":
		return ModeWO, nil
	default:
		return "", errors.Errorf("invalid or missing connection mode: %s", s)
	}
}

// CanRead returns true if the connection mode allows receiving messages/broadcasts.
func (m ConnectionMode) CanRead() bool {
	return m != ModeWO
}

// CanWrite returns true if the connection mode allows publishing messages.
func (m ConnectionMode) CanWrite() bool {
	return m != ModeRO
}

// ConnectionState represents the current lifecycle state of the WebSocket client.
type ConnectionState string

const (
	// StateDisconnected indicates the client is currently not connected.
	StateDisconnected ConnectionState = "disconnected"
	// StateConnecting indicates a connection attempt is actively in progress.
	StateConnecting ConnectionState = "connecting"
	// StateConnected indicates the WebSocket connection is open and active.
	StateConnected ConnectionState = "connected"
	// StateReconnecting indicates the client is attempting to re-establish a dropped connection.
	StateReconnecting ConnectionState = "reconnecting"
	// StateClosed indicates the client has been permanently closed via Close().
	StateClosed ConnectionState = "closed"
)

// EventHandler is a callback function invoked when a matching event is received.
type EventHandler func(evt *event.Event)

// UnsubscribeFunc is a function that removes a registered listener or subscription when called.
type UnsubscribeFunc func()

// Subscription represents an active channel subscription stream.
type Subscription interface {
	// Channel returns the subscribed channel name.
	Channel() string
	// Events returns the receive-only channel carrying incoming events.
	Events() <-chan *event.Event
	// Unsubscribe stops the subscription and releases resources.
	Unsubscribe() error
}

// ClientInterface defines the standard interface for communicating with a WSBroker server.
// Downstream consumers and tests can use this interface for dependency injection and mocking.
type ClientInterface interface {
	// ClientName returns the configured unique identifier of the WSBroker client.
	ClientName() string
	// Mode returns the configured connection mode (rw, ro, wo).
	Mode() ConnectionMode
	// Channels returns the slice of currently tracked channel subscriptions.
	Channels() []string
	// IsConnected returns true if the client is actively connected to the server.
	IsConnected() bool
	// GetState returns the current connection lifecycle state.
	GetState() ConnectionState

	// Connect initiates an asynchronous connection to the WSBroker server.
	Connect(ctx context.Context) error
	// ConnectSync connects synchronously and blocks until connected or ctx expires.
	ConnectSync(ctx context.Context) error
	// Close gracefully closes the WebSocket connection and releases background goroutines.
	Close() error

	// Send writes an event envelope to the WebSocket connection.
	Send(ctx context.Context, evt *event.Event) error
	// SendMessage sends a payload to a target (channel or direct client).
	SendMessage(ctx context.Context, target string, data any) error
	// Broadcast sends a broadcast payload to a channel.
	Broadcast(ctx context.Context, channel string, data any) error
	// SendDirect sends a direct message to a named client.
	SendDirect(ctx context.Context, targetClient string, data any) error
	// Request sends an event and waits synchronously for a correlated response matching evt.Token.
	Request(ctx context.Context, evt *event.Event) (*event.Event, error)

	// Attach subscribes the client to one or more channels dynamically.
	Attach(ctx context.Context, channels ...string) error
	// Detach unsubscribes the client from one or more channels dynamically.
	Detach(ctx context.Context, channels ...string) error
	// Subscribe returns an active streaming Subscription for the specified channel.
	Subscribe(ctx context.Context, channel string) (Subscription, error)

	// Ping sends an explicit ping request event to the server.
	Ping(ctx context.Context) error
	// QueryNTP sends a raw NTP packet to the server and waits for the response packet.
	QueryNTP(ctx context.Context, data []byte) ([]byte, error)
	// NTP queries NTP time through the wsbroker connection using github.com/beevik/ntp.
	NTP(ctx context.Context, opts ...ntp.QueryOptions) (*ntp.Response, error)
	// ClockOffset returns the latest measured clock offset relative to the NTP server.
	ClockOffset() time.Duration
	// Time returns the current local time adjusted by the client's latest measured NTP ClockOffset.
	Time() time.Time
	// SyncNTP performs an NTP query and returns the measured clock offset.
	SyncNTP(ctx context.Context, opts ...ntp.QueryOptions) (time.Duration, error)
	// NewNTPConn returns a net.Conn that routes NTP packets over the wsbroker WebSocket connection.
	NewNTPConn(ctx context.Context) net.Conn

	// QueryChannelInfo queries metadata and subscriber client names for a given channel.
	QueryChannelInfo(ctx context.Context, channel string) (*event.ChannelInfo, error)
	// ChannelInfo is an alias for QueryChannelInfo.
	ChannelInfo(ctx context.Context, channel string) (*event.ChannelInfo, error)

	// OnEvent registers a global event listener called for every received event.
	OnEvent(handler EventHandler) UnsubscribeFunc
	// OnMessage registers a listener for incoming TypeMessage events.
	OnMessage(handler EventHandler) UnsubscribeFunc
	// OnChannel registers a listener for events targeted to a specific channel.
	OnChannel(channel string, handler EventHandler) UnsubscribeFunc
	// OnDirect registers a listener for direct point-to-point messages sent to this client.
	OnDirect(handler EventHandler) UnsubscribeFunc
	// OnType registers a listener for events of a specific EventType.
	OnType(t event.EventType, handler EventHandler) UnsubscribeFunc

	// OnConnect registers a callback invoked when the client successfully connects.
	OnConnect(callback func()) UnsubscribeFunc
	// OnDisconnect registers a callback invoked when the connection is dropped.
	OnDisconnect(callback func(error)) UnsubscribeFunc
	// OnReconnect registers a callback invoked after a successful reconnection.
	OnReconnect(callback func()) UnsubscribeFunc
	// OnError registers a callback invoked when an error occurs during connection or reconnect.
	OnError(callback func(error)) UnsubscribeFunc
}

// BrokerClient is an alias for ClientInterface for compatibility with display clients and controllers.
type BrokerClient = ClientInterface
