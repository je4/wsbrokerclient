// Package client provides a robust, mTLS-capable WebSocket client for WSBroker
// with automatic reconnection, dynamic channel subscription tracking, and event routing.
package client

import (
	"crypto/tls"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbrokerclient/pkg/event"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

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

// Config defines the configuration parameters for initializing a WSBroker Client.
type Config struct {
	// Endpoint is the target WebSocket URL (e.g. "wss://localhost:8080/ws/client-1" or "http://localhost:8080").
	Endpoint string `json:"endpoint" toml:"endpoint"`

	// ClientName is the unique client identifier registered with WSBroker.
	// If omitted, a random client ID is generated or extracted from Endpoint URL.
	ClientName string `json:"client_name" toml:"client_name"`

	// InitialChannels is the list of channel names to subscribe to upon initial connection.
	InitialChannels []string `json:"initial_channels" toml:"initial_channels"`

	// Bearer is the optional Bearer token used for HTTP/WS authentication.
	Bearer string `json:"bearer" toml:"bearer"`

	// TLSConfig is a custom standard crypto/tls configuration.
	TLSConfig *tls.Config `json:"-" toml:"-"`

	// TLSCertLoader is the certloader configuration for dynamic mTLS and certificate rotation.
	TLSCertLoader *loader.Config `json:"tls" toml:"tls"`

	// AutoReconnect enables automatic reconnection upon unexpected disconnection.
	AutoReconnect bool `json:"auto_reconnect" toml:"auto_reconnect"`

	// ReconnectInterval is the initial wait duration before the first reconnection attempt.
	ReconnectInterval time.Duration `json:"reconnect_interval" toml:"reconnect_interval"`

	// MaxReconnectInterval is the upper bound duration for exponential backoff.
	MaxReconnectInterval time.Duration `json:"max_reconnect_interval" toml:"max_reconnect_interval"`

	// ReconnectBackoffFactor is the multiplier applied to backoff after each failed attempt.
	ReconnectBackoffFactor float64 `json:"reconnect_backoff_factor" toml:"reconnect_backoff_factor"`

	// MaxReconnectAttempts is the maximum number of reconnect attempts before giving up (0 = infinite).
	MaxReconnectAttempts int `json:"max_reconnect_attempts" toml:"max_reconnect_attempts"`

	// PingInterval is the interval at which client-side heartbeat pings are sent (0 = disabled).
	PingInterval time.Duration `json:"ping_interval" toml:"ping_interval"`

	// ConnectTimeout is the timeout duration for establishing a connection.
	ConnectTimeout time.Duration `json:"connect_timeout" toml:"connect_timeout"`

	// WriteTimeout is the timeout duration for write operations.
	WriteTimeout time.Duration `json:"write_timeout" toml:"write_timeout"`

	// Logger is the logger instance used for diagnostics and debug logging.
	Logger zLogger.ZLogger `json:"-" toml:"-"`
}

// DefaultConfig returns a Config initialized with sensible default values.
func DefaultConfig() Config {
	return Config{
		AutoReconnect:          true,
		ReconnectInterval:      1 * time.Second,
		MaxReconnectInterval:   30 * time.Second,
		ReconnectBackoffFactor: 1.5,
		MaxReconnectAttempts:   0, // infinite
		PingInterval:           20 * time.Second,
		ConnectTimeout:         10 * time.Second,
		WriteTimeout:           5 * time.Second,
	}
}
