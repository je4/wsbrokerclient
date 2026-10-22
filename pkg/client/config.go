package client

import (
	"crypto/tls"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// Config defines the configuration parameters for initializing a WSBroker Client.
type Config struct {
	// Endpoint is the target WebSocket URL (e.g. "wss://localhost:8080/ws/client-1" or "http://localhost:8080").
	Endpoint string `json:"endpoint" toml:"endpoint"`

	// ClientName is the unique client identifier registered with WSBroker.
	// If omitted, a random client ID is generated or extracted from Endpoint URL.
	ClientName string `json:"client_name" toml:"client_name"`

	// InitialChannels is the list of channel names to subscribe to upon initial connection.
	InitialChannels []string `json:"initial_channels" toml:"initial_channels"`

	// Mode is the connection mode (rw, ro, wo). Defaults to ModeRW ("rw").
	Mode ConnectionMode `json:"mode" toml:"mode"`

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

	// ReadLimit is the maximum message size in bytes that can be read over the WebSocket connection (default 16MB).
	ReadLimit int64 `json:"read_limit" toml:"read_limit"`

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
		Mode:                   ModeRW,
		AutoReconnect:          true,
		ReconnectInterval:      1 * time.Second,
		MaxReconnectInterval:   30 * time.Second,
		ReconnectBackoffFactor: 1.5,
		MaxReconnectAttempts:   0, // infinite
		PingInterval:           20 * time.Second,
		ReadLimit:              16 * 1024 * 1024, // 16 MB default read limit
		ConnectTimeout:         10 * time.Second,
		WriteTimeout:           5 * time.Second,
	}
}

// Validate validates the configuration parameters.
func (c *Config) Validate() error {
	if c.Mode != "" {
		if _, err := ParseConnectionMode(string(c.Mode)); err != nil {
			return err
		}
	}
	return nil
}
