package client

import (
	"crypto/tls"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// Option is a function that modifies Config.
type Option func(*Config)

// WithEndpoint sets the WebSocket target endpoint.
func WithEndpoint(endpoint string) Option {
	return func(c *Config) {
		c.Endpoint = endpoint
	}
}

// WithClientName sets the client identifier.
func WithClientName(name string) Option {
	return func(c *Config) {
		c.ClientName = name
	}
}

// WithInitialChannels specifies initial channels to subscribe to on connection.
func WithInitialChannels(channels ...string) Option {
	return func(c *Config) {
		c.InitialChannels = append(c.InitialChannels, channels...)
	}
}

// WithMode sets the connection mode (rw, ro, wo).
func WithMode(mode ConnectionMode) Option {
	return func(c *Config) {
		c.Mode = mode
	}
}

// WithBearerToken sets the Bearer authentication token.
func WithBearerToken(token string) Option {
	return func(c *Config) {
		c.Bearer = token
	}
}

// WithTLSConfig configures standard crypto/tls TLSConfig.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(c *Config) {
		c.TLSConfig = tlsConfig
	}
}

// WithTLSCertLoader configures certloader for dynamic mTLS certificates.
func WithTLSCertLoader(certLoaderConfig *loader.Config) Option {
	return func(c *Config) {
		c.TLSCertLoader = certLoaderConfig
	}
}

// WithAutoReconnect enables or disables automatic reconnection.
func WithAutoReconnect(enabled bool) Option {
	return func(c *Config) {
		c.AutoReconnect = enabled
	}
}

// WithReconnectStrategy configures the reconnection backoff parameters.
func WithReconnectStrategy(initial, max time.Duration, factor float64, maxAttempts int) Option {
	return func(c *Config) {
		if initial > 0 {
			c.ReconnectInterval = initial
		}
		if max > 0 {
			c.MaxReconnectInterval = max
		}
		if factor > 0 {
			c.ReconnectBackoffFactor = factor
		}
		c.MaxReconnectAttempts = maxAttempts
	}
}

// WithPingInterval sets the interval for heartbeat pings.
func WithPingInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.PingInterval = interval
	}
}

// WithConnectTimeout sets the timeout for establishing connections.
func WithConnectTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.ConnectTimeout = timeout
	}
}

// WithWriteTimeout sets the timeout for write operations.
func WithWriteTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.WriteTimeout = timeout
	}
}

// WithLogger sets the logger instance.
func WithLogger(logger zLogger.ZLogger) Option {
	return func(c *Config) {
		c.Logger = logger
	}
}
