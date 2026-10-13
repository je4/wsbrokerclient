package restclient

import (
	"crypto/tls"
	"time"

	"github.com/je4/utils/v2/pkg/zLogger"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// Option is a functional option for configuring RESTClient.
type Option func(*Config)

// WithBaseURL sets the base URL for the REST client.
func WithBaseURL(baseURL string) Option {
	return func(c *Config) {
		c.BaseURL = baseURL
	}
}

// WithBearerToken sets the Bearer authentication token.
func WithBearerToken(token string) Option {
	return func(c *Config) {
		c.Bearer = token
	}
}

// WithTLSConfig sets the TLS configuration.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(c *Config) {
		c.TLSConfig = tlsConfig
	}
}

// WithTLSCertLoader sets the certloader configuration for mTLS.
func WithTLSCertLoader(certLoader *loader.Config) Option {
	return func(c *Config) {
		c.TLSCertLoader = certLoader
	}
}

// WithTimeout sets the HTTP request timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.Timeout = timeout
	}
}

// WithLogger sets the diagnostic logger.
func WithLogger(logger zLogger.ZLogger) Option {
	return func(c *Config) {
		c.Logger = logger
	}
}
