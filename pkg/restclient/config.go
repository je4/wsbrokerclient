package restclient

import (
	"crypto/tls"
	"net/url"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// Config defines the configuration parameters for the REST client.
type Config struct {
	// BaseURL is the root address of the WSBroker service (e.g. "https://localhost:8080" or "http://localhost:8080/api/v1").
	BaseURL string `json:"base_url" toml:"base_url"`

	// Bearer is the optional Bearer token used for HTTP authorization.
	Bearer string `json:"bearer" toml:"bearer"`

	// TLSConfig is a custom standard crypto/tls configuration.
	TLSConfig *tls.Config `json:"-" toml:"-"`

	// TLSCertLoader is the certloader configuration for dynamic mTLS.
	TLSCertLoader *loader.Config `json:"tls" toml:"tls"`

	// Timeout is the HTTP request timeout.
	Timeout time.Duration `json:"timeout" toml:"timeout"`

	// Logger is the logger instance used for diagnostics.
	Logger zLogger.ZLogger `json:"-" toml:"-"`
}

// DefaultConfig returns a Config initialized with sensible default values.
func DefaultConfig() Config {
	return Config{
		BaseURL: "http://localhost:8080",
		Timeout: 15 * time.Second,
	}
}

// Validate validates the configuration parameters.
func (c *Config) Validate() error {
	if c.BaseURL != "" {
		if _, err := url.Parse(strings.TrimSpace(c.BaseURL)); err != nil {
			return errors.Wrapf(err, "invalid base URL: %s", c.BaseURL)
		}
	}
	return nil
}
