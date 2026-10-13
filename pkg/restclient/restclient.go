package restclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/rs/zerolog"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// RESTClient provides methods for interacting with WSBroker's RESTful API.
type RESTClient struct {
	httpClient *http.Client
	baseURL    string
	apiPrefix  string
	bearer     string
	logger     zLogger.ZLogger
	tlsCloser  io.Closer
}

// New creates and configures a new RESTClient.
func New(cfg Config, opts ...Option) (*RESTClient, error) {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.Timeout <= 0 {
		cfg.Timeout = 15 * time.Second
	}

	logger := cfg.Logger
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	baseURL := strings.TrimSpace(cfg.BaseURL)
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}

	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid base URL: %s", baseURL)
	}

	// Normalize schemes ws/wss -> http/https
	if u.Scheme == "ws" {
		u.Scheme = "http"
	} else if u.Scheme == "wss" {
		u.Scheme = "https"
	}

	var tlsCloser io.Closer
	tlsConfig := cfg.TLSConfig
	if cfg.TLSCertLoader != nil && tlsConfig == nil {
		var loaderErr error
		tlsConfig, tlsCloser, loaderErr = loader.CreateClientLoader(cfg.TLSCertLoader, logger)
		if loaderErr != nil {
			return nil, errors.Wrap(loaderErr, "failed to create TLS cert loader")
		}
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
	}

	basePath := strings.TrimRight(u.Path, "/")
	apiPrefix := basePath + "/api/v1"
	if strings.HasSuffix(basePath, "/api/v1") {
		apiPrefix = basePath
		u.Path = strings.TrimSuffix(basePath, "/api/v1")
	}

	rootURL := fmt.Sprintf("%s://%s", u.Scheme, u.Host)

	return &RESTClient{
		httpClient: httpClient,
		baseURL:    rootURL,
		apiPrefix:  apiPrefix,
		bearer:     cfg.Bearer,
		logger:     logger,
		tlsCloser:  tlsCloser,
	}, nil
}

// Close releases any allocated resources (such as dynamic certificate loaders).
func (c *RESTClient) Close() error {
	if c.tlsCloser != nil {
		return c.tlsCloser.Close()
	}
	return nil
}

func (c *RESTClient) doRequest(ctx context.Context, method, path string, body any, result any) error {
	fullURL := c.baseURL + c.apiPrefix + path

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return errors.Wrap(err, "failed to marshal request body")
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return errors.Wrapf(err, "failed to create %s request to %s", method, fullURL)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	if c.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+c.bearer)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "HTTP request %s %s failed", method, fullURL)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.Errorf("HTTP %d error from %s: %s", resp.StatusCode, fullURL, string(respBytes))
	}

	if result != nil && len(respBytes) > 0 {
		if strPtr, ok := result.(*string); ok {
			*strPtr = string(respBytes)
			return nil
		}
		if err := json.Unmarshal(respBytes, result); err != nil {
			return errors.Wrapf(err, "failed to unmarshal JSON response: %s", string(respBytes))
		}
	}

	return nil
}

// Ping checks if the WSBroker service is healthy.
func (c *RESTClient) Ping(ctx context.Context) (string, error) {
	var result string
	err := c.doRequest(ctx, http.MethodGet, "/ping", nil, &result)
	return strings.TrimSpace(result), err
}

// Publish dispatches a standardized Event or PublishRequest through the WSBroker REST endpoint.
func (c *RESTClient) Publish(ctx context.Context, evt *event.Event) (*PublishResponse, error) {
	if evt == nil {
		return nil, errors.New("event is nil")
	}

	req := PublishRequest{
		Source: evt.Source,
		Target: evt.Target,
		Type:   string(evt.Type),
		Token:  evt.Token,
		Data:   evt.Data,
	}

	var resp PublishResponse
	if err := c.doRequest(ctx, http.MethodPost, "/publish", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// PublishToChannel publishes a payload directly to a specific channel endpoint.
func (c *RESTClient) PublishToChannel(ctx context.Context, channel, source string, payload any) (*PublishResponse, error) {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return nil, errors.New("channel cannot be empty")
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal payload")
	}

	req := PublishRequest{
		Source: source,
		Target: channel,
		Type:   string(event.TypeMessage),
		Data:   raw,
	}

	path := "/channel/" + url.PathEscape(channel) + "/publish"
	var resp PublishResponse
	if err := c.doRequest(ctx, http.MethodPost, path, req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetChannels queries all active channels and their subscriber counts.
func (c *RESTClient) GetChannels(ctx context.Context) ([]ChannelInfo, error) {
	var result []ChannelInfo
	if err := c.doRequest(ctx, http.MethodGet, "/channels", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetClients returns a list of all currently connected client names.
func (c *RESTClient) GetClients(ctx context.Context) ([]string, error) {
	var result []string
	if err := c.doRequest(ctx, http.MethodGet, "/clients", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetChannelClients returns the list of client names subscribed to the specified channel.
func (c *RESTClient) GetChannelClients(ctx context.Context, channel string) ([]string, error) {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return nil, errors.New("channel cannot be empty")
	}

	path := "/channel/" + url.PathEscape(channel) + "/clients"
	var result []string
	if err := c.doRequest(ctx, http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// DisconnectClient forcefully terminates a connected client session.
func (c *RESTClient) DisconnectClient(ctx context.Context, clientName string) error {
	clientName = strings.TrimSpace(clientName)
	if clientName == "" {
		return errors.New("clientName cannot be empty")
	}

	path := "/client/" + url.PathEscape(clientName)
	return c.doRequest(ctx, http.MethodDelete, path, nil, nil)
}
