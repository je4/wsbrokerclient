package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/coder/websocket"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/rs/zerolog"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

var _ ClientInterface = (*Client)(nil)

// Client is the primary interface for communicating with a WSBroker server.
type Client struct {
	config Config
	logger zLogger.ZLogger

	state   ConnectionState
	stateMu sync.RWMutex

	conn    *websocket.Conn
	connMu  sync.RWMutex
	writeMu sync.Mutex

	tlsCloser io.Closer

	clientName string
	channels   map[string]struct{}
	channelsMu sync.RWMutex

	// Event listeners
	globalListeners []listenerEntry
	typeListeners   map[event.EventType][]listenerEntry
	chanListeners   map[string][]listenerEntry
	directListeners []listenerEntry
	nextListenerID  atomic.Uint64
	listenersMu     sync.RWMutex

	// Channel subscriptions (stream-based)
	subscriptions map[string][]*subscription
	subMu         sync.RWMutex

	// RPC / Request-Reply waiters correlated by event.Session
	waiters   map[string]chan *event.RawEvent
	waitersMu sync.RWMutex

	// Lifecycle callbacks
	onConnect    []func()
	onDisconnect []func(error)
	onReconnect  []func()
	onError      []func(error)
	lifecycleMu  sync.RWMutex

	clockOffset atomic.Int64 // in nanoseconds

	ctx       context.Context
	cancel    context.CancelFunc
	reconnCh  chan struct{}
	closeOnce sync.Once
	closed    atomic.Bool
	wg        sync.WaitGroup
}

// New creates and initializes a new WSBroker Client with the provided Config and Options.
func New(cfg Config, opts ...Option) (*Client, error) {
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 1 * time.Second
	}
	if cfg.MaxReconnectInterval <= 0 {
		cfg.MaxReconnectInterval = 30 * time.Second
	}
	if cfg.ReconnectBackoffFactor <= 1.0 {
		cfg.ReconnectBackoffFactor = 1.5
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
	if cfg.ReadLimit <= 0 {
		cfg.ReadLimit = 16 * 1024 * 1024
	}

	logger := cfg.Logger
	if logger == nil {
		nop := zerolog.Nop()
		logger = &nop
	}

	name := strings.TrimSpace(cfg.ClientName)
	if name == "" {
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		name = fmt.Sprintf("client-%s", hex.EncodeToString(b))
	}

	if cfg.Mode != "" {
		m, err := ParseConnectionMode(string(cfg.Mode))
		if err != nil {
			return nil, err
		}
		cfg.Mode = m
	} else {
		cfg.Mode = ModeRW
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		config:        cfg,
		logger:        logger,
		state:         StateDisconnected,
		clientName:    name,
		channels:      make(map[string]struct{}),
		typeListeners: make(map[event.EventType][]listenerEntry),
		chanListeners: make(map[string][]listenerEntry),
		subscriptions: make(map[string][]*subscription),
		waiters:       make(map[string]chan *event.RawEvent),
		ctx:           ctx,
		cancel:        cancel,
		reconnCh:      make(chan struct{}, 1),
	}

	for _, ch := range cfg.InitialChannels {
		ch = strings.TrimSpace(ch)
		if ch != "" {
			c.channels[ch] = struct{}{}
		}
	}

	// Setup TLS/mTLS if certloader is configured
	if cfg.TLSCertLoader != nil && cfg.TLSConfig == nil {
		tlsConfig, closer, err := loader.CreateClientLoader(cfg.TLSCertLoader, logger)
		if err != nil {
			cancel()
			return nil, errors.Wrap(err, "failed to initialize TLS cert loader")
		}
		c.config.TLSConfig = tlsConfig
		c.tlsCloser = closer
	}

	return c, nil
}

// IsConnected returns true if the client is actively connected to the server.
func (c *Client) IsConnected() bool {
	return c.GetState() == StateConnected
}

// GetState returns the current connection lifecycle state.
func (c *Client) GetState() ConnectionState {
	c.stateMu.RLock()
	defer c.stateMu.RUnlock()
	return c.state
}

func (c *Client) setState(st ConnectionState) {
	c.stateMu.Lock()
	c.state = st
	c.stateMu.Unlock()
}

// ClientName returns the registered client identifier.
func (c *Client) ClientName() string {
	return c.clientName
}

// Mode returns the configured connection mode (rw, ro, wo).
func (c *Client) Mode() ConnectionMode {
	return c.config.Mode
}

// Channels returns the slice of currently tracked channel subscriptions.
func (c *Client) Channels() []string {
	c.channelsMu.RLock()
	defer c.channelsMu.RUnlock()
	result := make([]string, 0, len(c.channels))
	for ch := range c.channels {
		result = append(result, ch)
	}
	return result
}

// Subscribe returns an active streaming Subscription for the specified channel.
// It automatically calls Attach on the channel.
func (c *Client) Subscribe(ctx context.Context, channel string) (Subscription, error) {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return nil, errors.New("channel name cannot be empty")
	}

	sub := newSubscription(channel, 100, c)

	c.subMu.Lock()
	c.subscriptions[channel] = append(c.subscriptions[channel], sub)
	c.subMu.Unlock()

	if err := c.Attach(ctx, channel); err != nil {
		_ = sub.Unsubscribe()
		return nil, err
	}

	return sub, nil
}

func (c *Client) removeSubscription(sub *subscription) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	list := c.subscriptions[sub.channel]
	for i, s := range list {
		if s == sub {
			c.subscriptions[sub.channel] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(c.subscriptions[sub.channel]) == 0 {
		delete(c.subscriptions, sub.channel)
	}
}

// Close gracefully closes the WebSocket connection and releases background goroutines.
func (c *Client) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.cancel()

		c.connMu.Lock()
		conn := c.conn
		c.conn = nil
		c.connMu.Unlock()

		if conn != nil {
			err = conn.Close(websocket.StatusNormalClosure, "client closing")
		}

		c.setState(StateClosed)

		if c.tlsCloser != nil {
			if closerErr := c.tlsCloser.Close(); closerErr != nil {
				c.logger.Error().Err(closerErr).Msg("error closing TLS cert loader")
			}
		}

		c.wg.Wait()
	})
	return err
}
