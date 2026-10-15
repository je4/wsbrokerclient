package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/rs/zerolog"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

type listenerEntry struct {
	id uint64
	h  EventHandler
}

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

// Connect opens the WebSocket connection to WSBroker.
// If AutoReconnect is true, it launches background reconnect monitoring on disconnection.
func (c *Client) Connect(ctx context.Context) error {
	if c.closed.Load() {
		return errors.New("client is closed")
	}

	c.setState(StateConnecting)

	if err := c.dial(ctx); err != nil {
		c.setState(StateDisconnected)
		if c.config.AutoReconnect {
			c.logger.Warn().Err(err).Msg("initial connection failed, starting background reconnect")
			c.startBackgroundLoops()
			return nil
		}
		return errors.Wrap(err, "failed to connect to wsbroker")
	}

	c.startBackgroundLoops()
	return nil
}

// ConnectSync attempts an initial connection and blocks until connected or ctx is cancelled.
// If the initial connection fails, an error is returned immediately without entering background reconnect.
func (c *Client) ConnectSync(ctx context.Context) error {
	if c.closed.Load() {
		return errors.New("client is closed")
	}

	c.setState(StateConnecting)
	if err := c.dial(ctx); err != nil {
		c.setState(StateDisconnected)
		return errors.Wrap(err, "failed to connect to wsbroker")
	}

	c.startBackgroundLoops()
	return nil
}

func (c *Client) startBackgroundLoops() {
	if c.config.AutoReconnect {
		c.wg.Add(1)
		go c.reconnectLoop()
	}
}

func (c *Client) buildURL() (string, http.Header, error) {
	rawURL := strings.TrimSpace(c.config.Endpoint)
	if rawURL == "" {
		return "", nil, errors.New("empty endpoint URL")
	}

	// Replace http/https with ws/wss
	if strings.HasPrefix(rawURL, "http://") {
		rawURL = "ws://" + strings.TrimPrefix(rawURL, "http://")
	} else if strings.HasPrefix(rawURL, "https://") {
		rawURL = "wss://" + strings.TrimPrefix(rawURL, "https://")
	} else if !strings.HasPrefix(rawURL, "ws://") && !strings.HasPrefix(rawURL, "wss://") {
		if c.config.TLSConfig != nil || c.config.TLSCertLoader != nil {
			rawURL = "wss://" + rawURL
		} else {
			rawURL = "ws://" + rawURL
		}
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", nil, errors.Wrapf(err, "invalid endpoint URL '%s'", rawURL)
	}

	mode := c.config.Mode
	if mode == "" {
		mode = ModeRW
	}

	trimmedPath := strings.Trim(u.Path, "/")
	if trimmedPath == "" {
		u.Path = "/ws/" + url.PathEscape(c.clientName) + "/" + string(mode)
	} else {
		parts := strings.Split(trimmedPath, "/")
		wsIdx := -1
		for i, part := range parts {
			if part == "ws" {
				wsIdx = i
				break
			}
		}

		if wsIdx == -1 {
			u.Path = "/" + trimmedPath + "/ws/" + url.PathEscape(c.clientName) + "/" + string(mode)
		} else {
			prefix := parts[:wsIdx]
			wsParts := parts[wsIdx:]

			switch len(wsParts) {
			case 1:
				// /ws -> /ws/:clientName/:mode
				wsParts = append(wsParts, url.PathEscape(c.clientName), string(mode))
			case 2:
				// /ws/:name -> /ws/:name/:mode
				wsParts = append(wsParts, string(mode))
			case 3:
				if wsParts[1] == "channel" {
					// /ws/channel/:channel -> /ws/channel/:channel/:mode
					wsParts = append(wsParts, string(mode))
				}
			case 4:
				// /ws/channel/:channel/:mode -> already has mode
			}

			fullParts := append(prefix, wsParts...)
			u.Path = "/" + strings.Join(fullParts, "/")
		}
	}

	// Append initial channels and token to query if not already present
	q := u.Query()
	if strings.Contains(u.Path, "/ws/channel/") {
		if q.Get("name") == "" && q.Get("client") == "" && c.clientName != "" {
			q.Set("name", c.clientName)
		}
	}
	c.channelsMu.RLock()
	for ch := range c.channels {
		found := false
		for _, existing := range q["channel"] {
			if existing == ch {
				found = true
				break
			}
		}
		if !found {
			q.Add("channel", ch)
		}
	}
	c.channelsMu.RUnlock()

	header := make(http.Header)
	if c.config.Bearer != "" {
		header.Set("Authorization", "Bearer "+c.config.Bearer)
		if q.Get("bearer") == "" && q.Get("token") == "" {
			q.Set("token", c.config.Bearer)
		}
	}

	u.RawQuery = q.Encode()
	return u.String(), header, nil
}

func (c *Client) dial(ctx context.Context) error {
	wsURL, header, err := c.buildURL()
	if err != nil {
		return err
	}

	c.logger.Debug().Msgf("dialing websocket at %s", wsURL)

	connectTimeout := c.config.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}

	dialCtx, dialCancel := context.WithTimeout(ctx, connectTimeout)
	defer dialCancel()

	opts := &websocket.DialOptions{
		HTTPHeader: header,
	}

	if c.config.TLSConfig != nil {
		var transport *http.Transport
		if defTr, ok := http.DefaultTransport.(*http.Transport); ok {
			transport = defTr.Clone()
			transport.TLSClientConfig = c.config.TLSConfig
		} else {
			transport = &http.Transport{
				TLSClientConfig: c.config.TLSConfig,
			}
		}

		opts.HTTPClient = &http.Client{
			Transport: transport,
			Timeout:   connectTimeout,
		}
	}

	conn, resp, err := websocket.Dial(dialCtx, wsURL, opts)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return errors.Wrapf(err, "websocket dial error to %s", wsURL)
	}

	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	c.setState(StateConnected)
	c.logger.Info().Msgf("connected to wsbroker as %s", c.clientName)

	c.notifyConnect()

	// Launch connection reader and ping loops
	c.wg.Add(1)
	go c.readLoop(conn)

	if c.config.PingInterval > 0 {
		c.wg.Add(1)
		go c.pingLoop(conn)
	}

	return nil
}

func (c *Client) readLoop(conn *websocket.Conn) {
	defer c.wg.Done()
	defer func() {
		c.connMu.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.connMu.Unlock()

		_ = conn.Close(websocket.StatusNormalClosure, "read loop terminated")
	}()

	for {
		if c.closed.Load() {
			return
		}

		var evt event.Event
		err := wsjson.Read(c.ctx, conn, &evt)
		if err != nil {
			if c.closed.Load() {
				return
			}
			status := websocket.CloseStatus(err)
			if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
				c.logger.Debug().Msgf("connection closed normally: %v", err)
			} else {
				c.logger.Warn().Err(err).Msg("connection read error")
			}

			c.setState(StateDisconnected)
			c.notifyDisconnect(err)
			c.triggerReconnect()
			return
		}

		c.handleIncomingEvent(&evt)
	}
}

func (c *Client) pingLoop(conn *websocket.Conn) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.config.PingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.connMu.RLock()
			currentConn := c.conn
			c.connMu.RUnlock()

			if currentConn != conn {
				return
			}

			pingEvt, _ := event.NewPingEvent(c.clientName)
			pCtx, pCancel := context.WithTimeout(c.ctx, c.config.WriteTimeout)
			_ = c.writeEvent(pCtx, currentConn, pingEvt)
			pCancel()
		}
	}
}

func (c *Client) handleIncomingEvent(evt *event.Event) {
	c.logger.Debug().Msgf("received event %s [%s -> %s]", evt.Type, evt.Source, evt.Target)

	// Automatic heartbeat response
	if evt.Type == event.TypePing {
		pongTarget := evt.Source
		if pongTarget == "" {
			pongTarget = "wsbroker"
		}
		pongEvt, _ := event.NewPongEvent(c.clientName, pongTarget, evt.Data)
		go func() {
			pCtx, pCancel := context.WithTimeout(c.ctx, c.config.WriteTimeout)
			defer pCancel()
			_ = c.Send(pCtx, pongEvt)
		}()
		return
	}

	// Dispatch to subscribers & listeners
	c.listenersMu.RLock()
	var global []EventHandler
	for _, entry := range c.globalListeners {
		global = append(global, entry.h)
	}
	var byType []EventHandler
	for _, entry := range c.typeListeners[evt.Type] {
		byType = append(byType, entry.h)
	}
	var byChan []EventHandler
	for _, entry := range c.chanListeners[evt.Target] {
		byChan = append(byChan, entry.h)
	}
	var direct []EventHandler
	if evt.Target == c.clientName {
		for _, entry := range c.directListeners {
			direct = append(direct, entry.h)
		}
	}
	c.listenersMu.RUnlock()

	for _, h := range global {
		c.invokeHandler(h, evt)
	}
	for _, h := range byType {
		c.invokeHandler(h, evt)
	}
	for _, h := range byChan {
		c.invokeHandler(h, evt)
	}
	for _, h := range direct {
		c.invokeHandler(h, evt)
	}

	// Deliver to stream subscriptions
	c.subMu.RLock()
	subs := append([]*subscription(nil), c.subscriptions[evt.Target]...)
	c.subMu.RUnlock()

	for _, sub := range subs {
		sub.deliver(evt)
	}
}

func (c *Client) invokeHandler(h EventHandler, evt *event.Event) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Error().Msgf("panic in event handler: %v", r)
			}
		}()
		h(evt)
	}()
}

func (c *Client) triggerReconnect() {
	select {
	case c.reconnCh <- struct{}{}:
	default:
	}
}

func (c *Client) reconnectLoop() {
	defer c.wg.Done()

	interval := c.config.ReconnectInterval
	attempts := 0

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.reconnCh:
		}

		if c.closed.Load() {
			return
		}

		c.setState(StateReconnecting)

		for {
			if c.closed.Load() {
				return
			}

			attempts++
			if c.config.MaxReconnectAttempts > 0 && attempts > c.config.MaxReconnectAttempts {
				c.logger.Error().Msgf("exceeded max reconnect attempts (%d)", c.config.MaxReconnectAttempts)
				c.setState(StateDisconnected)
				c.notifyError(errors.New("exceeded max reconnect attempts"))
				return
			}

			// Add jitter: 0% to 20%
			jitter := time.Duration(mathrand.Float64() * 0.2 * float64(interval))
			sleepDuration := interval + jitter

			c.logger.Info().Msgf("reconnecting in %v (attempt #%d)...", sleepDuration, attempts)

			select {
			case <-c.ctx.Done():
				return
			case <-time.After(sleepDuration):
			}

			if c.closed.Load() {
				return
			}

			dialCtx, dialCancel := context.WithTimeout(c.ctx, c.config.ConnectTimeout)
			err := c.dial(dialCtx)
			dialCancel()

			if err == nil {
				c.logger.Info().Msgf("successfully reconnected to wsbroker")
				interval = c.config.ReconnectInterval
				attempts = 0

				// Re-attach to active channels
				c.reAttachChannels()
				c.notifyReconnect()
				break
			}

			c.logger.Warn().Err(err).Msg("reconnect attempt failed")
			c.notifyError(err)

			// Exponential backoff
			nextInterval := time.Duration(float64(interval) * c.config.ReconnectBackoffFactor)
			if nextInterval > c.config.MaxReconnectInterval {
				nextInterval = c.config.MaxReconnectInterval
			}
			interval = nextInterval
		}
	}
}

func (c *Client) reAttachChannels() {
	c.channelsMu.RLock()
	channels := make([]string, 0, len(c.channels))
	for ch := range c.channels {
		channels = append(channels, ch)
	}
	c.channelsMu.RUnlock()

	for _, ch := range channels {
		evt, err := event.NewAttachEvent(c.clientName, ch)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(c.ctx, c.config.WriteTimeout)
		_ = c.Send(ctx, evt)
		cancel()
	}
}

func (c *Client) writeEvent(ctx context.Context, conn *websocket.Conn, evt *event.Event) error {
	if conn == nil {
		return errors.New("not connected")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return wsjson.Write(ctx, conn, evt)
}

// Send writes an event envelope to the WebSocket connection.
func (c *Client) Send(ctx context.Context, evt *event.Event) error {
	if c.closed.Load() {
		return errors.New("client is closed")
	}
	if evt == nil {
		return errors.New("event is nil")
	}
	if evt.Source == "" {
		evt.Source = c.clientName
	}

	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()

	if conn == nil {
		return errors.New("cannot send event: client is not connected")
	}

	timeout := c.config.WriteTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	writeCtx, writeCancel := context.WithTimeout(ctx, timeout)
	defer writeCancel()

	return c.writeEvent(writeCtx, conn, evt)
}

// SendMessage sends a payload to a target (channel or direct client).
func (c *Client) SendMessage(ctx context.Context, target string, data any) error {
	evt, err := event.NewMessageEvent(c.clientName, target, data)
	if err != nil {
		return err
	}
	return c.Send(ctx, evt)
}

// Broadcast sends a broadcast payload to a channel.
func (c *Client) Broadcast(ctx context.Context, channel string, data any) error {
	return c.SendMessage(ctx, channel, data)
}

// SendDirect sends a direct message to a named client.
func (c *Client) SendDirect(ctx context.Context, targetClient string, data any) error {
	return c.SendMessage(ctx, targetClient, data)
}

// Attach subscribes the client to one or more channels dynamically.
func (c *Client) Attach(ctx context.Context, channels ...string) error {
	for _, ch := range channels {
		ch = strings.TrimSpace(ch)
		if ch == "" {
			continue
		}

		c.channelsMu.Lock()
		c.channels[ch] = struct{}{}
		c.channelsMu.Unlock()

		if c.IsConnected() {
			evt, err := event.NewAttachEvent(c.clientName, ch)
			if err != nil {
				return err
			}
			if err := c.Send(ctx, evt); err != nil {
				return errors.Wrapf(err, "failed to send attach event for %s", ch)
			}
		}
	}
	return nil
}

// Detach unsubscribes the client from one or more channels dynamically.
func (c *Client) Detach(ctx context.Context, channels ...string) error {
	for _, ch := range channels {
		ch = strings.TrimSpace(ch)
		if ch == "" {
			continue
		}

		c.channelsMu.Lock()
		delete(c.channels, ch)
		c.channelsMu.Unlock()

		if c.IsConnected() {
			evt, err := event.NewDetachEvent(c.clientName, ch)
			if err != nil {
				return err
			}
			if err := c.Send(ctx, evt); err != nil {
				return errors.Wrapf(err, "failed to send detach event for %s", ch)
			}
		}
	}
	return nil
}

// Ping sends an explicit ping request event to the server.
func (c *Client) Ping(ctx context.Context) error {
	evt, err := event.NewPingEvent(c.clientName)
	if err != nil {
		return err
	}
	return c.Send(ctx, evt)
}

// QueryNTP sends a raw NTP packet (typically 48 bytes) to the server and waits for the response packet.
func (c *Client) QueryNTP(ctx context.Context, data []byte) ([]byte, error) {
	if !c.IsConnected() {
		return nil, errors.New("client is not connected")
	}

	evt, err := event.NewNTPQueryEvent(c.clientName, data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create NTP query event")
	}

	respCh := make(chan *event.Event, 1)

	unsubResp := c.OnType(event.TypeNTPResponse, func(e *event.Event) {
		select {
		case respCh <- e:
		default:
		}
	})
	defer unsubResp()

	unsubErr := c.OnType(event.TypeNTPError, func(e *event.Event) {
		select {
		case respCh <- e:
		default:
		}
	})
	defer unsubErr()

	if err := c.Send(ctx, evt); err != nil {
		return nil, errors.Wrap(err, "failed to send NTP query event")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errors.New("client closed")
	case resp := <-respCh:
		if resp.Type == event.TypeNTPError {
			var errMsg string
			if err := resp.UnmarshalData(&errMsg); err == nil {
				return nil, errors.Errorf("NTP error from server: %s", errMsg)
			}
			return nil, errors.New("NTP error from server")
		}
		raw, err := resp.GetData()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get NTP response data")
		}
		respBytes, ok := raw.([]byte)
		if !ok {
			return nil, errors.Errorf("unexpected NTP response data type %T", raw)
		}
		return respBytes, nil
	}
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

// OnEvent registers a global event listener called for every received event.
func (c *Client) OnEvent(handler EventHandler) UnsubscribeFunc {
	id := c.nextListenerID.Add(1)
	c.listenersMu.Lock()
	defer c.listenersMu.Unlock()
	c.globalListeners = append(c.globalListeners, listenerEntry{id: id, h: handler})

	return func() {
		c.listenersMu.Lock()
		defer c.listenersMu.Unlock()
		for i, entry := range c.globalListeners {
			if entry.id == id {
				c.globalListeners = append(c.globalListeners[:i], c.globalListeners[i+1:]...)
				break
			}
		}
	}
}

// OnMessage registers a listener for incoming TypeMessage events.
func (c *Client) OnMessage(handler EventHandler) UnsubscribeFunc {
	return c.OnType(event.TypeMessage, handler)
}

// OnChannel registers a listener for events targeted to a specific channel.
func (c *Client) OnChannel(channel string, handler EventHandler) UnsubscribeFunc {
	id := c.nextListenerID.Add(1)
	c.listenersMu.Lock()
	defer c.listenersMu.Unlock()
	c.chanListeners[channel] = append(c.chanListeners[channel], listenerEntry{id: id, h: handler})

	return func() {
		c.listenersMu.Lock()
		defer c.listenersMu.Unlock()
		list := c.chanListeners[channel]
		for i, entry := range list {
			if entry.id == id {
				c.chanListeners[channel] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(c.chanListeners[channel]) == 0 {
			delete(c.chanListeners, channel)
		}
	}
}

// OnDirect registers a listener for direct point-to-point messages sent to this client.
func (c *Client) OnDirect(handler EventHandler) UnsubscribeFunc {
	id := c.nextListenerID.Add(1)
	c.listenersMu.Lock()
	defer c.listenersMu.Unlock()
	c.directListeners = append(c.directListeners, listenerEntry{id: id, h: handler})

	return func() {
		c.listenersMu.Lock()
		defer c.listenersMu.Unlock()
		for i, entry := range c.directListeners {
			if entry.id == id {
				c.directListeners = append(c.directListeners[:i], c.directListeners[i+1:]...)
				break
			}
		}
	}
}

// OnType registers a listener for events of a specific EventType.
func (c *Client) OnType(t event.EventType, handler EventHandler) UnsubscribeFunc {
	id := c.nextListenerID.Add(1)
	c.listenersMu.Lock()
	defer c.listenersMu.Unlock()
	c.typeListeners[t] = append(c.typeListeners[t], listenerEntry{id: id, h: handler})

	return func() {
		c.listenersMu.Lock()
		defer c.listenersMu.Unlock()
		list := c.typeListeners[t]
		for i, entry := range list {
			if entry.id == id {
				c.typeListeners[t] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(c.typeListeners[t]) == 0 {
			delete(c.typeListeners, t)
		}
	}
}

// OnConnect registers a callback invoked when the client successfully connects.
func (c *Client) OnConnect(callback func()) UnsubscribeFunc {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.onConnect = append(c.onConnect, callback)
	return func() {}
}

// OnDisconnect registers a callback invoked when the connection is dropped.
func (c *Client) OnDisconnect(callback func(error)) UnsubscribeFunc {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.onDisconnect = append(c.onDisconnect, callback)
	return func() {}
}

// OnReconnect registers a callback invoked after a successful reconnection.
func (c *Client) OnReconnect(callback func()) UnsubscribeFunc {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.onReconnect = append(c.onReconnect, callback)
	return func() {}
}

// OnError registers a callback invoked when an error occurs during connection or reconnect.
func (c *Client) OnError(callback func(error)) UnsubscribeFunc {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.onError = append(c.onError, callback)
	return func() {}
}

func (c *Client) notifyConnect() {
	c.lifecycleMu.RLock()
	cbs := append([]func(){}, c.onConnect...)
	c.lifecycleMu.RUnlock()
	for _, cb := range cbs {
		go cb()
	}
}

func (c *Client) notifyDisconnect(err error) {
	c.lifecycleMu.RLock()
	cbs := append([]func(error){}, c.onDisconnect...)
	c.lifecycleMu.RUnlock()
	for _, cb := range cbs {
		go cb(err)
	}
}

func (c *Client) notifyReconnect() {
	c.lifecycleMu.RLock()
	cbs := append([]func(){}, c.onReconnect...)
	c.lifecycleMu.RUnlock()
	for _, cb := range cbs {
		go cb()
	}
}

func (c *Client) notifyError(err error) {
	c.lifecycleMu.RLock()
	cbs := append([]func(error){}, c.onError...)
	c.lifecycleMu.RUnlock()
	for _, cb := range cbs {
		go cb(err)
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
