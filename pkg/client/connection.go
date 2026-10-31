package client

import (
	"context"
	mathrand "math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/coder/websocket"
	"github.com/je4/wsbrokerclient/pkg/event"
)

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

	if c.config.ReadLimit > 0 {
		conn.SetReadLimit(c.config.ReadLimit)
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
		evt, err := event.NewRawEvent(event.TypeAttach, c.clientName, ch, ch)
		if err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(c.ctx, c.config.WriteTimeout)
		_ = c.Send(ctx, evt)
		cancel()
	}
}
