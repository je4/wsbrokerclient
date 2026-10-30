package wschat

import (
	"context"
	"fmt"
	"time"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/je4/wsbrokerclient"
	"github.com/je4/wsbrokerclient/pkg/client"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// ChatClient manages the integration between the WSBroker client library and the
// local in-memory MessageStore. It demonstrates production-ready usage of wsbrokerclient,
// including mTLS authentication, automatic reconnection with backoff, lifecycle hooks,
// dynamic channel attachment, message transmission, and inbound event routing.
type ChatClient struct {
	config *WSChatConfig
	store  *MessageStore
	client *client.Client
	logger zLogger.ZLogger
}

// NewChatClient constructs and configures a new ChatClient instance.
//
// Relevant wsbrokerclient concepts demonstrated here:
//   - Initializing client configuration using wsbrokerclient.DefaultConfig()
//   - Configuring the broker endpoint, unique client identifier, and initial subscribed channels
//   - Enabling resilient auto-reconnection with exponential backoff (ReconnectInterval, MaxReconnectInterval)
//   - Configuring mTLS client certificate authentication via go.ub.unibas.ch/cloud/certloader/v2
//   - Registering connection lifecycle callbacks (OnConnect, OnDisconnect, OnReconnect, OnError)
//   - Registering incoming message event listeners (OnMessage)
func NewChatClient(conf *WSChatConfig, store *MessageStore, logger zLogger.ZLogger) (*ChatClient, error) {
	// 1. Initialize client configuration with recommended defaults
	cfg := wsbrokerclient.DefaultConfig()
	cfg.Endpoint = conf.WSBroker
	cfg.ClientName = conf.Name
	cfg.InitialChannels = conf.Channels

	// Enable automatic reconnection on network drops or broker restarts
	cfg.AutoReconnect = true
	cfg.ReconnectInterval = 1 * time.Second
	cfg.MaxReconnectInterval = 10 * time.Second
	cfg.Logger = logger

	// 2. Configure mTLS via certloader if TLS parameters are specified.
	// certloader supports MiniVault PKI, local certificate files, environment variables,
	// development self-signed certs, or ACME certificates with dynamic hot-reloading.
	if conf.ClientTLS.Type == "" {
		return nil, errors.New("missing TLS type")
	}
	cfg.TLSCertLoader = &conf.ClientTLS

	// 3. Create the underlying wsbrokerclient instance
	wsClient, err := wsbrokerclient.New(cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create wsbroker client")
	}

	c := &ChatClient{
		config: conf,
		store:  store,
		client: wsClient,
		logger: logger,
	}

	// 4. Register lifecycle listeners to keep the application UI and store state in sync.
	// OnConnect is triggered when the initial WebSocket handshake succeeds.
	c.client.OnConnect(func() {
		if c.logger != nil {
			c.logger.Info().Str("client", conf.Name).Msg("connected to wsbroker")
		}
		c.store.SetStatus("Connected")
	})

	// OnDisconnect is triggered immediately when the connection is lost.
	c.client.OnDisconnect(func(err error) {
		if c.logger != nil {
			c.logger.Warn().Err(err).Msg("disconnected from wsbroker")
		}
		c.store.SetStatus("Disconnected")
	})

	// OnReconnect is triggered after a lost connection has been successfully restored.
	// wsbrokerclient automatically re-attaches to all previously subscribed channels.
	c.client.OnReconnect(func() {
		if c.logger != nil {
			c.logger.Info().Str("client", conf.Name).Msg("reconnected to wsbroker")
		}
		c.store.SetStatus("Connected")
	})

	// OnError is invoked when an error occurs during communication or reconnect attempts.
	c.client.OnError(func(err error) {
		if c.logger != nil {
			c.logger.Error().Err(err).Msg("wsbroker client error")
		}
		if c.client.GetState() == wsbrokerclient.StateReconnecting {
			c.store.SetStatus("Reconnecting")
		}
	})

	// 5. Register central message event listener.
	// Inbound messages from all subscribed channels will be routed to this callback.
	c.client.OnMessage(func(evt *event.RawEvent) {
		c.handleIncomingEvent(evt)
	})

	return c, nil
}

// Start initiates the WebSocket connection to the broker asynchronously.
// If AutoReconnect is true, initial handshake failures or transient network errors
// will trigger background retry attempts without crashing the application.
func (c *ChatClient) Start(ctx context.Context) error {
	c.store.SetStatus("Connecting")
	if err := c.client.Connect(ctx); err != nil {
		c.store.SetStatus("Disconnected")
		return errors.Wrap(err, "failed to start wsbroker client connection")
	}
	return nil
}

// SendMessage publishes a chat message to the specified broker channel.
// It also records the message immediately in the local store to provide instant
// UI feedback for the sender (optimistic update) without waiting for broker reflection.
func (c *ChatClient) SendMessage(ctx context.Context, channel string, content string) error {
	if channel == "" {
		return errors.New("cannot send message to empty channel")
	}
	if content == "" {
		return errors.New("cannot send empty message")
	}

	// Publish message payload to the channel via wsbrokerclient
	if err := c.client.SendMessage(ctx, channel, content); err != nil {
		return errors.Wrapf(err, "failed to send message to channel %s", channel)
	}

	// Optimistically append local message to store so the sender sees it immediately
	msgID := fmt.Sprintf("%d-%s", time.Now().UnixNano(), c.config.Name)
	c.store.AddMessage(ChatMessage{
		ID:        msgID,
		Sender:    c.config.Name,
		Channel:   channel,
		Content:   content,
		Timestamp: time.Now(),
		IsSelf:    true,
	})

	return nil
}

// AttachChannel dynamically subscribes to a new channel at runtime.
// The channel is registered in the local store and attached on the broker.
// wsbrokerclient remembers this channel in its internal subscription set and will
// automatically re-subscribe to it if a reconnect occurs.
func (c *ChatClient) AttachChannel(ctx context.Context, channel string) error {
	c.store.AddChannel(channel)
	if err := c.client.Attach(ctx, channel); err != nil {
		return errors.Wrapf(err, "failed to attach to channel %s", channel)
	}
	return nil
}

// handleIncomingEvent processes inbound event messages received from wsbrokerclient.
// It extracts message payload, target channel, and sender information, while filtering
// out self-sent echoes to prevent duplicate messages in the UI.
func (c *ChatClient) handleIncomingEvent(evt *event.RawEvent) {
	if evt == nil {
		return
	}
	// Determine channel: prefer explicit evt.Channel, fallback to evt.Target, then evt.Source
	channel := evt.Channel
	if channel == "" {
		channel = evt.Target
	}
	if channel == "" {
		channel = evt.Source
	}

	// Extract string payload safely
	content, err := evt.GetStringData()
	if err != nil {
		content = fmt.Sprintf("%v", evt.Data)
	}

	// Skip broker echo for self-sent messages since SendMessage already stored it locally
	isSelf := (evt.Source == c.config.Name)
	if isSelf {
		return
	}

	msgID := evt.ID
	if msgID == "" {
		msgID = fmt.Sprintf("%d-%s", time.Now().UnixNano(), evt.Source)
	}
	c.store.AddMessage(ChatMessage{
		ID:        msgID,
		RefID:     evt.RefID,
		Sender:    evt.Source,
		Channel:   channel,
		Content:   content,
		Timestamp: time.Now(),
		IsSelf:    isSelf,
	})
}

// Close gracefully terminates the WebSocket connection and releases network resources.
func (c *ChatClient) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}
