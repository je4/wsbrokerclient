package client

import (
	"context"
	"strings"
	"time"

	"emperror.dev/errors"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/wsbrokerclient/pkg/event"
)

func (c *Client) writeEvent(ctx context.Context, conn *websocket.Conn, evt *event.RawEvent) error {
	if conn == nil {
		return errors.New("not connected")
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return wsjson.Write(ctx, conn, evt)
}

// Send writes an event envelope to the WebSocket connection.
func (c *Client) Send(ctx context.Context, evt *event.RawEvent) error {
	if c.closed.Load() {
		return errors.New("client is closed")
	}
	if evt == nil {
		return errors.New("event is nil")
	}
	if evt.ID == "" {
		evt.ID = event.GenerateEventID()
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

// Reply sends a correlated TypeMessage response event to originalEvt carrying data payload.
func (c *Client) Reply(ctx context.Context, originalEvt *event.RawEvent, data any) error {
	return c.ReplyType(ctx, originalEvt, event.TypeMessage, data)
}

// ReplyType sends a correlated response event of the specified replyType to originalEvt.
func (c *Client) ReplyType(ctx context.Context, originalEvt *event.RawEvent, replyType event.EventType, data any) error {
	if originalEvt == nil {
		return errors.New("cannot reply to nil event")
	}
	replyEvt, err := originalEvt.CreateReply(replyType, data)
	if err != nil {
		return err
	}
	replyEvt.Source = c.clientName
	return c.Send(ctx, replyEvt)
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
			evt, err := event.NewRawEvent(event.TypeAttach, c.clientName, ch, ch)
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
			evt, err := event.NewRawEvent(event.TypeDetach, c.clientName, ch, ch)
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
	evt, err := event.NewRawEvent(event.TypePing, c.clientName, "", "ping")
	if err != nil {
		return err
	}
	return c.Send(ctx, evt)
}
