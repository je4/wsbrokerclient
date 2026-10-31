package client

import (
	"context"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/wsbrokerclient/pkg/event"
)

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

		var evt event.RawEvent
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

			pingEvt, _ := event.NewRawEvent(event.TypePing, c.clientName, "", "ping")
			pCtx, pCancel := context.WithTimeout(c.ctx, c.config.WriteTimeout)
			_ = c.writeEvent(pCtx, currentConn, pingEvt)
			pCancel()
		}
	}
}

func (c *Client) normalizeIncomingEvent(evt *event.RawEvent) {
	if evt == nil {
		return
	}

	// 1. If Target is empty, the recipient is this client
	if evt.Target == "" {
		evt.Target = c.clientName
	}

	// 2. Reverse check: if Channel is empty and Target is a known subscribed channel
	if evt.Channel == "" && evt.Target != "" {
		c.channelsMu.RLock()
		if _, exists := c.channels[evt.Target]; exists {
			evt.Channel = evt.Target
		}
		c.channelsMu.RUnlock()
	}
}

func (c *Client) handleIncomingEvent(evt *event.RawEvent) {
	c.normalizeIncomingEvent(evt)

	if evt.Channel != "" && evt.ID != "" {
		c.logger.Debug().Msgf("received event %s [%s -> %s (channel: %s, id: %s)]", evt.Type, evt.Source, evt.Target, evt.Channel, evt.ID)
	} else if evt.Channel != "" {
		c.logger.Debug().Msgf("received event %s [%s -> %s (channel: %s)]", evt.Type, evt.Source, evt.Target, evt.Channel)
	} else if evt.ID != "" {
		c.logger.Debug().Msgf("received event %s [%s -> %s (id: %s)]", evt.Type, evt.Source, evt.Target, evt.ID)
	} else {
		c.logger.Debug().Msgf("received event %s [%s -> %s]", evt.Type, evt.Source, evt.Target)
	}

	// Automatic heartbeat response
	if evt.Type == event.TypePing {
		pongTarget := evt.Source
		if pongTarget == "" {
			pongTarget = "wsbroker"
		}
		pongData := any(evt.Data)
		if len(evt.Data) == 0 {
			pongData = "pong"
		}
		pongEvt, _ := event.NewRawEvent(event.TypePong, c.clientName, pongTarget, pongData)
		pongEvt.RefID = evt.ID
		go func() {
			pCtx, pCancel := context.WithTimeout(c.ctx, c.config.WriteTimeout)
			defer pCancel()
			_ = c.Send(pCtx, pongEvt)
		}()
		return
	}

	// Dispatch to correlation session / RefID waiters
	if evt.Session != "" {
		c.waitersMu.RLock()
		ch, found := c.waiters[evt.Session]
		c.waitersMu.RUnlock()
		if found {
			select {
			case ch <- evt:
			default:
				c.logger.Warn().Str("session", evt.Session).Msg("waiter channel blocked or full")
			}
		}
	}
	if evt.RefID != "" {
		c.waitersMu.RLock()
		ch, found := c.waiters[evt.RefID]
		c.waitersMu.RUnlock()
		if found {
			select {
			case ch <- evt:
			default:
				c.logger.Warn().Str("refID", evt.RefID).Msg("waiter channel blocked or full")
			}
		}
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
	seenListener := make(map[uint64]struct{})
	addChanListeners := func(ch string) {
		if ch == "" {
			return
		}
		for _, entry := range c.chanListeners[ch] {
			if _, exists := seenListener[entry.id]; !exists {
				seenListener[entry.id] = struct{}{}
				byChan = append(byChan, entry.h)
			}
		}
	}
	if evt.Channel != "" {
		addChanListeners(evt.Channel)
	}
	if evt.Target != "" {
		addChanListeners(evt.Target)
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
	var subs []*subscription
	seenSub := make(map[*subscription]struct{})
	addSubscriptions := func(ch string) {
		if ch == "" {
			return
		}
		for _, sub := range c.subscriptions[ch] {
			if _, exists := seenSub[sub]; !exists {
				seenSub[sub] = struct{}{}
				subs = append(subs, sub)
			}
		}
	}
	if evt.Channel != "" {
		addSubscriptions(evt.Channel)
	}
	if evt.Target != "" {
		addSubscriptions(evt.Target)
	}
	c.subMu.RUnlock()

	for _, sub := range subs {
		sub.deliver(evt)
	}
}

func (c *Client) invokeHandler(h EventHandler, evt *event.RawEvent) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				c.logger.Error().Msgf("panic in event handler: %v", r)
			}
		}()
		h(evt)
	}()
}
