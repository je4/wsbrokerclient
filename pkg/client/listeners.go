package client

import (
	"github.com/je4/wsbrokerclient/pkg/event"
)

type listenerEntry struct {
	id uint64
	h  EventHandler
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
