package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"emperror.dev/errors"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// GenerateToken creates a cryptographically unique 16-byte random hex token.
func GenerateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", errors.Wrap(err, "failed to read secure random bytes")
	}
	return hex.EncodeToString(b), nil
}

// Request sends an event envelope and waits synchronously for a correlated response matching evt.Token or evt.RefID.
// If evt.ID is empty, a unique random ID is automatically generated.
// If evt.Token is empty, a unique random token is automatically generated.
// If evt.Source is empty, it is set to the client's configured name.
func (c *Client) Request(ctx context.Context, evt *event.Event) (*event.Event, error) {
	if c.closed.Load() {
		return nil, errors.New("client is closed")
	}
	if evt == nil {
		return nil, errors.New("cannot send nil request event")
	}
	if evt.ID == "" {
		evt.ID = event.GenerateEventID()
	}
	if evt.Token == "" {
		tok, err := GenerateToken()
		if err != nil {
			return nil, errors.Wrap(err, "failed to generate correlation token")
		}
		evt.Token = tok
	}
	if evt.Source == "" {
		evt.Source = c.clientName
	}

	ch := c.registerWaiter(evt.Token)
	defer c.unregisterWaiter(evt.Token)

	if evt.ID != "" {
		c.registerWaiterChannel(evt.ID, ch)
		defer c.unregisterWaiter(evt.ID)
	}

	if err := c.Send(ctx, evt); err != nil {
		return nil, errors.Wrap(err, "failed to send request event")
	}

	select {
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "request canceled or timed out waiting for response")
	case <-c.ctx.Done():
		return nil, errors.New("client closed while waiting for response")
	case resp, ok := <-ch:
		if !ok || resp == nil {
			return nil, errors.New("response channel closed")
		}
		return resp, nil
	}
}

func (c *Client) registerWaiter(token string) chan *event.Event {
	ch := make(chan *event.Event, 1)
	c.waitersMu.Lock()
	c.waiters[token] = ch
	c.waitersMu.Unlock()
	return ch
}

func (c *Client) registerWaiterChannel(key string, ch chan *event.Event) {
	c.waitersMu.Lock()
	c.waiters[key] = ch
	c.waitersMu.Unlock()
}

func (c *Client) unregisterWaiter(token string) {
	c.waitersMu.Lock()
	delete(c.waiters, token)
	c.waitersMu.Unlock()
}
