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

// Request0 sends an event envelope without waiting for any response.
func (c *Client) Request0(ctx context.Context, evt *event.Event) error {
	return c.RequestN(ctx, evt, 0, nil)
}

// Request sends an event envelope and waits synchronously for a single correlated response using RequestN.
func (c *Client) Request(ctx context.Context, evt *event.Event) (*event.Event, error) {
	ch := make(chan *event.Event, 1)
	if err := c.RequestN(ctx, evt, 1, ch); err != nil {
		return nil, errors.Wrap(err, "failed to send request")
	}
	return <-ch, nil
}

// Request1 sends an event envelope and waits synchronously for a single correlated response (alias for Request).
func (c *Client) Request1(ctx context.Context, evt *event.Event) (*event.Event, error) {
	return c.Request(ctx, evt)
}

// RequestN sends an event envelope with an event ID, waits to collect up to numResults
// correlated responses (matching RefID equal to evt.ID) or until ctx cancels/times out,
// and streams the received response events into ch.
func (c *Client) RequestN(ctx context.Context, evt *event.Event, numResults int, ch chan *event.Event) error {
	if c.closed.Load() {
		return errors.New("client is closed")
	}
	if evt == nil {
		return errors.New("cannot send nil request event")
	}
	if ch == nil && numResults > 0 {
		return errors.New("channel cannot be nil")
	}

	if evt.ID == "" {
		evt.ID = event.GenerateEventID()
	}

	if evt.Source == "" {
		evt.Source = c.clientName
	}

	if numResults <= 0 {
		return c.Send(ctx, evt)
	}

	waiterCh := make(chan *event.Event, numResults)
	c.registerWaiterChannel(evt.ID, waiterCh)
	defer c.unregisterWaiter(evt.ID)

	if err := c.Send(ctx, evt); err != nil {
		return errors.Wrap(err, "failed to send request event")
	}

	for range numResults {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "request canceled or timed out waiting for responses")
		case <-c.ctx.Done():
			return errors.New("client closed while waiting for responses")
		case resp, ok := <-waiterCh:
			if !ok || resp == nil {
				return errors.New("response channel closed")
			}
			select {
			case ch <- resp:
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "request canceled or timed out delivering response")
			case <-c.ctx.Done():
				return errors.New("client closed while delivering response")
			}
		}
	}

	return nil
}
