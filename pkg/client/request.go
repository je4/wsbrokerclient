package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"

	"emperror.dev/errors"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// GenerateSessionID creates a cryptographically unique 16-byte random hex session identifier.
func GenerateSessionID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", errors.Wrap(err, "failed to read secure random bytes")
	}
	return hex.EncodeToString(b), nil
}

func (c *Client) registerWaiter(key string) chan *event.RawEvent {
	ch := make(chan *event.RawEvent, 1)
	c.waitersMu.Lock()
	c.waiters[key] = ch
	c.waitersMu.Unlock()
	return ch
}

func (c *Client) registerWaiterChannel(key string, ch chan *event.RawEvent) {
	c.waitersMu.Lock()
	c.waiters[key] = ch
	c.waitersMu.Unlock()
}

func (c *Client) unregisterWaiter(key string) {
	c.waitersMu.Lock()
	delete(c.waiters, key)
	c.waitersMu.Unlock()
}

// Request0 sends an event envelope without waiting for any response.
func (c *Client) Request0(ctx context.Context, evt *event.RawEvent) error {
	return c.RequestN(ctx, evt, 0, nil)
}

// Request sends an event envelope and waits synchronously for a single correlated response using RequestN.
func (c *Client) Request(ctx context.Context, evt *event.RawEvent) (*event.RawEvent, error) {
	ch := make(chan *event.RawEvent, 1)
	if err := c.RequestN(ctx, evt, 1, ch); err != nil {
		return nil, errors.Wrap(err, "failed to send request")
	}
	return <-ch, nil
}

// Request1 sends an event envelope and waits synchronously for a single correlated response (alias for Request).
func (c *Client) Request1(ctx context.Context, evt *event.RawEvent) (*event.RawEvent, error) {
	return c.Request(ctx, evt)
}

// RequestN sends an event envelope with an event ID, waits to collect up to numResults
// correlated responses (matching RefID equal to evt.ID) or until ctx cancels/times out,
// and streams the received response events into ch.
func (c *Client) RequestN(ctx context.Context, evt *event.RawEvent, numResults int, ch chan *event.RawEvent) error {
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

	waiterCh := make(chan *event.RawEvent, numResults)
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

// RequestTyped sends a RawEvent request and unmarshals the correlated response payload into Resp.
func RequestTyped[Resp any](ctx context.Context, c ClientInterface, evt *event.RawEvent) (*event.Event[Resp], error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	if evt == nil {
		return nil, errors.New("request event cannot be nil")
	}
	rawResp, err := c.Request(ctx, evt)
	if err != nil {
		return nil, err
	}
	return event.ToTyped[Resp](rawResp)
}

// RequestPayload creates and sends a typed request, returning a strongly-typed response.
func RequestPayload[Resp any, Req any](ctx context.Context, c ClientInterface, target string, reqType event.EventType, payload Req) (*event.Event[Resp], error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	typedReq, err := event.NewEventTyped(reqType, c.ClientName(), target, payload)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create typed request event")
	}
	rawReq, err := event.ToRaw(typedReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert typed request to raw event")
	}
	return RequestTyped[Resp](ctx, c, rawReq)
}
