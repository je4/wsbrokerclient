package client

import (
	"sync"

	"github.com/je4/wsbrokerclient/pkg/event"
)

type subscription struct {
	channel string
	events  chan *event.Event
	client  *Client
	once    sync.Once
	closed  bool
	mu      sync.RWMutex
}

func newSubscription(channel string, bufferSize int, c *Client) *subscription {
	if bufferSize <= 0 {
		bufferSize = 100
	}
	return &subscription{
		channel: channel,
		events:  make(chan *event.Event, bufferSize),
		client:  c,
	}
}

// Channel returns the subscribed channel name.
func (s *subscription) Channel() string {
	return s.channel
}

// Events returns the receive-only channel carrying incoming events.
func (s *subscription) Events() <-chan *event.Event {
	return s.events
}

// Unsubscribe stops the subscription and removes it from the client.
func (s *subscription) Unsubscribe() error {
	var err error
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()

		if s.client != nil {
			s.client.removeSubscription(s)
		}
		close(s.events)
	})
	return err
}

func (s *subscription) deliver(evt *event.Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return
	}
	select {
	case s.events <- evt:
	default:
		// Drop or notify overflow if buffer is full to prevent deadlocking the reader loop
		if s.client != nil && s.client.logger != nil {
			s.client.logger.Warn().Msgf("subscription for channel %s buffer full, dropping event %s", s.channel, evt.String())
		}
	}
}
