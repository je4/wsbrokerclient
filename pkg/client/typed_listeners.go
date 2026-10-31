package client

import (
	"context"
	"sync"

	"emperror.dev/errors"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// TypedEventHandler is a callback function invoked with a strongly-typed Event[T].
type TypedEventHandler[T any] func(evt *event.Event[T])

// TypedSubscription represents an active streaming channel subscription producing strongly-typed events.
type TypedSubscription[T any] interface {
	// Channel returns the subscribed channel name.
	Channel() string
	// Events returns the receive-only channel carrying typed incoming events.
	Events() <-chan *event.Event[T]
	// Unsubscribe stops the subscription and releases resources.
	Unsubscribe() error
}

type typedSubscriptionImpl[T any] struct {
	sub    Subscription
	events chan *event.Event[T]
	cancel context.CancelFunc
	once   sync.Once
}

func (s *typedSubscriptionImpl[T]) Channel() string {
	if s.sub == nil {
		return ""
	}
	return s.sub.Channel()
}

func (s *typedSubscriptionImpl[T]) Events() <-chan *event.Event[T] {
	return s.events
}

func (s *typedSubscriptionImpl[T]) Unsubscribe() error {
	var err error
	s.once.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		if s.sub != nil {
			err = s.sub.Unsubscribe()
		}
	})
	return err
}

// OnTyped registers a listener for events of a specific EventType with automatic unmarshaling into Event[T].
// An optional errHandler callback can be provided to handle unmarshaling errors.
func OnTyped[T any](c ClientInterface, eventType event.EventType, handler TypedEventHandler[T], errHandler ...func(raw *event.RawEvent, err error)) UnsubscribeFunc {
	if c == nil || handler == nil {
		return func() {}
	}
	return c.OnType(eventType, func(raw *event.RawEvent) {
		typed, err := event.ToTyped[T](raw, eventType)
		if err != nil {
			if len(errHandler) > 0 && errHandler[0] != nil {
				errHandler[0](raw, err)
			}
			return
		}
		handler(typed)
	})
}

// OnChannelTyped registers a listener for events targeted to a channel with automatic unmarshaling into Event[T].
// An optional errHandler callback can be provided to handle unmarshaling errors.
func OnChannelTyped[T any](c ClientInterface, channel string, handler TypedEventHandler[T], errHandler ...func(raw *event.RawEvent, err error)) UnsubscribeFunc {
	if c == nil || handler == nil {
		return func() {}
	}
	return c.OnChannel(channel, func(raw *event.RawEvent) {
		typed, err := event.ToTyped[T](raw)
		if err != nil {
			if len(errHandler) > 0 && errHandler[0] != nil {
				errHandler[0](raw, err)
			}
			return
		}
		handler(typed)
	})
}

// OnEventTyped registers a global event listener with automatic unmarshaling into Event[T].
// An optional errHandler callback can be provided to handle unmarshaling errors.
func OnEventTyped[T any](c ClientInterface, handler TypedEventHandler[T], errHandler ...func(raw *event.RawEvent, err error)) UnsubscribeFunc {
	if c == nil || handler == nil {
		return func() {}
	}
	return c.OnEvent(func(raw *event.RawEvent) {
		typed, err := event.ToTyped[T](raw)
		if err != nil {
			if len(errHandler) > 0 && errHandler[0] != nil {
				errHandler[0](raw, err)
			}
			return
		}
		handler(typed)
	})
}

// SubscribeTyped returns a streaming TypedSubscription for the given channel, emitting decoded *event.Event[T].
func SubscribeTyped[T any](ctx context.Context, c ClientInterface, channel string, bufferSize ...int) (TypedSubscription[T], error) {
	if c == nil {
		return nil, errors.New("client cannot be nil")
	}
	sub, err := c.Subscribe(ctx, channel)
	if err != nil {
		return nil, err
	}

	bufSize := 100
	if len(bufferSize) > 0 && bufferSize[0] > 0 {
		bufSize = bufferSize[0]
	}

	subCtx, cancel := context.WithCancel(ctx)
	typedSub := &typedSubscriptionImpl[T]{
		sub:    sub,
		events: make(chan *event.Event[T], bufSize),
		cancel: cancel,
	}

	go func() {
		defer close(typedSub.events)
		for {
			select {
			case <-subCtx.Done():
				return
			case rawEvt, ok := <-sub.Events():
				if !ok {
					return
				}
				typed, err := event.ToTyped[T](rawEvt)
				if err != nil {
					continue
				}
				select {
				case typedSub.events <- typed:
				case <-subCtx.Done():
					return
				}
			}
		}
	}()

	return typedSub, nil
}
