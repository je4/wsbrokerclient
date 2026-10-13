// Package client provides a robust, mTLS-capable WebSocket client for WSBroker
// with automatic reconnection, dynamic channel subscription tracking, and event routing.
package client

import (
	"strings"

	"emperror.dev/errors"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// ConnectionMode specifies the access permission mode of a WebSocket connection.
type ConnectionMode string

const (
	// ModeRW allows both reading (subscribing to events) and writing (publishing events).
	ModeRW ConnectionMode = "rw"
	// ModeRO allows only reading (subscribing to events), rejecting publish attempts.
	ModeRO ConnectionMode = "ro"
	// ModeWO allows only writing (publishing events), bypassing all outgoing broadcast deliveries.
	ModeWO ConnectionMode = "wo"
)

// ParseConnectionMode parses and validates a string into a ConnectionMode.
func ParseConnectionMode(s string) (ConnectionMode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "rw", "readwrite":
		return ModeRW, nil
	case "ro", "readonly":
		return ModeRO, nil
	case "wo", "w", "writeonly":
		return ModeWO, nil
	default:
		return "", errors.Errorf("invalid or missing connection mode: %s", s)
	}
}

// CanRead returns true if the connection mode allows receiving messages/broadcasts.
func (m ConnectionMode) CanRead() bool {
	return m != ModeWO
}

// CanWrite returns true if the connection mode allows publishing messages.
func (m ConnectionMode) CanWrite() bool {
	return m != ModeRO
}

// ConnectionState represents the current lifecycle state of the WebSocket client.
type ConnectionState string

const (
	// StateDisconnected indicates the client is currently not connected.
	StateDisconnected ConnectionState = "disconnected"
	// StateConnecting indicates a connection attempt is actively in progress.
	StateConnecting ConnectionState = "connecting"
	// StateConnected indicates the WebSocket connection is open and active.
	StateConnected ConnectionState = "connected"
	// StateReconnecting indicates the client is attempting to re-establish a dropped connection.
	StateReconnecting ConnectionState = "reconnecting"
	// StateClosed indicates the client has been permanently closed via Close().
	StateClosed ConnectionState = "closed"
)

// EventHandler is a callback function invoked when a matching event is received.
type EventHandler func(evt *event.Event)

// UnsubscribeFunc is a function that removes a registered listener or subscription when called.
type UnsubscribeFunc func()

// Subscription represents an active channel subscription stream.
type Subscription interface {
	// Channel returns the subscribed channel name.
	Channel() string
	// Events returns the receive-only channel carrying incoming events.
	Events() <-chan *event.Event
	// Unsubscribe stops the subscription and releases resources.
	Unsubscribe() error
}
