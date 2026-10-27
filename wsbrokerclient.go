// Package wsbrokerclient provides a high-performance, mTLS-enabled Go client library
// for WSBroker, featuring automatic reconnection, dynamic channel subscriptions,
// point-to-point and broadcast messaging, and RESTful management APIs.
package wsbrokerclient

import (
	"github.com/beevik/ntp"
	"github.com/je4/wsbrokerclient/pkg/client"
	"github.com/je4/wsbrokerclient/pkg/event"
	"github.com/je4/wsbrokerclient/pkg/restclient"
)

// Type aliases for convenient top-level access.
type (
	// Client is the WebSocket client for WSBroker.
	Client = client.Client

	// ClientInterface defines the standard interface for communicating with a WSBroker server.
	ClientInterface = client.ClientInterface

	// BrokerClient is an alias for ClientInterface for compatibility with downstream controllers and clients.
	BrokerClient = client.BrokerClient

	// Config defines client configuration options.
	Config = client.Config

	// Option modifies client configuration.
	Option = client.Option

	// ConnectionState represents the state of the connection.
	ConnectionState = client.ConnectionState

	// ConnectionMode specifies the access permission mode of a WebSocket connection.
	ConnectionMode = client.ConnectionMode

	// EventHandler is a callback invoked when an event is received.
	EventHandler = client.EventHandler

	// UnsubscribeFunc stops a subscription or listener.
	UnsubscribeFunc = client.UnsubscribeFunc

	// Subscription represents an active channel subscription stream.
	Subscription = client.Subscription

	// Event is the standardized message envelope.
	Event = event.Event

	// EventType is the category of message.
	EventType = event.EventType

	// WSMessage represents a logged WebSocket message event for telemetry and debugging.
	WSMessage = event.WSMessage

	// NTPResponse is the NTP query response from github.com/beevik/ntp.
	NTPResponse = ntp.Response

	// NTPQueryOptions holds options for querying NTP time.
	NTPQueryOptions = ntp.QueryOptions

	// RESTClient is the HTTP client for WSBroker REST APIs.
	RESTClient = restclient.RESTClient

	// RESTConfig defines configuration for RESTClient.
	RESTConfig = restclient.Config

	// RESTOption modifies RESTClient configuration.
	RESTOption = restclient.Option

	// ChannelInfo holds channel subscriber and client details.
	ChannelInfo = event.ChannelInfo

	// FileMessage represents a file attachment payload.
	FileMessage = event.FileMessage

	// PublishResponse is returned after a REST publish.
	PublishResponse = restclient.PublishResponse
)

// Standard EventType constants.
const (
	TypeAttach              = event.TypeAttach
	TypeDetach              = event.TypeDetach
	TypeMessage             = event.TypeMessage
	TypeStringMessage       = event.TypeStringMessage
	TypeFile                = event.TypeFile
	TypePing                = event.TypePing
	TypePong                = event.TypePong
	TypeNTPQuery            = event.TypeNTPQuery
	TypeNTPResponse         = event.TypeNTPResponse
	TypeNTPError            = event.TypeNTPError
	TypeChannelInfoQuery    = event.TypeChannelInfoQuery
	TypeChannelInfoResponse = event.TypeChannelInfoResponse
	TypeChannelInfoError    = event.TypeChannelInfoError
)

// Standard ConnectionState constants.
const (
	StateDisconnected = client.StateDisconnected
	StateConnecting   = client.StateConnecting
	StateConnected    = client.StateConnected
	StateReconnecting = client.StateReconnecting
	StateClosed       = client.StateClosed
)

// Standard ConnectionMode constants.
const (
	ModeRW = client.ModeRW
	ModeRO = client.ModeRO
	ModeWO = client.ModeWO
)

// ParseConnectionMode parses and validates a string into a ConnectionMode.
func ParseConnectionMode(s string) (ConnectionMode, error) {
	return client.ParseConnectionMode(s)
}

// New creates a new WSBroker WebSocket client.
func New(cfg Config, opts ...Option) (*Client, error) {
	return client.New(cfg, opts...)
}

// DefaultConfig returns default client configuration.
func DefaultConfig() Config {
	return client.DefaultConfig()
}

// DefaultRESTConfig returns default REST client configuration.
func DefaultRESTConfig() RESTConfig {
	return restclient.DefaultConfig()
}

// NewRESTClient creates a new WSBroker REST client.
func NewRESTClient(cfg RESTConfig, opts ...RESTOption) (*RESTClient, error) {
	return restclient.New(cfg, opts...)
}

// Functional options re-exported from client package.
var (
	WithEndpoint          = client.WithEndpoint
	WithClientName        = client.WithClientName
	WithInitialChannels   = client.WithInitialChannels
	WithMode              = client.WithMode
	WithBearerToken       = client.WithBearerToken
	WithTLSConfig         = client.WithTLSConfig
	WithTLSCertLoader     = client.WithTLSCertLoader
	WithAutoReconnect     = client.WithAutoReconnect
	WithReconnectStrategy = client.WithReconnectStrategy
	WithPingInterval      = client.WithPingInterval
	WithReadLimit         = client.WithReadLimit
	WithConnectTimeout    = client.WithConnectTimeout
	WithWriteTimeout      = client.WithWriteTimeout
	WithLogger            = client.WithLogger
)

// Event constructors re-exported from event package.
var (
	NewEvent                    = event.NewEvent
	NewMessageEvent             = event.NewMessageEvent
	NewFileEvent                = event.NewFileEvent
	NewFileMessage              = event.NewFileMessage
	NewAttachEvent              = event.NewAttachEvent
	NewDetachEvent              = event.NewDetachEvent
	NewPingEvent                = event.NewPingEvent
	NewPongEvent                = event.NewPongEvent
	NewGenericStringMessage     = event.NewGenericStringMessage
	NewNTPQueryEvent            = event.NewNTPQueryEvent
	NewNTPResponseEvent         = event.NewNTPResponseEvent
	NewNTPErrorEvent            = event.NewNTPErrorEvent
	NewChannelInfoQueryEvent    = event.NewChannelInfoQueryEvent
	NewChannelInfoResponseEvent = event.NewChannelInfoResponseEvent
	NewChannelInfoErrorEvent    = event.NewChannelInfoErrorEvent
	NewReplyEvent               = event.NewReplyEvent
	NewWSMessage                = event.NewWSMessage
	GenerateEventID             = event.GenerateEventID
	GenerateToken               = client.GenerateToken
)
