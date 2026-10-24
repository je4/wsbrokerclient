package event

// EventType represents the category of event sent across WebSocket channels.
type EventType string

const (
	// TypeAttach is sent by a client to subscribe to one or more channels.
	TypeAttach EventType = "attach"
	// TypeDetach is sent by a client to unsubscribe from one or more channels.
	TypeDetach EventType = "detach"
	// TypeMessage represents a standard message carrying a data payload.
	TypeMessage EventType = "message"
	// TypeStringMessage is an alias for TypeMessage for string payloads.
	TypeStringMessage EventType = "message"
	// TypeFile represents a binary file payload with attached metadata.
	TypeFile EventType = "file"
	// TypePing represents a ping heartbeat or health-check event.
	TypePing EventType = "ping"
	// TypePong represents a pong heartbeat response event.
	TypePong EventType = "pong"
	// TypeNTPQuery is sent by a client to query NTP time via raw 48-byte UDP packet.
	TypeNTPQuery EventType = "ntp-query"
	// TypeNTPResponse is sent by the server with the raw NTP response packet.
	TypeNTPResponse EventType = "ntp-response"
	// TypeNTPError is sent by the server if NTP querying fails.
	TypeNTPError EventType = "ntp-error"
	// TypeChannelInfoQuery is sent by a client to query metadata and subscriber members of a channel.
	TypeChannelInfoQuery EventType = "channel-info-query"
	// TypeChannelInfoResponse is sent by the server with the ChannelInfo data.
	TypeChannelInfoResponse EventType = "channel-info-response"
	// TypeChannelInfoError is sent by the server if channel-info querying fails or is unauthorized.
	TypeChannelInfoError EventType = "channel-info-error"
)

// DataInterface provides a contract for typed data objects that can supply their event type and string representation.
type DataInterface interface {
	String() string
	Type() EventType
}
