package event

import (
	"encoding/json/jsontext"
	"encoding/json/v2"

	"emperror.dev/errors"
)

// NewEvent constructs a new Event instance by JSON-serializing the given DataInterface.
func NewEvent(data DataInterface, target string, token string) (*Event, error) {
	if data == nil {
		return nil, errors.New("event data cannot be nil")
	}
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   data.Type(),
		Target: target,
		Token:  token,
		Data:   jsonStr,
	}, nil
}

// NewMessageEvent creates an Event of type TypeMessage carrying arbitrary data as JSON payload.
func NewMessageEvent(source, target string, data any) (*Event, error) {
	raw, err := marshalPayload(data)
	if err != nil {
		return nil, err
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeMessage,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
}

// NewFileEvent creates an Event of type TypeFile carrying a FileMessage payload.
func NewFileEvent(source, target string, file *FileMessage, token ...string) (*Event, error) {
	if file == nil {
		return nil, errors.New("file message cannot be nil")
	}
	var tok string
	if len(token) > 0 {
		tok = token[0]
	}
	raw, err := json.Marshal(file)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal file event data: %v", file.Filename)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeFile,
		Source: source,
		Target: target,
		Token:  tok,
		Data:   raw,
	}, nil
}

// NewAttachEvent creates an Event of type TypeAttach to subscribe to a channel.
func NewAttachEvent(source, channel string) (*Event, error) {
	raw, err := json.Marshal(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal attach channel: %s", channel)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeAttach,
		Source: source,
		Target: channel,
		Data:   raw,
	}, nil
}

// NewDetachEvent creates an Event of type TypeDetach to unsubscribe from a channel.
func NewDetachEvent(source, channel string) (*Event, error) {
	raw, err := json.Marshal(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal detach channel: %s", channel)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeDetach,
		Source: source,
		Target: channel,
		Data:   raw,
	}, nil
}

// NewPingEvent creates a ping heartbeat Event.
func NewPingEvent(source string, data ...any) (*Event, error) {
	var raw jsontext.Value
	if len(data) > 0 && data[0] != nil {
		var err error
		raw, err = marshalPayload(data[0])
		if err != nil {
			return nil, err
		}
	} else {
		raw = jsontext.Value(`"ping"`)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypePing,
		Source: source,
		Data:   raw,
	}, nil
}

// NewPongEvent creates a pong heartbeat response Event.
// If an optional RefID string is passed as a subsequent argument, it is assigned to RefID.
func NewPongEvent(source, target string, data ...any) (*Event, error) {
	var raw jsontext.Value
	if len(data) > 0 && data[0] != nil {
		var err error
		raw, err = marshalPayload(data[0])
		if err != nil {
			return nil, err
		}
	} else {
		raw = jsontext.Value(`"pong"`)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypePong,
		Source: source,
		Target: target,
		Data:   raw,
	}
	if len(data) > 1 {
		if refStr, ok := data[1].(string); ok {
			evt.RefID = refStr
		}
	}
	return evt, nil
}

// NewNTPQueryEvent creates an Event of type TypeNTPQuery with raw NTP packet bytes.
func NewNTPQueryEvent(source string, data []byte) (*Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP query data")
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeNTPQuery,
		Source: source,
		Data:   raw,
	}, nil
}

// NewNTPResponseEvent creates an Event of type TypeNTPResponse with raw NTP response bytes.
// An optional refID can be passed to correlate with the query event ID.
func NewNTPResponseEvent(source, target string, data []byte, refID ...string) (*Event, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP response data")
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeNTPResponse,
		Source: source,
		Target: target,
		Data:   raw,
	}
	if len(refID) > 0 {
		evt.RefID = refID[0]
	}
	return evt, nil
}

// NewNTPErrorEvent creates an Event of type TypeNTPError with an error message string.
// An optional refID can be passed to correlate with the query event ID.
func NewNTPErrorEvent(source, target, errMsg string, refID ...string) (*Event, error) {
	raw, err := json.Marshal(errMsg)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP error message: %s", errMsg)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeNTPError,
		Source: source,
		Target: target,
		Data:   raw,
	}
	if len(refID) > 0 {
		evt.RefID = refID[0]
	}
	return evt, nil
}

// NewChannelInfoQueryEvent creates an Event of type TypeChannelInfoQuery for querying channel metadata and members.
// The target channel name can be passed as target (and is also stored as string data payload).
func NewChannelInfoQueryEvent(source, target string) (*Event, error) {
	raw, err := json.Marshal(target)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info query target: %s", target)
	}
	return &Event{
		ID:     GenerateEventID(),
		Type:   TypeChannelInfoQuery,
		Source: source,
		Target: target,
		Data:   raw,
	}, nil
}

// NewChannelInfoResponseEvent creates an Event of type TypeChannelInfoResponse containing ChannelInfo.
// Optional source and refID strings can be provided.
func NewChannelInfoResponseEvent(target string, info ChannelInfo, source ...string) (*Event, error) {
	src := "wsbroker"
	if len(source) > 0 && source[0] != "" {
		src = source[0]
	}
	raw, err := json.Marshal(info)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info: %v", info)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeChannelInfoResponse,
		Source: src,
		Target: target,
		Data:   raw,
	}
	if len(source) > 1 {
		evt.RefID = source[1]
	}
	return evt, nil
}

// NewChannelInfoErrorEvent creates an Event of type TypeChannelInfoError with an error message string.
// Optional source and refID strings can be provided.
func NewChannelInfoErrorEvent(target, errMsg string, source ...string) (*Event, error) {
	src := "wsbroker"
	if len(source) > 0 && source[0] != "" {
		src = source[0]
	}
	raw, err := json.Marshal(errMsg)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info error message: %s", errMsg)
	}
	evt := &Event{
		ID:     GenerateEventID(),
		Type:   TypeChannelInfoError,
		Source: src,
		Target: target,
		Data:   raw,
	}
	if len(source) > 1 {
		evt.RefID = source[1]
	}
	return evt, nil
}

// NewReplyEvent creates a response Event correlating to original with the given replyType and data.
func NewReplyEvent(original *Event, replyType EventType, data any) (*Event, error) {
	if original == nil {
		return nil, errors.New("original event cannot be nil")
	}
	return original.CreateReply(replyType, data)
}
