package event

import (
	"encoding/json/jsontext"
	"encoding/json/v2"

	"emperror.dev/errors"
)

// ToTyped converts a RawEvent into a strongly-typed Event[T] by unmarshaling its JSON payload into T.
func ToTyped[T any](raw *RawEvent) (*Event[T], error) {
	if raw == nil {
		return nil, errors.New("cannot convert nil raw event to typed event")
	}
	var data T
	if len(raw.Data) > 0 {
		if err := json.Unmarshal(raw.Data, &data); err != nil {
			return nil, errors.Wrapf(err, "failed to unmarshal raw event payload into %T", data)
		}
	}
	return &Event[T]{
		BaseEvent: raw.BaseEvent,
		Data:      data,
	}, nil
}

// ToRaw converts a typed Event[T] into a RawEvent by JSON-marshaling its payload into jsontext.Value.
func ToRaw[T any](evt *Event[T]) (*RawEvent, error) {
	if evt == nil {
		return nil, errors.New("cannot convert nil typed event to raw event")
	}
	rawBytes, err := marshalPayload(evt.Data)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal typed event payload of type %T", evt.Data)
	}
	return &RawEvent{
		BaseEvent: evt.BaseEvent,
		Data:      rawBytes,
	}, nil
}

// NewEventTyped constructs a strongly-typed Event[T] with generated ID and metadata.
func NewEventTyped[T any](eventType EventType, source, target string, data T) (*Event[T], error) {
	return &Event[T]{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   eventType,
			Source: source,
			Target: target,
		},
		Data: data,
	}, nil
}

// NewEvent constructs a new RawEvent instance by JSON-serializing the given DataInterface.
func NewEvent(data DataInterface, target string, token string) (*RawEvent, error) {
	if data == nil {
		return nil, errors.New("event data cannot be nil")
	}
	jsonStr, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal event data: %v", data)
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   data.Type(),
			Target: target,
			Token:  token,
		},
		Data: jsonStr,
	}, nil
}

// NewMessageEvent creates a RawEvent of type TypeMessage carrying arbitrary data as JSON payload.
func NewMessageEvent(source, target string, data any) (*RawEvent, error) {
	raw, err := marshalPayload(data)
	if err != nil {
		return nil, err
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeMessage,
			Source: source,
			Target: target,
		},
		Data: raw,
	}, nil
}

// NewFileEvent creates a RawEvent of type TypeFile carrying a FileMessage payload.
func NewFileEvent(source, target string, file *FileMessage, token ...string) (*RawEvent, error) {
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
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeFile,
			Source: source,
			Target: target,
			Token:  tok,
		},
		Data: raw,
	}, nil
}

// NewAttachEvent creates a RawEvent of type TypeAttach to subscribe to a channel.
func NewAttachEvent(source, channel string) (*RawEvent, error) {
	raw, err := json.Marshal(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal attach channel: %s", channel)
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeAttach,
			Source: source,
			Target: channel,
		},
		Data: raw,
	}, nil
}

// NewDetachEvent creates a RawEvent of type TypeDetach to unsubscribe from a channel.
func NewDetachEvent(source, channel string) (*RawEvent, error) {
	raw, err := json.Marshal(channel)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal detach channel: %s", channel)
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeDetach,
			Source: source,
			Target: channel,
		},
		Data: raw,
	}, nil
}

// NewPingEvent creates a ping heartbeat RawEvent.
func NewPingEvent(source string, data ...any) (*RawEvent, error) {
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
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypePing,
			Source: source,
		},
		Data: raw,
	}, nil
}

// NewPongEvent creates a pong heartbeat response RawEvent.
// If an optional RefID string is passed as a subsequent argument, it is assigned to RefID.
func NewPongEvent(source, target string, data ...any) (*RawEvent, error) {
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
	evt := &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypePong,
			Source: source,
			Target: target,
		},
		Data: raw,
	}
	if len(data) > 1 {
		if refStr, ok := data[1].(string); ok {
			evt.RefID = refStr
		}
	}
	return evt, nil
}

// NewNTPQueryEvent creates a RawEvent of type TypeNTPQuery with raw NTP packet bytes.
func NewNTPQueryEvent(source string, data []byte) (*RawEvent, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP query data")
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeNTPQuery,
			Source: source,
		},
		Data: raw,
	}, nil
}

// NewNTPResponseEvent creates a RawEvent of type TypeNTPResponse with raw NTP response bytes.
// An optional refID can be passed to correlate with the query event ID.
func NewNTPResponseEvent(source, target string, data []byte, refID ...string) (*RawEvent, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP response data")
	}
	evt := &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeNTPResponse,
			Source: source,
			Target: target,
		},
		Data: raw,
	}
	if len(refID) > 0 {
		evt.RefID = refID[0]
	}
	return evt, nil
}

// NewNTPErrorEvent creates a RawEvent of type TypeNTPError with an error message string.
// An optional refID can be passed to correlate with the query event ID.
func NewNTPErrorEvent(source, target, errMsg string, refID ...string) (*RawEvent, error) {
	raw, err := json.Marshal(errMsg)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal NTP error message: %s", errMsg)
	}
	evt := &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeNTPError,
			Source: source,
			Target: target,
		},
		Data: raw,
	}
	if len(refID) > 0 {
		evt.RefID = refID[0]
	}
	return evt, nil
}

// NewChannelInfoQueryEvent creates a RawEvent of type TypeChannelInfoQuery for querying channel metadata and members.
// The target channel name can be passed as target (and is also stored as string data payload).
func NewChannelInfoQueryEvent(source, target string) (*RawEvent, error) {
	raw, err := json.Marshal(target)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info query target: %s", target)
	}
	return &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeChannelInfoQuery,
			Source: source,
			Target: target,
		},
		Data: raw,
	}, nil
}

// NewChannelInfoResponseEvent creates a RawEvent of type TypeChannelInfoResponse containing ChannelInfo.
// Optional source and refID strings can be provided.
func NewChannelInfoResponseEvent(target string, info ChannelInfo, source ...string) (*RawEvent, error) {
	src := "wsbroker"
	if len(source) > 0 && source[0] != "" {
		src = source[0]
	}
	raw, err := json.Marshal(info)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info: %v", info)
	}
	evt := &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeChannelInfoResponse,
			Source: src,
			Target: target,
		},
		Data: raw,
	}
	if len(source) > 1 {
		evt.RefID = source[1]
	}
	return evt, nil
}

// NewChannelInfoErrorEvent creates a RawEvent of type TypeChannelInfoError with an error message string.
// Optional source and refID strings can be provided.
func NewChannelInfoErrorEvent(target, errMsg string, source ...string) (*RawEvent, error) {
	src := "wsbroker"
	if len(source) > 0 && source[0] != "" {
		src = source[0]
	}
	raw, err := json.Marshal(errMsg)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot marshal channel info error message: %s", errMsg)
	}
	evt := &RawEvent{
		BaseEvent: BaseEvent{
			ID:     GenerateEventID(),
			Type:   TypeChannelInfoError,
			Source: src,
			Target: target,
		},
		Data: raw,
	}
	if len(source) > 1 {
		evt.RefID = source[1]
	}
	return evt, nil
}

// NewReplyEvent creates a response RawEvent correlating to original with the given replyType and data.
func NewReplyEvent(original *RawEvent, replyType EventType, data any) (*RawEvent, error) {
	if original == nil {
		return nil, errors.New("original event cannot be nil")
	}
	return original.CreateReply(replyType, data)
}
