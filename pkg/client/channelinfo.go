package client

import (
	"context"
	"strings"

	"emperror.dev/errors"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// QueryChannelInfo queries metadata and subscriber client names for a given channel over the WebSocket connection.
// It sends a TypeChannelInfoQuery event and waits for a TypeChannelInfoResponse or TypeChannelInfoError event.
func (c *Client) QueryChannelInfo(ctx context.Context, channel string) (*event.ChannelInfo, error) {
	channel = strings.TrimSpace(channel)
	if channel == "" {
		return nil, errors.New("channel name cannot be empty")
	}

	if !c.IsConnected() {
		return nil, errors.New("client is not connected")
	}

	evt, err := event.NewRawEvent(event.TypeChannelInfoQuery, c.clientName, channel, channel)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create channel info query event")
	}
	if evt.ID == "" {
		evt.ID = event.GenerateEventID()
	}

	respCh := make(chan *event.RawEvent, 1)

	unsubResp := c.OnType(event.TypeChannelInfoResponse, func(e *event.RawEvent) {
		if e.RefID != "" {
			if e.RefID == evt.ID {
				select {
				case respCh <- e:
				default:
				}
			}
			return
		}
		var info event.ChannelInfo
		if err := e.UnmarshalData(&info); err == nil {
			if info.Name == channel {
				select {
				case respCh <- e:
				default:
				}
			}
		}
	})
	defer unsubResp()

	unsubErr := c.OnType(event.TypeChannelInfoError, func(e *event.RawEvent) {
		if e.RefID != "" {
			if e.RefID == evt.ID {
				select {
				case respCh <- e:
				default:
				}
			}
			return
		}
		select {
		case respCh <- e:
		default:
		}
	})
	defer unsubErr()

	if err := c.Send(ctx, evt); err != nil {
		return nil, errors.Wrap(err, "failed to send channel info query event")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errors.New("client closed")
	case resp := <-respCh:
		if resp.Type == event.TypeChannelInfoError {
			var errMsg string
			if err := resp.UnmarshalData(&errMsg); err == nil {
				return nil, errors.Errorf("channel info error from server: %s", errMsg)
			}
			return nil, errors.New("channel info error from server")
		}
		raw, err := resp.GetData()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get channel info response data")
		}
		ci, ok := raw.(*event.ChannelInfo)
		if !ok {
			return nil, errors.Errorf("unexpected channel info response data type %T", raw)
		}
		return ci, nil
	}
}

// ChannelInfo queries metadata and subscriber client names for a given channel over the WebSocket connection.
// It is an alias for QueryChannelInfo.
func (c *Client) ChannelInfo(ctx context.Context, channel string) (*event.ChannelInfo, error) {
	return c.QueryChannelInfo(ctx, channel)
}
