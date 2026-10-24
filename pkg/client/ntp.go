package client

import (
	"context"
	"net"
	"time"

	"emperror.dev/errors"
	"github.com/beevik/ntp"
	"github.com/je4/wsbrokerclient/pkg/event"
)

// QueryNTP sends a raw NTP packet (typically 48 bytes) to the server and waits for the response packet.
func (c *Client) QueryNTP(ctx context.Context, data []byte) ([]byte, error) {
	if !c.IsConnected() {
		return nil, errors.New("client is not connected")
	}

	evt, err := event.NewNTPQueryEvent(c.clientName, data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create NTP query event")
	}
	if evt.ID == "" {
		evt.ID = event.GenerateEventID()
	}

	respCh := make(chan *event.Event, 1)

	unsubResp := c.OnType(event.TypeNTPResponse, func(e *event.Event) {
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
	defer unsubResp()

	unsubErr := c.OnType(event.TypeNTPError, func(e *event.Event) {
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
		return nil, errors.Wrap(err, "failed to send NTP query event")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errors.New("client closed")
	case resp := <-respCh:
		if resp.Type == event.TypeNTPError {
			var errMsg string
			if err := resp.UnmarshalData(&errMsg); err == nil {
				return nil, errors.Errorf("NTP error from server: %s", errMsg)
			}
			return nil, errors.New("NTP error from server")
		}
		raw, err := resp.GetData()
		if err != nil {
			return nil, errors.Wrap(err, "failed to get NTP response data")
		}
		respBytes, ok := raw.([]byte)
		if !ok {
			return nil, errors.Errorf("unexpected NTP response data type %T", raw)
		}
		return respBytes, nil
	}
}

// NTP queries NTP time through the wsbroker connection using github.com/beevik/ntp.
// It returns the full *ntp.Response including ClockOffset, RTT, Stratum, Precision, etc.
// Upon success, the client's internal clock offset is updated and can be queried via ClockOffset() or Time().
func (c *Client) NTP(ctx context.Context, opts ...ntp.QueryOptions) (*ntp.Response, error) {
	if !c.IsConnected() {
		return nil, errors.New("client is not connected")
	}

	var opt ntp.QueryOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	if opt.Timeout <= 0 {
		opt.Timeout = 10 * time.Second
	}

	conn := newNTPConn(ctx, c)
	defer conn.Close()

	opt.Dialer = func(localAddress, remoteAddress string) (net.Conn, error) {
		return conn, nil
	}

	response, err := ntp.QueryWithOptions("wsbroker", opt)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query NTP via wsbroker")
	}

	c.clockOffset.Store(int64(response.ClockOffset))
	c.logger.Info().Msgf("NTP clock offset: %s", response.ClockOffset)
	return response, nil
}

// ClockOffset returns the latest measured clock offset relative to the NTP server.
// If NTP has not been queried yet, it returns 0.
func (c *Client) ClockOffset() time.Duration {
	return time.Duration(c.clockOffset.Load())
}

// Time returns the current local time adjusted by the client's latest measured NTP ClockOffset.
func (c *Client) Time() time.Time {
	return time.Now().Add(c.ClockOffset())
}

// SyncNTP performs an NTP query and returns the measured clock offset.
func (c *Client) SyncNTP(ctx context.Context, opts ...ntp.QueryOptions) (time.Duration, error) {
	resp, err := c.NTP(ctx, opts...)
	if err != nil {
		return 0, err
	}
	return resp.ClockOffset, nil
}

// NewNTPConn returns a net.Conn that routes NTP packets over the wsbroker WebSocket connection.
func (c *Client) NewNTPConn(ctx context.Context) net.Conn {
	return newNTPConn(ctx, c)
}
