package client

import (
	"context"
	"net"
	"time"

	"emperror.dev/errors"
	"github.com/beevik/ntp"
)

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
