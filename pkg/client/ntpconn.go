package client

import (
	"context"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"emperror.dev/errors"
)

type ntpAddr string

func (a ntpAddr) Network() string { return "wsbroker-ntp" }
func (a ntpAddr) String() string  { return string(a) }

// ntpConn implements net.Conn for NTP queries by utilizing Client.QueryNTP over WebSocket.
type ntpConn struct {
	client        *Client
	ctx           context.Context
	cancel        context.CancelFunc
	deadlineMu    sync.RWMutex
	deadline      time.Time
	readDeadline  time.Time
	writeDeadline time.Time
	respData      []byte
	respErr       error
	done          chan struct{}
	closeOnce     sync.Once
	closed        atomic.Bool
}

func newNTPConn(ctx context.Context, c *Client) *ntpConn {
	connCtx, cancel := context.WithCancel(ctx)
	return &ntpConn{
		client: c,
		ctx:    connCtx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

func (conn *ntpConn) Write(b []byte) (int, error) {
	if conn.closed.Load() {
		return 0, net.ErrClosed
	}

	conn.deadlineMu.RLock()
	d := conn.writeDeadline
	if d.IsZero() {
		d = conn.deadline
	}
	conn.deadlineMu.RUnlock()

	var writeCtx context.Context
	var cancel context.CancelFunc
	if !d.IsZero() {
		if time.Now().After(d) {
			return 0, os.ErrDeadlineExceeded
		}
		writeCtx, cancel = context.WithDeadline(conn.ctx, d)
		defer cancel()
	} else {
		writeCtx = conn.ctx
	}

	resp, err := conn.client.QueryNTP(writeCtx, b)
	conn.respData = resp
	conn.respErr = err
	close(conn.done)

	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (conn *ntpConn) Read(b []byte) (int, error) {
	if conn.closed.Load() {
		return 0, net.ErrClosed
	}

	conn.deadlineMu.RLock()
	d := conn.readDeadline
	if d.IsZero() {
		d = conn.deadline
	}
	conn.deadlineMu.RUnlock()

	var timeoutCh <-chan time.Time
	if !d.IsZero() {
		dur := time.Until(d)
		if dur <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		timer := time.NewTimer(dur)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case <-conn.done:
		if conn.respErr != nil {
			return 0, conn.respErr
		}
		n := copy(b, conn.respData)
		return n, nil
	case <-timeoutCh:
		return 0, os.ErrDeadlineExceeded
	case <-conn.ctx.Done():
		return 0, conn.ctx.Err()
	case <-conn.client.ctx.Done():
		return 0, errors.New("client connection closed")
	}
}

func (conn *ntpConn) Close() error {
	conn.closeOnce.Do(func() {
		conn.closed.Store(true)
		conn.cancel()
	})
	return nil
}

func (conn *ntpConn) LocalAddr() net.Addr {
	return ntpAddr(conn.client.clientName)
}

func (conn *ntpConn) RemoteAddr() net.Addr {
	return ntpAddr("wsbroker")
}

func (conn *ntpConn) SetDeadline(t time.Time) error {
	conn.deadlineMu.Lock()
	conn.deadline = t
	conn.deadlineMu.Unlock()
	return nil
}

func (conn *ntpConn) SetReadDeadline(t time.Time) error {
	conn.deadlineMu.Lock()
	conn.readDeadline = t
	conn.deadlineMu.Unlock()
	return nil
}

func (conn *ntpConn) SetWriteDeadline(t time.Time) error {
	conn.deadlineMu.Lock()
	conn.writeDeadline = t
	conn.deadlineMu.Unlock()
	return nil
}

var _ net.Conn = (*ntpConn)(nil)
