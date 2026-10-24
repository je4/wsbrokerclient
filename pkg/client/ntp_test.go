package client_test

import (
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/je4/wsbrokerclient/pkg/client"
	"github.com/je4/wsbrokerclient/pkg/event"
)

const ntpEpochOffset = 2208988800

func timeToNTPBytes(t time.Time) []byte {
	secs := uint32(t.Unix() + ntpEpochOffset)
	frac := uint32((uint64(t.Nanosecond()) << 32) / 1_000_000_000)
	b := make([]byte, 8)
	binary.BigEndian.PutUint32(b[0:4], secs)
	binary.BigEndian.PutUint32(b[4:8], frac)
	return b
}

func createMockNTPResponse(reqPacket []byte, serverTime time.Time) []byte {
	resp := make([]byte, 48)
	// Leap = 0, Version = 4, Mode = 4 (server) -> (0 << 6) | (4 << 3) | 4 = 0x24 (36)
	version := byte(4)
	if len(reqPacket) > 0 {
		version = (reqPacket[0] >> 3) & 0x07
	}
	resp[0] = (0 << 6) | (version << 3) | 4
	resp[1] = 1    // stratum 1
	resp[2] = 4    // poll
	resp[3] = 0xec // precision -20
	// Reference ID: "LOCL"
	copy(resp[12:16], []byte("LOCL"))

	refBytes := timeToNTPBytes(serverTime.Add(-1 * time.Minute))
	copy(resp[16:24], refBytes)

	// Origin Timestamp = Client Transmit Timestamp (bytes 40-47 of req)
	if len(reqPacket) >= 48 {
		copy(resp[24:32], reqPacket[40:48])
	}

	// Receive Timestamp
	rxBytes := timeToNTPBytes(serverTime)
	copy(resp[32:40], rxBytes)

	// Transmit Timestamp
	txBytes := timeToNTPBytes(serverTime.Add(100 * time.Microsecond))
	copy(resp[40:48], txBytes)

	return resp
}

func setupNTPMockServer(t *testing.T, shouldFail bool) (*httptest.Server, func()) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		for {
			var evt event.Event
			if err := wsjson.Read(r.Context(), conn, &evt); err != nil {
				break
			}

			if evt.Type == event.TypeNTPQuery {
				if shouldFail {
					errEvt, _ := event.NewNTPErrorEvent("mockbroker", evt.Source, "NTP upstream unreachable", evt.ID)
					_ = wsjson.Write(r.Context(), conn, errEvt)
					continue
				}

				raw, err := evt.GetData()
				if err != nil {
					continue
				}
				reqBytes, ok := raw.([]byte)
				if !ok {
					continue
				}

				respPacket := createMockNTPResponse(reqBytes, time.Now())
				respEvt, _ := event.NewNTPResponseEvent("mockbroker", evt.Source, respPacket, evt.ID)
				_ = wsjson.Write(r.Context(), conn, respEvt)
			}
		}
	})

	ts := httptest.NewServer(handler)
	return ts, ts.Close
}

func TestNTPQueryAndSync(t *testing.T) {
	ts, cleanup := setupNTPMockServer(t, false)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("ntp-client-1"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// 1. Test low-level QueryNTP
	reqBuf := make([]byte, 48)
	reqBuf[0] = 0x23 // version 4, mode 3 (client)
	copy(reqBuf[40:48], timeToNTPBytes(time.Now()))

	respBytes, err := cli.QueryNTP(ctx, reqBuf)
	if err != nil {
		t.Fatalf("QueryNTP failed: %v", err)
	}
	if len(respBytes) != 48 {
		t.Fatalf("expected 48 bytes, got %d", len(respBytes))
	}

	// 2. Test high-level NTP() using beevik/ntp
	ntpResp, err := cli.NTP(ctx)
	if err != nil {
		t.Fatalf("cli.NTP failed: %v", err)
	}
	if ntpResp == nil {
		t.Fatal("expected non-nil ntpResp")
	}
	if err := ntpResp.Validate(); err != nil {
		t.Fatalf("ntpResp validation failed: %v", err)
	}

	// 3. Test ClockOffset and Time
	offset := cli.ClockOffset()
	adjustedTime := cli.Time()
	if adjustedTime.IsZero() {
		t.Fatal("adjustedTime is zero")
	}

	// 4. Test SyncNTP
	syncOffset, err := cli.SyncNTP(ctx)
	if err != nil {
		t.Fatalf("SyncNTP failed: %v", err)
	}
	if syncOffset != cli.ClockOffset() {
		t.Fatalf("expected SyncNTP return value (%v) to match ClockOffset() (%v)", syncOffset, cli.ClockOffset())
	}

	t.Logf("Measured NTP ClockOffset: %v, offset: %v", offset, syncOffset)
}

func TestNTPErrorHandling(t *testing.T) {
	ts, cleanup := setupNTPMockServer(t, true)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cli, err := client.New(client.DefaultConfig(),
		client.WithEndpoint(ts.URL),
		client.WithClientName("ntp-client-err"),
	)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer cli.Close()

	if err := cli.ConnectSync(ctx); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	_, err = cli.NTP(ctx)
	if err == nil {
		t.Fatal("expected NTP to fail, but it succeeded")
	}
	if !strings.Contains(err.Error(), "NTP error from server") && !strings.Contains(err.Error(), "NTP upstream unreachable") {
		t.Fatalf("unexpected error message: %v", err)
	}
}
