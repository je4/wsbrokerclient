package ntp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	beevikntp "github.com/beevik/ntp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockNTPClient struct {
	offset time.Duration
	rtt    time.Duration
	err    error
}

func (m *mockNTPClient) NTP(ctx context.Context, opts ...beevikntp.QueryOptions) (*beevikntp.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &beevikntp.Response{
		Time:          time.Now().UTC(),
		ReferenceTime: time.Now().UTC().Add(-1 * time.Minute),
		ClockOffset:   m.offset,
		RTT:           m.rtt,
		Stratum:       1,
		ReferenceID:   0x47505300, // "GPS"
		Precision:     time.Microsecond,
		Leap:          beevikntp.LeapNoWarning,
	}, nil
}

func TestNTPStatus_JSONSerialization(t *testing.T) {
	status := NTPStatus{
		Server:        "wsbroker",
		Synced:        true,
		LastSync:      time.Now().UTC().Truncate(time.Millisecond),
		Time:          time.Now().UTC().Truncate(time.Millisecond),
		Offset:        150 * time.Millisecond,
		OffsetString:  "150ms",
		RTT:           20 * time.Millisecond,
		RTTString:     "20ms",
		Stratum:       1,
		ReferenceID:   "GPS",
		Precision:     "1µs",
		LeapIndicator: "no_warning",
	}

	data, err := json.Marshal(status)
	require.NoError(t, err)

	var decoded NTPStatus
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, status.Server, decoded.Server)
	assert.Equal(t, status.Synced, decoded.Synced)
	assert.Equal(t, status.Offset, decoded.Offset)
	assert.Equal(t, status.RTT, decoded.RTT)
	assert.Equal(t, status.Stratum, decoded.Stratum)
	assert.Equal(t, status.ReferenceID, decoded.ReferenceID)
}

func TestNTPService_LifecycleAndSync(t *testing.T) {
	mock := &mockNTPClient{
		offset: 12 * time.Millisecond,
		rtt:    4 * time.Millisecond,
	}

	svc := New(mock, "wsbroker", 50*time.Millisecond, 2*time.Second, nil)
	assert.False(t, svc.IsSynced())

	err := svc.Sync()
	require.NoError(t, err)
	assert.True(t, svc.IsSynced())
	assert.Equal(t, 12*time.Millisecond, svc.Offset())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	svc.Start(ctx)
	time.Sleep(100 * time.Millisecond)
	assert.True(t, svc.IsSynced())

	svc.Stop()
}
