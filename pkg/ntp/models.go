package ntp

import (
	"encoding/json"
	"time"
)

// NTPStatus encapsulates the network time synchronization state and metrics.
type NTPStatus struct {
	Server        string        `json:"server"`
	Synced        bool          `json:"synced"`
	LastSync      time.Time     `json:"last_sync"`
	Time          time.Time     `json:"time"`
	Offset        time.Duration `json:"-"`
	OffsetNanos   int64         `json:"offset_nanos"`
	OffsetString  string        `json:"offset"`
	RTT           time.Duration `json:"-"`
	RTTNanos      int64         `json:"rtt_nanos"`
	RTTString     string        `json:"rtt"`
	Stratum       int           `json:"stratum"`
	ReferenceID   string        `json:"reference_id"`
	Precision     string        `json:"precision"`
	LeapIndicator string        `json:"leap_indicator"`
	Error         string        `json:"error,omitempty"`
}

type ntpStatusAlias NTPStatus

// MarshalJSON customizes JSON encoding to maintain compatibility with duration representations.
func (s NTPStatus) MarshalJSON() ([]byte, error) {
	alias := struct {
		ntpStatusAlias
		OffsetNanos int64 `json:"offset_nanos"`
		RTTNanos    int64 `json:"rtt_nanos"`
	}{
		ntpStatusAlias: ntpStatusAlias(s),
		OffsetNanos:    int64(s.Offset),
		RTTNanos:       int64(s.RTT),
	}
	return json.Marshal(alias)
}

// UnmarshalJSON customizes JSON decoding to populate time.Duration fields.
func (s *NTPStatus) UnmarshalJSON(data []byte) error {
	var alias struct {
		ntpStatusAlias
		OffsetNanos int64 `json:"offset_nanos"`
		RTTNanos    int64 `json:"rtt_nanos"`
	}
	if err := json.Unmarshal(data, &alias); err != nil {
		return err
	}
	*s = NTPStatus(alias.ntpStatusAlias)
	s.Offset = time.Duration(alias.OffsetNanos)
	s.RTT = time.Duration(alias.RTTNanos)
	return nil
}
