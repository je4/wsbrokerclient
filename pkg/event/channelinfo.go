package event

import (
	"fmt"
)

// ChannelInfo describes a channel, its total subscriber count, and its connected client members.
type ChannelInfo struct {
	Name        string   `json:"name"`
	Subscribers int      `json:"subscribers"`
	Clients     []string `json:"clients"`
}

// String returns a human-readable representation of ChannelInfo.
func (c *ChannelInfo) String() string {
	if c == nil {
		return "<nil channel info>"
	}
	return fmt.Sprintf("Channel: %s (%d subscribers)", c.Name, c.Subscribers)
}

// Type returns the EventType for ChannelInfo response messages.
func (c *ChannelInfo) Type() EventType {
	return TypeChannelInfoResponse
}

var _ DataInterface = (*ChannelInfo)(nil)
