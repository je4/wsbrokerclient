package wschat

import (
	"sync"
	"time"
)

// ChatMessage represents a single message in a channel.
type ChatMessage struct {
	ID        string    `json:"id"`
	Sender    string    `json:"sender"`
	Channel   string    `json:"channel"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
	IsSelf    bool      `json:"is_self"`
}

// FormattedTime returns a user-friendly timestamp formatted as HH:MM:SS.
func (m ChatMessage) FormattedTime() string {
	if m.Timestamp.IsZero() {
		return ""
	}
	return m.Timestamp.Format("15:04:05")
}

// MessageStore provides thread-safe in-memory message history and channel management.
type MessageStore struct {
	mu                    sync.RWMutex
	clientName            string
	messages              map[string][]ChatMessage
	channels              []string
	activeChannel         string
	status                string
	maxMessagesPerChannel int
}

// NewMessageStore creates a new MessageStore initialized with channels and client name.
func NewMessageStore(clientName string, channels []string, maxMessagesPerChannel int) *MessageStore {
	if maxMessagesPerChannel <= 0 {
		maxMessagesPerChannel = 200
	}
	channelsCopy := make([]string, 0, len(channels))
	for _, ch := range channels {
		if ch != "" {
			channelsCopy = append(channelsCopy, ch)
		}
	}
	active := ""
	if len(channelsCopy) > 0 {
		active = channelsCopy[0]
	}
	return &MessageStore{
		clientName:            clientName,
		messages:              make(map[string][]ChatMessage),
		channels:              channelsCopy,
		activeChannel:         active,
		status:                "Disconnected",
		maxMessagesPerChannel: maxMessagesPerChannel,
	}
}

// AddMessage appends a message to the corresponding channel, pruning if exceeding limit.
func (s *MessageStore) AddMessage(msg ChatMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}
	if msg.Sender == s.clientName {
		msg.IsSelf = true
	}

	channel := msg.Channel
	list := s.messages[channel]
	list = append(list, msg)
	if len(list) > s.maxMessagesPerChannel {
		list = list[len(list)-s.maxMessagesPerChannel:]
	}
	s.messages[channel] = list

	// Ensure channel exists in channel list
	found := false
	for _, ch := range s.channels {
		if ch == channel {
			found = true
			break
		}
	}
	if !found && channel != "" {
		s.channels = append(s.channels, channel)
	}
}

// GetMessages returns a copy of messages for the specified channel.
func (s *MessageStore) GetMessages(channel string) []ChatMessage {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list, ok := s.messages[channel]
	if !ok {
		return []ChatMessage{}
	}
	res := make([]ChatMessage, len(list))
	copy(res, list)
	return res
}

// GetChannels returns the list of active channels.
func (s *MessageStore) GetChannels() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]string, len(s.channels))
	copy(res, s.channels)
	return res
}

// AddChannel adds a new channel if it doesn't already exist.
func (s *MessageStore) AddChannel(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if channel == "" {
		return
	}
	for _, ch := range s.channels {
		if ch == channel {
			return
		}
	}
	s.channels = append(s.channels, channel)
	if s.activeChannel == "" {
		s.activeChannel = channel
	}
}

// GetActiveChannel returns the currently selected channel.
func (s *MessageStore) GetActiveChannel() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeChannel
}

// SetActiveChannel changes the currently selected channel.
func (s *MessageStore) SetActiveChannel(channel string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeChannel = channel
}

// GetStatus returns the connection status string.
func (s *MessageStore) GetStatus() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

// SetStatus updates the connection status string.
func (s *MessageStore) SetStatus(status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

// GetClientName returns the configured client name.
func (s *MessageStore) GetClientName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.clientName
}

// StoreStats holds summary counts.
type StoreStats struct {
	TotalMessages int
	ChannelCount  int
	ActiveChannel string
	Status        string
	ClientName    string
}

// GetStats returns summary statistics.
func (s *MessageStore) GetStats() StoreStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	total := 0
	for _, msgs := range s.messages {
		total += len(msgs)
	}
	return StoreStats{
		TotalMessages: total,
		ChannelCount:  len(s.channels),
		ActiveChannel: s.activeChannel,
		Status:        s.status,
		ClientName:    s.clientName,
	}
}
