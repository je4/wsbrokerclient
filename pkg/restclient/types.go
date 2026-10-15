// Package restclient provides a client for WSBroker's HTTP REST management and publish APIs.
package restclient

import (
	"encoding/json/jsontext"
)

// PublishRequest defines the JSON payload for publishing messages via REST API endpoints.
type PublishRequest struct {
	Source  string         `json:"source"`
	Target  string         `json:"target,omitempty"`
	Type    string         `json:"type"`
	Token   string         `json:"token,omitempty"`
	Data    jsontext.Value `json:"data,omitempty"`
	Message string         `json:"message,omitempty"`
}

// PublishResponse defines the JSON response returned after a message is dispatched.
type PublishResponse struct {
	Target      string `json:"target"`
	Type        string `json:"type"`
	Sent        bool   `json:"sent"`
	Subscribers int    `json:"subscribers"`
}

// ChannelInfo describes a channel, its total subscriber count, and its connected client members.
type ChannelInfo struct {
	Name        string   `json:"name"`
	Subscribers int      `json:"subscribers"`
	Clients     []string `json:"clients,omitempty"`
}
