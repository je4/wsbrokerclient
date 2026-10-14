package wschat

import (
	"bytes"
	"net/http"
	"strings"
)

// IndexData holds parameters for rendering index.gohtml.
type IndexData struct {
	ClientName    string
	Channels      []string
	ActiveChannel string
	Status        string
	Messages      []ChatMessage
	BrokerURL     string
}

// MessagesData holds parameters for rendering messages.gohtml.
type MessagesData struct {
	ActiveChannel string
	Messages      []ChatMessage
}

// ChannelsData holds parameters for rendering channels.gohtml.
type ChannelsData struct {
	Channels      []string
	ActiveChannel string
}

// StatusData holds parameters for rendering status.gohtml.
type StatusData struct {
	Status string
}

// registerRoutes sets up HTTP routes for the web interface and HTMX partial endpoints.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", s.handleIndex)
	mux.HandleFunc("GET /chat/messages", s.handleMessages)
	mux.HandleFunc("POST /chat/send", s.handleSend)
	mux.HandleFunc("POST /chat/channel", s.handleSelectChannel)
	mux.HandleFunc("GET /chat/channels", s.handleChannels)
	mux.HandleFunc("GET /chat/status", s.handleStatus)
	mux.HandleFunc("POST /chat/join", s.handleJoinChannel)
}

// handleIndex renders the complete initial HTML page (index.gohtml).
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	activeCh := s.store.GetActiveChannel()
	channels := s.store.GetChannels()
	messages := s.store.GetMessages(activeCh)
	status := s.store.GetStatus()

	data := IndexData{
		ClientName:    s.store.GetClientName(),
		Channels:      channels,
		ActiveChannel: activeCh,
		Status:        status,
		Messages:      messages,
		BrokerURL:     s.config.WSBroker,
	}

	var buf bytes.Buffer
	if err := indexTemplate.Execute(&buf, data); err != nil {
		if s.logger != nil {
			s.logger.Error().Err(err).Msg("failed to execute index template")
		}
		http.Error(w, "Failed to render page", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// handleMessages renders the messages partial (messages.gohtml) for HTMX polling or channel switching.
func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	channel := strings.TrimSpace(r.URL.Query().Get("channel"))
	if channel == "" {
		channel = s.store.GetActiveChannel()
	}

	messages := s.store.GetMessages(channel)
	data := MessagesData{
		ActiveChannel: channel,
		Messages:      messages,
	}

	var buf bytes.Buffer
	if err := messagesTemplate.Execute(&buf, data); err != nil {
		http.Error(w, "Failed to render messages", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// handleSend receives form data from the web UI, dispatches the message via ChatClient (wsbrokerclient),
// and returns the updated messages template.
func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	channel := strings.TrimSpace(r.FormValue("channel"))
	if channel == "" {
		channel = s.store.GetActiveChannel()
	}
	message := strings.TrimSpace(r.FormValue("message"))

	if message != "" && channel != "" {
		if s.chatClient != nil {
			if err := s.chatClient.SendMessage(r.Context(), channel, message); err != nil {
				if s.logger != nil {
					s.logger.Error().Err(err).Str("channel", channel).Msg("failed to send chat message")
				}
			}
		}
	}

	messages := s.store.GetMessages(channel)
	data := MessagesData{
		ActiveChannel: channel,
		Messages:      messages,
	}

	var buf bytes.Buffer
	if err := messagesTemplate.Execute(&buf, data); err != nil {
		http.Error(w, "Failed to render messages", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// handleSelectChannel switches the currently active channel in the store and triggers a channel list refresh.
func (s *Server) handleSelectChannel(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	channel := strings.TrimSpace(r.FormValue("channel"))
	if channel == "" {
		channel = strings.TrimSpace(r.URL.Query().Get("channel"))
	}
	if channel != "" {
		s.store.SetActiveChannel(channel)
	} else {
		channel = s.store.GetActiveChannel()
	}

	messages := s.store.GetMessages(channel)
	data := MessagesData{
		ActiveChannel: channel,
		Messages:      messages,
	}

	var buf bytes.Buffer
	if err := messagesTemplate.Execute(&buf, data); err != nil {
		http.Error(w, "Failed to render messages", http.StatusInternalServerError)
		return
	}

	w.Header().Set("HX-Trigger", "refresh-channels")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// handleChannels renders the channel list partial (channels.gohtml).
func (s *Server) handleChannels(w http.ResponseWriter, r *http.Request) {
	channels := s.store.GetChannels()
	activeCh := s.store.GetActiveChannel()

	data := ChannelsData{
		Channels:      channels,
		ActiveChannel: activeCh,
	}

	var buf bytes.Buffer
	if err := channelsTemplate.Execute(&buf, data); err != nil {
		http.Error(w, "Failed to render channels", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// handleStatus renders the connection status partial (status.gohtml) for HTMX status polling.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.store.GetStatus()
	data := StatusData{
		Status: status,
	}

	var buf bytes.Buffer
	if err := statusTemplate.Execute(&buf, data); err != nil {
		http.Error(w, "Failed to render status", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}

// handleJoinChannel handles dynamic channel joining from the web UI, attaching to the broker channel
// via ChatClient (wsbrokerclient.Attach) and updating the active channel.
func (s *Server) handleJoinChannel(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	channel := strings.TrimSpace(r.FormValue("channel"))
	if channel != "" {
		s.store.AddChannel(channel)
		s.store.SetActiveChannel(channel)
		if s.chatClient != nil {
			_ = s.chatClient.AttachChannel(r.Context(), channel)
		}
	} else {
		channel = s.store.GetActiveChannel()
	}

	messages := s.store.GetMessages(channel)
	data := MessagesData{
		ActiveChannel: channel,
		Messages:      messages,
	}

	var buf bytes.Buffer
	if err := messagesTemplate.Execute(&buf, data); err != nil {
		http.Error(w, "Failed to render messages", http.StatusInternalServerError)
		return
	}

	w.Header().Set("HX-Trigger", "refresh-channels")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(buf.Bytes())
}
