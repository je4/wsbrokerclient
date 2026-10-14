package wschat

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/je4/utils/v2/pkg/zLogger"
	"go.ub.unibas.ch/cloud/certloader/v2/pkg/loader"
)

// Server represents the HTTPS web application server for wschat.
// It hosts the HTMX-driven web interface and proxies user interactions to wsbrokerclient.
type Server struct {
	config     *WSChatConfig
	store      *MessageStore
	chatClient *ChatClient
	logger     zLogger.ZLogger
	httpServer *http.Server
	listener   net.Listener
	tlsLoader  io.Closer
	port       int
	url        string
	mu         sync.Mutex
	wg         sync.WaitGroup
	running    bool
}

// NewServer initializes and configures a new Server instance.
func NewServer(conf *WSChatConfig, store *MessageStore, chatClient *ChatClient, logger zLogger.ZLogger) (*Server, error) {
	s := &Server{
		config:     conf,
		store:      store,
		chatClient: chatClient,
		logger:     logger,
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s, nil
}

// Start binds to an available random port on localhost (127.0.0.1:0), initializes WebTLS,
// and begins serving HTTPS requests in a background goroutine.
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return errors.New("server already running")
	}

	// 1. Create Server TLS config using certloader
	var tlsConfig *tls.Config
	var tlsLoader io.Closer
	var err error

	webtls := s.config.WebTLS
	if webtls.Type == "" {
		webtls.Type = "DEV"
	}
	tlsConfig, tlsLoader, err = loader.CreateServerLoader(false, &webtls, nil, s.logger)
	if err != nil {
		return errors.Wrap(err, "cannot create server TLS loader")
	}
	s.tlsLoader = tlsLoader

	// 2. Listen on dynamic random port (127.0.0.1:0) to prevent port collisions
	tcpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if tlsLoader != nil {
			_ = tlsLoader.Close()
		}
		return errors.Wrap(err, "failed to bind dynamic tcp listener on 127.0.0.1:0")
	}

	tcpAddr, ok := tcpLn.Addr().(*net.TCPAddr)
	if !ok {
		_ = tcpLn.Close()
		if tlsLoader != nil {
			_ = tlsLoader.Close()
		}
		return errors.New("failed to cast listener address to *net.TCPAddr")
	}

	s.port = tcpAddr.Port
	s.url = fmt.Sprintf("https://localhost:%d", s.port)
	s.listener = tls.NewListener(tcpLn, tlsConfig)
	s.running = true

	if s.logger != nil {
		s.logger.Info().Str("url", s.url).Int("port", s.port).Msg("wschat HTTPS server listening")
	}
	fmt.Printf("\n>>> WSChat web interface running at: %s <<<\n\n", s.url)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.httpServer.Serve(s.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			if s.logger != nil {
				s.logger.Error().Err(err).Msg("http server error")
			}
		}
	}()

	return nil
}

// URL returns the bound HTTPS base URL (e.g. "https://localhost:54321").
func (s *Server) URL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.url
}

// Port returns the dynamically bound port number.
func (s *Server) Port() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.port
}

// GracefulStop gracefully shuts down the HTTP server and frees TLS loader resources.
func (s *Server) GracefulStop(ctx context.Context) error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil
	}
	s.running = false
	s.mu.Unlock()

	var errs []error
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if s.tlsLoader != nil {
		if err := s.tlsLoader.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	s.wg.Wait()
	if len(errs) > 0 {
		return errors.Combine(errs...)
	}
	return nil
}

// Close immediately closes the listener and server with a default timeout.
func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.GracefulStop(ctx)
}
