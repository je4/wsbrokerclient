package ntp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"emperror.dev/errors"
	"github.com/beevik/ntp"
	"github.com/je4/utils/v2/pkg/zLogger"
)

// DefaultServer is the default NTP server identifier when querying via WSBroker or standard NTP.
const DefaultServer = "wsbroker"

// DefaultInterval is the default periodic synchronization interval.
const DefaultInterval = 10 * time.Minute

// DefaultTimeout is the default query timeout.
const DefaultTimeout = 5 * time.Second

// NTPClient defines the interface for querying NTP, satisfied by wsbrokerclient.Client.
type NTPClient interface {
	NTP(ctx context.Context, opts ...ntp.QueryOptions) (*ntp.Response, error)
}

// Option represents a functional configuration option for Service.
type Option func(*Service)

// WithClient sets the NTPClient implementation (e.g. wsbrokerclient.Client).
func WithClient(client NTPClient) Option {
	return func(s *Service) {
		s.client = client
	}
}

// WithServer sets the server identifier/address.
func WithServer(server string) Option {
	return func(s *Service) {
		if server != "" {
			s.server = server
		}
	}
}

// WithInterval sets the periodic sync interval.
func WithInterval(interval time.Duration) Option {
	return func(s *Service) {
		if interval > 0 {
			s.interval = interval
		}
	}
}

// WithTimeout sets the NTP query timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(s *Service) {
		if timeout > 0 {
			s.timeout = timeout
		}
	}
}

// WithLogger sets the logger.
func WithLogger(logger zLogger.ZLogger) Option {
	return func(s *Service) {
		s.logger = logger
	}
}

// Service manages background NTP synchronization and provides cached, thread-safe status snapshots.
type Service struct {
	client   NTPClient
	server   string
	interval time.Duration
	timeout  time.Duration
	logger   zLogger.ZLogger

	mu     sync.RWMutex
	status NTPStatus

	cancel context.CancelFunc
	done   chan struct{}
}

// New creates a new NTP service instance with optional functional options.
func New(client NTPClient, server string, interval time.Duration, timeout time.Duration, logger zLogger.ZLogger, opts ...Option) *Service {
	if server == "" {
		server = DefaultServer
	}
	if interval <= 0 {
		interval = DefaultInterval
	}
	if timeout <= 0 {
		timeout = DefaultTimeout
	}

	s := &Service{
		client:   client,
		server:   server,
		interval: interval,
		timeout:  timeout,
		logger:   logger,
		status: NTPStatus{
			Server: server,
			Synced: false,
		},
		done: make(chan struct{}),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// SetClient updates the underlying NTPClient.
func (s *Service) SetClient(client NTPClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.client = client
}

// Start initiates the background NTP synchronization worker.
func (s *Service) Start(ctx context.Context) {
	s.mu.Lock()
	if s.cancel != nil {
		s.mu.Unlock()
		return // already running
	}

	workerCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	s.mu.Unlock()

	// Initial sync immediately
	if err := s.Sync(); err != nil && s.logger != nil {
		s.logger.Warn().Err(err).Str("server", s.server).Msg("initial NTP synchronization failed")
	}

	go s.workerLoop(workerCtx)
}

// Stop terminates the background synchronization worker.
func (s *Service) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.mu.Unlock()

	if cancel != nil {
		cancel()
		<-s.done
	}
}

func (s *Service) workerLoop(ctx context.Context) {
	defer close(s.done)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// If initial sync didn't succeed (e.g. waiting for WS client to connect), retry with fast interval
	retryTicker := time.NewTicker(3 * time.Second)
	defer retryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-retryTicker.C:
			s.mu.RLock()
			synced := s.status.Synced
			s.mu.RUnlock()
			if !synced {
				if err := s.Sync(); err == nil {
					retryTicker.Stop()
				}
			} else {
				retryTicker.Stop()
			}
		case <-ticker.C:
			if err := s.Sync(); err != nil && s.logger != nil {
				s.logger.Warn().Err(err).Str("server", s.server).Msg("periodic NTP synchronization failed")
			}
		}
	}
}

// Sync performs an NTP query and updates the cached status in a thread-safe manner.
func (s *Service) Sync() error {
	s.mu.RLock()
	client := s.client
	server := s.server
	timeout := s.timeout
	s.mu.RUnlock()

	status, err := s.query(client, server, timeout)

	s.mu.Lock()
	defer s.mu.Unlock()

	if err != nil {
		s.status.Synced = false
		s.status.Error = err.Error()
		return err
	}

	s.status = status
	if s.logger != nil {
		s.logger.Debug().
			Str("server", status.Server).
			Str("offset", status.OffsetString).
			Str("rtt", status.RTTString).
			Int("stratum", status.Stratum).
			Msg("NTP synchronization successful")
	}
	return nil
}

// Status returns a thread-safe snapshot of the current NTP status.
func (s *Service) Status() NTPStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *Service) query(client NTPClient, server string, timeout time.Duration) (NTPStatus, error) {
	opts := ntp.QueryOptions{
		Timeout: timeout,
	}

	var resp *ntp.Response
	var err error

	if client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		resp, err = client.NTP(ctx, opts)
	} else {
		resp, err = ntp.QueryWithOptions(server, opts)
	}

	if err != nil {
		return NTPStatus{
			Server: server,
			Synced: false,
			Error:  err.Error(),
		}, errors.Wrapf(err, "failed to query NTP from %s", server)
	}

	if err := resp.Validate(); err != nil {
		return NTPStatus{
			Server: server,
			Synced: false,
			Error:  err.Error(),
		}, errors.Wrapf(err, "invalid NTP response from %s", server)
	}

	refID := formatReferenceID(resp.ReferenceID, resp.Stratum)
	leapStr := formatLeapIndicator(resp.Leap)

	status := NTPStatus{
		Server:        server,
		Synced:        true,
		LastSync:      time.Now().UTC(),
		Time:          resp.Time.UTC(),
		Offset:        resp.ClockOffset,
		OffsetNanos:   int64(resp.ClockOffset),
		OffsetString:  resp.ClockOffset.Round(time.Microsecond).String(),
		RTT:           resp.RTT,
		RTTNanos:      int64(resp.RTT),
		RTTString:     resp.RTT.Round(time.Microsecond).String(),
		Stratum:       int(resp.Stratum),
		ReferenceID:   refID,
		Precision:     resp.Precision.String(),
		LeapIndicator: leapStr,
		Error:         "",
	}

	return status, nil
}

func formatReferenceID(refID uint32, stratum uint8) string {
	if stratum <= 1 {
		// 4 ASCII characters for Kiss-o'-Death or Primary Reference Identifier
		bytes := []byte{
			byte((refID >> 24) & 0xFF),
			byte((refID >> 16) & 0xFF),
			byte((refID >> 8) & 0xFF),
			byte(refID & 0xFF),
		}
		var clean []byte
		for _, b := range bytes {
			if b >= 32 && b <= 126 {
				clean = append(clean, b)
			}
		}
		if len(clean) > 0 {
			return string(clean)
		}
		return fmt.Sprintf("0x%08X", refID)
	}

	// For IPv4 secondary servers, ReferenceID is often IPv4 address
	ip := net.IPv4(
		byte((refID>>24)&0xFF),
		byte((refID>>16)&0xFF),
		byte((refID>>8)&0xFF),
		byte(refID&0xFF),
	)
	return ip.String()
}

func formatLeapIndicator(leap ntp.LeapIndicator) string {
	switch leap {
	case ntp.LeapNoWarning:
		return "no_warning"
	case ntp.LeapAddSecond:
		return "add_second"
	case ntp.LeapDelSecond:
		return "delete_second"
	case ntp.LeapNotInSync:
		return "not_in_sync"
	default:
		return fmt.Sprintf("unknown(%d)", leap)
	}
}

// Offset returns the current measured clock offset from the cached status.
func (s *Service) Offset() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status.Offset
}

// Time returns the current local time adjusted by the latest NTP offset.
func (s *Service) Time() time.Time {
	return time.Now().Add(s.Offset())
}

// IsSynced returns true if the last NTP sync was successful.
func (s *Service) IsSynced() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status.Synced
}
