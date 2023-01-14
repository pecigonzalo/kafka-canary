package signals

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/pecigonzalo/kafka-canary/internal/workers"
	"github.com/rs/zerolog"
)

type Shutdown struct {
	logger                *zerolog.Logger
	serverShutdownTimeout time.Duration
}

func NewShutdown(serverShutdownTimeout time.Duration, logger *zerolog.Logger) (*Shutdown, error) {
	srv := &Shutdown{
		logger:                logger,
		serverShutdownTimeout: serverShutdownTimeout,
	}

	return srv, nil
}

func (s *Shutdown) Graceful(stopCh <-chan struct{}, httpServer *http.Server, canaryManager workers.Worker, healthy *int32, ready *int32) {
	ctx := context.Background()

	// wait for SIGTERM or SIGINT
	<-stopCh
	ctx, cancel := context.WithTimeout(ctx, s.serverShutdownTimeout)
	defer cancel()

	s.logger.Info().
		Msg("Shutting down canary manager")
	canaryManager.Stop()

	// all calls to /healthz and /readyz will fail from now on
	atomic.StoreInt32(healthy, 0)
	atomic.StoreInt32(ready, 0)

	s.logger.Info().
		Dur("timeout", s.serverShutdownTimeout).
		Msg("Shutting down HTTP/HTTPS server")

	// wait for Kubernetes readiness probe to remove this instance from the load balancer
	// the readiness check interval must be lower than the timeout
	time.Sleep(3 * time.Second)

	// determine if the http server was started
	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil {
			s.logger.Error().Err(err).Msg("HTTP server graceful shutdown failed")
		}
	}

}
