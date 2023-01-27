package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"github.com/justinas/alice"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/hlog"
)

var (
	healthy int32
	ready   int32
)

type Server struct {
	config  *Config
	router  *mux.Router
	handler http.Handler
	chain   alice.Chain
	logger  *zerolog.Logger
}

func NewServer(config *Config, logger *zerolog.Logger) (*Server, error) {
	srv := &Server{
		config: config,
		router: mux.NewRouter(),
		chain:  alice.New(),
		logger: logger,
	}

	return srv, nil
}

func (s *Server) ListenAndServe() (*http.Server, *int32, *int32) {
	go s.startMetricsServer()

	// Register Handlers
	s.router.Handle("/metrics", promhttp.Handler())
	s.router.HandleFunc("/healthz", s.healthzHandler).Methods("GET")
	s.router.HandleFunc("/readyz", s.readyzHandler).Methods("GET")

	// Register middlewares
	logger := s.logger.With().Logger()
	chain := s.chain.Append(hlog.NewHandler(logger))

	// Install some provided extra handler to set some request's context fields.
	// Thanks to that handler, all our logs will come with some prepopulated fields.
	chain = chain.Append(hlog.AccessHandler(func(r *http.Request, status, size int, duration time.Duration) {
		// NOTE: There has to be a more elegant way of doing this
		if r.URL.String() == "/metrics" {
			return
		}
		hlog.FromRequest(r).Info().
			Str("method", r.Method).
			Stringer("url", r.URL).
			Int("status", status).
			Int("size", size).
			Dur("duration", duration).
			Msg("")
	}))
	s.handler = chain.Then(s.router)

	// create the http server
	srv := s.startServer()

	// signal Kubernetes the server is ready to receive traffic
	atomic.StoreInt32(&healthy, 1)
	atomic.StoreInt32(&ready, 1)

	return srv, &healthy, &ready
}

func (s *Server) startServer() *http.Server {
	srv := &http.Server{
		Addr:         s.config.Host + ":" + s.config.Port,
		WriteTimeout: 30 * time.Second,
		ReadTimeout:  30 * time.Second,
		IdleTimeout:  2 * 30 * time.Second,
		Handler:      s.handler,
	}

	// start the server in the background
	go func() {
		s.logger.Info().
			Str("addr", srv.Addr).
			Msg("Starting HTTP Server")
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			s.logger.Fatal().
				Err(err).
				Msg("HTTP server crashed")
		}
	}()

	// return the server and routine
	return srv
}

func (s *Server) startMetricsServer() {
	mux := http.DefaultServeMux
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("OK"))
		if err != nil {
			s.logger.Err(err).Msg("Write error")
		}
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%v", 8081),
		Handler: mux,
	}

	err := srv.ListenAndServe()
	if err != nil {
		s.logger.Err(err).Msg("Metrics server error")
	}
}

func (s *Server) healthzHandler(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&healthy) == 1 {
		s.JSONResponse(w, r, map[string]string{"status": "OK"})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func (s *Server) readyzHandler(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&ready) == 1 {
		s.JSONResponse(w, r, map[string]string{"status": "OK"})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func (s *Server) JSONResponse(w http.ResponseWriter, r *http.Request, result interface{}) {
	body, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		s.logger.Error().Err(err).Msg("JSON mashal failed")
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(prettyJSON(body))
	if err != nil {
		s.logger.Err(err).Msg("Write error")
	}
}

func (s *Server) JSONResponseCode(w http.ResponseWriter, r *http.Request, result interface{}, responseCode int) {
	body, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		s.logger.Error().Err(err).Msg("JSON marshal failed")
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(responseCode)
	_, err = w.Write(prettyJSON(body))
	if err != nil {
		s.logger.Err(err).Msg("Write error")
	}
}

func prettyJSON(b []byte) []byte {
	var out bytes.Buffer
	json.Indent(&out, b, "", "  ") // nolint: errcheck
	return out.Bytes()
}
