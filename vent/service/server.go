package service

import (
	"context"
	"net/http"

	"github.com/monax/bosmarmot/vent/config"
	"github.com/monax/bosmarmot/vent/logger"
)

// Server exposes HTTP endpoints for the service
type Server struct {
	Config   *config.Flags
	Log      *logger.Logger
	Consumer *Consumer
	mux      *http.ServeMux
	stopCh   chan bool
}

// NewServer returns a new HTTP server
func NewServer(cfg *config.Flags, log *logger.Logger, consumer *Consumer) *Server {
	// setup handlers
	mux := http.NewServeMux()

	mux.HandleFunc("/health", healthHandler(log, consumer))

	return &Server{
		Config:   cfg,
		Log:      log,
		Consumer: consumer,
		mux:      mux,
		stopCh:   make(chan bool, 1),
	}
}

// Run starts the HTTP server
func (s *Server) Run() {
	s.Log.Info("msg", "Starting HTTP Server")

	// start http server
	httpServer := &http.Server{Addr: s.Config.HTTPAddr, Handler: s}

	go func() {
		s.Log.Info("msg", "HTTP Server listening", "address", s.Config.HTTPAddr)
		httpServer.ListenAndServe()
	}()

	// wait for stop signal
	<-s.stopCh

	s.Log.Info("msg", "Shutting down HTTP Server...")

	httpServer.Shutdown(context.Background())
}

// ServeHTTP dispatches the HTTP requests using the Server Mux
func (s *Server) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	s.mux.ServeHTTP(resp, req)
}

// Shutdown gracefully shuts down the HTTP Server
func (s *Server) Shutdown() {
	s.stopCh <- true
}

func healthHandler(log *logger.Logger, consumer *Consumer) func(resp http.ResponseWriter, req *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
		err := consumer.Health()
		if err != nil {
			resp.WriteHeader(http.StatusServiceUnavailable)
		} else {
			resp.WriteHeader(http.StatusOK)
		}

		log.Debug("msg", "GET /health", "err", err)
	}
}
