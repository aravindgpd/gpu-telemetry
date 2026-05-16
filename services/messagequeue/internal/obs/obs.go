// Package obs serves /healthz, /readyz, and /metrics on a dedicated HTTP port.
// It is intentionally tiny — no business logic — so it can be copy-pasted into
// every service. Prometheus uses the default registry from client_golang, which
// any package can register collectors against without explicit wiring.
package obs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// ReadyFunc is a user-supplied readiness probe. Return nil for ready; any error
// surfaces as 503 from /readyz with the error message in the response body.
type ReadyFunc func() error

// Server is the running HTTP server handle.
type Server struct {
	srv    *http.Server
	logger *zap.Logger
}

// Start launches the HTTP server in a background goroutine. The returned
// Server's Shutdown method should be called from the caller's shutdown path.
func Start(port int, logger *zap.Logger, ready ReadyFunc) *Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, `{"status":"ok"}`)
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if ready != nil {
			if err := ready(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = fmt.Fprintf(w, `{"status":"not_ready","error":%q}`+"\n", err.Error())
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, `{"status":"ready"}`)
	})

	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", port),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("obs server listening", zap.Int("port", port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("obs server error", zap.Error(err))
		}
	}()

	return &Server{srv: srv, logger: logger}
}

// Shutdown gracefully stops the server. Safe to call multiple times.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil || s.srv == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}
