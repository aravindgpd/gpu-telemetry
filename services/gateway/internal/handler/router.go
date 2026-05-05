// Package handler contains HTTP handlers and the chi router for the API Gateway.
package handler

import (
	"net/http"

	"github.com/aravindgpd/gpu-telemetry/gateway/internal/config"
	"github.com/aravindgpd/gpu-telemetry/gateway/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

// Handler holds shared dependencies for all HTTP handlers.
type Handler struct {
	db     store.Repository
	logger *zap.Logger
	cfg    *config.Config
}

// NewRouter wires all routes and returns the root http.Handler.
func NewRouter(db store.Repository, logger *zap.Logger, cfg *config.Config) http.Handler {
	h := &Handler{db: db, logger: logger, cfg: cfg}

	r := chi.NewRouter()

	// Middleware stack
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(zapLogger(logger))
	r.Use(middleware.Recoverer)

	// Health probes (no /api/v1 prefix — called by Kubernetes)
	r.Get("/healthz", h.Healthz)
	r.Get("/readyz", h.Readyz)

	// Metrics endpoint served by Prometheus handler (registered in main)
	// r.Handle("/metrics", promhttp.Handler())

	// Swagger UI — served at /swagger/
	// r.Get("/swagger/*", httpSwagger.Handler())

	// API v1
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/gpus", h.ListGPUs)
		r.Get("/gpus/{id}/telemetry", h.GetTelemetry)
	})

	return r
}
