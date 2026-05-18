// Package handler contains HTTP handlers and the chi router for the API Gateway.
package handler

import (
	"net/http"

	"github.com/aravindgpd/gpu-telemetry/gateway/internal/config"
	"github.com/aravindgpd/gpu-telemetry/gateway/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	"go.uber.org/zap"

	// Side-effect import: the generated docs package registers the OpenAPI
	// spec with swaggo's global registry, which httpSwagger.Handler reads from.
	_ "github.com/aravindgpd/gpu-telemetry/gateway/docs"
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

	// Swagger UI:
	//   /swagger/index.html → interactive API explorer
	//   /swagger/doc.json   → raw OpenAPI spec
	// The trailing wildcard route is required because httpSwagger.Handler
	// serves a tree of static assets (index.html, swagger-ui.css, ...).
	r.Get("/swagger/*", httpSwagger.Handler(
		httpSwagger.URL("/swagger/doc.json"),
	))
	// Redirect bare /swagger to the index for convenience.
	r.Get("/swagger", http.RedirectHandler("/swagger/index.html", http.StatusMovedPermanently).ServeHTTP)

	// API v1
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/gpus", h.ListGPUs)                       // filters: model_name (ILIKE), hostname (ILIKE), limit, offset
		r.Get("/gpus/{id}/telemetry", h.GetTelemetry)    // per-GPU telemetry
		r.Get("/telemetry", h.QueryTelemetry)            // cross-GPU telemetry (model_name = ILIKE)
		r.Get("/models", h.ListModels)                   // discovery: distinct model names + counts
	})

	return r
}
