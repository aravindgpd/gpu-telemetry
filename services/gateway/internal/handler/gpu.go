package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/aravindgpd/gpu-telemetry/gateway/internal/store"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// ListGPUs returns GPUs for which telemetry has been collected, with optional
// filters and pagination.
//
// @Summary     List GPUs
// @Description Returns the registered GPUs. Filter by model_name or hostname; paginate with limit/offset.
// @Tags        gpus
// @Produce     json
// @Param       model_name  query string false "Filter to one model. Case-insensitive substring match — 'h100' matches 'NVIDIA H100 80GB HBM3'. See /models for what's available."
// @Param       hostname    query string false "Filter to one host. Case-insensitive substring match."
// @Param       limit       query int    false "Maximum records to return (default 100, max 1000)"
// @Param       offset      query int    false "Pagination offset (default 0)"
// @Success     200 {array}  store.GPU
// @Failure     400 {object} errorResponse
// @Failure     500 {object} errorResponse
// @Router      /gpus [get]
func (h *Handler) ListGPUs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit, ok := h.parseLimit(w, q.Get("limit"))
	if !ok {
		return
	}
	offset, ok := h.parseOffset(w, q.Get("offset"))
	if !ok {
		return
	}

	filter := store.GPUFilter{
		ModelName: q.Get("model_name"),
		Hostname:  q.Get("hostname"),
		Limit:     limit,
		Offset:    offset,
	}

	gpus, err := h.db.QueryGPUs(r.Context(), filter)
	if err != nil {
		h.logger.Error("QueryGPUs failed",
			zap.String("model_name", filter.ModelName),
			zap.String("hostname", filter.Hostname),
			zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to list GPUs")
		return
	}
	writeJSON(w, http.StatusOK, gpus)
}

// QueryTelemetry returns telemetry samples across one or many GPUs, filtered
// by any combination of metric_name, model_name, uuid, and time bounds.
//
// @Summary     Query telemetry across GPUs
// @Description Cross-GPU telemetry search. All filters are optional; combine freely.
// @Tags        telemetry
// @Produce     json
// @Param       metric_name query string false "DCGM metric filter (e.g. DCGM_FI_DEV_GPU_UTIL)"
// @Param       model_name  query string false "GPU model filter. Case-insensitive substring match — 'h100' is enough; the full 'NVIDIA H100 80GB HBM3' also works. See /models for what's available."
// @Param       uuid        query string false "Filter to one specific GPU UUID"
// @Param       start_time  query string false "Inclusive lower bound on sample_at (RFC3339)"
// @Param       end_time    query string false "Inclusive upper bound on sample_at (RFC3339)"
// @Param       limit       query int    false "Maximum records to return (default 100, max 1000)"
// @Param       offset      query int    false "Pagination offset (default 0)"
// @Success     200 {array}  store.TelemetryRecord
// @Failure     400 {object} errorResponse
// @Failure     500 {object} errorResponse
// @Router      /telemetry [get]
func (h *Handler) QueryTelemetry(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	startTime, ok := h.parseTime(w, q.Get("start_time"), "start_time")
	if !ok {
		return
	}
	endTime, ok := h.parseTime(w, q.Get("end_time"), "end_time")
	if !ok {
		return
	}
	limit, ok := h.parseLimit(w, q.Get("limit"))
	if !ok {
		return
	}
	offset, ok := h.parseOffset(w, q.Get("offset"))
	if !ok {
		return
	}

	filter := store.TelemetryFilter{
		UUID:       q.Get("uuid"),
		MetricName: q.Get("metric_name"),
		ModelName:  q.Get("model_name"),
		StartTime:  startTime,
		EndTime:    endTime,
		Limit:      limit,
		Offset:     offset,
	}

	records, err := h.db.QueryTelemetry(r.Context(), filter)
	if err != nil {
		h.logger.Error("QueryTelemetry failed",
			zap.String("uuid", filter.UUID),
			zap.String("metric_name", filter.MetricName),
			zap.String("model_name", filter.ModelName),
			zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to query telemetry")
		return
	}
	writeJSON(w, http.StatusOK, records)
}

// parseLimit reads ?limit=N from a query string, clamps to [1, MaxLimit], and
// falls back to DefaultLimit when absent. Returns (value, ok); ok=false means
// the helper already wrote a 400 to w.
func (h *Handler) parseLimit(w http.ResponseWriter, raw string) (int, bool) {
	if raw == "" {
		return h.cfg.DefaultLimit, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		writeError(w, http.StatusBadRequest, "invalid limit")
		return 0, false
	}
	if n > h.cfg.MaxLimit {
		n = h.cfg.MaxLimit
	}
	return n, true
}

// parseOffset reads ?offset=N, defaults to 0, rejects negatives.
func (h *Handler) parseOffset(w http.ResponseWriter, raw string) (int, bool) {
	if raw == "" {
		return 0, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		writeError(w, http.StatusBadRequest, "invalid offset")
		return 0, false
	}
	return n, true
}

// parseTime reads an optional RFC3339 query param. `name` is used in the error
// body so callers know which field was malformed.
func (h *Handler) parseTime(w http.ResponseWriter, raw, name string) (*time.Time, bool) {
	if raw == "" {
		return nil, true
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid "+name+": must be RFC3339")
		return nil, false
	}
	return &t, true
}

// GetTelemetry returns DCGM metric samples for a specific GPU ordered by sample_at.
//
// @Summary     Get GPU telemetry
// @Description Returns DCGM metric samples for the specified GPU UUID, ordered by sample_at ascending. Optionally filter by metric_name.
// @Tags        gpus
// @Produce     json
// @Param       id          path  string  true  "GPU UUID (e.g. GPU-5fd4f087-86f3-7a43-...)"
// @Param       metric_name query string  false "DCGM metric filter (e.g. DCGM_FI_DEV_GPU_UTIL). Omit for all metrics."
// @Param       start_time  query string  false "Inclusive lower bound on sample_at (RFC3339, e.g. 2025-07-18T20:42:34Z)"
// @Param       end_time    query string  false "Inclusive upper bound on sample_at (RFC3339)"
// @Param       limit       query int     false "Maximum records to return (default 100, max 1000)"
// @Param       offset      query int     false "Pagination offset (default 0)"
// @Success     200 {array}  store.TelemetryRecord
// @Failure     400 {object} errorResponse
// @Failure     404 {object} errorResponse
// @Failure     500 {object} errorResponse
// @Router      /gpus/{id}/telemetry [get]
func (h *Handler) GetTelemetry(w http.ResponseWriter, r *http.Request) {
	uuid := chi.URLParam(r, "id")
	q := r.URL.Query()

	metricName := q.Get("metric_name")

	startTime, ok := h.parseTime(w, q.Get("start_time"), "start_time")
	if !ok {
		return
	}
	endTime, ok := h.parseTime(w, q.Get("end_time"), "end_time")
	if !ok {
		return
	}
	limit, ok := h.parseLimit(w, q.Get("limit"))
	if !ok {
		return
	}
	offset, ok := h.parseOffset(w, q.Get("offset"))
	if !ok {
		return
	}

	records, err := h.db.GetTelemetry(r.Context(), uuid, metricName, startTime, endTime, limit, offset)
	if err != nil {
		h.logger.Error("GetTelemetry failed",
			zap.String("uuid", uuid),
			zap.String("metric_name", metricName),
			zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to retrieve telemetry")
		return
	}

	writeJSON(w, http.StatusOK, records)
}

// ListModels returns the distinct GPU models seen so far + count of GPUs per model.
//
// @Summary     List GPU models
// @Description Discovery endpoint — lists every distinct GPU model_name in the database with a count of GPUs running it. Use the model_name from this list as the `?model_name=` filter on /gpus and /telemetry.
// @Tags        gpus
// @Produce     json
// @Success     200 {array}  store.ModelSummary
// @Failure     500 {object} errorResponse
// @Router      /models [get]
func (h *Handler) ListModels(w http.ResponseWriter, r *http.Request) {
	models, err := h.db.ListModels(r.Context())
	if err != nil {
		h.logger.Error("ListModels failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to list models")
		return
	}
	writeJSON(w, http.StatusOK, models)
}

// Healthz is the liveness probe endpoint.
//
// @Summary  Liveness probe
// @Tags     health
// @Produce  json
// @Success  200 {object} map[string]string
// @Router   /healthz [get]
func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Readyz is the readiness probe endpoint — checks DB connectivity.
//
// @Summary  Readiness probe
// @Tags     health
// @Produce  json
// @Success  200 {object} map[string]string
// @Failure  503 {object} errorResponse
// @Router   /readyz [get]
func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Ping(r.Context()); err != nil {
		h.logger.Warn("readyz: database not ready", zap.Error(err))
		writeError(w, http.StatusServiceUnavailable, "database not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// writeJSON encodes v as JSON and writes it with the given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type errorResponse struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
