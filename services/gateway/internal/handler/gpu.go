package handler

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

// ListGPUs returns all GPUs for which telemetry data is available.
//
// @Summary     List all GPUs
// @Description Returns a list of all GPU IDs for which telemetry data has been collected.
// @Tags        gpus
// @Produce     json
// @Success     200 {array}  store.GPU
// @Failure     500 {object} errorResponse
// @Router      /gpus [get]
func (h *Handler) ListGPUs(w http.ResponseWriter, r *http.Request) {
	gpus, err := h.db.ListGPUs(r.Context())
	if err != nil {
		h.logger.Error("ListGPUs failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to list GPUs")
		return
	}
	writeJSON(w, http.StatusOK, gpus)
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

	var startTime, endTime *time.Time
	if s := q.Get("start_time"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid start_time: must be RFC3339")
			return
		}
		startTime = &t
	}
	if e := q.Get("end_time"); e != "" {
		t, err := time.Parse(time.RFC3339, e)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid end_time: must be RFC3339")
			return
		}
		endTime = &t
	}

	limit := h.cfg.DefaultLimit
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		if n > h.cfg.MaxLimit {
			n = h.cfg.MaxLimit
		}
		limit = n
	}

	offset := 0
	if o := q.Get("offset"); o != "" {
		n, err := strconv.Atoi(o)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid offset")
			return
		}
		offset = n
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
