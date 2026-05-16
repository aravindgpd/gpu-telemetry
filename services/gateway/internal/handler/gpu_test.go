package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aravindgpd/gpu-telemetry/gateway/internal/config"
	"github.com/aravindgpd/gpu-telemetry/gateway/internal/store"
	"go.uber.org/zap"
)

// fakeRepo is a hand-rolled mock for store.Repository. Test cases set the
// fields they care about; unset method behaviours fall back to safe defaults.
type fakeRepo struct {
	gpus       []store.GPU
	gpusErr    error
	telemetry  []store.TelemetryRecord
	telErr     error
	pingErr    error
	lastUUID   string
	lastMetric string
	lastStart  *time.Time
	lastEnd    *time.Time
	lastLimit  int
	lastOffset int
}

func (r *fakeRepo) ListGPUs(ctx context.Context) ([]store.GPU, error) {
	return r.gpus, r.gpusErr
}

func (r *fakeRepo) GetTelemetry(
	ctx context.Context,
	uuid, metricName string,
	startTime, endTime *time.Time,
	limit, offset int,
) ([]store.TelemetryRecord, error) {
	r.lastUUID = uuid
	r.lastMetric = metricName
	r.lastStart = startTime
	r.lastEnd = endTime
	r.lastLimit = limit
	r.lastOffset = offset
	return r.telemetry, r.telErr
}

func (r *fakeRepo) Ping(ctx context.Context) error { return r.pingErr }
func (r *fakeRepo) Close()                         {}

// newTestServer builds a Handler + chi router with the supplied fake repo and
// returns an httptest server ready to accept requests.
func newTestServer(t *testing.T, repo *fakeRepo) *httptest.Server {
	t.Helper()
	cfg := &config.Config{
		HTTPPort:     0,
		DefaultLimit: 100,
		MaxLimit:     1000,
	}
	r := NewRouter(repo, zap.NewNop(), cfg)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv
}

// ─── /gpus ─────────────────────────────────────────────────────────────────────

// TestListGPUs_Success returns the seeded GPUs as JSON.
func TestListGPUs_Success(t *testing.T) {
	repo := &fakeRepo{
		gpus: []store.GPU{
			{UUID: "GPU-aaa", ModelName: "H100"},
			{UUID: "GPU-bbb", ModelName: "A100"},
		},
	}
	srv := newTestServer(t, repo)

	resp, err := http.Get(srv.URL + "/api/v1/gpus")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	var got []store.GPU
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != 2 || got[0].UUID != "GPU-aaa" {
		t.Errorf("body = %+v, want 2 GPUs starting with GPU-aaa", got)
	}
}

// TestListGPUs_DBError returns 500 when the repository errors.
func TestListGPUs_DBError(t *testing.T) {
	repo := &fakeRepo{gpusErr: errors.New("boom")}
	srv := newTestServer(t, repo)

	resp, err := http.Get(srv.URL + "/api/v1/gpus")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// ─── /gpus/{id}/telemetry ──────────────────────────────────────────────────────

// TestGetTelemetry_Success_AllParams threads every supported query parameter
// through and verifies it lands at the repo.
func TestGetTelemetry_Success_AllParams(t *testing.T) {
	repo := &fakeRepo{
		telemetry: []store.TelemetryRecord{
			{ID: 1, UUID: "GPU-aaa", MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 42},
		},
	}
	srv := newTestServer(t, repo)

	url := srv.URL + "/api/v1/gpus/GPU-aaa/telemetry" +
		"?metric_name=DCGM_FI_DEV_GPU_UTIL" +
		"&start_time=2025-07-18T20:42:30Z" +
		"&end_time=2025-07-18T20:42:50Z" +
		"&limit=50&offset=10"

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	if repo.lastUUID != "GPU-aaa" {
		t.Errorf("lastUUID = %q", repo.lastUUID)
	}
	if repo.lastMetric != "DCGM_FI_DEV_GPU_UTIL" {
		t.Errorf("lastMetric = %q", repo.lastMetric)
	}
	if repo.lastStart == nil || repo.lastEnd == nil {
		t.Errorf("expected both start/end to be parsed, got start=%v end=%v", repo.lastStart, repo.lastEnd)
	}
	if repo.lastLimit != 50 || repo.lastOffset != 10 {
		t.Errorf("lastLimit=%d lastOffset=%d", repo.lastLimit, repo.lastOffset)
	}
}

// TestGetTelemetry_DefaultLimit: omitted ?limit gets the configured default.
func TestGetTelemetry_DefaultLimit(t *testing.T) {
	repo := &fakeRepo{}
	srv := newTestServer(t, repo)

	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-x/telemetry")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if repo.lastLimit != 100 { // matches cfg.DefaultLimit
		t.Errorf("lastLimit = %d, want 100", repo.lastLimit)
	}
}

// TestGetTelemetry_LimitCappedAtMax: ?limit=99999 must be capped at MaxLimit.
func TestGetTelemetry_LimitCappedAtMax(t *testing.T) {
	repo := &fakeRepo{}
	srv := newTestServer(t, repo)

	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-x/telemetry?limit=99999")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if repo.lastLimit != 1000 { // matches cfg.MaxLimit
		t.Errorf("lastLimit = %d, want 1000 (capped)", repo.lastLimit)
	}
}

// TestGetTelemetry_BadLimit: non-numeric limit yields 400.
func TestGetTelemetry_BadLimit(t *testing.T) {
	srv := newTestServer(t, &fakeRepo{})
	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-x/telemetry?limit=abc")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// TestGetTelemetry_BadStartTime: malformed RFC3339 yields 400.
func TestGetTelemetry_BadStartTime(t *testing.T) {
	srv := newTestServer(t, &fakeRepo{})
	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-x/telemetry?start_time=yesterday")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
	body, _ := readAll(resp.Body)
	if !strings.Contains(body, "start_time") {
		t.Errorf("error body should mention start_time; got %q", body)
	}
}

// TestGetTelemetry_BadOffset: negative offset yields 400.
func TestGetTelemetry_BadOffset(t *testing.T) {
	srv := newTestServer(t, &fakeRepo{})
	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-x/telemetry?offset=-5")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

// TestGetTelemetry_DBError: repository error → 500 with JSON body.
func TestGetTelemetry_DBError(t *testing.T) {
	repo := &fakeRepo{telErr: errors.New("db down")}
	srv := newTestServer(t, repo)

	resp, err := http.Get(srv.URL + "/api/v1/gpus/GPU-x/telemetry")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", resp.StatusCode)
	}
}

// ─── /healthz, /readyz ─────────────────────────────────────────────────────────

// TestHealthz: always 200 regardless of DB state.
func TestHealthz(t *testing.T) {
	srv := newTestServer(t, &fakeRepo{pingErr: errors.New("db down")})
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz = %d, want 200 even with bad DB", resp.StatusCode)
	}
}

// TestReadyz_DBHealthy: DB ping succeeds → 200.
func TestReadyz_DBHealthy(t *testing.T) {
	srv := newTestServer(t, &fakeRepo{}) // pingErr=nil
	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", resp.StatusCode)
	}
}

// TestReadyz_DBDown: DB ping fails → 503.
func TestReadyz_DBDown(t *testing.T) {
	srv := newTestServer(t, &fakeRepo{pingErr: errors.New("connection refused")})
	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d, want 503", resp.StatusCode)
	}
}

// helpers

func readAll(r interface{ Read([]byte) (int, error) }) (string, error) {
	buf := make([]byte, 4096)
	n, err := r.Read(buf)
	if err != nil && err.Error() != "EOF" {
		return "", err
	}
	return string(buf[:n]), nil
}
