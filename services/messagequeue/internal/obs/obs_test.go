package obs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"
)

// startOnFreePort binds to :0 to get a free port, returning the chosen port
// and the running Server. Cleanup is wired via t.Cleanup.
func startOnFreePort(t *testing.T, ready ReadyFunc) (int, *Server) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()

	srv := Start(port, zap.NewNop(), ready)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	// Give the server a moment to bind before we hit it.
	waitForServer(t, port, 2*time.Second)
	return port, srv
}

// waitForServer polls until the obs port accepts a TCP connection or the
// timeout elapses.
func waitForServer(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not start on port %d within %v", port, timeout)
}

// TestHealthzAlwaysOK verifies /healthz is 200 regardless of the readiness func.
func TestHealthzAlwaysOK(t *testing.T) {
	port, _ := startOnFreePort(t, func() error { return errors.New("not ready") })
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", port))
	if err != nil {
		t.Fatalf("Get /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/healthz status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "ok") {
		t.Errorf("/healthz body = %q, want to contain 'ok'", body)
	}
}

// TestReadyzReportsReadyState: ready func nil/nil-error → 200, error → 503.
func TestReadyzReportsReadyState(t *testing.T) {
	cases := []struct {
		name       string
		ready      ReadyFunc
		wantStatus int
	}{
		{"nil func", nil, http.StatusOK},
		{"returns nil", func() error { return nil }, http.StatusOK},
		{"returns err", func() error { return errors.New("db down") }, http.StatusServiceUnavailable},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			port, _ := startOnFreePort(t, c.ready)
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", port))
			if err != nil {
				t.Fatalf("Get /readyz: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != c.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, c.wantStatus)
			}
		})
	}
}

// TestMetricsEndpointServesPrometheus checks the /metrics endpoint returns
// a Prometheus-formatted body (recognisable by the # HELP comments).
func TestMetricsEndpointServesPrometheus(t *testing.T) {
	port, _ := startOnFreePort(t, nil)
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if err != nil {
		t.Fatalf("Get /metrics: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/metrics status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "# HELP") {
		t.Errorf("/metrics body missing Prometheus markers; first 100 bytes: %q", body[:min(100, len(body))])
	}
}

// TestShutdownIsIdempotent and works on a nil server.
func TestShutdownIsIdempotent(t *testing.T) {
	var nilSrv *Server
	if err := nilSrv.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown on nil server should be no-op, got %v", err)
	}

	port, srv := startOnFreePort(t, nil)
	_ = port
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Errorf("first Shutdown: %v", err)
	}
	// Second call should also be safe.
	if err := srv.Shutdown(ctx); err != nil && err != http.ErrServerClosed {
		t.Errorf("second Shutdown: %v", err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
