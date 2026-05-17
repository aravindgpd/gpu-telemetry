package config

import (
	"testing"
)

// TestLoadDefaults verifies the zero-env configuration matches documented defaults.
func TestLoadDefaults(t *testing.T) {
	clearEnv(t,
		"GRPC_PORT", "METRICS_PORT", "MQ_PARTITIONS",
		"MQ_RING_BUFFER_SIZE", "MQ_WAL_DIR", "MQ_WAL_SYNC_BYTES",
		"MQ_OVERFLOW_POLICY",
	)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.GRPCPort != 9090 {
		t.Errorf("GRPCPort = %d, want 9090", cfg.GRPCPort)
	}
	if cfg.MetricsPort != 9091 {
		t.Errorf("MetricsPort = %d, want 9091", cfg.MetricsPort)
	}
	if cfg.Partitions != 10 {
		t.Errorf("Partitions = %d, want 10", cfg.Partitions)
	}
	if cfg.RingBufferSize != 65536 {
		t.Errorf("RingBufferSize = %d, want 65536", cfg.RingBufferSize)
	}
	if cfg.OverflowPolicy != "drop" {
		t.Errorf("OverflowPolicy = %q, want drop", cfg.OverflowPolicy)
	}
}

// TestLoadOverridesFromEnv verifies env vars override defaults.
func TestLoadOverridesFromEnv(t *testing.T) {
	t.Setenv("GRPC_PORT", "5555")
	t.Setenv("MQ_PARTITIONS", "4")
	t.Setenv("MQ_RING_BUFFER_SIZE", "1024") // power of 2
	t.Setenv("MQ_OVERFLOW_POLICY", "block")
	t.Setenv("MQ_WAL_DIR", "/tmp/custom-wal")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.GRPCPort != 5555 {
		t.Errorf("GRPCPort = %d, want 5555", cfg.GRPCPort)
	}
	if cfg.Partitions != 4 {
		t.Errorf("Partitions = %d, want 4", cfg.Partitions)
	}
	if cfg.RingBufferSize != 1024 {
		t.Errorf("RingBufferSize = %d, want 1024", cfg.RingBufferSize)
	}
	if cfg.OverflowPolicy != "block" {
		t.Errorf("OverflowPolicy = %q, want block", cfg.OverflowPolicy)
	}
	if cfg.WALDir != "/tmp/custom-wal" {
		t.Errorf("WALDir = %q, want /tmp/custom-wal", cfg.WALDir)
	}
}

// TestLoadRejectsNonPowerOfTwoRingBuffer: the ring buffer requires a power of 2
// so the modulo can be optimised to a bitmask later. Validation enforces this.
func TestLoadRejectsNonPowerOfTwoRingBuffer(t *testing.T) {
	t.Setenv("MQ_RING_BUFFER_SIZE", "1000") // not a power of 2
	if _, err := Load(); err == nil {
		t.Error("Load should reject non-power-of-2 RingBufferSize")
	}
}

// TestLoadRejectsInvalidOverflowPolicy.
func TestLoadRejectsInvalidOverflowPolicy(t *testing.T) {
	t.Setenv("MQ_OVERFLOW_POLICY", "explode")
	if _, err := Load(); err == nil {
		t.Error("Load should reject unknown OverflowPolicy")
	}
}

// TestIsPowerOfTwo covers the helper directly.
func TestIsPowerOfTwo(t *testing.T) {
	cases := []struct {
		n    int
		want bool
	}{
		{0, false}, {-2, false},
		{1, true}, {2, true}, {4, true}, {1024, true}, {65536, true},
		{3, false}, {6, false}, {1000, false},
	}
	for _, c := range cases {
		if got := isPowerOfTwo(c.n); got != c.want {
			t.Errorf("isPowerOfTwo(%d) = %v, want %v", c.n, got, c.want)
		}
	}
}

// clearEnv unsets the named vars so a test sees true defaults regardless of
// what the developer has exported in their shell.
func clearEnv(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		t.Setenv(n, "")
	}
}
