// Package config loads Message Queue broker configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration for the MQ broker.
type Config struct {
	GRPCPort       int    // GRPC_PORT (default: 9090)
	MetricsPort    int    // METRICS_PORT (default: 9091)
	Partitions     int    // MQ_PARTITIONS (default: 10)
	RingBufferSize int    // MQ_RING_BUFFER_SIZE (default: 65536, must be power of 2)
	WALDir         string // MQ_WAL_DIR (default: /tmp/mq-wal)
	WALSyncBytes   int    // MQ_WAL_SYNC_BYTES (default: 4096)
	OverflowPolicy string // MQ_OVERFLOW_POLICY: "drop" or "block" (default: drop)
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		GRPCPort:       envInt("GRPC_PORT", 9090),
		MetricsPort:    envInt("METRICS_PORT", 9091),
		Partitions:     envInt("MQ_PARTITIONS", 10),
		RingBufferSize: envInt("MQ_RING_BUFFER_SIZE", 65536),
		WALDir:         envStr("MQ_WAL_DIR", "/tmp/mq-wal"),
		WALSyncBytes:   envInt("MQ_WAL_SYNC_BYTES", 4096),
		OverflowPolicy: envStr("MQ_OVERFLOW_POLICY", "drop"),
	}

	if !isPowerOfTwo(cfg.RingBufferSize) {
		return nil, fmt.Errorf("MQ_RING_BUFFER_SIZE must be a power of 2, got %d", cfg.RingBufferSize)
	}
	if cfg.OverflowPolicy != "drop" && cfg.OverflowPolicy != "block" {
		return nil, fmt.Errorf("MQ_OVERFLOW_POLICY must be 'drop' or 'block', got %q", cfg.OverflowPolicy)
	}

	return cfg, nil
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}
