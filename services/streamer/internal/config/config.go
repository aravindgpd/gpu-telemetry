// Package config loads Telemetry Streamer configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration for the Streamer.
type Config struct {
	MQAddress        string // MQ_ADDRESS (default: localhost:9090)
	Topic            string // MQ_TOPIC (default: gpu-telemetry)
	StreamerIndex    int    // STREAMER_INDEX — pod ordinal, 0-based (default: 0)
	StreamerTotal    int    // STREAMER_TOTAL — total streamer replicas (default: 1)
	CSVPath          string // CSV_PATH (default: /data/gpu_telemetry.csv)
	StreamIntervalMs int    // STREAM_INTERVAL_MS — delay between rows in ms (default: 100)
	MetricsPort      int    // METRICS_PORT (default: 9091)
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		MQAddress:        envStr("MQ_ADDRESS", "localhost:9090"),
		Topic:            envStr("MQ_TOPIC", "gpu-telemetry"),
		StreamerIndex:    envInt("STREAMER_INDEX", 0),
		StreamerTotal:    envInt("STREAMER_TOTAL", 1),
		CSVPath:          envStr("CSV_PATH", "/data/sample_data.csv"),
		StreamIntervalMs: envInt("STREAM_INTERVAL_MS", 100),
		MetricsPort:      envInt("METRICS_PORT", 9091),
	}

	if cfg.StreamerIndex < 0 {
		return nil, fmt.Errorf("STREAMER_INDEX must be >= 0, got %d", cfg.StreamerIndex)
	}
	if cfg.StreamerTotal < 1 {
		return nil, fmt.Errorf("STREAMER_TOTAL must be >= 1, got %d", cfg.StreamerTotal)
	}
	if cfg.StreamerIndex >= cfg.StreamerTotal {
		// This is almost always caused by scaling the StatefulSet via
		// `kubectl scale` instead of `helm upgrade`. `kubectl scale` adds
		// new pods (so STREAMER_INDEX gets values >= old replica count)
		// but does NOT re-render the pod template, so STREAMER_TOTAL stays
		// at the old value. Always scale via Helm:
		//   helm upgrade <release> <chart> --reuse-values --set streamer.replicaCount=N
		return nil, fmt.Errorf(
			"STREAMER_INDEX (%d) must be < STREAMER_TOTAL (%d); "+
				"this usually means the StatefulSet was scaled via 'kubectl scale' "+
				"(which leaves STREAMER_TOTAL stale). Rerun with "+
				"'helm upgrade --reuse-values --set streamer.replicaCount=%d' so the "+
				"pod template is re-rendered with the new fleet size.",
			cfg.StreamerIndex, cfg.StreamerTotal, cfg.StreamerIndex+1)
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
