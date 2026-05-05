// Package config loads Telemetry Collector configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration for the Collector.
type Config struct {
	MQAddress    string // MQ_ADDRESS (default: localhost:9090)
	Topic        string // MQ_TOPIC (default: gpu-telemetry)
	ConsumerID   string // CONSUMER_ID — unique per pod, set to pod name in k8s (default: collector-0)
	ConsumerGroup string // CONSUMER_GROUP (default: collector-group)
	DatabaseURL  string // DATABASE_URL — PostgreSQL DSN (required)
	MetricsPort  int    // METRICS_PORT (default: 9091)
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		MQAddress:     envStr("MQ_ADDRESS", "localhost:9090"),
		Topic:         envStr("MQ_TOPIC", "gpu-telemetry"),
		ConsumerID:    envStr("CONSUMER_ID", "collector-0"),
		ConsumerGroup: envStr("CONSUMER_GROUP", "collector-group"),
		DatabaseURL:   os.Getenv("DATABASE_URL"),
		MetricsPort:   envInt("METRICS_PORT", 9091),
	}

	if cfg.DatabaseURL == "" {
		return nil, fmt.Errorf("DATABASE_URL is required")
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
