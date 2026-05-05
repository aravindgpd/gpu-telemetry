// Package config loads API Gateway configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds all runtime configuration for the API Gateway.
type Config struct {
	HTTPPort    int    // HTTP_PORT (default: 8080)
	MetricsPort int    // METRICS_PORT (default: 9091)
	DatabaseURL string // DATABASE_URL — PostgreSQL DSN (required)
	MaxLimit    int    // API_MAX_LIMIT — max page size for telemetry queries (default: 1000)
	DefaultLimit int   // API_DEFAULT_LIMIT — default page size (default: 100)
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (*Config, error) {
	cfg := &Config{
		HTTPPort:     envInt("HTTP_PORT", 8080),
		MetricsPort:  envInt("METRICS_PORT", 9091),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		MaxLimit:     envInt("API_MAX_LIMIT", 1000),
		DefaultLimit: envInt("API_DEFAULT_LIMIT", 100),
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
