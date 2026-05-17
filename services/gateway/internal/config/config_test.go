package config

import "testing"

// TestLoadDefaults: with DATABASE_URL set, all other fields take documented defaults.
func TestLoadDefaults(t *testing.T) {
	clearEnv(t, "HTTP_PORT", "METRICS_PORT", "API_MAX_LIMIT", "API_DEFAULT_LIMIT")
	t.Setenv("DATABASE_URL", "postgres://user:pwd@localhost/db")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want 8080", cfg.HTTPPort)
	}
	if cfg.MetricsPort != 9091 {
		t.Errorf("MetricsPort = %d", cfg.MetricsPort)
	}
	if cfg.MaxLimit != 1000 {
		t.Errorf("MaxLimit = %d, want 1000", cfg.MaxLimit)
	}
	if cfg.DefaultLimit != 100 {
		t.Errorf("DefaultLimit = %d, want 100", cfg.DefaultLimit)
	}
}

// TestLoadRequiresDatabaseURL.
func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Error("Load should require DATABASE_URL")
	}
}

// TestLoadEnvOverrides.
func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("HTTP_PORT", "9080")
	t.Setenv("API_MAX_LIMIT", "5000")
	t.Setenv("API_DEFAULT_LIMIT", "25")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPPort != 9080 {
		t.Errorf("HTTPPort = %d", cfg.HTTPPort)
	}
	if cfg.MaxLimit != 5000 {
		t.Errorf("MaxLimit = %d", cfg.MaxLimit)
	}
	if cfg.DefaultLimit != 25 {
		t.Errorf("DefaultLimit = %d", cfg.DefaultLimit)
	}
}

func clearEnv(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		t.Setenv(n, "")
	}
}
