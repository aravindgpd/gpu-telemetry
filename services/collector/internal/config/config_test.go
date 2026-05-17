package config

import "testing"

// TestLoadDefaults — DATABASE_URL must be set; others fall back to documented defaults.
func TestLoadDefaults(t *testing.T) {
	clearEnv(t, "MQ_ADDRESS", "MQ_TOPIC", "CONSUMER_ID", "CONSUMER_GROUP", "METRICS_PORT")
	t.Setenv("DATABASE_URL", "postgres://user:pwd@localhost/db")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.MQAddress != "localhost:9090" {
		t.Errorf("MQAddress = %q", cfg.MQAddress)
	}
	if cfg.Topic != "gpu-telemetry" {
		t.Errorf("Topic = %q", cfg.Topic)
	}
	if cfg.ConsumerID != "collector-0" {
		t.Errorf("ConsumerID = %q", cfg.ConsumerID)
	}
	if cfg.ConsumerGroup != "collector-group" {
		t.Errorf("ConsumerGroup = %q", cfg.ConsumerGroup)
	}
	if cfg.MetricsPort != 9091 {
		t.Errorf("MetricsPort = %d", cfg.MetricsPort)
	}
}

// TestLoadRequiresDatabaseURL: without DATABASE_URL the Collector can't run.
func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if _, err := Load(); err == nil {
		t.Error("Load should require DATABASE_URL")
	}
}

// TestLoadEnvOverrides verifies each env var wins.
func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("MQ_ADDRESS", "broker:9090")
	t.Setenv("CONSUMER_ID", "pod-7")
	t.Setenv("CONSUMER_GROUP", "alt-group")
	t.Setenv("METRICS_PORT", "9999")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQAddress != "broker:9090" {
		t.Errorf("MQAddress = %q", cfg.MQAddress)
	}
	if cfg.ConsumerID != "pod-7" {
		t.Errorf("ConsumerID = %q", cfg.ConsumerID)
	}
	if cfg.ConsumerGroup != "alt-group" {
		t.Errorf("ConsumerGroup = %q", cfg.ConsumerGroup)
	}
	if cfg.MetricsPort != 9999 {
		t.Errorf("MetricsPort = %d", cfg.MetricsPort)
	}
}

func clearEnv(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		t.Setenv(n, "")
	}
}
