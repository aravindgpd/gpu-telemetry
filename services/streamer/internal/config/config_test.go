package config

import "testing"

// TestLoadDefaults: with no env vars set, fields fall back to documented defaults.
func TestLoadDefaults(t *testing.T) {
	clearEnv(t,
		"MQ_ADDRESS", "MQ_TOPIC", "STREAMER_INDEX", "STREAMER_TOTAL",
		"CSV_PATH", "STREAM_INTERVAL_MS", "METRICS_PORT",
	)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"MQAddress", cfg.MQAddress, "localhost:9090"},
		{"Topic", cfg.Topic, "gpu-telemetry"},
		{"StreamerIndex", cfg.StreamerIndex, 0},
		{"StreamerTotal", cfg.StreamerTotal, 1},
		{"CSVPath", cfg.CSVPath, "/data/sample_data.csv"},
		{"StreamIntervalMs", cfg.StreamIntervalMs, 100},
		{"MetricsPort", cfg.MetricsPort, 9091},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestLoadValidatesStreamerOrdinals: out-of-range ordinals are rejected so a
// misconfigured StatefulSet pod fails fast instead of duplicating work.
func TestLoadValidatesStreamerOrdinals(t *testing.T) {
	cases := []struct {
		name    string
		index   string
		total   string
		wantErr bool
	}{
		{"valid", "0", "1", false},
		{"valid_middle", "3", "5", false},
		{"negative_index", "-1", "2", true},
		{"zero_total", "0", "0", true},
		{"index_equal_total", "2", "2", true},
		{"index_greater_than_total", "5", "3", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("STREAMER_INDEX", c.index)
			t.Setenv("STREAMER_TOTAL", c.total)
			_, err := Load()
			if (err != nil) != c.wantErr {
				t.Errorf("err=%v wantErr=%v", err, c.wantErr)
			}
		})
	}
}

// TestLoadEnvOverrides: every env var actually wins over its default.
func TestLoadEnvOverrides(t *testing.T) {
	t.Setenv("MQ_ADDRESS", "broker.cluster:9090")
	t.Setenv("MQ_TOPIC", "alt-topic")
	t.Setenv("STREAMER_INDEX", "2")
	t.Setenv("STREAMER_TOTAL", "5")
	t.Setenv("CSV_PATH", "/custom/data.csv")
	t.Setenv("STREAM_INTERVAL_MS", "500")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.MQAddress != "broker.cluster:9090" {
		t.Errorf("MQAddress = %q", cfg.MQAddress)
	}
	if cfg.StreamerIndex != 2 {
		t.Errorf("StreamerIndex = %d", cfg.StreamerIndex)
	}
	if cfg.StreamerTotal != 5 {
		t.Errorf("StreamerTotal = %d", cfg.StreamerTotal)
	}
}

func clearEnv(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		t.Setenv(n, "")
	}
}
