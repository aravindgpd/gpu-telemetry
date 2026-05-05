// Package store handles PostgreSQL persistence for the Telemetry Collector.
package store

import (
	"context"
	"time"
)

// TelemetryRecord is the parsed, ready-to-persist telemetry sample.
// It mirrors one DCGM exporter CSV row (long/melted format: one record per
// <timestamp, gpu, metric_name> sample).
type TelemetryRecord struct {
	UUID       string    // GPU UUID, e.g. "GPU-5fd4f087-86f3-7a43-..."
	MetricName string    // e.g. "DCGM_FI_DEV_GPU_UTIL"
	IngestedAt time.Time // streamer publish time (canonical telemetry timestamp)
	SampleAt   time.Time // original DCGM scrape timestamp from CSV
	Value      float64   // numeric metric value (covers int and float DCGM fields)
	Container  string    // optional, k8s context
	Pod        string    // optional, k8s context
	Namespace  string    // optional, k8s context
	LabelsRaw  string    // raw Prometheus label string from CSV, unparsed
}

// Repository defines the write operations the Collector needs.
type Repository interface {
	// UpsertGPU ensures a GPU dimension row exists keyed by uuid. The
	// non-key fields are refreshed on every call to track current state
	// (e.g. host reassignment, device renumbering).
	UpsertGPU(ctx context.Context, uuid, gpuIndex, device, modelName, hostname string) error

	// InsertTelemetry persists a telemetry sample. Duplicates with the same
	// (uuid, metric_name, sample_at) are silently ignored — the unique
	// constraint provides idempotent re-delivery.
	InsertTelemetry(ctx context.Context, rec TelemetryRecord) error

	// Migrate runs database migrations on startup.
	Migrate(ctx context.Context) error

	// Ping checks database connectivity.
	Ping(ctx context.Context) error

	// Close releases connection pool resources.
	Close()
}
