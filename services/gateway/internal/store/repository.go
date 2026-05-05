// Package store defines the Repository interface and data models for the API Gateway.
package store

import (
	"context"
	"time"
)

// GPU represents a GPU dimension row from the gpus table.
type GPU struct {
	UUID      string    `json:"uuid"`
	GPUIndex  string    `json:"gpu_index"`
	Device    string    `json:"device"`
	ModelName string    `json:"model_name"`
	Hostname  string    `json:"hostname"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TelemetryRecord is a single DCGM metric sample for a GPU.
type TelemetryRecord struct {
	ID         int64     `json:"id"`
	UUID       string    `json:"uuid"`
	MetricName string    `json:"metric_name"`
	IngestedAt time.Time `json:"ingested_at"`
	SampleAt   time.Time `json:"sample_at"`
	Value      float64   `json:"value"`
	Container  string    `json:"container,omitempty"`
	Pod        string    `json:"pod,omitempty"`
	Namespace  string    `json:"namespace,omitempty"`
	LabelsRaw  string    `json:"labels_raw,omitempty"`
}

// Repository is the read-only data access interface used by the API Gateway.
type Repository interface {
	// ListGPUs returns all GPUs for which telemetry data exists.
	ListGPUs(ctx context.Context) ([]GPU, error)

	// GetTelemetry returns telemetry samples for a GPU ordered by sample_at ASC.
	// metricName is optional ("" = all metrics). startTime/endTime are optional
	// inclusive bounds on sample_at.
	GetTelemetry(
		ctx context.Context,
		uuid, metricName string,
		startTime, endTime *time.Time,
		limit, offset int,
	) ([]TelemetryRecord, error)

	// Ping checks database connectivity (used for readiness probes).
	Ping(ctx context.Context) error

	// Close releases connection pool resources.
	Close()
}
