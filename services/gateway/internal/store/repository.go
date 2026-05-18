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

// GPUFilter narrows the set of GPUs returned by QueryGPUs. All fields are
// optional — empty strings mean "no filter on this dimension".
//
// ModelName and Hostname match case-insensitively as substrings (ILIKE %X%),
// so users can write `?model_name=h100` instead of the verbose
// `?model_name=NVIDIA%20H100%2080GB%20HBM3`. Exact strings still work
// because an exact string trivially matches itself as a substring.
type GPUFilter struct {
	ModelName string
	Hostname  string
	Limit     int
	Offset    int
}

// TelemetryFilter narrows the set of telemetry samples returned by
// QueryTelemetry. All fields are optional — zero values mean "no filter".
// ModelName triggers a JOIN against the gpus dimension table and matches
// case-insensitively as a substring (see GPUFilter).
type TelemetryFilter struct {
	UUID       string     // "" = any GPU (exact match — UUIDs are stable identifiers)
	MetricName string     // "" = any metric (exact match — metric names are well-known)
	ModelName  string     // "" = any model (substring/ILIKE match for user friendliness)
	StartTime  *time.Time // nil = no lower bound
	EndTime    *time.Time // nil = no upper bound
	Limit      int
	Offset     int
}

// ModelSummary is one row of "GPU model → how many we've seen", used by the
// /api/v1/models discovery endpoint so users can find the spelling of the
// model they care about without scanning all GPUs.
type ModelSummary struct {
	ModelName string `json:"model_name"`
	GPUCount  int    `json:"gpu_count"`
}

// Repository is the read-only data access interface used by the API Gateway.
type Repository interface {
	// ListGPUs returns all GPUs for which telemetry data exists.
	// Equivalent to QueryGPUs(GPUFilter{Limit: large}); kept for backwards compat.
	ListGPUs(ctx context.Context) ([]GPU, error)

	// QueryGPUs returns GPUs filtered by the supplied criteria, with pagination.
	QueryGPUs(ctx context.Context, f GPUFilter) ([]GPU, error)

	// GetTelemetry returns telemetry samples for ONE GPU ordered by sample_at ASC.
	// metricName is optional ("" = all metrics). startTime/endTime are optional
	// inclusive bounds on sample_at.
	GetTelemetry(
		ctx context.Context,
		uuid, metricName string,
		startTime, endTime *time.Time,
		limit, offset int,
	) ([]TelemetryRecord, error)

	// QueryTelemetry is the cross-GPU search variant. Any combination of
	// uuid/metric_name/model_name/time bounds may be applied. Returns
	// samples ordered by sample_at ASC.
	QueryTelemetry(ctx context.Context, f TelemetryFilter) ([]TelemetryRecord, error)

	// ListModels returns each distinct GPU model with a count of GPUs running
	// it. Used by the /api/v1/models discovery endpoint so callers know what
	// `model_name` values are accepted by the other filters.
	ListModels(ctx context.Context) ([]ModelSummary, error)

	// Ping checks database connectivity (used for readiness probes).
	Ping(ctx context.Context) error

	// Close releases connection pool resources.
	Close()
}
