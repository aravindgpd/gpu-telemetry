package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// postgresRepo implements Repository using a pgx connection pool.
type postgresRepo struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewPostgres creates a new PostgreSQL-backed Repository.
//
// Resilient to cold-start ordering: pool.Ping is wrapped in retry-with-backoff
// so the Gateway can start before Postgres on a different machine and wait
// politely instead of crashlooping.
func NewPostgres(ctx context.Context, dsn string, logger *zap.Logger) (Repository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}

	logger.Info("connecting to PostgreSQL")
	if err := retryWithBackoff(ctx, logger, "postgres.Ping", pool.Ping); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database ping (gave up): %w", err)
	}

	return &postgresRepo{pool: pool, logger: logger}, nil
}

func (r *postgresRepo) Close() {
	r.pool.Close()
}

func (r *postgresRepo) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// ListGPUs returns every GPU dimension row, unfiltered, unpaginated.
// Implemented as a thin wrapper over QueryGPUs with a generous limit so the
// existing API contract is preserved.
func (r *postgresRepo) ListGPUs(ctx context.Context) ([]GPU, error) {
	return r.QueryGPUs(ctx, GPUFilter{Limit: 10_000, Offset: 0})
}

// QueryGPUs returns GPU rows filtered by the supplied criteria. Empty
// ModelName/Hostname fields mean "no filter on this dimension" — the predicate
// is skipped via the NULL-or-equals pattern, which keeps the query plan stable
// regardless of which filters are populated.
//
// ModelName and Hostname use case-insensitive substring matching (ILIKE) so
// users can write `?model_name=h100` instead of the verbose
// `?model_name=NVIDIA%20H100%2080GB%20HBM3`. The user input is wrapped in '%'
// here, NOT in the query string — so users can't inject SQL wildcards.
func (r *postgresRepo) QueryGPUs(ctx context.Context, f GPUFilter) ([]GPU, error) {
	if f.Limit <= 0 {
		f.Limit = 100
	}
	if f.Offset < 0 {
		f.Offset = 0
	}

	modelArg := likeArg(f.ModelName)
	hostArg := likeArg(f.Hostname)

	rows, err := r.pool.Query(ctx,
		`SELECT uuid,
		        COALESCE(gpu_index, '') AS gpu_index,
		        COALESCE(device,    '') AS device,
		        model_name,
		        COALESCE(hostname,  '') AS hostname,
		        created_at, updated_at
		 FROM gpus
		 WHERE ($1::text IS NULL OR model_name ILIKE $1)
		   AND ($2::text IS NULL OR hostname   ILIKE $2)
		 ORDER BY uuid
		 LIMIT $3 OFFSET $4`,
		modelArg, hostArg, f.Limit, f.Offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query gpus: %w", err)
	}
	defer rows.Close()

	var gpus []GPU
	for rows.Next() {
		var g GPU
		if err := rows.Scan(
			&g.UUID, &g.GPUIndex, &g.Device, &g.ModelName,
			&g.Hostname, &g.CreatedAt, &g.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan gpu row: %w", err)
		}
		gpus = append(gpus, g)
	}
	return gpus, rows.Err()
}

// GetTelemetry returns telemetry samples for a GPU with optional metric and
// time-window filtering.
func (r *postgresRepo) GetTelemetry(
	ctx context.Context,
	uuid, metricName string,
	startTime, endTime *time.Time,
	limit, offset int,
) ([]TelemetryRecord, error) {
	// Empty metricName => no metric filter (NULL skips the predicate).
	var metricArg any
	if metricName != "" {
		metricArg = metricName
	}

	query := `
		SELECT id, uuid, metric_name, ingested_at, sample_at, value,
		       COALESCE(container,  '') AS container,
		       COALESCE(pod,        '') AS pod,
		       COALESCE(namespace,  '') AS namespace,
		       COALESCE(labels_raw, '') AS labels_raw
		FROM telemetry_samples
		WHERE uuid = $1
		  AND ($2::text        IS NULL OR metric_name = $2)
		  AND ($3::timestamptz IS NULL OR sample_at  >= $3)
		  AND ($4::timestamptz IS NULL OR sample_at  <= $4)
		ORDER BY sample_at ASC
		LIMIT $5 OFFSET $6`

	rows, err := r.pool.Query(ctx, query, uuid, metricArg, startTime, endTime, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("query telemetry: %w", err)
	}
	defer rows.Close()

	var records []TelemetryRecord
	for rows.Next() {
		var rec TelemetryRecord
		if err := rows.Scan(
			&rec.ID, &rec.UUID, &rec.MetricName,
			&rec.IngestedAt, &rec.SampleAt, &rec.Value,
			&rec.Container, &rec.Pod, &rec.Namespace, &rec.LabelsRaw,
		); err != nil {
			return nil, fmt.Errorf("scan telemetry row: %w", err)
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

// QueryTelemetry is the cross-GPU search. Any combination of UUID,
// MetricName, ModelName, and time bounds may be applied. ModelName triggers
// a JOIN with the gpus dimension table; the join is unconditional but the
// model_name predicate is NULL-skipped so plans stay stable when the filter
// isn't supplied.
func (r *postgresRepo) QueryTelemetry(ctx context.Context, f TelemetryFilter) ([]TelemetryRecord, error) {
	if f.Limit <= 0 {
		f.Limit = 100
	}
	if f.Offset < 0 {
		f.Offset = 0
	}

	var uuidArg, metricArg any
	if f.UUID != "" {
		uuidArg = f.UUID
	}
	if f.MetricName != "" {
		metricArg = f.MetricName
	}
	// model_name uses ILIKE substring matching for user friendliness (so
	// `model_name=h100` matches "NVIDIA H100 80GB HBM3"). UUIDs and metric
	// names are stable identifiers — they keep exact-match semantics.
	modelArg := likeArg(f.ModelName)

	query := `
		SELECT ts.id, ts.uuid, ts.metric_name, ts.ingested_at, ts.sample_at, ts.value,
		       COALESCE(ts.container,  '') AS container,
		       COALESCE(ts.pod,        '') AS pod,
		       COALESCE(ts.namespace,  '') AS namespace,
		       COALESCE(ts.labels_raw, '') AS labels_raw
		FROM telemetry_samples ts
		JOIN gpus g ON ts.uuid = g.uuid
		WHERE ($1::text        IS NULL OR ts.uuid        = $1)
		  AND ($2::text        IS NULL OR ts.metric_name = $2)
		  AND ($3::text        IS NULL OR g.model_name  ILIKE $3)
		  AND ($4::timestamptz IS NULL OR ts.sample_at  >= $4)
		  AND ($5::timestamptz IS NULL OR ts.sample_at  <= $5)
		ORDER BY ts.sample_at ASC
		LIMIT $6 OFFSET $7`

	rows, err := r.pool.Query(ctx, query,
		uuidArg, metricArg, modelArg, f.StartTime, f.EndTime, f.Limit, f.Offset,
	)
	if err != nil {
		return nil, fmt.Errorf("query telemetry (cross-gpu): %w", err)
	}
	defer rows.Close()

	var records []TelemetryRecord
	for rows.Next() {
		var rec TelemetryRecord
		if err := rows.Scan(
			&rec.ID, &rec.UUID, &rec.MetricName,
			&rec.IngestedAt, &rec.SampleAt, &rec.Value,
			&rec.Container, &rec.Pod, &rec.Namespace, &rec.LabelsRaw,
		); err != nil {
			return nil, fmt.Errorf("scan telemetry row: %w", err)
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

// ListModels returns each distinct GPU model with a count of GPUs running it.
// Used by the /api/v1/models discovery endpoint so users can find the exact
// model spelling without having to guess.
func (r *postgresRepo) ListModels(ctx context.Context) ([]ModelSummary, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT model_name, COUNT(*)::int AS gpu_count
		 FROM gpus
		 GROUP BY model_name
		 ORDER BY gpu_count DESC, model_name ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("query models: %w", err)
	}
	defer rows.Close()

	var out []ModelSummary
	for rows.Next() {
		var m ModelSummary
		if err := rows.Scan(&m.ModelName, &m.GPUCount); err != nil {
			return nil, fmt.Errorf("scan model row: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// likeArg wraps user input in '%' wildcards for ILIKE substring matching,
// preserving the "empty means no filter" contract by returning a nil any.
// Wildcards in the user's own string are NOT escaped — that's fine here
// because: (a) the value is bound as a parameter (no SQL injection), and
// (b) accidental % typed by a user just makes their search broader.
func likeArg(s string) any {
	if s == "" {
		return nil
	}
	return "%" + s + "%"
}
