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
func NewPostgres(ctx context.Context, dsn string, logger *zap.Logger) (Repository, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("pgxpool.New: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("database ping failed: %w", err)
	}
	return &postgresRepo{pool: pool, logger: logger}, nil
}

func (r *postgresRepo) Close() {
	r.pool.Close()
}

func (r *postgresRepo) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// ListGPUs returns all GPU dimension rows. Nullable columns are coalesced to
// empty strings so scanning into plain `string` fields is safe.
func (r *postgresRepo) ListGPUs(ctx context.Context) ([]GPU, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT uuid,
		        COALESCE(gpu_index, '') AS gpu_index,
		        COALESCE(device,    '') AS device,
		        model_name,
		        COALESCE(hostname,  '') AS hostname,
		        created_at, updated_at
		 FROM gpus
		 ORDER BY uuid`,
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
