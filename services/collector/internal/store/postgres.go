package store

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type postgresRepo struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// NewPostgres creates a PostgreSQL-backed Repository for the Collector.
//
// Resilient to cold-start ordering: pgxpool.New itself is non-blocking, so we
// then loop on pool.Ping with exponential backoff until the database becomes
// reachable or ctx is cancelled. This lets the Collector start in any order
// relative to Postgres — including on a different machine over a network that
// hasn't converged yet.
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

func (r *postgresRepo) Close() { r.pool.Close() }

func (r *postgresRepo) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

// Migrate is implemented in migrate.go (it loads embedded SQL files from
// migrations/ and applies any not yet recorded in schema_migrations).

// UpsertGPU inserts a GPU dimension row, refreshing the non-key fields on conflict
// so they reflect the most recently observed state.
func (r *postgresRepo) UpsertGPU(ctx context.Context, uuid, gpuIndex, device, modelName, hostname string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO gpus (uuid, gpu_index, device, model_name, hostname)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (uuid) DO UPDATE SET
		   gpu_index  = EXCLUDED.gpu_index,
		   device     = EXCLUDED.device,
		   model_name = EXCLUDED.model_name,
		   hostname   = EXCLUDED.hostname,
		   updated_at = NOW()`,
		uuid, gpuIndex, device, modelName, hostname,
	)
	if err != nil {
		return fmt.Errorf("upsert gpu %s: %w", uuid, err)
	}
	return nil
}

// InsertTelemetry persists a telemetry sample, ignoring duplicates by
// (uuid, metric_name, sample_at).
func (r *postgresRepo) InsertTelemetry(ctx context.Context, rec TelemetryRecord) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO telemetry_samples
		  (uuid, metric_name, ingested_at, sample_at, value,
		   container, pod, namespace, labels_raw)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (uuid, metric_name, sample_at) DO NOTHING`,
		rec.UUID, rec.MetricName, rec.IngestedAt, rec.SampleAt, rec.Value,
		rec.Container, rec.Pod, rec.Namespace, rec.LabelsRaw,
	)
	if err != nil {
		return fmt.Errorf("insert telemetry %s/%s at %v: %w",
			rec.UUID, rec.MetricName, rec.SampleAt, err)
	}
	return nil
}
