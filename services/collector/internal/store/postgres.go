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

func (r *postgresRepo) Close() { r.pool.Close() }

func (r *postgresRepo) Ping(ctx context.Context) error { return r.pool.Ping(ctx) }

// Migrate runs the embedded SQL migrations.
func (r *postgresRepo) Migrate(ctx context.Context) error {
	_, err := r.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	r.logger.Info("database migrations applied")
	return nil
}

// schema defines the full database DDL. Idempotent — safe to run on every start.
//
// Long-format design: one row per <gpu, metric_name, sample_at> sample,
// matching the DCGM exporter CSV layout.
const schema = `
CREATE TABLE IF NOT EXISTS gpus (
    uuid       VARCHAR(64)  PRIMARY KEY,
    gpu_index  VARCHAR(8),
    device     VARCHAR(32),
    model_name VARCHAR(128) NOT NULL,
    hostname   VARCHAR(128),
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS telemetry_samples (
    id           BIGSERIAL        PRIMARY KEY,
    uuid         VARCHAR(64)      NOT NULL REFERENCES gpus(uuid),
    metric_name  VARCHAR(64)      NOT NULL,
    ingested_at  TIMESTAMPTZ      NOT NULL,
    sample_at    TIMESTAMPTZ      NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    container    VARCHAR(128),
    pod          VARCHAR(128),
    namespace    VARCHAR(128),
    labels_raw   TEXT,
    UNIQUE (uuid, metric_name, sample_at)
);

CREATE INDEX IF NOT EXISTS telemetry_samples_uuid_time
    ON telemetry_samples (uuid, sample_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_samples_metric_time
    ON telemetry_samples (metric_name, sample_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_samples_uuid_metric_time
    ON telemetry_samples (uuid, metric_name, sample_at DESC);
`

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
