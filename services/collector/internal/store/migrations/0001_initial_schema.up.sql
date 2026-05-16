-- 0001_initial_schema.up.sql
-- Long/melted format: one row per <gpu, metric_name, sample_at> sample.
-- Mirrors the DCGM exporter CSV layout.

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
