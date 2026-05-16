package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

//go:embed migrations/*.up.sql
var migrationsFS embed.FS

// migrationsTable is a single-row-per-applied-migration ledger.
const migrationsTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT        PRIMARY KEY,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);`

// Migrate applies any unapplied migrations from the embedded migrations/ dir
// in version order. Each migration runs in its own transaction so a failure
// halfway through leaves the database in a consistent state.
func (r *postgresRepo) Migrate(ctx context.Context) error {
	if _, err := r.pool.Exec(ctx, migrationsTable); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	applied, err := loadAppliedVersions(ctx, r)
	if err != nil {
		return fmt.Errorf("load applied versions: %w", err)
	}

	files, err := listMigrationFiles()
	if err != nil {
		return fmt.Errorf("list migrations: %w", err)
	}

	for _, f := range files {
		version := versionFromFilename(f)
		if _, ok := applied[version]; ok {
			continue
		}

		body, err := migrationsFS.ReadFile("migrations/" + f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}

		r.logger.Info("applying migration", zap.String("version", version), zap.String("file", f))

		if err := r.applyOne(ctx, version, string(body)); err != nil {
			return fmt.Errorf("apply %s: %w", f, err)
		}
	}

	r.logger.Info("database migrations up to date", zap.Int("known", len(files)))
	return nil
}

// applyOne runs one migration's SQL plus the schema_migrations insert in a
// single transaction so partial application is impossible.
func (r *postgresRepo) applyOne(ctx context.Context, version, sql string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, sql); err != nil {
		return fmt.Errorf("exec migration: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, version); err != nil {
		return fmt.Errorf("record version: %w", err)
	}
	return tx.Commit(ctx)
}

func loadAppliedVersions(ctx context.Context, r *postgresRepo) (map[string]struct{}, error) {
	rows, err := r.pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = struct{}{}
	}
	return out, rows.Err()
}

func listMigrationFiles() ([]string, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		files = append(files, name)
	}
	if len(files) == 0 {
		return nil, errors.New("no migration files found in embedded fs")
	}
	sort.Strings(files) // numbered prefix gives natural lexicographic order
	return files, nil
}

// versionFromFilename extracts "0001" from "0001_initial_schema.up.sql".
func versionFromFilename(filename string) string {
	if idx := strings.Index(filename, "_"); idx > 0 {
		return filename[:idx]
	}
	return strings.TrimSuffix(filename, ".up.sql")
}
