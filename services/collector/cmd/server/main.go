// Package main is the entry point for the Telemetry Collector service.
// The Collector subscribes to the custom Message Queue, parses telemetry
// messages and persists them to PostgreSQL.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aravindgpd/gpu-telemetry/collector/internal/config"
	"github.com/aravindgpd/gpu-telemetry/collector/internal/consumer"
	"github.com/aravindgpd/gpu-telemetry/collector/internal/obs"
	"github.com/aravindgpd/gpu-telemetry/collector/internal/store"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	db, err := store.NewPostgres(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		logger.Fatal("failed to connect to database", zap.Error(err))
	}
	defer db.Close()

	if err := db.Migrate(ctx); err != nil {
		logger.Fatal("failed to run migrations", zap.Error(err))
	}

	c, err := consumer.New(cfg, db, logger)
	if err != nil {
		logger.Fatal("failed to create consumer", zap.Error(err))
	}
	defer c.Close()

	// Observability sidecar: /healthz, /readyz, /metrics. Readiness reports DB health.
	obsServer := obs.Start(cfg.MetricsPort, logger, func() error {
		return db.Ping(context.Background())
	})
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obsServer.Shutdown(shutdownCtx)
	}()

	logger.Info("telemetry collector starting",
		zap.String("consumer_id", cfg.ConsumerID),
		zap.String("mq_address", cfg.MQAddress),
		zap.String("topic", cfg.Topic),
		zap.Int("metrics_port", cfg.MetricsPort),
	)

	if err := c.Run(ctx); err != nil {
		logger.Error("collector exited with error", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("telemetry collector stopped")
}
