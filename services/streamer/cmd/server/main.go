// Package main is the entry point for the Telemetry Streamer service.
// The Streamer reads GPU telemetry from a CSV file in a loop and publishes
// each row to the custom Message Queue broker.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aravindgpd/gpu-telemetry/streamer/internal/config"
	"github.com/aravindgpd/gpu-telemetry/streamer/internal/coordinator"
	"github.com/aravindgpd/gpu-telemetry/streamer/internal/obs"
	"github.com/aravindgpd/gpu-telemetry/streamer/internal/publisher"
	"github.com/aravindgpd/gpu-telemetry/streamer/internal/reader"
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

	coord := coordinator.New(cfg.StreamerIndex, cfg.StreamerTotal)

	csvReader := reader.New(cfg.CSVPath, cfg.StreamIntervalMs, logger)

	// Observability sidecar starts FIRST so /healthz answers immediately —
	// even while we wait for the MQ broker to come up. This means a healthy
	// "I'm alive but my dependency isn't ready" liveness probe works in any
	// deployment topology (different machines, restarts in any order).
	obsServer := obs.Start(cfg.MetricsPort, logger, func() error { return nil })
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obsServer.Shutdown(shutdownCtx)
	}()

	logger.Info("telemetry streamer starting",
		zap.Int("index", cfg.StreamerIndex),
		zap.Int("total", cfg.StreamerTotal),
		zap.Int("metrics_port", cfg.MetricsPort),
		zap.String("csv_path", cfg.CSVPath),
		zap.String("mq_address", cfg.MQAddress),
	)

	// Wait (with backoff) for the MQ broker to be reachable. publisher.New
	// retries internally until ctx is cancelled, so this returns nil quickly
	// when the broker is already up and politely waits otherwise.
	pub, err := publisher.New(ctx, cfg, logger)
	if err != nil {
		// Only reached if ctx is cancelled before the broker came up
		// (i.e. SIGTERM during the wait). Treat as a clean shutdown.
		logger.Info("streamer shutting down before publisher was ready", zap.Error(err))
		return
	}
	defer pub.Close()

	if err := csvReader.Stream(ctx, coord, pub); err != nil {
		logger.Error("streamer exited with error", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("telemetry streamer stopped")
}
