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

	pub, err := publisher.New(cfg, logger)
	if err != nil {
		logger.Fatal("failed to create publisher", zap.Error(err))
	}
	defer pub.Close()

	csvReader := reader.New(cfg.CSVPath, cfg.StreamIntervalMs, logger)

	// Observability sidecar: /healthz, /readyz, /metrics on a dedicated port.
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

	if err := csvReader.Stream(ctx, coord, pub); err != nil {
		logger.Error("streamer exited with error", zap.Error(err))
		os.Exit(1)
	}
	logger.Info("telemetry streamer stopped")
}
