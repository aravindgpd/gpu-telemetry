// Package main is the entry point for the custom Message Queue broker service.
// The broker exposes a gRPC API used by Streamers (producers) and Collectors (consumers).
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/config"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/obs"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/server"
	"go.uber.org/zap"
)

const defaultTopic = "gpu-telemetry"

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync() //nolint:errcheck

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		logger.Fatal("failed to listen", zap.Int("port", cfg.GRPCPort), zap.Error(err))
	}

	grpcServer, broker, err := server.New(cfg, logger)
	if err != nil {
		logger.Fatal("failed to create gRPC server", zap.Error(err))
	}
	defer func() {
		if err := broker.Close(); err != nil {
			logger.Error("broker close error", zap.Error(err))
		}
	}()

	// Pre-provision the default telemetry topic so producers/consumers can
	// connect immediately on startup. Idempotent if already created.
	if err := broker.CreateTopic(defaultTopic, int32(cfg.Partitions)); err != nil {
		logger.Fatal("failed to create default topic", zap.Error(err))
	}

	// Observability sidecar: /healthz, /readyz, /metrics on a dedicated port.
	obsServer := obs.Start(cfg.MetricsPort, logger, func() error { return nil })
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obsServer.Shutdown(shutdownCtx)
	}()

	logger.Info("message queue broker starting",
		zap.Int("grpc_port", cfg.GRPCPort),
		zap.Int("metrics_port", cfg.MetricsPort),
		zap.Int("partitions", cfg.Partitions),
		zap.Int("ring_buffer_size", cfg.RingBufferSize),
		zap.String("wal_dir", cfg.WALDir),
		zap.String("default_topic", defaultTopic),
	)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down message queue broker")
	grpcServer.GracefulStop()
	logger.Info("message queue broker stopped")
}
