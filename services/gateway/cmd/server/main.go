// Package main is the entry point for the API Gateway service.
// The Gateway exposes a REST API for querying GPU telemetry stored in PostgreSQL.
//
// @title           GPU Telemetry API
// @version         1.0
// @description     REST API for querying GPU telemetry data from an AI cluster.
// @termsOfService  http://swagger.io/terms/
//
// @contact.name   API Support
// @contact.email  aravindgpd@gmail.com
//
// @license.name  MIT
// @license.url   https://opensource.org/licenses/MIT
//
// @host      localhost:8080
// @BasePath  /api/v1
//
// @schemes http https
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aravindgpd/gpu-telemetry/gateway/internal/config"
	"github.com/aravindgpd/gpu-telemetry/gateway/internal/handler"
	"github.com/aravindgpd/gpu-telemetry/gateway/internal/store"
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

	router := handler.NewRouter(db, logger, cfg)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	logger.Info("API gateway starting",
		zap.Int("port", cfg.HTTPPort),
		zap.String("swagger_ui", fmt.Sprintf("http://localhost:%d/swagger/", cfg.HTTPPort)),
	)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down API gateway")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}
	logger.Info("API gateway stopped")
}
