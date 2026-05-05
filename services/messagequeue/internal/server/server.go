// Package server wires the gRPC server with the MQ broker.
package server

import (
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// New creates a configured gRPC server. The broker implementation is registered
// once the proto-generated code is available (Phase 2).
func New(cfg *config.Config, logger *zap.Logger) (*grpc.Server, error) {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor(logger)),
		grpc.ChainStreamInterceptor(loggingStreamInterceptor(logger)),
	)

	// Enable server reflection for tooling (grpcurl, etc.)
	reflection.Register(srv)

	// TODO(Phase 2): register MQ broker service
	// mqpb.RegisterMessageQueueServer(srv, broker.New(cfg, logger))

	return srv, nil
}
