// Package server wires the gRPC server with the MQ broker.
package server

import (
	mqpb "github.com/aravindgpd/gpu-telemetry/proto/mq"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/broker"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// New creates a configured gRPC server with the MessageQueue service registered
// and the broker attached. The returned broker should be Close()d on shutdown.
func New(cfg *config.Config, logger *zap.Logger) (*grpc.Server, *broker.Broker, error) {
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(loggingUnaryInterceptor(logger)),
		grpc.ChainStreamInterceptor(loggingStreamInterceptor(logger)),
	)

	b := broker.New(cfg, logger)

	mqpb.RegisterMessageQueueServer(srv, NewService(b, cfg, logger))
	reflection.Register(srv)

	return srv, b, nil
}
