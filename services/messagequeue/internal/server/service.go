package server

import (
	"context"
	"errors"
	"io"

	mqpb "github.com/aravindgpd/gpu-telemetry/proto/mq"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/broker"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// service implements mqpb.MessageQueueServer by delegating to a broker.
type service struct {
	mqpb.UnimplementedMessageQueueServer
	broker *broker.Broker
	cfg    *config.Config
	logger *zap.Logger
}

// NewService constructs the gRPC-facing service backed by the supplied broker.
func NewService(b *broker.Broker, cfg *config.Config, logger *zap.Logger) mqpb.MessageQueueServer {
	return &service{broker: b, cfg: cfg, logger: logger}
}

// CreateTopic provisions a new topic. Idempotent: re-creating with the same
// partition count is a no-op.
func (s *service) CreateTopic(ctx context.Context, req *mqpb.CreateTopicRequest) (*mqpb.CreateTopicResponse, error) {
	if err := s.broker.CreateTopic(req.Topic, req.Partitions); err != nil {
		return &mqpb.CreateTopicResponse{Ok: false, Error: err.Error()}, nil
	}
	return &mqpb.CreateTopicResponse{Ok: true}, nil
}

// Publish handles bidirectional streaming: the producer sends PublishRequests
// continuously, and the broker returns one PublishResponse per request with
// the assigned offset.
func (s *service) Publish(stream mqpb.MessageQueue_PublishServer) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		assignedPartition, offset, pubErr := s.broker.Publish(
			req.Topic, req.Partition, req.Payload, req.Headers,
		)

		resp := &mqpb.PublishResponse{
			Partition: assignedPartition,
			Offset:    offset,
		}
		if pubErr != nil {
			resp.Error = pubErr.Error()
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// Subscribe pushes a server-streaming sequence of DeliveryMessages to the
// consumer. The stream ends when (a) the consumer's gRPC context is cancelled,
// or (b) the broker rebalances this member out — in which case a final
// DeliveryMessage with rebalance_signal=true is sent before returning.
func (s *service) Subscribe(req *mqpb.SubscribeRequest, stream mqpb.MessageQueue_SubscribeServer) error {
	sub, err := s.broker.Subscribe(req.Topic, req.ConsumerGroup, req.ConsumerId)
	if err != nil {
		return status.Error(codes.NotFound, err.Error())
	}
	defer sub.Cleanup()

	ctx := stream.Context()

	for {
		// 1) First, drain anything currently available across all owned partitions.
		delivered := sub.PollOnce()
		for _, d := range delivered {
			msg := &mqpb.DeliveryMessage{
				Partition:   d.Partition,
				Offset:      d.Offset(),
				Payload:     d.Payload(),
				Headers:     d.Headers(),
				TimestampNs: d.Timestamp(),
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}

		// 2) If we delivered something, immediately try again — a fast publisher
		//    may have queued more by now.
		if len(delivered) > 0 {
			continue
		}

		// 3) Nothing was available. Block until: ctx cancelled, rebalance, or new data.
		select {
		case <-ctx.Done():
			return nil

		case <-sub.Done():
			// Rebalance exit: the same consumer will re-Subscribe immediately,
			// so we must NOT remove it from the group — the deferred Cleanup
			// would otherwise flip the member count and trigger a fresh
			// rebalance for the surviving members (ping-pong storm).
			sub.SkipLeaveOnCleanup()
			// Final rebalance signal so the client's existing handler ([consumer.go:97])
			// returns from runOnce and reconnects.
			rebalanceMsg := &mqpb.DeliveryMessage{
				RebalanceSignal:    true,
				AssignedPartitions: sub.AssignedPartitions(),
			}
			_ = stream.Send(rebalanceMsg)
			return nil

		case <-sub.Notify():
			// At least one owned partition has new data — loop back to PollOnce.
			continue
		}
	}
}

// Acknowledge commits a processed offset back to the broker.
func (s *service) Acknowledge(ctx context.Context, req *mqpb.AcknowledgeRequest) (*mqpb.AcknowledgeResponse, error) {
	if err := s.broker.Acknowledge(req.Topic, req.ConsumerGroup, req.Partition, req.Offset); err != nil {
		return &mqpb.AcknowledgeResponse{Ok: false, Error: err.Error()}, nil
	}
	return &mqpb.AcknowledgeResponse{Ok: true}, nil
}

// GetOffsets returns the committed offsets for all partitions of a consumer group.
func (s *service) GetOffsets(ctx context.Context, req *mqpb.GetOffsetsRequest) (*mqpb.GetOffsetsResponse, error) {
	offsets, err := s.broker.GetOffsets(req.Topic, req.ConsumerGroup)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &mqpb.GetOffsetsResponse{PartitionOffsets: offsets}, nil
}

// HealthCheck returns broker readiness.
func (s *service) HealthCheck(ctx context.Context, req *mqpb.HealthCheckRequest) (*mqpb.HealthCheckResponse, error) {
	return &mqpb.HealthCheckResponse{
		Ready:   true,
		Version: "0.1.0",
	}, nil
}
