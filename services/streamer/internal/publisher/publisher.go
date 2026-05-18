// Package publisher wraps the MQ gRPC client for publishing TelemetryRecord messages.
package publisher

import (
	"context"
	"fmt"
	"io"

	mqpb "github.com/aravindgpd/gpu-telemetry/proto/mq"
	telemetrypb "github.com/aravindgpd/gpu-telemetry/proto/telemetry"
	"github.com/aravindgpd/gpu-telemetry/streamer/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

// Publisher holds an open bidirectional Publish stream to the MQ broker.
type Publisher struct {
	cfg    *config.Config
	logger *zap.Logger
	conn   *grpc.ClientConn
	stream mqpb.MessageQueue_PublishClient
}

// New dials the MQ broker, ensures the topic exists, opens a Publish stream,
// and starts a background goroutine that drains acknowledgement responses.
//
// Resilient to cold-start ordering: if the MQ broker isn't reachable yet, the
// CreateTopic and Publish-stream-open calls retry with exponential backoff
// until they succeed or ctx is cancelled. This lets you start the Streamer
// before the MQ broker in any deployment topology — including different
// machines on different network paths — without crashlooping.
func New(ctx context.Context, cfg *config.Config, logger *zap.Logger) (*Publisher, error) {
	conn, err := grpc.NewClient(cfg.MQAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient %s: %w", cfg.MQAddress, err)
	}

	client := mqpb.NewMessageQueueClient(conn)

	// Wait for the broker to come up. CreateTopic is the first real RPC and
	// will fail until the broker is listening; the retry loop is what gives
	// us "start in any order" semantics.
	logger.Info("connecting to MQ broker",
		zap.String("address", cfg.MQAddress),
		zap.String("topic", cfg.Topic))

	createErr := retryWithBackoff(ctx, logger, "CreateTopic", func(ctx context.Context) error {
		resp, err := client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
			Topic:      cfg.Topic,
			Partitions: int32(cfg.StreamerTotal),
		})
		if err != nil {
			return err
		}
		if resp.Error != "" {
			// Broker is reachable; topic already exists (concurrent Streamer
			// pod won the race). Not a retryable error.
			logger.Info("topic already exists, continuing",
				zap.String("topic", cfg.Topic),
				zap.String("broker_note", resp.Error))
		}
		return nil
	})
	if createErr != nil {
		conn.Close()
		return nil, fmt.Errorf("CreateTopic %q: %w", cfg.Topic, createErr)
	}

	var stream mqpb.MessageQueue_PublishClient
	openErr := retryWithBackoff(ctx, logger, "open Publish stream", func(ctx context.Context) error {
		s, err := client.Publish(ctx)
		if err != nil {
			return err
		}
		stream = s
		return nil
	})
	if openErr != nil {
		conn.Close()
		return nil, fmt.Errorf("open Publish stream: %w", openErr)
	}

	p := &Publisher{cfg: cfg, logger: logger, conn: conn, stream: stream}
	go p.drainResponses()
	return p, nil
}

// Publish serialises rec as a proto payload and sends it to the given partition.
func (p *Publisher) Publish(_ context.Context, rec *telemetrypb.TelemetryRecord, partition int32) error {
	payload, err := proto.Marshal(rec)
	if err != nil {
		return fmt.Errorf("proto.Marshal: %w", err)
	}
	return p.stream.Send(&mqpb.PublishRequest{
		Topic:     p.cfg.Topic,
		Partition: partition,
		Payload:   payload,
	})
}

// Close gracefully shuts down the Publish stream and the gRPC connection.
func (p *Publisher) Close() {
	if p.stream != nil {
		_ = p.stream.CloseSend()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
}

// drainResponses consumes PublishResponse messages from the broker so that
// gRPC flow-control does not stall the send path.
func (p *Publisher) drainResponses() {
	for {
		resp, err := p.stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			p.logger.Debug("publish response stream closed", zap.Error(err))
			return
		}
		if resp.Error != "" {
			p.logger.Warn("broker rejected publish",
				zap.Int32("partition", resp.Partition),
				zap.Int64("offset", resp.Offset),
				zap.String("error", resp.Error))
		}
	}
}
