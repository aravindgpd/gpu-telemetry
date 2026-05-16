// Package consumer subscribes to the MQ topic and persists telemetry records
// to PostgreSQL via the store.Repository interface.
package consumer

import (
	"context"
	"fmt"
	"time"

	mqpb "github.com/aravindgpd/gpu-telemetry/proto/mq"
	telemetrypb "github.com/aravindgpd/gpu-telemetry/proto/telemetry"
	"github.com/aravindgpd/gpu-telemetry/collector/internal/config"
	"github.com/aravindgpd/gpu-telemetry/collector/internal/store"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const reconnectDelay = 2 * time.Second

// Consumer subscribes to the MQ broker and writes each received record to
// PostgreSQL.  It reconnects automatically on stream errors and respects
// broker rebalance signals.
type Consumer struct {
	cfg    *config.Config
	db     store.Repository
	logger *zap.Logger
	conn   *grpc.ClientConn
	client mqpb.MessageQueueClient
}

// New dials the MQ broker and returns a Consumer ready to call Run.
func New(cfg *config.Config, db store.Repository, logger *zap.Logger) (*Consumer, error) {
	conn, err := grpc.NewClient(cfg.MQAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc.NewClient %s: %w", cfg.MQAddress, err)
	}
	return &Consumer{
		cfg:    cfg,
		db:     db,
		logger: logger,
		conn:   conn,
		client: mqpb.NewMessageQueueClient(conn),
	}, nil
}

// Close releases the gRPC connection.
func (c *Consumer) Close() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

// Run subscribes to the configured topic and processes messages until ctx is
// cancelled.  Stream errors and rebalance signals both trigger a reconnect.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := c.runOnce(ctx); err != nil {
			c.logger.Error("subscription error, reconnecting",
				zap.String("consumer_id", c.cfg.ConsumerID),
				zap.Error(err))
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(reconnectDelay):
			}
		}
	}
}

// runOnce opens one Subscribe stream and processes messages until the stream
// closes or ctx is cancelled.  It returns nil on a clean rebalance so Run
// immediately reconnects without delay.
func (c *Consumer) runOnce(ctx context.Context) error {
	stream, err := c.client.Subscribe(ctx, &mqpb.SubscribeRequest{
		Topic:         c.cfg.Topic,
		ConsumerGroup: c.cfg.ConsumerGroup,
		ConsumerId:    c.cfg.ConsumerID,
	})
	if err != nil {
		return fmt.Errorf("Subscribe: %w", err)
	}

	c.logger.Info("subscribed to topic",
		zap.String("topic", c.cfg.Topic),
		zap.String("consumer_id", c.cfg.ConsumerID),
		zap.String("group", c.cfg.ConsumerGroup))

	for {
		msg, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream.Recv: %w", err)
		}

		if msg.RebalanceSignal {
			c.logger.Info("rebalance signal received, reconnecting",
				zap.Int32s("new_partitions", msg.AssignedPartitions))
			return nil
		}

		if err := c.process(ctx, msg); err != nil {
			// Log and continue: a single bad message should not stop the consumer.
			c.logger.Error("failed to process message",
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err))
			continue
		}

		if err := c.ack(ctx, msg.Partition, msg.Offset); err != nil {
			c.logger.Warn("acknowledge failed",
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err))
		}
	}
}

// process deserialises the MQ payload, upserts the GPU dimension row, and
// inserts the telemetry sample.
func (c *Consumer) process(ctx context.Context, msg *mqpb.DeliveryMessage) error {
	var rec telemetrypb.TelemetryRecord
	if err := proto.Unmarshal(msg.Payload, &rec); err != nil {
		return fmt.Errorf("proto.Unmarshal: %w", err)
	}

	if err := c.db.UpsertGPU(ctx, rec.Uuid, rec.GpuIndex, rec.Device, rec.ModelName, rec.Hostname); err != nil {
		return fmt.Errorf("UpsertGPU %s: %w", rec.Uuid, err)
	}

	if err := c.db.InsertTelemetry(ctx, store.TelemetryRecord{
		UUID:       rec.Uuid,
		MetricName: rec.MetricName,
		IngestedAt: time.Unix(0, rec.IngestedUnixNs),
		SampleAt:   time.Unix(0, rec.SampleUnixNs),
		Value:      rec.Value,
		Container:  rec.Container,
		Pod:        rec.Pod,
		Namespace:  rec.Namespace,
		LabelsRaw:  rec.LabelsRaw,
	}); err != nil {
		return fmt.Errorf("InsertTelemetry %s/%s: %w", rec.Uuid, rec.MetricName, err)
	}

	return nil
}

// ack commits the processed offset back to the broker.
func (c *Consumer) ack(ctx context.Context, partition int32, offset int64) error {
	resp, err := c.client.Acknowledge(ctx, &mqpb.AcknowledgeRequest{
		Topic:         c.cfg.Topic,
		ConsumerGroup: c.cfg.ConsumerGroup,
		Partition:     partition,
		Offset:        offset,
	})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("broker declined ack: %s", resp.Error)
	}
	return nil
}
