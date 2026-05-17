package server

import (
	"context"
	"net"
	"testing"
	"time"

	mqpb "github.com/aravindgpd/gpu-telemetry/proto/mq"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/broker"
	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// startServer wires a real Broker behind an in-process gRPC server bound to a
// bufconn listener. Returns a client connected over the same listener.
func startServer(t *testing.T) (mqpb.MessageQueueClient, *broker.Broker) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	cfg := &config.Config{
		GRPCPort:       0,
		MetricsPort:    0,
		Partitions:     4,
		RingBufferSize: 16,
		WALDir:         t.TempDir(),
		WALSyncBytes:   0,
		OverflowPolicy: "drop",
	}
	logger := zap.NewNop()

	b := broker.New(cfg, logger)
	if err := b.CreateTopic("gpu-telemetry", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	srv := grpc.NewServer()
	mqpb.RegisterMessageQueueServer(srv, NewService(b, cfg, logger))

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.GracefulStop()
		_ = b.Close()
	})

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return mqpb.NewMessageQueueClient(conn), b
}

// TestHealthCheckResponds: simplest unary RPC — sanity check that the wiring
// works end-to-end.
func TestServiceHealthCheck(t *testing.T) {
	client, _ := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.HealthCheck(ctx, &mqpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("HealthCheck: %v", err)
	}
	if !resp.Ready {
		t.Errorf("Ready = false, want true")
	}
}

// TestCreateTopicIdempotent: re-creating an existing topic with the same
// partition count succeeds via the service layer.
func TestServiceCreateTopicIdempotent(t *testing.T) {
	client, _ := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
		Topic:      "gpu-telemetry",
		Partitions: 4,
	})
	if err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	if !resp.Ok {
		t.Errorf("Ok = false, error=%q", resp.Error)
	}
}

// TestCreateTopicMismatchPartitions returns ok=false with a descriptive error.
func TestServiceCreateTopicMismatchPartitions(t *testing.T) {
	client, _ := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, _ := client.CreateTopic(ctx, &mqpb.CreateTopicRequest{
		Topic:      "gpu-telemetry",
		Partitions: 99, // existing topic has 4
	})
	if resp.Ok {
		t.Error("CreateTopic with mismatched partition count should report ok=false")
	}
	if resp.Error == "" {
		t.Error("expected non-empty error message")
	}
}

// TestPublishStream sends two messages over the bidi Publish stream and
// verifies the broker assigns sequential offsets.
func TestServicePublishStream(t *testing.T) {
	client, _ := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := client.Publish(ctx)
	if err != nil {
		t.Fatalf("Publish open: %v", err)
	}

	for i := 0; i < 2; i++ {
		if err := stream.Send(&mqpb.PublishRequest{
			Topic:     "gpu-telemetry",
			Partition: 0,
			Payload:   []byte{byte(i)},
		}); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		if resp.Offset != int64(i) {
			t.Errorf("offset = %d, want %d", resp.Offset, i)
		}
		if resp.Partition != 0 {
			t.Errorf("partition = %d, want 0", resp.Partition)
		}
	}
	_ = stream.CloseSend()
}

// TestAcknowledgeAndGetOffsets round-trips a commit and reads it back.
func TestServiceAcknowledgeAndGetOffsets(t *testing.T) {
	client, _ := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Subscribe first so the consumer group is materialised on the broker side.
	sub, err := client.Subscribe(ctx, &mqpb.SubscribeRequest{
		Topic:         "gpu-telemetry",
		ConsumerGroup: "grp",
		ConsumerId:    "c0",
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Drain initial state; we don't care about the messages.
	go func() {
		for {
			if _, err := sub.Recv(); err != nil {
				return
			}
		}
	}()
	time.Sleep(100 * time.Millisecond) // let the group register

	ack, err := client.Acknowledge(ctx, &mqpb.AcknowledgeRequest{
		Topic:         "gpu-telemetry",
		ConsumerGroup: "grp",
		Partition:     2,
		Offset:        17,
	})
	if err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if !ack.Ok {
		t.Errorf("Ack ok=false, error=%q", ack.Error)
	}

	offsets, err := client.GetOffsets(ctx, &mqpb.GetOffsetsRequest{
		Topic:         "gpu-telemetry",
		ConsumerGroup: "grp",
	})
	if err != nil {
		t.Fatalf("GetOffsets: %v", err)
	}
	if got := offsets.PartitionOffsets[2]; got != 17 {
		t.Errorf("offsets[2] = %d, want 17", got)
	}
}

// TestSubscribeReceivesPublishedMessage exercises the full Publish→Subscribe
// path: publisher writes one message, subscriber receives it on the right
// partition with the right offset.
func TestServiceSubscribeReceivesPublishedMessage(t *testing.T) {
	client, _ := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	sub, err := client.Subscribe(ctx, &mqpb.SubscribeRequest{
		Topic:         "gpu-telemetry",
		ConsumerGroup: "grp",
		ConsumerId:    "c0",
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// Publish in a separate goroutine after a brief wait so the subscriber is
	// already polling.
	go func() {
		time.Sleep(50 * time.Millisecond)
		pub, _ := client.Publish(context.Background())
		_ = pub.Send(&mqpb.PublishRequest{
			Topic:     "gpu-telemetry",
			Partition: 1,
			Payload:   []byte("hello-world"),
		})
		_, _ = pub.Recv()
		_ = pub.CloseSend()
	}()

	msg, err := sub.Recv()
	if err != nil {
		t.Fatalf("sub.Recv: %v", err)
	}
	if msg.Partition != 1 || string(msg.Payload) != "hello-world" {
		t.Errorf("got partition=%d payload=%q", msg.Partition, msg.Payload)
	}
}
