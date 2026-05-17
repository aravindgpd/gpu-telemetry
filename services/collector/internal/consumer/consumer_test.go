package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	mqpb "github.com/aravindgpd/gpu-telemetry/proto/mq"
	telemetrypb "github.com/aravindgpd/gpu-telemetry/proto/telemetry"
	"github.com/aravindgpd/gpu-telemetry/collector/internal/config"
	"github.com/aravindgpd/gpu-telemetry/collector/internal/store"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// fakeStore is a minimal store.Repository for tests. It records every call so
// assertions can inspect what the consumer asked the DB to do.
type fakeStore struct {
	upsertCalls       []upsertArgs
	insertCalls       []store.TelemetryRecord
	upsertErr         error
	insertErr         error
	insertFirstNError int // first N InsertTelemetry calls return insertErr; subsequent succeed
}

type upsertArgs struct {
	UUID, GpuIndex, Device, ModelName, Hostname string
}

func (f *fakeStore) UpsertGPU(ctx context.Context, uuid, gpuIndex, device, modelName, hostname string) error {
	f.upsertCalls = append(f.upsertCalls, upsertArgs{uuid, gpuIndex, device, modelName, hostname})
	return f.upsertErr
}

func (f *fakeStore) InsertTelemetry(ctx context.Context, rec store.TelemetryRecord) error {
	f.insertCalls = append(f.insertCalls, rec)
	if f.insertFirstNError > 0 {
		f.insertFirstNError--
		return f.insertErr
	}
	return nil
}

func (f *fakeStore) Migrate(ctx context.Context) error    { return nil }
func (f *fakeStore) Ping(ctx context.Context) error       { return nil }
func (f *fakeStore) Close()                               {}

// newTestConsumer builds a Consumer wired to a fake store. No real gRPC dial
// happens — grpc.NewClient inside New is non-blocking, so the test never
// actually contacts the broker.
func newTestConsumer(t *testing.T, fs *fakeStore) *Consumer {
	t.Helper()
	c, err := New(testCfg(), fs, zap.NewNop())
	if err != nil {
		t.Fatalf("consumer.New: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// testCfg returns a config.Config with placeholder values. process() never
// reads cfg fields, but New() needs a non-empty MQAddress so grpc.NewClient
// doesn't reject it. The actual gRPC connection is lazy and never opens.
func testCfg() *config.Config {
	return &config.Config{
		MQAddress:     "127.0.0.1:65535",
		Topic:         "gpu-telemetry",
		ConsumerID:    "test-consumer",
		ConsumerGroup: "test-group",
		DatabaseURL:   "postgres://x",
	}
}

// telemetryDelivery builds a DeliveryMessage whose payload is a marshalled
// TelemetryRecord with the given fields.
func telemetryDelivery(t *testing.T, rec *telemetrypb.TelemetryRecord) *mqpb.DeliveryMessage {
	t.Helper()
	payload, err := proto.Marshal(rec)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	return &mqpb.DeliveryMessage{
		Partition:   0,
		Offset:      42,
		Payload:     payload,
		TimestampNs: time.Now().UnixNano(),
	}
}

// TestProcessSuccess: a well-formed delivery → UpsertGPU then InsertTelemetry,
// both with the right arguments.
func TestProcessSuccess(t *testing.T) {
	fs := &fakeStore{}
	c := newTestConsumer(t, fs)

	now := time.Now().UnixNano()
	msg := telemetryDelivery(t, &telemetrypb.TelemetryRecord{
		IngestedUnixNs: now,
		SampleUnixNs:   now,
		MetricName:     "DCGM_FI_DEV_GPU_UTIL",
		Uuid:           "GPU-aaa",
		GpuIndex:       "0",
		Device:         "nvidia0",
		ModelName:      "NVIDIA H100",
		Hostname:       "host-1",
		Value:          42.5,
		LabelsRaw:      "labels-here",
	})

	if err := c.process(context.Background(), msg); err != nil {
		t.Fatalf("process: %v", err)
	}

	if len(fs.upsertCalls) != 1 {
		t.Fatalf("UpsertGPU calls = %d, want 1", len(fs.upsertCalls))
	}
	if fs.upsertCalls[0].UUID != "GPU-aaa" || fs.upsertCalls[0].ModelName != "NVIDIA H100" {
		t.Errorf("UpsertGPU args wrong: %+v", fs.upsertCalls[0])
	}

	if len(fs.insertCalls) != 1 {
		t.Fatalf("InsertTelemetry calls = %d, want 1", len(fs.insertCalls))
	}
	rec := fs.insertCalls[0]
	if rec.UUID != "GPU-aaa" || rec.MetricName != "DCGM_FI_DEV_GPU_UTIL" || rec.Value != 42.5 {
		t.Errorf("InsertTelemetry args wrong: %+v", rec)
	}
	if rec.SampleAt.UnixNano() != now {
		t.Errorf("SampleAt = %v (ns=%d), want ns=%d", rec.SampleAt, rec.SampleAt.UnixNano(), now)
	}
}

// TestProcessInvalidPayload: bad protobuf bytes return an error and never
// touch the store.
func TestProcessInvalidPayload(t *testing.T) {
	fs := &fakeStore{}
	c := newTestConsumer(t, fs)

	msg := &mqpb.DeliveryMessage{
		Partition: 0,
		Offset:    1,
		Payload:   []byte{0xff, 0xff, 0xff}, // not a valid proto
	}

	if err := c.process(context.Background(), msg); err == nil {
		t.Error("expected error for invalid payload, got nil")
	}
	if len(fs.upsertCalls) != 0 || len(fs.insertCalls) != 0 {
		t.Error("store was touched despite proto.Unmarshal failure")
	}
}

// TestProcessUpsertError: UpsertGPU fails → process errors, InsertTelemetry
// is never called (FK constraint would fail anyway).
func TestProcessUpsertError(t *testing.T) {
	fs := &fakeStore{upsertErr: errors.New("constraint violation")}
	c := newTestConsumer(t, fs)

	msg := telemetryDelivery(t, &telemetrypb.TelemetryRecord{
		Uuid: "GPU-x", MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 1,
	})
	if err := c.process(context.Background(), msg); err == nil {
		t.Error("expected error from UpsertGPU, got nil")
	}
	if len(fs.insertCalls) != 0 {
		t.Errorf("InsertTelemetry should not run when UpsertGPU fails, got %d calls", len(fs.insertCalls))
	}
}

// TestProcessInsertError: InsertTelemetry fails → process errors after UpsertGPU.
func TestProcessInsertError(t *testing.T) {
	fs := &fakeStore{insertErr: errors.New("io error"), insertFirstNError: 1}
	c := newTestConsumer(t, fs)

	msg := telemetryDelivery(t, &telemetrypb.TelemetryRecord{
		Uuid: "GPU-x", MetricName: "DCGM_FI_DEV_GPU_UTIL", Value: 1,
	})
	if err := c.process(context.Background(), msg); err == nil {
		t.Error("expected error from InsertTelemetry, got nil")
	}
	if len(fs.upsertCalls) != 1 {
		t.Errorf("UpsertGPU should still have run, got %d calls", len(fs.upsertCalls))
	}
}

// TestProcessTimestampMapping: ingested_unix_ns and sample_unix_ns are mapped
// to time.Time correctly.
func TestProcessTimestampMapping(t *testing.T) {
	fs := &fakeStore{}
	c := newTestConsumer(t, fs)

	ingested := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	sample := ingested + int64(time.Second) // 1s later
	msg := telemetryDelivery(t, &telemetrypb.TelemetryRecord{
		IngestedUnixNs: ingested,
		SampleUnixNs:   sample,
		Uuid:           "GPU-y",
		MetricName:     "DCGM_FI_DEV_GPU_TEMP",
		Value:          65,
	})

	if err := c.process(context.Background(), msg); err != nil {
		t.Fatalf("process: %v", err)
	}
	rec := fs.insertCalls[0]
	if rec.IngestedAt.UnixNano() != ingested {
		t.Errorf("IngestedAt mismatch: got %d want %d", rec.IngestedAt.UnixNano(), ingested)
	}
	if rec.SampleAt.UnixNano() != sample {
		t.Errorf("SampleAt mismatch: got %d want %d", rec.SampleAt.UnixNano(), sample)
	}
}
