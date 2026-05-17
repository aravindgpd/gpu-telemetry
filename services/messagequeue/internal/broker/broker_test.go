package broker

import (
	"errors"
	"testing"
	"time"

	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/config"
	"go.uber.org/zap"
)

// brokerCfg returns a default broker config rooted in t.TempDir() so each test
// gets its own WAL directory.
func brokerCfg(t *testing.T) *config.Config {
	t.Helper()
	return &config.Config{
		GRPCPort:       9090,
		MetricsPort:    9091,
		Partitions:     4,
		RingBufferSize: 16,
		WALDir:         t.TempDir(),
		WALSyncBytes:   0,
		OverflowPolicy: "drop",
	}
}

// TestBrokerCreateTopicAndPublish: end-to-end through the orchestrator.
func TestBrokerCreateTopicAndPublish(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })

	if err := b.CreateTopic("t1", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	partition, offset, err := b.Publish("t1", 2, []byte("hello"), nil)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if partition != 2 || offset != 0 {
		t.Errorf("Publish returned (partition=%d, offset=%d), want (2, 0)", partition, offset)
	}
}

// TestBrokerCreateTopicIdempotent: re-creating with the same partition count
// is a no-op; different partition count is an error.
func TestBrokerCreateTopicIdempotent(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })

	if err := b.CreateTopic("t1", 4); err != nil {
		t.Fatalf("first CreateTopic: %v", err)
	}
	if err := b.CreateTopic("t1", 4); err != nil {
		t.Errorf("re-create with same partitions should be nil, got %v", err)
	}
	if err := b.CreateTopic("t1", 8); !errors.Is(err, errTopicExists) {
		t.Errorf("re-create with different partitions: expected errTopicExists, got %v", err)
	}
}

// TestBrokerCreateTopicEmptyName: name="" must be rejected.
func TestBrokerCreateTopicEmptyName(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })

	if err := b.CreateTopic("", 4); err == nil {
		t.Error("CreateTopic with empty name should error")
	}
}

// TestBrokerPublishUnknownTopic returns errTopicNotFound.
func TestBrokerPublishUnknownTopic(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })

	if _, _, err := b.Publish("missing", 0, []byte("x"), nil); !errors.Is(err, errTopicNotFound) {
		t.Errorf("expected errTopicNotFound, got %v", err)
	}
}

// TestBrokerPublishRoundRobin: partition=-1 distributes across all partitions.
func TestBrokerPublishRoundRobin(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })
	if err := b.CreateTopic("t1", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}

	used := make(map[int32]int)
	for i := 0; i < 12; i++ {
		p, _, err := b.Publish("t1", -1, []byte("x"), nil)
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		used[p]++
	}
	if len(used) != 4 {
		t.Errorf("expected all 4 partitions used, got %v", used)
	}
}

// TestBrokerSubscribeRequiresMemberID.
func TestBrokerSubscribeRequiresMemberID(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })
	_ = b.CreateTopic("t1", 4)

	if _, err := b.Subscribe("t1", "grp", ""); err == nil {
		t.Error("Subscribe with empty memberID should error")
	}
}

// TestBrokerSubscribeUnknownTopic returns errTopicNotFound.
func TestBrokerSubscribeUnknownTopic(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })

	if _, err := b.Subscribe("nope", "grp", "c0"); !errors.Is(err, errTopicNotFound) {
		t.Errorf("expected errTopicNotFound, got %v", err)
	}
}

// TestBrokerSubscribeAndPoll: subscribe, publish, then PollOnce returns the message.
func TestBrokerSubscribeAndPoll(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })
	_ = b.CreateTopic("t1", 4)

	sub, err := b.Subscribe("t1", "grp", "c0")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(sub.Cleanup)

	// Publish to a partition this subscriber owns. Sole member → owns all.
	if _, _, err := b.Publish("t1", 0, []byte("hello"), nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Brief wait for the partition's Append to flush; PollOnce is non-blocking.
	delivered := waitForDelivery(t, sub, 1, time.Second)
	if len(delivered) != 1 {
		t.Fatalf("expected 1 delivered, got %d", len(delivered))
	}
	if string(delivered[0].Payload()) != "hello" {
		t.Errorf("payload = %q, want hello", delivered[0].Payload())
	}
}

// TestBrokerAcknowledgeAndOffsets: acks advance committed; GetOffsets reflects state.
func TestBrokerAcknowledgeAndOffsets(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })
	_ = b.CreateTopic("t1", 4)

	// A Subscribe is needed to materialise the group.
	sub, err := b.Subscribe("t1", "grp", "c0")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(sub.Cleanup)

	if err := b.Acknowledge("t1", "grp", 0, 17); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	if err := b.Acknowledge("t1", "grp", 2, 5); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}

	offsets, err := b.GetOffsets("t1", "grp")
	if err != nil {
		t.Fatalf("GetOffsets: %v", err)
	}
	if offsets[0] != 17 || offsets[2] != 5 {
		t.Errorf("offsets = %v, want {0:17, 2:5}", offsets)
	}
}

// TestBrokerAcknowledgeUnknownGroup errors.
func TestBrokerAcknowledgeUnknownGroup(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })
	_ = b.CreateTopic("t1", 4)

	if err := b.Acknowledge("t1", "nope", 0, 1); err == nil {
		t.Error("Acknowledge on unknown group should error")
	}
}

// TestBrokerGetOffsetsUnknownGroup returns an empty map (no commits yet) not an error.
func TestBrokerGetOffsetsUnknownGroup(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })
	_ = b.CreateTopic("t1", 4)

	offsets, err := b.GetOffsets("t1", "nope")
	if err != nil {
		t.Errorf("GetOffsets should not error for unknown group, got %v", err)
	}
	if len(offsets) != 0 {
		t.Errorf("offsets = %v, want empty", offsets)
	}
}

// TestBrokerWALReplaysOnRestart: a publish persists to WAL, broker close+reopen
// rebuilds the partition and the offset survives.
func TestBrokerWALReplaysOnRestart(t *testing.T) {
	cfg := brokerCfg(t)

	// First broker: publish two messages, close.
	b1 := New(cfg, zap.NewNop())
	if err := b1.CreateTopic("t1", 4); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := b1.Publish("t1", 0, []byte("survivor"), nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := b1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second broker: same WAL dir. CreateTopic must replay the records.
	b2 := New(cfg, zap.NewNop())
	t.Cleanup(func() { _ = b2.Close() })
	if err := b2.CreateTopic("t1", 4); err != nil {
		t.Fatalf("CreateTopic after restart: %v", err)
	}

	// Subscribe and poll — should see both surviving messages on partition 0.
	sub, err := b2.Subscribe("t1", "grp", "c0")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(sub.Cleanup)

	delivered := waitForDelivery(t, sub, 2, time.Second)
	if len(delivered) != 2 {
		t.Fatalf("expected 2 replayed messages, got %d", len(delivered))
	}
	for _, d := range delivered {
		if string(d.Payload()) != "survivor" {
			t.Errorf("payload = %q, want survivor", d.Payload())
		}
	}
}

// TestSubscriptionSkipLeaveOnCleanupDeferEviction: cleanup with skipLeave does
// NOT immediately remove the member from the group. Confirms the grace-timer
// behaviour without actually waiting 30s.
func TestSubscriptionSkipLeaveOnCleanupDeferEviction(t *testing.T) {
	b := New(brokerCfg(t), zap.NewNop())
	t.Cleanup(func() { _ = b.Close() })
	_ = b.CreateTopic("t1", 4)

	sub, err := b.Subscribe("t1", "grp", "c0")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	sub.SkipLeaveOnCleanup()
	sub.Cleanup()

	// Member should still be present in the group.
	key := groupKey{topic: "t1", group: "grp"}
	g := b.groups[key]
	if g == nil {
		t.Fatal("group not registered")
	}
	g.mu.Lock()
	_, stillThere := g.members["c0"]
	g.mu.Unlock()
	if !stillThere {
		t.Error("member was removed on skipLeave Cleanup — grace period not honoured")
	}
}

// helpers

// waitForDelivery polls the subscription until `expected` messages are
// delivered or the timeout fires. PollOnce is non-blocking, so we sleep
// briefly between calls.
func waitForDelivery(t *testing.T, sub *Subscription, expected int, timeout time.Duration) []DeliveredSlot {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var out []DeliveredSlot
	for time.Now().Before(deadline) && len(out) < expected {
		out = append(out, sub.PollOnce()...)
		if len(out) < expected {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return out
}
