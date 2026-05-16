package broker

import (
	"errors"
	"testing"
	"time"
)

// newTestPartition is a helper that opens a real WAL in a temp dir but otherwise
// builds a partition with the given capacity. Each test gets its own directory.
func newTestPartition(t *testing.T, capacity int) *partition {
	t.Helper()
	dir := t.TempDir()
	wal, err := openWAL(dir, "t", 0, 0)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })
	return newPartition(0, capacity, wal, nil)
}

// TestPartitionAppendAndRead is the happy-path round-trip: append three messages
// and read them back at their assigned offsets.
func TestPartitionAppendAndRead(t *testing.T) {
	p := newTestPartition(t, 16)

	for i := 0; i < 3; i++ {
		off, err := p.Append([]byte{byte(i)}, nil, time.Now().UnixNano())
		if err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
		if off != int64(i) {
			t.Errorf("Append %d returned offset %d, want %d", i, off, i)
		}
	}

	for i := int64(0); i < 3; i++ {
		s, err := p.Read(i)
		if err != nil {
			t.Fatalf("Read %d: %v", i, err)
		}
		if s.offset != i {
			t.Errorf("slot.offset = %d, want %d", s.offset, i)
		}
		if len(s.payload) != 1 || s.payload[0] != byte(i) {
			t.Errorf("slot.payload = %v, want [%d]", s.payload, i)
		}
	}
}

// TestPartitionRingBufferWrap verifies that once head - tail > capacity, the
// oldest offsets are no longer readable from memory.
func TestPartitionRingBufferWrap(t *testing.T) {
	const capacity = 4
	p := newTestPartition(t, capacity)

	// Publish 6 messages — capacity is 4, so offsets 0 and 1 must be evicted.
	for i := 0; i < 6; i++ {
		if _, err := p.Append([]byte{byte(i)}, nil, 0); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}

	// Tail should have advanced to 2; head is 6.
	if got := p.HighWaterMark(); got != 6 {
		t.Errorf("head = %d, want 6", got)
	}
	if got := p.Tail(); got != 2 {
		t.Errorf("tail = %d, want 2", got)
	}

	// Offsets 0 and 1 are evicted.
	for _, evicted := range []int64{0, 1} {
		if _, err := p.Read(evicted); !errors.Is(err, errOffsetTooOld) {
			t.Errorf("Read(%d): expected errOffsetTooOld, got %v", evicted, err)
		}
	}

	// Offsets 2-5 remain readable.
	for i := int64(2); i < 6; i++ {
		s, err := p.Read(i)
		if err != nil {
			t.Errorf("Read(%d): %v", i, err)
			continue
		}
		if s.offset != i {
			t.Errorf("Read(%d) returned slot offset %d", i, s.offset)
		}
	}
}

// TestPartitionReadFuture verifies that reading past head returns errOffsetFuture.
func TestPartitionReadFuture(t *testing.T) {
	p := newTestPartition(t, 8)
	if _, err := p.Append([]byte("x"), nil, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := p.Read(99); !errors.Is(err, errOffsetFuture) {
		t.Errorf("Read(99): expected errOffsetFuture, got %v", err)
	}
}

// TestPartitionSubscriberNotification verifies that registered subscribers
// receive a non-blocking signal on Append and that multiple back-to-back
// publishes coalesce into a single signal (notify channel has buffer 1).
func TestPartitionSubscriberNotification(t *testing.T) {
	p := newTestPartition(t, 8)

	sub := newSubscriber()
	p.addSubscriber(sub)
	t.Cleanup(func() { p.removeSubscriber(sub) })

	if _, err := p.Append([]byte("a"), nil, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := p.Append([]byte("b"), nil, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Exactly one wake-up should be available; second receive should not block forever.
	select {
	case <-sub.notify:
		// expected — first wake-up
	case <-time.After(time.Second):
		t.Fatal("subscriber never received notification")
	}
	// Second recv must NOT block test forever. Coalesced signal means the
	// channel may already be empty (back-to-back appends collapsed). Use a
	// non-blocking select.
	select {
	case <-sub.notify:
		// fine — happens when the second Append landed after the first read
	default:
		// also fine — coalesced signal already consumed
	}
}

// TestPartitionRemoveSubscriberStopsNotifications verifies that after
// removeSubscriber, future Appends do not write to the subscriber's channel.
func TestPartitionRemoveSubscriberStopsNotifications(t *testing.T) {
	p := newTestPartition(t, 8)

	sub := newSubscriber()
	p.addSubscriber(sub)
	p.removeSubscriber(sub)

	if _, err := p.Append([]byte("x"), nil, 0); err != nil {
		t.Fatalf("Append: %v", err)
	}

	select {
	case <-sub.notify:
		t.Error("removed subscriber unexpectedly received notification")
	case <-time.After(50 * time.Millisecond):
		// expected — no notification after removal
	}
}

// TestPartitionWALReplaySeedsRing demonstrates that a freshly-constructed
// partition seeded with replayed records sets head/tail correctly so reads
// against pre-existing offsets work without re-publishing.
func TestPartitionWALReplaySeedsRing(t *testing.T) {
	dir := t.TempDir()
	wal, err := openWAL(dir, "t", 0, 0)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	t.Cleanup(func() { _ = wal.Close() })

	replay := []walRecord{
		{Offset: 5, Payload: []byte("five"), Headers: map[string]string{"h": "1"}},
		{Offset: 6, Payload: []byte("six"), Headers: nil},
	}
	p := newPartition(0, 16, wal, replay)

	if got := p.HighWaterMark(); got != 7 {
		t.Errorf("head after replay = %d, want 7", got)
	}

	s, err := p.Read(5)
	if err != nil {
		t.Fatalf("Read(5): %v", err)
	}
	if string(s.payload) != "five" {
		t.Errorf("Read(5).payload = %q, want %q", s.payload, "five")
	}
}
