package broker

import (
	"errors"
	"testing"
)

// newTestTopic builds a topic with N partitions backed by real WALs in a temp dir.
func newTestTopic(t *testing.T, partitions int32) *topic {
	t.Helper()
	dir := t.TempDir()
	parts := make([]*partition, partitions)
	for i := int32(0); i < partitions; i++ {
		wal, err := openWAL(dir, "test", i, 0)
		if err != nil {
			t.Fatalf("openWAL %d: %v", i, err)
		}
		t.Cleanup(func() { _ = wal.Close() })
		parts[i] = newPartition(i, 16, wal, nil)
	}
	return &topic{name: "test", partitions: parts}
}

// TestTopicSelectPartitionExplicit: a non-negative partition is returned as-is
// when within range.
func TestTopicSelectPartitionExplicit(t *testing.T) {
	tp := newTestTopic(t, 4)

	cases := []int32{0, 1, 2, 3}
	for _, want := range cases {
		got, err := tp.selectPartition(want)
		if err != nil {
			t.Errorf("selectPartition(%d): %v", want, err)
		}
		if got != want {
			t.Errorf("selectPartition(%d) = %d, want %d", want, got, want)
		}
	}
}

// TestTopicSelectPartitionOutOfRange: explicit partition >= numPartitions errors.
func TestTopicSelectPartitionOutOfRange(t *testing.T) {
	tp := newTestTopic(t, 4)
	if _, err := tp.selectPartition(99); !errors.Is(err, errPartitionNotFound) {
		t.Errorf("expected errPartitionNotFound for partition=99, got %v", err)
	}
}

// TestTopicSelectPartitionRoundRobin: partition=-1 cycles through 0..N-1.
func TestTopicSelectPartitionRoundRobin(t *testing.T) {
	tp := newTestTopic(t, 3)

	seen := make([]int32, 0, 9)
	for i := 0; i < 9; i++ {
		p, err := tp.selectPartition(-1)
		if err != nil {
			t.Fatalf("selectPartition(-1): %v", err)
		}
		seen = append(seen, p)
	}

	// 9 cycles over 3 partitions → each appears exactly 3 times.
	counts := make(map[int32]int)
	for _, p := range seen {
		counts[p]++
	}
	for p, n := range counts {
		if n != 3 {
			t.Errorf("partition %d seen %d times, want 3", p, n)
		}
	}
	if len(counts) != 3 {
		t.Errorf("round-robin only visited %d partitions, want 3", len(counts))
	}
}

// TestTopicNumPartitions returns the slice length as int32.
func TestTopicNumPartitions(t *testing.T) {
	tp := newTestTopic(t, 7)
	if got := tp.numPartitions(); got != 7 {
		t.Errorf("numPartitions() = %d, want 7", got)
	}
}

// TestTopicClose flushes every partition's WAL without error.
func TestTopicClose(t *testing.T) {
	tp := newTestTopic(t, 3)
	if err := tp.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
