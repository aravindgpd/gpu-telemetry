package broker

import (
	"sort"
	"testing"
)

// TestGroupSingleMember: only one consumer joins → it owns every partition,
// and there are no other members to evict.
func TestGroupSingleMember(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 10)

	member, assigned, evicted := g.Join("collector-0")

	if member == nil || member.id != "collector-0" {
		t.Fatalf("member: got %+v", member)
	}
	if len(evicted) != 0 {
		t.Errorf("expected no evictions, got %d", len(evicted))
	}
	if len(assigned) != 10 {
		t.Errorf("expected 10 partitions assigned, got %d", len(assigned))
	}
	for i, p := range assigned {
		if p != int32(i) {
			t.Errorf("assigned[%d] = %d, want %d", i, p, i)
		}
	}
}

// TestGroupSecondMemberJoinsTriggersRebalance: when collector-1 joins a group
// already holding collector-0, partitions split and collector-0 is evicted.
func TestGroupSecondMemberJoinsTriggersRebalance(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 10)
	first, _, _ := g.Join("collector-0")

	_, assigned, evicted := g.Join("collector-1")

	// collector-1 should own odd partitions: 1,3,5,7,9
	wantC1 := []int32{1, 3, 5, 7, 9}
	if !sameOrderedSlice(assigned, wantC1) {
		t.Errorf("collector-1 assigned = %v, want %v", assigned, wantC1)
	}

	// collector-0 should be evicted (its set shrank from {0..9} to {0,2,4,6,8}).
	if len(evicted) != 1 || evicted[0].id != "collector-0" {
		t.Errorf("expected evicted=[collector-0], got %v", evictedIDs(evicted))
	}
	// Its done channel should now be closed.
	select {
	case <-first.done:
		// expected
	default:
		t.Error("evicted member's done channel was not closed")
	}
}

// TestGroupReJoinAfterRebalanceIsStable: when collector-0 reconnects after
// being evicted, the assignment recomputed for the same fleet must equal the
// previous one — so collector-1 should NOT be re-evicted.
func TestGroupReJoinAfterRebalanceIsStable(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 10)
	g.Join("collector-0")
	c1, _, _ := g.Join("collector-1") // collector-0 was evicted

	// collector-0 reconnects with same ID
	_, _, evicted := g.Join("collector-0")

	// Only the previous-same-ID member is "evicted" (already-closed channel).
	// collector-1 must NOT be re-evicted because its assignment didn't change.
	for _, e := range evicted {
		if e.id == "collector-1" {
			t.Errorf("collector-1 was evicted on rejoin — rebalance is not stable")
		}
	}
	// Verify collector-1's done channel is still open.
	select {
	case <-c1.done:
		t.Error("collector-1's done channel was closed by stable rejoin")
	default:
		// expected
	}
}

// TestGroupSameIDReplacesOldSession: a duplicate Join with the same memberID
// evicts the existing session (this is how reconnect works).
func TestGroupSameIDReplacesOldSession(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 4)
	first, _, _ := g.Join("c0")
	second, _, evicted := g.Join("c0")

	if first == second {
		t.Error("re-Join returned the same member handle")
	}
	if len(evicted) != 1 || evicted[0] != first {
		t.Errorf("expected evicted=[first], got %v", evicted)
	}
	select {
	case <-first.done:
		// expected
	default:
		t.Error("old session's done channel was not closed")
	}
}

// TestGroupLeaveRedistributes: collector-1 leaves, partitions go back to
// collector-0, and collector-0 is evicted (its set grew).
func TestGroupLeaveRedistributes(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 10)
	g.Join("collector-0")
	g.Join("collector-1")
	// collector-0 reconnects after being evicted
	c0v2, _, _ := g.Join("collector-0")

	evicted := g.Leave("collector-1")

	if len(evicted) != 1 || evicted[0].id != "collector-0" {
		t.Errorf("expected collector-0 evicted (set expanded), got %v", evictedIDs(evicted))
	}
	// c0v2.done should now be closed.
	select {
	case <-c0v2.done:
		// expected
	default:
		t.Error("collector-0's done was not closed when collector-1 left")
	}
}

// TestGroupLeaveUnknownMemberIsNoOp: removing a member that never joined
// returns nil and doesn't panic.
func TestGroupLeaveUnknownMemberIsNoOp(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 4)
	if got := g.Leave("nobody"); got != nil {
		t.Errorf("Leave(unknown) should return nil, got %v", got)
	}
}

// TestGroupAcknowledgeMonotonic: only forward-moving offsets stick.
// Late or duplicate acks must NOT rewind the committed cursor.
func TestGroupAcknowledgeMonotonic(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 4)

	g.Acknowledge(0, 5)
	g.Acknowledge(0, 3) // late ack — should be ignored
	g.Acknowledge(0, 5) // duplicate — also no-op

	if got := g.StartingOffset(0); got != 6 {
		t.Errorf("StartingOffset = %d, want 6 (5+1)", got)
	}

	g.Acknowledge(0, 10) // forward — should win
	if got := g.StartingOffset(0); got != 11 {
		t.Errorf("after forward ack: StartingOffset = %d, want 11", got)
	}
}

// TestGroupStartingOffsetForUncommittedPartition: a partition with no committed
// offset starts from 0.
func TestGroupStartingOffsetForUncommittedPartition(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 4)
	if got := g.StartingOffset(2); got != 0 {
		t.Errorf("uncommitted partition StartingOffset = %d, want 0", got)
	}
}

// TestGroupSnapshotCommitted: returns a copy that is independent of subsequent
// state changes (no aliasing bug).
func TestGroupSnapshotCommitted(t *testing.T) {
	g := newConsumerGroup("topic", "grp", 4)
	g.Acknowledge(0, 100)
	g.Acknowledge(1, 200)

	snap := g.SnapshotCommitted()
	if snap[0] != 100 || snap[1] != 200 {
		t.Errorf("snapshot = %v, want {0:100, 1:200}", snap)
	}

	// Mutate the live state — snapshot should not change.
	g.Acknowledge(0, 999)
	if snap[0] != 100 {
		t.Error("snapshot was aliased to the live committed map")
	}
}

// TestComputeAssignmentsDeterministic: same membership produces the same map,
// regardless of insertion order — that's what makes stable rebalance work.
func TestComputeAssignmentsDeterministic(t *testing.T) {
	const numPartitions int32 = 6
	a := map[string]*groupMember{"a": {id: "a"}, "b": {id: "b"}, "c": {id: "c"}}
	b := map[string]*groupMember{"c": {id: "c"}, "a": {id: "a"}, "b": {id: "b"}}

	out1 := computeAssignments(a, numPartitions)
	out2 := computeAssignments(b, numPartitions)

	if len(out1) != int(numPartitions) {
		t.Fatalf("len(out1) = %d, want %d", len(out1), numPartitions)
	}
	for p, owner := range out1 {
		if out2[p] != owner {
			t.Errorf("partition %d: out1=%s, out2=%s", p, owner, out2[p])
		}
	}
}

// helpers

func sameOrderedSlice(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	aCopy := append([]int32(nil), a...)
	bCopy := append([]int32(nil), b...)
	sort.Slice(aCopy, func(i, j int) bool { return aCopy[i] < aCopy[j] })
	sort.Slice(bCopy, func(i, j int) bool { return bCopy[i] < bCopy[j] })
	for i := range aCopy {
		if aCopy[i] != bCopy[i] {
			return false
		}
	}
	return true
}

func evictedIDs(ms []*groupMember) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = m.id
	}
	return out
}
