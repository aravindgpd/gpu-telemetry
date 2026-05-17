package broker

import (
	"sort"
	"sync"
)

// consumerGroup is the broker-side state for one (topic, consumer_group) pair.
//
// Rebalance model: every Join() and Leave() recomputes a round-robin
// partition→member assignment. Members whose assigned set actually changed
// have their `done` channel closed so their gRPC stream exits with a
// rebalance signal. Members whose assignment did NOT change keep streaming.
// This avoids "stop the world" thrashing when a single new member joins a
// large group.
type consumerGroup struct {
	mu sync.Mutex

	topic         string
	name          string
	numPartitions int32

	members     map[string]*groupMember // by consumer_id
	assignments map[int32]string        // partition → owner consumer_id
	committed   map[int32]int64         // partition → highest acked offset
}

// groupMember represents one live subscription. The done channel is closed
// when this member must reconnect (rebalanced or replaced by same-id member).
type groupMember struct {
	id   string
	done chan struct{}
}

func newConsumerGroup(topic, name string, numPartitions int32) *consumerGroup {
	return &consumerGroup{
		topic:         topic,
		name:          name,
		numPartitions: numPartitions,
		members:       make(map[string]*groupMember),
		assignments:   make(map[int32]string),
		committed:     make(map[int32]int64),
	}
}

// Join registers `memberID`, recomputes assignments, and returns:
//   - the new member handle (with its done channel),
//   - the partitions assigned to this member,
//   - the OTHER members whose assigned set changed and therefore need to reconnect.
//
// If a member with the same ID was already present (reconnecting client),
// it is evicted before the new member is added.
func (g *consumerGroup) Join(memberID string) (*groupMember, []int32, []*groupMember) {
	g.mu.Lock()
	defer g.mu.Unlock()

	var evicted []*groupMember

	if old, ok := g.members[memberID]; ok {
		closeOnce(old.done)
		evicted = append(evicted, old)
	}

	member := &groupMember{
		id:   memberID,
		done: make(chan struct{}),
	}
	g.members[memberID] = member

	oldAssignments := g.assignments
	g.assignments = computeAssignments(g.members, g.numPartitions)

	oldByMember := invert(oldAssignments)
	newByMember := invert(g.assignments)

	for id, m := range g.members {
		if id == memberID {
			continue // the joiner does not evict itself
		}
		if !equalInt32Sets(oldByMember[id], newByMember[id]) {
			closeOnce(m.done)
			evicted = append(evicted, m)
		}
	}

	return member, sortedPartitionsFor(memberID, g.assignments), evicted
}

// Leave removes a member, recomputes assignments, and returns the OTHER
// members whose assigned set changed (so the caller can let them reconnect).
func (g *consumerGroup) Leave(memberID string) []*groupMember {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leaveLocked(memberID)
}

// LeaveIfSameMember removes memberID only if the current entry's pointer
// equals expected — guarding against evicting a member that has already
// re-Subscribed under the same ID (Join replaces the *groupMember pointer
// even when the ID is unchanged).
//
// Used by the grace-period eviction path: after a rebalance-driven exit we
// keep the member in the group briefly so an immediate reconnect is a no-op.
// If the consumer never comes back (crashed pod, replaced by a pod with a
// different ID), this is how the stale entry eventually gets cleaned up.
//
// Returns the (memberRemoved, otherEvicted) pair: memberRemoved is true if
// the expected pointer matched and we removed the entry; otherEvicted lists
// the OTHER members whose assignment changed as a result.
func (g *consumerGroup) LeaveIfSameMember(memberID string, expected *groupMember) (bool, []*groupMember) {
	g.mu.Lock()
	defer g.mu.Unlock()
	cur, ok := g.members[memberID]
	if !ok || cur != expected {
		return false, nil
	}
	return true, g.leaveLocked(memberID)
}

// leaveLocked is the shared body of Leave and LeaveIfSameMember. Caller must
// hold g.mu and have already verified the member exists.
func (g *consumerGroup) leaveLocked(memberID string) []*groupMember {
	if _, ok := g.members[memberID]; !ok {
		return nil
	}
	delete(g.members, memberID)

	oldAssignments := g.assignments
	g.assignments = computeAssignments(g.members, g.numPartitions)

	oldByMember := invert(oldAssignments)
	newByMember := invert(g.assignments)

	var evicted []*groupMember
	for id, m := range g.members {
		if !equalInt32Sets(oldByMember[id], newByMember[id]) {
			closeOnce(m.done)
			evicted = append(evicted, m)
		}
	}
	return evicted
}

// Acknowledge updates the highest committed offset for `partition`.
// Lower-or-equal offsets are ignored (commits are monotonic).
func (g *consumerGroup) Acknowledge(partition int32, offset int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if cur, ok := g.committed[partition]; !ok || offset > cur {
		g.committed[partition] = offset
	}
}

// StartingOffset returns the offset a consumer should resume from for the
// given partition: committed+1 if the group has a commit, else 0.
func (g *consumerGroup) StartingOffset(partition int32) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if cur, ok := g.committed[partition]; ok {
		return cur + 1
	}
	return 0
}

// SnapshotCommitted returns a copy of the committed-offset map.
func (g *consumerGroup) SnapshotCommitted() map[int32]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make(map[int32]int64, len(g.committed))
	for k, v := range g.committed {
		out[k] = v
	}
	return out
}

// computeAssignments returns a deterministic round-robin partition→owner map.
// Sorting member IDs guarantees that the same fleet always produces the same
// assignment, which keeps repeated Join calls idempotent for steady-state.
func computeAssignments(members map[string]*groupMember, numPartitions int32) map[int32]string {
	out := make(map[int32]string, numPartitions)
	if len(members) == 0 || numPartitions == 0 {
		return out
	}
	ids := make([]string, 0, len(members))
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for p := int32(0); p < numPartitions; p++ {
		out[p] = ids[int(p)%len(ids)]
	}
	return out
}

func invert(m map[int32]string) map[string]map[int32]struct{} {
	out := make(map[string]map[int32]struct{})
	for p, owner := range m {
		if out[owner] == nil {
			out[owner] = make(map[int32]struct{})
		}
		out[owner][p] = struct{}{}
	}
	return out
}

func equalInt32Sets(a, b map[int32]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func sortedPartitionsFor(memberID string, assignments map[int32]string) []int32 {
	var out []int32
	for p, owner := range assignments {
		if owner == memberID {
			out = append(out, p)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
