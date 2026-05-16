// Package broker implements the in-process state of the custom Message Queue:
// topics, partitions, ring buffers, write-ahead logs, and consumer groups.
// It is transport-agnostic — the gRPC service layer calls into a Broker.
package broker

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aravindgpd/gpu-telemetry/messagequeue/internal/config"
	"go.uber.org/zap"
)

// Broker is the top-level registry of topics and consumer groups.
type Broker struct {
	cfg    *config.Config
	logger *zap.Logger

	mu     sync.RWMutex
	topics map[string]*topic
	groups map[groupKey]*consumerGroup
}

type groupKey struct {
	topic string
	group string
}

// New constructs a Broker using `cfg` for ring buffer / WAL settings.
func New(cfg *config.Config, logger *zap.Logger) *Broker {
	return &Broker{
		cfg:    cfg,
		logger: logger,
		topics: make(map[string]*topic),
		groups: make(map[groupKey]*consumerGroup),
	}
}

// CreateTopic provisions a new topic with `partitions` partitions. If the topic
// already exists with the same partition count, returns nil (idempotent).
// On startup the broker also calls this to materialise topics from on-disk WAL.
func (b *Broker) CreateTopic(name string, partitions int32) error {
	if name == "" {
		return errors.New("topic name is required")
	}
	if partitions <= 0 {
		partitions = int32(b.cfg.Partitions)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.topics[name]; ok {
		if existing.numPartitions() == partitions {
			return nil // idempotent
		}
		return fmt.Errorf("%w: %q (existing=%d, requested=%d)",
			errTopicExists, name, existing.numPartitions(), partitions)
	}

	parts := make([]*partition, partitions)
	for i := int32(0); i < partitions; i++ {
		var replay []walRecord
		if _, err := replayWAL(b.cfg.WALDir, name, i, func(rec walRecord) {
			replay = append(replay, rec)
		}); err != nil {
			return fmt.Errorf("replay wal topic=%s partition=%d: %w", name, i, err)
		}

		wal, err := openWAL(b.cfg.WALDir, name, i, b.cfg.WALSyncBytes)
		if err != nil {
			return fmt.Errorf("open wal topic=%s partition=%d: %w", name, i, err)
		}

		parts[i] = newPartition(i, b.cfg.RingBufferSize, wal, replay)
	}

	b.topics[name] = &topic{name: name, partitions: parts}
	b.logger.Info("topic created",
		zap.String("topic", name),
		zap.Int32("partitions", partitions),
		zap.Int("ring_buffer_size", b.cfg.RingBufferSize))
	return nil
}

// Publish persists `payload` into the chosen partition and returns the
// assigned offset. partition < 0 selects round-robin.
func (b *Broker) Publish(topicName string, partition int32, payload []byte, headers map[string]string) (int32, int64, error) {
	b.mu.RLock()
	t, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return 0, 0, errTopicNotFound
	}
	chosen, err := t.selectPartition(partition)
	if err != nil {
		return 0, 0, err
	}
	offset, err := t.partitions[chosen].Append(payload, headers, time.Now().UnixNano())
	if err != nil {
		return 0, 0, err
	}
	return chosen, offset, nil
}

// Subscribe returns a Subscription handle for one consumer in a group.
// The caller (the gRPC stream handler) reads messages via Subscription.Next
// until the member is rebalanced or its context is cancelled.
func (b *Broker) Subscribe(topicName, groupName, memberID string) (*Subscription, error) {
	if memberID == "" {
		return nil, errors.New("consumer_id is required")
	}

	b.mu.RLock()
	t, ok := b.topics[topicName]
	b.mu.RUnlock()
	if !ok {
		return nil, errTopicNotFound
	}

	// Get-or-create the consumer group.
	key := groupKey{topic: topicName, group: groupName}
	b.mu.Lock()
	g, ok := b.groups[key]
	if !ok {
		g = newConsumerGroup(topicName, groupName, t.numPartitions())
		b.groups[key] = g
	}
	b.mu.Unlock()

	member, assigned, evicted := g.Join(memberID)

	for _, e := range evicted {
		b.logger.Info("group member evicted by rebalance",
			zap.String("topic", topicName),
			zap.String("group", groupName),
			zap.String("evicted_id", e.id))
	}

	// One subscriber, registered on every owned partition. A publish to any of
	// those partitions wakes the same notify channel.
	sharedSub := newSubscriber()
	cursors := make(map[int32]int64, len(assigned))
	for _, p := range assigned {
		t.partitions[p].addSubscriber(sharedSub)
		cursors[p] = g.StartingOffset(p)
	}

	b.logger.Info("group member subscribed",
		zap.String("topic", topicName),
		zap.String("group", groupName),
		zap.String("member_id", memberID),
		zap.Int32s("assigned_partitions", assigned))

	return &Subscription{
		broker:     b,
		topic:      t,
		group:      g,
		groupName:  groupName,
		memberID:   memberID,
		member:     member,
		assigned:   assigned,
		cursors:    cursors,
		sharedSub:  sharedSub,
	}, nil
}

// Acknowledge commits an offset for the (topic, group, partition) tuple.
func (b *Broker) Acknowledge(topicName, groupName string, partition int32, offset int64) error {
	b.mu.RLock()
	g, ok := b.groups[groupKey{topic: topicName, group: groupName}]
	b.mu.RUnlock()
	if !ok {
		return fmt.Errorf("consumer group %q not found for topic %q", groupName, topicName)
	}
	g.Acknowledge(partition, offset)
	return nil
}

// GetOffsets returns a snapshot of committed offsets for a consumer group.
func (b *Broker) GetOffsets(topicName, groupName string) (map[int32]int64, error) {
	b.mu.RLock()
	g, ok := b.groups[groupKey{topic: topicName, group: groupName}]
	b.mu.RUnlock()
	if !ok {
		return map[int32]int64{}, nil // empty == no commits yet
	}
	return g.SnapshotCommitted(), nil
}

// Close flushes all WALs and closes file handles.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var firstErr error
	for _, t := range b.topics {
		if err := t.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Subscription is one consumer's active read cursor across its assigned
// partitions. The gRPC stream handler calls Next in a loop until either the
// stream context is cancelled or the member is rebalanced out.
type Subscription struct {
	broker    *Broker
	topic     *topic
	group     *consumerGroup
	groupName string
	memberID  string
	member    *groupMember
	assigned  []int32
	cursors   map[int32]int64
	sharedSub *subscriber
}

// AssignedPartitions returns the partitions this subscription owns at the
// time it was created.
func (s *Subscription) AssignedPartitions() []int32 {
	return s.assigned
}

// Done returns a channel that closes when this member must reconnect
// (rebalance or duplicate-ID eviction).
func (s *Subscription) Done() <-chan struct{} {
	return s.member.done
}

// Notify returns a channel that receives a value whenever ANY owned partition
// gains new data. The buffer size is 1 — multiple concurrent publishes coalesce
// into a single wake-up.
func (s *Subscription) Notify() <-chan struct{} {
	return s.sharedSub.notify
}

// PollOnce reads at most one message from each owned partition and returns
// the slice of (partition, slot) it produced. If nothing was available, the
// returned slice is empty. The caller is expected to push these to the gRPC
// stream and then either call PollOnce again or wait on Notify/Done.
func (s *Subscription) PollOnce() []DeliveredSlot {
	out := make([]DeliveredSlot, 0, len(s.assigned))
	for _, p := range s.assigned {
		cursor := s.cursors[p]
		sl, err := s.topic.partitions[p].Read(cursor)
		switch {
		case err == nil:
			out = append(out, DeliveredSlot{Partition: p, Slot: sl})
			s.cursors[p] = cursor + 1
		case errors.Is(err, errOffsetTooOld):
			// We fell off the ring. Jump cursor to the partition's tail so the
			// stream does not stall. Logged for visibility.
			newCursor := s.topic.partitions[p].Tail()
			s.broker.logger.Warn("consumer fell off ring buffer; advancing cursor",
				zap.String("topic", s.topic.name),
				zap.String("group", s.groupName),
				zap.String("member_id", s.memberID),
				zap.Int32("partition", p),
				zap.Int64("from", cursor),
				zap.Int64("to", newCursor))
			s.cursors[p] = newCursor
		case errors.Is(err, errOffsetFuture):
			// No new data in this partition right now.
			continue
		default:
			s.broker.logger.Error("partition read error",
				zap.Int32("partition", p),
				zap.Error(err))
		}
	}
	return out
}

// Cleanup detaches the subscription from every partition and removes the
// member from its group. Always safe to call; idempotent.
func (s *Subscription) Cleanup() {
	for _, p := range s.assigned {
		s.topic.partitions[p].removeSubscriber(s.sharedSub)
	}
	evicted := s.group.Leave(s.memberID)
	for _, e := range evicted {
		s.broker.logger.Info("group member evicted on leave-rebalance",
			zap.String("topic", s.topic.name),
			zap.String("group", s.groupName),
			zap.String("evicted_id", e.id))
	}
}

// DeliveredSlot pairs a partition ID with the slot that was read from it.
type DeliveredSlot struct {
	Partition int32
	Slot      slot
}

// Payload exposes the message bytes for the gRPC handler.
func (d DeliveredSlot) Payload() []byte { return d.Slot.payload }

// Headers exposes the message headers map.
func (d DeliveredSlot) Headers() map[string]string { return d.Slot.headers }

// Offset exposes the message offset.
func (d DeliveredSlot) Offset() int64 { return d.Slot.offset }

// Timestamp exposes the publish timestamp (Unix nanoseconds).
func (d DeliveredSlot) Timestamp() int64 { return d.Slot.timestamp }
