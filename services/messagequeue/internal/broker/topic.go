package broker

import (
	"errors"
	"sync/atomic"
)

// topic is a fixed-size collection of partitions.
type topic struct {
	name       string
	partitions []*partition

	// rrCounter advances when Publish is called with partition=-1, distributing
	// such "broker-pick" publishes round-robin across partitions.
	rrCounter atomic.Int64
}

func (t *topic) numPartitions() int32 {
	return int32(len(t.partitions))
}

// selectPartition returns p when p >= 0, otherwise picks the next partition
// in round-robin order.
func (t *topic) selectPartition(p int32) (int32, error) {
	if p >= 0 {
		if int(p) >= len(t.partitions) {
			return 0, errPartitionNotFound
		}
		return p, nil
	}
	n := int32(len(t.partitions))
	if n == 0 {
		return 0, errPartitionNotFound
	}
	return int32(t.rrCounter.Add(1)-1) % n, nil
}

// Close flushes every partition's WAL.
func (t *topic) Close() error {
	var firstErr error
	for _, p := range t.partitions {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

var (
	errTopicExists       = errors.New("topic already exists")
	errTopicNotFound     = errors.New("topic not found")
	errPartitionNotFound = errors.New("partition not found")
)
