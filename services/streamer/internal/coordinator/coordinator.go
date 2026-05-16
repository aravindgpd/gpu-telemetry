// Package coordinator implements row-partition assignment for the Streamer.
package coordinator

// Coordinator partitions CSV rows across multiple Streamer replicas so each
// row is published exactly once across the fleet.  Streamer with ordinal index
// I is responsible for rows where row_number % total == index.
type Coordinator struct {
	index int
	total int
}

// New creates a Coordinator for a Streamer with the given zero-based ordinal
// index within a fleet of total replicas.
func New(index, total int) *Coordinator {
	return &Coordinator{index: index, total: total}
}

// ShouldPublish reports whether this Streamer instance is responsible for
// rowNum (0-based, counting data rows only, not the CSV header row).
func (c *Coordinator) ShouldPublish(rowNum int) bool {
	return rowNum%c.total == c.index
}

// Partition returns the MQ partition this Streamer publishes to.
// One Streamer maps to one partition for deterministic per-GPU ordering.
func (c *Coordinator) Partition() int32 {
	return int32(c.index)
}
