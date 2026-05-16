package broker

import (
	"errors"
	"fmt"
	"sync"
)

// slot is one in-memory message stored in the ring buffer.
type slot struct {
	offset    int64
	payload   []byte
	headers   map[string]string
	timestamp int64
}

// subscriber is a non-blocking notification handle held by an active consumer.
// The same subscriber is registered on every partition the consumer owns so a
// publish on any of those partitions wakes the consumer's stream.
type subscriber struct {
	notify chan struct{}
}

// newSubscriber builds a subscriber with a buffered notify channel. The buffer
// of 1 is what makes notifications coalesce: if the consumer is slow, multiple
// publishes will collapse into a single wake-up rather than blocking the writer.
func newSubscriber() *subscriber {
	return &subscriber{notify: make(chan struct{}, 1)}
}

// partition is one append-only log backed by a fixed-size ring buffer plus a
// write-ahead log on disk. Reads are O(1) by offset; writes are O(1) plus an
// fsync amortised across many records by the WAL.
type partition struct {
	id       int32
	capacity int

	mu    sync.Mutex
	slots []slot
	head  int64 // next offset to assign
	tail  int64 // oldest offset still resident in memory

	subs map[*subscriber]struct{}
	wal  *walWriter
}

// newPartition builds a partition with its WAL replayed into the ring buffer.
// `replay` must be the records returned by replayWAL in original publish order.
func newPartition(id int32, capacity int, wal *walWriter, replay []walRecord) *partition {
	p := &partition{
		id:       id,
		capacity: capacity,
		slots:    make([]slot, capacity),
		subs:     make(map[*subscriber]struct{}),
		wal:      wal,
	}
	for _, rec := range replay {
		idx := rec.Offset % int64(capacity)
		p.slots[idx] = slot{
			offset:  rec.Offset,
			payload: rec.Payload,
			headers: rec.Headers,
		}
		p.head = rec.Offset + 1
	}
	if p.head > int64(capacity) {
		p.tail = p.head - int64(capacity)
	}
	return p
}

// Append durably writes one message to the WAL, then stores it in the ring
// buffer at slot `offset % capacity` (overwriting whatever lived there) and
// wakes every registered subscriber. Returns the assigned offset.
func (p *partition) Append(payload []byte, headers map[string]string, timestamp int64) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	offset := p.head

	if err := p.wal.Append(walRecord{
		Offset:  offset,
		Payload: payload,
		Headers: headers,
	}); err != nil {
		return 0, fmt.Errorf("partition %d wal append: %w", p.id, err)
	}

	idx := offset % int64(p.capacity)
	p.slots[idx] = slot{
		offset:    offset,
		payload:   payload,
		headers:   headers,
		timestamp: timestamp,
	}
	p.head++
	if p.head-p.tail > int64(p.capacity) {
		p.tail = p.head - int64(p.capacity)
	}

	// Coalesced non-blocking notify: subscribers see "something happened, go look"
	// rather than receiving a message per publish. This keeps the writer fast
	// even when many consumers are subscribed.
	for s := range p.subs {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
	return offset, nil
}

// Read returns the slot stored at `offset` if it is still resident in the ring.
// Returns errOffsetTooOld if the offset has been overwritten or never existed,
// or errOffsetFuture if the offset has not been published yet.
func (p *partition) Read(offset int64) (slot, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if offset < p.tail {
		return slot{}, errOffsetTooOld
	}
	if offset >= p.head {
		return slot{}, errOffsetFuture
	}
	idx := offset % int64(p.capacity)
	s := p.slots[idx]
	// Defensive — a concurrent overwrite between tail check and read.
	if s.offset != offset {
		return slot{}, errOffsetTooOld
	}
	return s, nil
}

// HighWaterMark returns the next offset that will be assigned (== current head).
func (p *partition) HighWaterMark() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.head
}

// Tail returns the lowest offset still resident in the ring buffer.
func (p *partition) Tail() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tail
}

func (p *partition) addSubscriber(s *subscriber) {
	p.mu.Lock()
	p.subs[s] = struct{}{}
	p.mu.Unlock()
}

func (p *partition) removeSubscriber(s *subscriber) {
	p.mu.Lock()
	delete(p.subs, s)
	p.mu.Unlock()
}

// Close flushes the WAL and releases the file handle.
func (p *partition) Close() error {
	if p.wal == nil {
		return nil
	}
	return p.wal.Close()
}

var (
	errOffsetTooOld = errors.New("offset is older than the partition's tail; data is in WAL only")
	errOffsetFuture = errors.New("offset has not been published yet")
)
