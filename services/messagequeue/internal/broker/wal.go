package broker

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// walRecord is one persisted message in the write-ahead log.
type walRecord struct {
	Offset  int64
	Payload []byte
	Headers map[string]string
}

// walWriter appends records to a partition's write-ahead log file.
//
// File format (big-endian):
//
//	┌──────────┬────────┬──────────────┬─────────┬────────────┬──────────────────┐
//	│ body_len │ offset │ payload_len  │ payload │ n_headers  │ key/val pairs    │
//	│  uint64  │  int64 │   uint32     │  bytes  │   uint32   │ (len-prefixed)   │
//	└──────────┴────────┴──────────────┴─────────┴────────────┴──────────────────┘
//
// body_len is the byte count of everything that follows it, so a reader can
// pre-allocate one buffer per record and recover from partial writes.
type walWriter struct {
	mu        sync.Mutex
	file      *os.File
	bw        *bufio.Writer
	syncBytes int
	pending   int
}

// openWAL opens (or creates) the WAL file for one partition and returns a writer.
func openWAL(dir, topic string, partition int32, syncBytes int) (*walWriter, error) {
	if syncBytes <= 0 {
		syncBytes = 4096
	}
	if err := os.MkdirAll(filepath.Join(dir, topic), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir wal dir: %w", err)
	}
	path := filepath.Join(dir, topic, fmt.Sprintf("%d.wal", partition))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal %s: %w", path, err)
	}
	return &walWriter{
		file:      f,
		bw:        bufio.NewWriterSize(f, 64*1024),
		syncBytes: syncBytes,
	}, nil
}

// Append writes one record. Returns only after the record is in the OS buffer;
// fsync happens lazily once `syncBytes` bytes have accumulated.
func (w *walWriter) Append(rec walRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	bodyLen := 8 + 4 + len(rec.Payload) + 4 // offset + payload_len + payload + n_headers
	for k, v := range rec.Headers {
		bodyLen += 4 + len(k) + 4 + len(v)
	}

	if err := binary.Write(w.bw, binary.BigEndian, uint64(bodyLen)); err != nil {
		return err
	}
	if err := binary.Write(w.bw, binary.BigEndian, rec.Offset); err != nil {
		return err
	}
	if err := binary.Write(w.bw, binary.BigEndian, uint32(len(rec.Payload))); err != nil {
		return err
	}
	if _, err := w.bw.Write(rec.Payload); err != nil {
		return err
	}
	if err := binary.Write(w.bw, binary.BigEndian, uint32(len(rec.Headers))); err != nil {
		return err
	}
	for k, v := range rec.Headers {
		if err := binary.Write(w.bw, binary.BigEndian, uint32(len(k))); err != nil {
			return err
		}
		if _, err := w.bw.WriteString(k); err != nil {
			return err
		}
		if err := binary.Write(w.bw, binary.BigEndian, uint32(len(v))); err != nil {
			return err
		}
		if _, err := w.bw.WriteString(v); err != nil {
			return err
		}
	}

	w.pending += 8 + bodyLen
	if w.pending >= w.syncBytes {
		if err := w.bw.Flush(); err != nil {
			return err
		}
		if err := w.file.Sync(); err != nil {
			return err
		}
		w.pending = 0
	}
	return nil
}

// Close flushes and closes the underlying file.
func (w *walWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// replayWAL reads every record in the partition's WAL file in order, invoking
// `consume` for each. Returns the next offset to assign (one past the highest
// seen). A missing file yields (0, nil) — fresh partition.
func replayWAL(dir, topic string, partition int32, consume func(walRecord)) (int64, error) {
	path := filepath.Join(dir, topic, fmt.Sprintf("%d.wal", partition))
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 64*1024)
	var nextOffset int64

	for {
		var bodyLen uint64
		if err := binary.Read(br, binary.BigEndian, &bodyLen); err != nil {
			if errors.Is(err, io.EOF) {
				return nextOffset, nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nextOffset, nil
			}
			return nextOffset, err
		}
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(br, body); err != nil {
			// Partial trailing record from a crashed write — stop here.
			return nextOffset, nil
		}

		rec, err := decodeRecord(body)
		if err != nil {
			return nextOffset, err
		}
		consume(rec)
		if rec.Offset+1 > nextOffset {
			nextOffset = rec.Offset + 1
		}
	}
}

func decodeRecord(body []byte) (walRecord, error) {
	if len(body) < 16 {
		return walRecord{}, errors.New("wal record too short")
	}
	pos := 0
	offset := int64(binary.BigEndian.Uint64(body[pos : pos+8]))
	pos += 8
	payloadLen := binary.BigEndian.Uint32(body[pos : pos+4])
	pos += 4
	if pos+int(payloadLen)+4 > len(body) {
		return walRecord{}, errors.New("wal record truncated in payload")
	}
	payload := make([]byte, payloadLen)
	copy(payload, body[pos:pos+int(payloadLen)])
	pos += int(payloadLen)

	nHeaders := binary.BigEndian.Uint32(body[pos : pos+4])
	pos += 4

	headers := make(map[string]string, nHeaders)
	for i := uint32(0); i < nHeaders; i++ {
		if pos+4 > len(body) {
			return walRecord{}, errors.New("wal record truncated in header key length")
		}
		kLen := binary.BigEndian.Uint32(body[pos : pos+4])
		pos += 4
		if pos+int(kLen) > len(body) {
			return walRecord{}, errors.New("wal record truncated in header key")
		}
		k := string(body[pos : pos+int(kLen)])
		pos += int(kLen)

		if pos+4 > len(body) {
			return walRecord{}, errors.New("wal record truncated in header value length")
		}
		vLen := binary.BigEndian.Uint32(body[pos : pos+4])
		pos += 4
		if pos+int(vLen) > len(body) {
			return walRecord{}, errors.New("wal record truncated in header value")
		}
		v := string(body[pos : pos+int(vLen)])
		pos += int(vLen)

		headers[k] = v
	}
	return walRecord{Offset: offset, Payload: payload, Headers: headers}, nil
}
