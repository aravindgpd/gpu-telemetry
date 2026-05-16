package broker

import (
	"os"
	"path/filepath"
	"testing"
)

// TestWALWriteAndReplay round-trips three records through the WAL and verifies
// every field survives serialisation byte-for-byte.
func TestWALWriteAndReplay(t *testing.T) {
	dir := t.TempDir()
	const topic = "telemetry"
	const partition int32 = 0

	w, err := openWAL(dir, topic, partition, 0)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}

	want := []walRecord{
		{Offset: 0, Payload: []byte("alpha"), Headers: map[string]string{"k": "v"}},
		{Offset: 1, Payload: []byte("beta"), Headers: nil},
		{Offset: 2, Payload: []byte{}, Headers: map[string]string{"a": "1", "b": "2"}},
	}
	for _, r := range want {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append %d: %v", r.Offset, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var got []walRecord
	next, err := replayWAL(dir, topic, partition, func(r walRecord) {
		got = append(got, r)
	})
	if err != nil {
		t.Fatalf("replayWAL: %v", err)
	}
	if next != 3 {
		t.Errorf("nextOffset = %d, want 3", next)
	}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Offset != want[i].Offset {
			t.Errorf("rec %d: offset = %d, want %d", i, got[i].Offset, want[i].Offset)
		}
		if string(got[i].Payload) != string(want[i].Payload) {
			t.Errorf("rec %d: payload = %q, want %q", i, got[i].Payload, want[i].Payload)
		}
		if len(got[i].Headers) != len(want[i].Headers) {
			t.Errorf("rec %d: headers len = %d, want %d", i, len(got[i].Headers), len(want[i].Headers))
		}
		for k, v := range want[i].Headers {
			if got[i].Headers[k] != v {
				t.Errorf("rec %d: header %q = %q, want %q", i, k, got[i].Headers[k], v)
			}
		}
	}
}

// TestWALReplayMissingFile is the cold-start case: no WAL file yet, replay
// should return (0, nil) rather than an error.
func TestWALReplayMissingFile(t *testing.T) {
	dir := t.TempDir()
	next, err := replayWAL(dir, "missing", 0, func(walRecord) { t.Fatal("callback should not run") })
	if err != nil {
		t.Errorf("expected nil error for missing file, got %v", err)
	}
	if next != 0 {
		t.Errorf("nextOffset = %d, want 0", next)
	}
}

// TestWALPartialTrailingRecord truncates a WAL mid-record (simulating a crash
// before fsync) and verifies replay stops cleanly at the last whole record
// instead of returning an error.
func TestWALPartialTrailingRecord(t *testing.T) {
	dir := t.TempDir()
	const topic = "tel"
	const partition int32 = 0

	w, err := openWAL(dir, topic, partition, 0)
	if err != nil {
		t.Fatalf("openWAL: %v", err)
	}
	for i := int64(0); i < 3; i++ {
		if err := w.Append(walRecord{Offset: i, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a torn write by truncating the last few bytes off the file.
	path := filepath.Join(dir, topic, "0.wal")
	if err := truncateBy(path, 4); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	var got []walRecord
	next, err := replayWAL(dir, topic, partition, func(r walRecord) {
		got = append(got, r)
	})
	if err != nil {
		t.Errorf("replay should tolerate trailing truncation, got error: %v", err)
	}
	if len(got) < 2 {
		t.Errorf("expected at least 2 whole records to replay, got %d", len(got))
	}
	if next < 2 {
		t.Errorf("nextOffset = %d, want >= 2", next)
	}
}

// truncateBy removes n bytes from the end of path.
func truncateBy(path string, n int64) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return os.Truncate(path, info.Size()-n)
}
