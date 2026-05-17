package reader

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	telemetrypb "github.com/aravindgpd/gpu-telemetry/proto/telemetry"
	"github.com/aravindgpd/gpu-telemetry/streamer/internal/coordinator"
	"go.uber.org/zap"
)

// fakePublisher records every Publish call. Tests inspect `records` to assert
// what the Stream loop sent.
type fakePublisher struct {
	mu          sync.Mutex
	records     []fakePublished
	publishErr  error
	failAfterN  int // if > 0, return publishErr from the Nth call onwards
}

type fakePublished struct {
	UUID      string
	Metric    string
	Partition int32
}

func (f *fakePublisher) Publish(ctx context.Context, rec *telemetrypb.TelemetryRecord, partition int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failAfterN > 0 && len(f.records) >= f.failAfterN {
		return f.publishErr
	}
	f.records = append(f.records, fakePublished{
		UUID:      rec.Uuid,
		Metric:    rec.MetricName,
		Partition: partition,
	})
	return nil
}

func (f *fakePublisher) Count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.records)
}

func (f *fakePublisher) Snapshot() []fakePublished {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]fakePublished, len(f.records))
	copy(out, f.records)
	return out
}

// writeTempCSV creates a temp file containing `body` (no header is prepended;
// caller supplies the full file) and returns its path.
func writeTempCSV(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.csv")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	return path
}

// sampleCSV is a header + 3 valid data rows.
const sampleCSV = `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-01-01T00:00:00Z,DCGM_FI_DEV_GPU_UTIL,0,nvidia0,GPU-aaa,H100,host1,,,,42.5,labels
2025-01-01T00:00:01Z,DCGM_FI_DEV_POWER_USAGE,0,nvidia0,GPU-aaa,H100,host1,,,,310.2,labels
2025-01-01T00:00:02Z,DCGM_FI_DEV_GPU_TEMP,1,nvidia1,GPU-bbb,H100,host2,,,,68,labels
`

// validRow returns a representative DCGM CSV row with all 12 columns populated.
func validRow() []string {
	return []string{
		"2025-07-18T20:42:34Z",                  // timestamp (ignored per spec)
		"DCGM_FI_DEV_GPU_UTIL",                  // metric_name
		"0",                                     // gpu_id (per-host index)
		"nvidia0",                               // device
		"GPU-5fd4f087-86f3-7a43-b711-XYZ",       // uuid
		"NVIDIA H100 80GB HBM3",                 // modelName
		"mtv5-dgx1-hgpu-031",                    // hostname
		"telemetry-container",                   // container
		"telemetry-pod-0",                       // pod
		"gpu-telemetry",                         // namespace
		"42.5",                                  // value
		`UUID="GPU-5fd4f087",__name__="..."`,    // labels_raw
	}
}

// TestParseRowHappyPath checks every field is mapped from the right CSV column
// into the right proto field, and that the canonical timestamp is the
// Streamer's wall-clock (not the CSV column).
func TestParseRowHappyPath(t *testing.T) {
	before := time.Now().UnixNano()
	rec, err := parseRow(validRow())
	after := time.Now().UnixNano()

	if err != nil {
		t.Fatalf("parseRow returned error: %v", err)
	}

	checks := []struct {
		name string
		got  string
		want string
	}{
		{"MetricName", rec.MetricName, "DCGM_FI_DEV_GPU_UTIL"},
		{"GpuIndex", rec.GpuIndex, "0"},
		{"Device", rec.Device, "nvidia0"},
		{"Uuid", rec.Uuid, "GPU-5fd4f087-86f3-7a43-b711-XYZ"},
		{"ModelName", rec.ModelName, "NVIDIA H100 80GB HBM3"},
		{"Hostname", rec.Hostname, "mtv5-dgx1-hgpu-031"},
		{"Container", rec.Container, "telemetry-container"},
		{"Pod", rec.Pod, "telemetry-pod-0"},
		{"Namespace", rec.Namespace, "gpu-telemetry"},
		{"LabelsRaw", rec.LabelsRaw, `UUID="GPU-5fd4f087",__name__="..."`},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", c.name, c.got, c.want)
		}
	}

	if rec.Value != 42.5 {
		t.Errorf("Value = %v, want 42.5", rec.Value)
	}

	// Per project spec, both timestamps come from time.Now() at parse time,
	// not from the CSV's first column.
	if rec.IngestedUnixNs != rec.SampleUnixNs {
		t.Errorf("IngestedUnixNs (%d) != SampleUnixNs (%d) — should be equal",
			rec.IngestedUnixNs, rec.SampleUnixNs)
	}
	if rec.IngestedUnixNs < before || rec.IngestedUnixNs > after {
		t.Errorf("IngestedUnixNs = %d, want in [%d, %d]", rec.IngestedUnixNs, before, after)
	}
}

// TestParseRowIgnoresCSVTimestamp: a deliberately-bogus value in the timestamp
// column must NOT cause a parse error. The column is decorative.
func TestParseRowIgnoresCSVTimestamp(t *testing.T) {
	row := validRow()
	row[colTimestamp] = "this-is-not-a-date"

	rec, err := parseRow(row)
	if err != nil {
		t.Fatalf("parseRow should not validate the timestamp column, got: %v", err)
	}
	if rec == nil {
		t.Fatal("parseRow returned nil record")
	}
}

// TestParseRowInvalidValueColumn: the value column must be a parseable float.
func TestParseRowInvalidValueColumn(t *testing.T) {
	row := validRow()
	row[colValue] = "not-a-number"

	if _, err := parseRow(row); err == nil {
		t.Error("parseRow should reject non-numeric value, got nil error")
	}
}

// TestParseRowEmptyOptionalColumns: empty container/pod/namespace strings
// pass through verbatim (they're optional Kubernetes context fields).
func TestParseRowEmptyOptionalColumns(t *testing.T) {
	row := validRow()
	row[colContainer] = ""
	row[colPod] = ""
	row[colNamespace] = ""

	rec, err := parseRow(row)
	if err != nil {
		t.Fatalf("parseRow: %v", err)
	}
	if rec.Container != "" || rec.Pod != "" || rec.Namespace != "" {
		t.Errorf("empty cols not preserved: container=%q pod=%q namespace=%q",
			rec.Container, rec.Pod, rec.Namespace)
	}
}

// TestParseRowIntegerValue: integer-only DCGM fields like FB_USED come through
// the same float64 path. ParseFloat handles "32768" without complaint.
func TestParseRowIntegerValue(t *testing.T) {
	row := validRow()
	row[colMetricName] = "DCGM_FI_DEV_FB_USED"
	row[colValue] = "32768"

	rec, err := parseRow(row)
	if err != nil {
		t.Fatalf("parseRow: %v", err)
	}
	if rec.Value != 32768 {
		t.Errorf("Value = %v, want 32768", rec.Value)
	}
}

// ─── Stream loop tests ────────────────────────────────────────────────────────

// TestStreamPublishesAllRowsForSoleStreamer: with STREAMER_TOTAL=1 the single
// reader publishes every CSV row, then loops back to the beginning.
func TestStreamPublishesAllRowsForSoleStreamer(t *testing.T) {
	path := writeTempCSV(t, sampleCSV)
	fp := &fakePublisher{}
	r := New(path, 0, zap.NewNop()) // 0ms interval — drains as fast as possible
	c := coordinator.New(0, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Stream(ctx, c, fp) }()

	// Wait until at least 6 rows publish (2 full passes through the 3-row CSV)
	// or 2 seconds, then cancel.
	waitFor(t, func() bool { return fp.Count() >= 6 }, 2*time.Second)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Stream returned err: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stream did not exit within 1s after cancel")
	}

	got := fp.Snapshot()
	if len(got) < 6 {
		t.Fatalf("published %d records, want at least 6 (two passes)", len(got))
	}
	// First three records must be the three CSV rows in order.
	wantUUIDs := []string{"GPU-aaa", "GPU-aaa", "GPU-bbb"}
	for i, want := range wantUUIDs {
		if got[i].UUID != want {
			t.Errorf("record %d UUID = %q, want %q", i, got[i].UUID, want)
		}
	}
}

// TestStreamPartitioningHonoursCoordinator: with STREAMER_TOTAL=2, index 0
// publishes rows 0,2,4,... and index 1 publishes rows 1,3,5,...
func TestStreamPartitioningHonoursCoordinator(t *testing.T) {
	path := writeTempCSV(t, sampleCSV)

	for _, idx := range []int{0, 1} {
		idx := idx
		t.Run("idx="+itoa(idx), func(t *testing.T) {
			fp := &fakePublisher{}
			r := New(path, 0, zap.NewNop())
			c := coordinator.New(idx, 2)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- r.Stream(ctx, c, fp) }()

			// Three rows per pass; this streamer owns roughly half. Wait for
			// at least two publishes from this streamer.
			waitFor(t, func() bool { return fp.Count() >= 2 }, 2*time.Second)
			cancel()
			<-done

			// Every record published by this streamer must be tagged with its
			// own partition number (== index).
			for _, rec := range fp.Snapshot() {
				if int(rec.Partition) != idx {
					t.Errorf("record on partition %d, expected %d", rec.Partition, idx)
				}
			}
		})
	}
}

// TestStreamMissingFileReturnsError: ctx is alive but the CSV doesn't exist.
// Stream must surface the open error rather than spinning silently.
func TestStreamMissingFileReturnsError(t *testing.T) {
	r := New("/definitely/not/a/path.csv", 0, zap.NewNop())
	c := coordinator.New(0, 1)
	fp := &fakePublisher{}

	err := r.Stream(context.Background(), c, fp)
	if err == nil || !strings.Contains(err.Error(), "open") {
		t.Errorf("Stream(missing file) = %v, want open error", err)
	}
}

// TestStreamPublisherErrorPropagates: if the publisher returns an error
// mid-stream, Stream surfaces it (so the caller can decide to reconnect).
func TestStreamPublisherErrorPropagates(t *testing.T) {
	path := writeTempCSV(t, sampleCSV)
	fp := &fakePublisher{publishErr: errors.New("network dead"), failAfterN: 1}
	r := New(path, 0, zap.NewNop())
	c := coordinator.New(0, 1)

	err := r.Stream(context.Background(), c, fp)
	if err == nil {
		t.Fatal("Stream should return publisher error")
	}
	if !strings.Contains(err.Error(), "publish") {
		t.Errorf("error = %v, want to mention publish", err)
	}
}

// TestStreamHonoursContextCancellation: a cancelled context exits cleanly with
// no error (the streamer signals normal shutdown via ctx.Done).
func TestStreamHonoursContextCancellation(t *testing.T) {
	path := writeTempCSV(t, sampleCSV)
	fp := &fakePublisher{}
	r := New(path, 0, zap.NewNop())
	c := coordinator.New(0, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel BEFORE Stream starts — should exit immediately

	if err := r.Stream(ctx, c, fp); err != nil {
		t.Errorf("Stream(cancelled ctx) = %v, want nil", err)
	}
}

// TestStreamMalformedRowSkippedNotFatal: an unparseable row is logged and
// skipped; subsequent rows publish normally.
func TestStreamMalformedRowSkippedNotFatal(t *testing.T) {
	body := `timestamp,metric_name,gpu_id,device,uuid,modelName,Hostname,container,pod,namespace,value,labels_raw
2025-01-01T00:00:00Z,DCGM_FI_DEV_GPU_UTIL,0,nvidia0,GPU-aaa,H100,host1,,,,not-a-float,labels
2025-01-01T00:00:01Z,DCGM_FI_DEV_GPU_UTIL,0,nvidia0,GPU-aaa,H100,host1,,,,42.0,labels
`
	path := writeTempCSV(t, body)
	fp := &fakePublisher{}
	r := New(path, 0, zap.NewNop())
	c := coordinator.New(0, 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Stream(ctx, c, fp) }()

	// At least one valid row should publish despite the bad one above it.
	waitFor(t, func() bool { return fp.Count() >= 1 }, 2*time.Second)
	cancel()
	<-done

	for _, r := range fp.Snapshot() {
		if r.UUID == "" {
			t.Errorf("got empty UUID — malformed row was not skipped")
		}
	}
}

// helpers

// waitFor polls cond until it's true or timeout fires. Fails the test if the
// condition never becomes true.
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
