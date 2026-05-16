package reader

import (
	"testing"
	"time"
)

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
