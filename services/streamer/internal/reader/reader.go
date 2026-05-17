// Package reader parses DCGM exporter CSV files and streams TelemetryRecord
// messages to the MQ broker in a continuous loop.
package reader

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	telemetrypb "github.com/aravindgpd/gpu-telemetry/proto/telemetry"
	"github.com/aravindgpd/gpu-telemetry/streamer/internal/coordinator"
	"go.uber.org/zap"
)

// DCGM CSV column positions.
// Header: timestamp, metric_name, gpu_id, device, uuid, modelName,
//
//	Hostname, container, pod, namespace, value, labels_raw
const (
	colTimestamp  = 0
	colMetricName = 1
	colGpuIndex   = 2  // CSV "gpu_id" → proto gpu_index (per-host 0–7)
	colDevice     = 3
	colUUID       = 4
	colModelName  = 5
	colHostname   = 6
	colContainer  = 7
	colPod        = 8
	colNamespace  = 9
	colValue      = 10
	colLabelsRaw  = 11
	minCols       = 12
)

// Publisher is the narrow interface this package needs from the MQ publisher.
// The concrete *publisher.Publisher satisfies it; tests use a fake.
type Publisher interface {
	Publish(ctx context.Context, rec *telemetrypb.TelemetryRecord, partition int32) error
}

// Reader loops over a DCGM CSV file, publishing each row it is responsible for.
type Reader struct {
	csvPath    string
	intervalMs int
	logger     *zap.Logger
}

// New creates a Reader that will replay the CSV at csvPath, sleeping intervalMs
// milliseconds between published rows.
func New(csvPath string, intervalMs int, logger *zap.Logger) *Reader {
	return &Reader{csvPath: csvPath, intervalMs: intervalMs, logger: logger}
}

// Stream reads the CSV file in a continuous loop until ctx is cancelled.
// On each complete pass the row counter resets so rows are replayed from the
// beginning, simulating a live data feed.
func (r *Reader) Stream(ctx context.Context, coord *coordinator.Coordinator, pub Publisher) error {
	interval := time.Duration(r.intervalMs) * time.Millisecond
	rowNum := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if err := r.streamOnce(ctx, coord, pub, &rowNum, interval); err != nil {
			return err
		}
	}
}

// streamOnce reads the CSV file from the beginning to EOF, publishing rows
// assigned to this Streamer instance.  rowNum is updated across calls so that
// partition assignment is stable even across file rewinds.
func (r *Reader) streamOnce(
	ctx context.Context,
	coord *coordinator.Coordinator,
	pub Publisher,
	rowNum *int,
	interval time.Duration,
) error {
	f, err := os.Open(r.csvPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", r.csvPath, err)
	}
	defer f.Close()

	cr := csv.NewReader(f)
	// Skip header row.
	if _, err := cr.Read(); err != nil {
		return fmt.Errorf("read CSV header: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		row, err := cr.Read()
		if err == io.EOF {
			// One full pass complete; reset row counter and restart the loop.
			*rowNum = 0
			return nil
		}
		if err != nil {
			return fmt.Errorf("read CSV row: %w", err)
		}

		currentRow := *rowNum
		*rowNum++

		if !coord.ShouldPublish(currentRow) {
			continue
		}

		if len(row) < minCols {
			r.logger.Warn("skipping short CSV row",
				zap.Int("row_num", currentRow),
				zap.Int("got_cols", len(row)))
			continue
		}

		rec, err := parseRow(row)
		if err != nil {
			r.logger.Warn("skipping unparseable CSV row",
				zap.Int("row_num", currentRow),
				zap.Error(err))
			continue
		}

		if err := pub.Publish(ctx, rec, coord.Partition()); err != nil {
			return fmt.Errorf("publish row %d: %w", currentRow, err)
		}

		r.logger.Debug("published row",
			zap.Int("row_num", currentRow),
			zap.String("uuid", rec.Uuid),
			zap.String("metric", rec.MetricName))

		if interval > 0 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(interval):
			}
		}
	}
}

// parseRow converts one DCGM CSV data row into a TelemetryRecord proto.
// Per the project spec, the CSV's timestamp column is decorative — the
// canonical sample timestamp is the wall-clock time at which the Streamer
// processes the row, set fresh on every loop pass.
func parseRow(row []string) (*telemetrypb.TelemetryRecord, error) {
	value, err := strconv.ParseFloat(row[colValue], 64)
	if err != nil {
		return nil, fmt.Errorf("parse value %q: %w", row[colValue], err)
	}

	now := time.Now().UnixNano()

	return &telemetrypb.TelemetryRecord{
		IngestedUnixNs: now,
		SampleUnixNs:   now,
		MetricName:     row[colMetricName],
		GpuIndex:       row[colGpuIndex],
		Device:         row[colDevice],
		Uuid:           row[colUUID],
		ModelName:      row[colModelName],
		Hostname:       row[colHostname],
		Container:      row[colContainer],
		Pod:            row[colPod],
		Namespace:      row[colNamespace],
		Value:          value,
		LabelsRaw:      row[colLabelsRaw],
	}, nil
}
