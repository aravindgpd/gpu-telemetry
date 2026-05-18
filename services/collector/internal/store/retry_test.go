package store

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestRetrySucceedsImmediately: fn returns nil first try → no waiting.
func TestRetrySucceedsImmediately(t *testing.T) {
	calls := atomic.Int32{}
	err := retryWithBackoff(context.Background(), zap.NewNop(), "test", func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if calls.Load() != 1 {
		t.Errorf("called %d times, want 1", calls.Load())
	}
}

// TestRetrySucceedsAfterTransientFailures: simulates a dependency that's
// down for the first N attempts then recovers — the canonical case.
func TestRetrySucceedsAfterTransientFailures(t *testing.T) {
	calls := atomic.Int32{}
	err := retryWithBackoff(context.Background(), zap.NewNop(), "test", func(context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("not ready yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil after recovery", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("called %d times, want 3", got)
	}
}

// TestRetryRespectsContextCancellation: a cancelled ctx breaks the loop with
// ctx.Err() rather than running forever.
func TestRetryRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay so the first backoff sleep is interrupted.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := retryWithBackoff(ctx, zap.NewNop(), "test", func(context.Context) error {
		return errors.New("always fails")
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// TestRetryStopsAtContextDeadline: an expired deadline ctx exits with
// context.DeadlineExceeded.
func TestRetryStopsAtContextDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err := retryWithBackoff(ctx, zap.NewNop(), "test", func(context.Context) error {
		return errors.New("always fails")
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}
