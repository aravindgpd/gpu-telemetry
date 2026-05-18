package publisher

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// retryWithBackoff runs fn until it returns nil or ctx is cancelled.
// Backoff doubles each failure, capped at maxBackoff. The label appears in
// every warn log so operators can see *which* dependency we're waiting on.
//
// Use this for "wait until a dependency comes up" rather than for transient
// errors mid-stream — once a Publish stream is open, gRPC handles its own
// reconnection at the transport layer.
func retryWithBackoff(ctx context.Context, logger *zap.Logger, label string, fn func(context.Context) error) error {
	const (
		initial = 200 * time.Millisecond
		maxWait = 10 * time.Second
	)
	backoff := initial
	attempt := 0

	for {
		attempt++
		if err := fn(ctx); err == nil {
			if attempt > 1 {
				logger.Info("dependency reachable",
					zap.String("op", label),
					zap.Int("attempts", attempt))
			}
			return nil
		} else {
			logger.Warn("waiting for dependency",
				zap.String("op", label),
				zap.Int("attempt", attempt),
				zap.Duration("next_retry_in", backoff),
				zap.Error(err))
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		if backoff < maxWait {
			backoff *= 2
			if backoff > maxWait {
				backoff = maxWait
			}
		}
	}
}
