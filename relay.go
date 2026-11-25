package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// Relay polls a Store and publishes pending messages.
type Relay struct {
	Store     Store
	Publisher Publisher
	BatchSize int           // default 100
	Interval  time.Duration // idle poll interval; default 1s
	Logger    *slog.Logger  // default slog.Default()
	// StopOnError keeps per-batch ordering strict: after the first publish
	// failure the rest of the batch waits for the next round. Default true
	// via NewRelay.
	StopOnError bool
}

// NewRelay returns a Relay with defaults applied.
func NewRelay(s Store, p Publisher) *Relay {
	return &Relay{Store: s, Publisher: p, BatchSize: 100, Interval: time.Second, StopOnError: true}
}

// Run processes batches until ctx is cancelled. When a batch is full it
// immediately polls again; otherwise it waits Interval.
func (r *Relay) Run(ctx context.Context) error {
	log := r.logger()
	for {
		n, err := r.ProcessOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("outbox relay batch failed", slog.String("error", err.Error()))
		}
		wait := r.interval()
		if n >= r.batchSize() && err == nil {
			wait = 0
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// ProcessOnce claims one batch, publishes it, and returns how many messages
// were published.
func (r *Relay) ProcessOnce(ctx context.Context) (int, error) {
	b, err := r.Store.Claim(ctx, r.batchSize())
	if err != nil {
		return 0, err
	}
	msgs := b.Messages()
	if len(msgs) == 0 {
		return 0, b.Rollback()
	}
	var published []int64
	var pubErr error
	for _, m := range msgs {
		if err := r.Publisher.Publish(ctx, m); err != nil {
			pubErr = err
			r.logger().Warn("outbox publish failed",
				slog.Int64("id", m.ID), slog.String("topic", m.Topic), slog.String("error", err.Error()))
			if mfErr := b.MarkFailed(ctx, m.ID, truncate(err.Error(), 500)); mfErr != nil {
				_ = b.Rollback()
				return 0, mfErr
			}
			if r.StopOnError {
				break
			}
			continue
		}
		published = append(published, m.ID)
	}
	if err := b.MarkPublished(ctx, published); err != nil {
		_ = b.Rollback()
		return 0, err
	}
	if err := b.Commit(); err != nil {
		return 0, err
	}
	return len(published), pubErr
}

func (r *Relay) batchSize() int {
	if r.BatchSize <= 0 {
		return 100
	}
	return r.BatchSize
}

func (r *Relay) interval() time.Duration {
	if r.Interval <= 0 {
		return time.Second
	}
	return r.Interval
}

func (r *Relay) logger() *slog.Logger {
	if r.Logger == nil {
		return slog.Default()
	}
	return r.Logger
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
