// Package outbox implements the transactional outbox pattern.
//
// Write the event in the same database transaction as your business change
// (Enqueue), and a Relay publishes pending events to your broker (NATS,
// Kafka, RabbitMQ, ...). The event is published only if the transaction
// commits, and it is retried until the broker accepts it. Delivery is
// at-least-once, so consumers should deduplicate on Message.ID.
package outbox

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Message is one outbox event.
type Message struct {
	ID        int64
	Topic     string            // subject / topic / routing key
	Key       string            // partition or ordering key (optional)
	Payload   []byte            // usually JSON
	Headers   map[string]string // trace context, event type, ...
	CreatedAt time.Time
	Attempts  int
}

// Publisher sends a message to the broker. It must return only after the
// broker has acknowledged the message.
type Publisher interface {
	Publish(ctx context.Context, m Message) error
}

// PublisherFunc adapts a function to Publisher.
type PublisherFunc func(ctx context.Context, m Message) error

func (f PublisherFunc) Publish(ctx context.Context, m Message) error { return f(ctx, m) }

// Batch is a set of claimed messages. No other relay can claim them until
// the batch is committed or rolled back.
type Batch interface {
	Messages() []Message
	MarkPublished(ctx context.Context, ids []int64) error
	MarkFailed(ctx context.Context, id int64, reason string) error
	Commit() error
	Rollback() error
}

// Store claims pending messages.
type Store interface {
	Claim(ctx context.Context, limit int) (Batch, error)
}

// Execer is satisfied by *sql.Tx (and *sql.DB, though using a Tx is the point).
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// ErrEmptyTopic is returned by Enqueue for messages without a topic.
var ErrEmptyTopic = errors.New("outbox: message topic is empty")
