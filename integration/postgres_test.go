package integration_test

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	. "github.com/shani8615/go-outbox"
)

// Integration test: set OUTBOX_TEST_DSN, e.g.
//
//	cd integration && go mod tidy
//	OUTBOX_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable go test ./...
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newRelay(s Store, p Publisher, batch int) *Relay {
	r := NewRelay(s, p)
	r.BatchSize = batch
	r.Logger = quiet
	return r
}

func openTestDB(t *testing.T) *sql.DB {
	dsn := os.Getenv("OUTBOX_TEST_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`DROP TABLE IF EXISTS outbox_test`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(Schema("outbox_test")); err != nil {
		t.Fatal(err)
	}
	return db
}

// enqueue commits msgs in their own transaction, failing the test on error.
func enqueue(t *testing.T, db *sql.DB, store *Postgres, msgs ...Message) {
	t.Helper()
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Enqueue(ctx, tx, msgs...); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// claim claims a batch and always releases it when the test ends, so a
// failing assertion can't leave row locks behind and hang later tests.
func claim(t *testing.T, store *Postgres, limit int) Batch {
	t.Helper()
	b, err := store.Claim(context.Background(), limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Rollback() })
	return b
}

func TestPostgresCommitAndRollback(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	store, err := NewPostgres(db, "outbox_test")
	if err != nil {
		t.Fatal(err)
	}

	// Rolled-back business transaction: event must not exist.
	tx, _ := db.BeginTx(ctx, nil)
	_ = store.Enqueue(ctx, tx, Message{Topic: "orders.created", Payload: []byte(`{"id":1}`)})
	_ = tx.Rollback()

	tx, _ = db.BeginTx(ctx, nil)
	if err := store.Enqueue(ctx, tx, Message{Topic: "orders.created", Key: "o-2", Payload: []byte(`{"id":2}`),
		Headers: map[string]string{"traceparent": "00-abc-def-01"}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var got []Message
	var mu sync.Mutex
	pub := PublisherFunc(func(_ context.Context, m Message) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, m)
		return nil
	})
	r := newRelay(store, pub, 10)
	if n, err := r.ProcessOnce(ctx); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if string(got[0].Payload) != `{"id":2}` || got[0].Key != "o-2" || got[0].Headers["traceparent"] == "" {
		t.Fatalf("got=%+v", got[0])
	}
	if n, _ := r.ProcessOnce(ctx); n != 0 {
		t.Fatal("published message must not be sent again")
	}
}

func TestPostgresSkipLocked(t *testing.T) {
	db := openTestDB(t)
	store, _ := NewPostgres(db, "outbox_test")
	for i := 0; i < 5; i++ {
		enqueue(t, db, store, Message{Topic: "t", Payload: []byte("x")})
	}

	b1 := claim(t, store, 3)
	b2 := claim(t, store, 3)
	if len(b1.Messages()) != 3 || len(b2.Messages()) != 2 {
		t.Fatalf("b1=%d b2=%d", len(b1.Messages()), len(b2.Messages()))
	}
}

// Regression: a nil payload was sent to Postgres as NULL and violated the
// NOT NULL constraint, so Enqueue failed for events without a body.
func TestPostgresNilPayload(t *testing.T) {
	db := openTestDB(t)
	store, _ := NewPostgres(db, "outbox_test")
	enqueue(t, db, store, Message{Topic: "t"})

	b := claim(t, store, 10)
	if len(b.Messages()) != 1 || len(b.Messages()[0].Payload) != 0 {
		t.Fatalf("messages=%+v", b.Messages())
	}
}

func TestPostgresParksAfterMaxAttempts(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	store, _ := NewPostgres(db, "outbox_test")
	store.MaxAttempts = 2
	enqueue(t, db, store, Message{Topic: "t", Payload: []byte("x")})

	r := newRelay(store, PublisherFunc(func(context.Context, Message) error { return sql.ErrConnDone }), 10)
	_, _ = r.ProcessOnce(ctx)
	_, _ = r.ProcessOnce(ctx)
	if b := claim(t, store, 10); len(b.Messages()) != 0 {
		t.Fatal("message should be parked after MaxAttempts failures")
	}
	var lastErr string
	if err := db.QueryRow(`SELECT last_error FROM outbox_test`).Scan(&lastErr); err != nil || lastErr == "" {
		t.Fatalf("last_error not recorded: %q err=%v", lastErr, err)
	}
}
