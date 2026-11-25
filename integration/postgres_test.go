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
	ctx := context.Background()
	store, _ := NewPostgres(db, "outbox_test")
	tx, _ := db.BeginTx(ctx, nil)
	for i := 0; i < 5; i++ {
		_ = store.Enqueue(ctx, tx, Message{Topic: "t"})
	}
	_ = tx.Commit()

	b1, err := store.Claim(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := store.Claim(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(b1.Messages()) != 3 || len(b2.Messages()) != 2 {
		t.Fatalf("b1=%d b2=%d", len(b1.Messages()), len(b2.Messages()))
	}
	_ = b1.Rollback()
	_ = b2.Rollback()
}

func TestPostgresParksAfterMaxAttempts(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	store, _ := NewPostgres(db, "outbox_test")
	store.MaxAttempts = 2
	tx, _ := db.BeginTx(ctx, nil)
	_ = store.Enqueue(ctx, tx, Message{Topic: "t"})
	_ = tx.Commit()

	r := newRelay(store, PublisherFunc(func(context.Context, Message) error { return sql.ErrConnDone }), 10)
	_, _ = r.ProcessOnce(ctx)
	_, _ = r.ProcessOnce(ctx)
	b, _ := store.Claim(ctx, 10)
	defer func() { _ = b.Rollback() }()
	if len(b.Messages()) != 0 {
		t.Fatal("message should be parked after MaxAttempts failures")
	}
	var lastErr string
	_ = db.QueryRow(`SELECT last_error FROM outbox_test`).Scan(&lastErr)
	if lastErr == "" {
		t.Fatal("last_error not recorded")
	}
}
