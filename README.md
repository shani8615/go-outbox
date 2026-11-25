# go-outbox

[![ci](https://github.com/shani8615/go-outbox/actions/workflows/ci.yml/badge.svg)](https://github.com/shani8615/go-outbox/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/shani8615/go-outbox.svg)](https://pkg.go.dev/github.com/shani8615/go-outbox)
[![Release](https://img.shields.io/github/v/release/shani8615/go-outbox?sort=semver)](https://github.com/shani8615/go-outbox/releases)
[![Go version](https://img.shields.io/github/go-mod/go-version/shani8615/go-outbox)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

The **transactional outbox pattern** for Go + PostgreSQL. It works with any broker: NATS, Kafka, RabbitMQ, SQS.

**The problem:** your service saves an order and then publishes `orders.created`. If the publish fails after the commit, the event is lost. If you publish before the commit and the commit fails, you've announced an order that doesn't exist.

**The fix:** write the event into an `outbox` table *in the same transaction* as the order. A relay then publishes pending rows and marks them done.

```
┌──────────── one DB transaction ────────────┐
│ INSERT INTO orders ...                      │
│ INSERT INTO outbox (topic, payload) ...     │──► Relay ──► NATS / Kafka
└─────────────────────────────────────────────┘    (SKIP LOCKED, retries, ordering)
```

```bash
go get github.com/shani8615/go-outbox
```

## Usage

```go
db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
_, _ = db.Exec(outbox.Schema("outbox")) // or put it in your migrations
store, _ := outbox.NewPostgres(db, "outbox")

// 1. In your business transaction
tx, _ := db.BeginTx(ctx, nil)
_, _ = tx.ExecContext(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, id, total)
_ = store.Enqueue(ctx, tx, outbox.Message{
    Topic:   "orders.created",
    Key:     id,
    Payload: payload,
    Headers: map[string]string{"event_type": "OrderCreated"},
})
_ = tx.Commit()

// 2. Somewhere (same process or a separate worker), run the relay
nc, _ := nats.Connect(nats.DefaultURL)
js, _ := jetstream.New(nc)
relay := outbox.NewRelay(store, outbox.PublisherFunc(func(ctx context.Context, m outbox.Message) error {
    msg := nats.NewMsg(m.Topic)
    msg.Data = m.Payload
    msg.Header.Set(jetstream.MsgIDHeader, strconv.FormatInt(m.ID, 10)) // broker-side dedup
    for k, v := range m.Headers {
        msg.Header.Set(k, v)
    }
    _, err := js.PublishMsg(ctx, msg)
    return err
}))
go relay.Run(ctx)
```

## Guarantees

- **At-least-once delivery.** A message can be published twice if the relay crashes between the broker ack and the DB commit. Consumers should deduplicate on `Message.ID`, or use broker dedup as in the example above.
- **Ordering.** Messages are claimed in `id` order. By default a publish failure stops the batch, so later messages don't overtake earlier ones.
- **Horizontal scaling.** `FOR UPDATE SKIP LOCKED` lets several relays run at once without double-claiming.
- **Poison messages.** Set `store.MaxAttempts` to park a message after N failures. `last_error` records why.

## Testing

Unit tests use the in-memory store. Postgres integration tests run when `OUTBOX_TEST_DSN` is set:

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:16
cd integration && go mod tidy
OUTBOX_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable go test -race ./...
```

## Why go-outbox?

Several Go outbox libraries exist, including Watermill's forwarder (if you already use [Watermill](https://github.com/ThreeDotsLabs/watermill)) and driver-specific ones like [pgx-outbox](https://github.com/nikolayk812/pgx-outbox). go-outbox's choices:

- **`database/sql`**, so it works with pgx's stdlib driver, lib/pq, or anything else, inside the transaction you already have.
- **Any broker** through a one-function `Publisher`: NATS JetStream, Kafka, RabbitMQ, SQS, HTTP.
- **Several relays at once** with `FOR UPDATE SKIP LOCKED`, without double-claiming.
- **Ordering preserved** by stopping the batch on the first failure.
- **Poison messages parked** after `MaxAttempts`, with `last_error` recorded.
- **No dependencies**, with integration tests against a real Postgres in CI.

If your stack is already Watermill, use its forwarder. go-outbox is for when you want just the outbox and nothing else.

## License

MIT
