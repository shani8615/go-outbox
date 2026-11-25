package outbox

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Schema returns the DDL for the outbox table.
func Schema(table string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    id           BIGSERIAL PRIMARY KEY,
    topic        TEXT        NOT NULL,
    msg_key      TEXT        NOT NULL DEFAULT '',
    payload      BYTEA       NOT NULL,
    headers      JSONB       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INT         NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE INDEX IF NOT EXISTS %[2]s_pending_idx ON %[1]s (id) WHERE published_at IS NULL;`, table, strings.ReplaceAll(table, ".", "_"))
}

var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$`)

// Postgres is a Store backed by PostgreSQL using FOR UPDATE SKIP LOCKED, so
// several relay instances can run side by side without double-publishing.
type Postgres struct {
	db    *sql.DB
	table string
	// MaxAttempts parks messages after this many failures (0 = never park).
	MaxAttempts int
}

// NewPostgres returns a Postgres store for the given table (default "outbox").
func NewPostgres(db *sql.DB, table string) (*Postgres, error) {
	if table == "" {
		table = "outbox"
	}
	if !identRe.MatchString(table) {
		return nil, fmt.Errorf("outbox: invalid table name %q", table)
	}
	return &Postgres{db: db, table: table}, nil
}

// Enqueue inserts messages using tx — call it inside the same transaction as
// your business write.
func (p *Postgres) Enqueue(ctx context.Context, tx Execer, msgs ...Message) error {
	for _, m := range msgs {
		if m.Topic == "" {
			return ErrEmptyTopic
		}
		h := m.Headers
		if h == nil {
			h = map[string]string{}
		}
		hb, err := json.Marshal(h)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`INSERT INTO `+p.table+` (topic, msg_key, payload, headers) VALUES ($1, $2, $3, $4)`,
			m.Topic, m.Key, m.Payload, hb)
		if err != nil {
			return fmt.Errorf("outbox: enqueue: %w", err)
		}
	}
	return nil
}

// Claim locks up to limit pending messages in id order.
func (p *Postgres) Claim(ctx context.Context, limit int) (Batch, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	q := `SELECT id, topic, msg_key, payload, headers, created_at, attempts FROM ` + p.table +
		` WHERE published_at IS NULL`
	args := []any{limit}
	if p.MaxAttempts > 0 {
		q += ` AND attempts < $2`
		args = append(args, p.MaxAttempts)
	}
	q += ` ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED`
	rows, err := tx.QueryContext(ctx, q, args...)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var msgs []Message
	for rows.Next() {
		var m Message
		var hb []byte
		if err := rows.Scan(&m.ID, &m.Topic, &m.Key, &m.Payload, &hb, &m.CreatedAt, &m.Attempts); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		if len(hb) > 0 {
			_ = json.Unmarshal(hb, &m.Headers)
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return &pgBatch{tx: tx, table: p.table, msgs: msgs}, nil
}

type pgBatch struct {
	tx    *sql.Tx
	table string
	msgs  []Message
}

func (b *pgBatch) Messages() []Message { return b.msgs }

func (b *pgBatch) MarkPublished(ctx context.Context, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}
	_, err := b.tx.ExecContext(ctx,
		`UPDATE `+b.table+` SET published_at = now() WHERE id IN (`+strings.Join(ph, ",")+`)`, args...)
	return err
}

func (b *pgBatch) MarkFailed(ctx context.Context, id int64, reason string) error {
	_, err := b.tx.ExecContext(ctx,
		`UPDATE `+b.table+` SET attempts = attempts + 1, last_error = $2 WHERE id = $1`, id, reason)
	return err
}

func (b *pgBatch) Commit() error   { return b.tx.Commit() }
func (b *pgBatch) Rollback() error { return b.tx.Rollback() }
