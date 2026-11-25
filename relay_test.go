package outbox

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type recorder struct {
	mu     sync.Mutex
	got    []Message
	failOn map[int64]int // id -> remaining failures
}

func (r *recorder) Publish(_ context.Context, m Message) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failOn[m.ID] > 0 {
		r.failOn[m.ID]--
		return errors.New("broker unavailable")
	}
	r.got = append(r.got, m)
	return nil
}

func newRelay(s Store, p Publisher, batch int) *Relay {
	r := NewRelay(s, p)
	r.BatchSize = batch
	r.Logger = quiet
	return r
}

func TestPublishesInOrder(t *testing.T) {
	s := NewMemory()
	_ = s.Add(Message{Topic: "orders.created", Payload: []byte("1")}, Message{Topic: "orders.created", Payload: []byte("2")})
	pub := &recorder{}
	n, err := newRelay(s, pub, 10).ProcessOnce(context.Background())
	if err != nil || n != 2 || s.Pending() != 0 {
		t.Fatalf("n=%d err=%v pending=%d", n, err, s.Pending())
	}
	if string(pub.got[0].Payload) != "1" || string(pub.got[1].Payload) != "2" {
		t.Fatal("order not preserved")
	}
}

func TestFailureStopsBatchAndRetries(t *testing.T) {
	s := NewMemory()
	_ = s.Add(Message{Topic: "t", Payload: []byte("a")}, Message{Topic: "t", Payload: []byte("b")}, Message{Topic: "t", Payload: []byte("c")})
	pub := &recorder{failOn: map[int64]int{2: 1}}
	r := newRelay(s, pub, 10)

	n, err := r.ProcessOnce(context.Background())
	if err == nil || n != 1 || s.Pending() != 2 {
		t.Fatalf("first round: n=%d err=%v pending=%d", n, err, s.Pending())
	}
	n, err = r.ProcessOnce(context.Background())
	if err != nil || n != 2 || s.Pending() != 0 {
		t.Fatalf("second round: n=%d err=%v pending=%d", n, err, s.Pending())
	}
	var order string
	for _, m := range pub.got {
		order += string(m.Payload)
	}
	if order != "abc" {
		t.Fatalf("order=%s", order)
	}
}

func TestConcurrentClaimsDoNotOverlap(t *testing.T) {
	s := NewMemory()
	for i := 0; i < 10; i++ {
		_ = s.Add(Message{Topic: "t"})
	}
	b1, _ := s.Claim(context.Background(), 6)
	b2, _ := s.Claim(context.Background(), 6)
	if len(b1.Messages()) != 6 || len(b2.Messages()) != 4 {
		t.Fatalf("b1=%d b2=%d", len(b1.Messages()), len(b2.Messages()))
	}
	seen := map[int64]bool{}
	for _, m := range append(b1.Messages(), b2.Messages()...) {
		if seen[m.ID] {
			t.Fatalf("message %d claimed twice", m.ID)
		}
		seen[m.ID] = true
	}
	_ = b1.Rollback()
	b3, _ := s.Claim(context.Background(), 10)
	if len(b3.Messages()) != 6 {
		t.Fatalf("rolled-back messages must be claimable again, got %d", len(b3.Messages()))
	}
}

func TestRunStopsOnCancel(t *testing.T) {
	s := NewMemory()
	_ = s.Add(Message{Topic: "t"})
	pub := &recorder{}
	r := newRelay(s, pub, 10)
	r.Interval = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v", err)
	}
	if len(pub.got) != 1 {
		t.Fatalf("published=%d", len(pub.got))
	}
}

func TestEmptyTopicRejected(t *testing.T) {
	if err := NewMemory().Add(Message{}); !errors.Is(err, ErrEmptyTopic) {
		t.Fatal("want ErrEmptyTopic")
	}
	if _, err := NewPostgres(nil, "outbox; DROP TABLE users"); err == nil {
		t.Fatal("unsafe table name must be rejected")
	}
}
