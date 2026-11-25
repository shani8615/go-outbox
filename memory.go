package outbox

import (
	"context"
	"sync"
	"time"
)

// Memory is an in-process Store for tests and examples. It is not durable.
type Memory struct {
	mu      sync.Mutex
	nextID  int64
	pending []*memMsg
	locked  map[int64]bool
}

type memMsg struct {
	Message
	published bool
	lastError string
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory { return &Memory{locked: map[int64]bool{}} }

// Add appends messages as if a transaction had committed.
func (s *Memory) Add(msgs ...Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range msgs {
		if m.Topic == "" {
			return ErrEmptyTopic
		}
		s.nextID++
		m.ID = s.nextID
		m.CreatedAt = time.Now()
		s.pending = append(s.pending, &memMsg{Message: m})
	}
	return nil
}

// Pending returns how many messages are not yet published.
func (s *Memory) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, m := range s.pending {
		if !m.published {
			n++
		}
	}
	return n
}

// Claim implements Store.
func (s *Memory) Claim(_ context.Context, limit int) (Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := &memBatch{s: s, updates: map[int64]func(*memMsg){}}
	for _, m := range s.pending {
		if len(b.msgs) >= limit {
			break
		}
		if m.published || s.locked[m.ID] {
			continue
		}
		s.locked[m.ID] = true
		b.msgs = append(b.msgs, m.Message)
	}
	return b, nil
}

type memBatch struct {
	s       *Memory
	msgs    []Message
	updates map[int64]func(*memMsg)
	order   []int64
}

func (b *memBatch) Messages() []Message { return b.msgs }

func (b *memBatch) MarkPublished(_ context.Context, ids []int64) error {
	for _, id := range ids {
		b.add(id, func(m *memMsg) { m.published = true })
	}
	return nil
}

func (b *memBatch) MarkFailed(_ context.Context, id int64, reason string) error {
	b.add(id, func(m *memMsg) { m.Attempts++; m.lastError = reason })
	return nil
}

func (b *memBatch) add(id int64, f func(*memMsg)) {
	prev := b.updates[id]
	b.updates[id] = func(m *memMsg) {
		if prev != nil {
			prev(m)
		}
		f(m)
	}
	b.order = append(b.order, id)
}

func (b *memBatch) Commit() error {
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	for _, m := range b.s.pending {
		if f, ok := b.updates[m.ID]; ok {
			f(m)
		}
	}
	b.unlock()
	return nil
}

func (b *memBatch) Rollback() error {
	b.s.mu.Lock()
	defer b.s.mu.Unlock()
	b.unlock()
	return nil
}

func (b *memBatch) unlock() {
	for _, m := range b.msgs {
		delete(b.s.locked, m.ID)
	}
}
