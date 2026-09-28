package pipeline

import (
	"context"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"sync"
	"testing"
	"time"
)

// A batch is bounded by bytes as well as by events. Five hundred events is a
// few kilobytes of Redis counters or eight gigabytes of MongoDB documents, and
// only one of those is a batch.
func TestABatchIsBoundedByBytesAsWellAsEvents(t *testing.T) {
	if defaultMaxBytes <= 0 {
		t.Fatal("a batch has no byte limit, so its size is whatever the source sends")
	}
	if got := (Limits{}).maxBytes(); got != defaultMaxBytes {
		t.Errorf("maxBytes with nothing set = %d, want the default %d", got, defaultMaxBytes)
	}
	if got := (Limits{MaxBytes: 1024}).maxBytes(); got != 1024 {
		t.Errorf("maxBytes = %d, want what the task asked for", got)
	}

	// Well under the event limit, over the byte limit.
	b := &batch{}
	for i := 0; i < 8; i++ {
		b.add(&domain.Event{Bytes: defaultMaxBytes / 4, EndsTransaction: true})
	}
	if len(b.events) >= (Limits{}).maxEvents() {
		t.Fatalf("the batch reached the event limit (%d events), so this proves nothing",
			len(b.events))
	}
	if !b.full(Limits{}) {
		t.Errorf("a batch holding %d bytes is not full, so a batch of large events is unbounded",
			b.bytes)
	}
}

// The queue's capacity counts events, which does not bound memory. The budget
// does, and the reader waits on it.
func TestTheReaderWaitsOnTheByteBudget(t *testing.T) {
	b := newBudget(1000)
	ctx := context.Background()

	b.acquire(ctx, 600)
	if got := b.heldBytes(); got != 600 {
		t.Fatalf("held = %d, want 600", got)
	}

	var wg sync.WaitGroup
	admitted := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		b.acquire(ctx, 600) // 1200 > 1000, so this must wait
		close(admitted)
	}()

	select {
	case <-admitted:
		t.Fatal("the second acquire was admitted, so the budget bounds nothing")
	case <-time.After(50 * time.Millisecond):
	}

	b.release(600)
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("room was given back and the waiter was not woken")
	}
	wg.Wait()
}

// An event larger than the whole budget still has to get through: nothing will
// ever free enough room for it, and a reader that stops for good is worse than
// one that holds one big event.
func TestAnEventLargerThanTheBudgetIsStillAdmitted(t *testing.T) {
	b := newBudget(1000)
	done := make(chan struct{})
	go func() { b.acquire(context.Background(), 5000); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("an event larger than the budget was never admitted, so the reader stopped for good")
	}
}

// A cancelled context releases every waiter rather than leaving the reader
// blocked through shutdown.
func TestCancellingReleasesTheWaiters(t *testing.T) {
	b := newBudget(100)
	b.acquire(context.Background(), 100)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { b.acquire(ctx, 100); close(done) }()

	select {
	case <-done:
		t.Fatal("admitted while the budget was full")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a cancelled reader is still waiting on the budget")
	}
}
