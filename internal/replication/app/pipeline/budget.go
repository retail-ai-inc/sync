package pipeline

import (
	"context"
	"sync"
)

// A byte budget for the change data held between the reader and the applier.
//
// The queue is bounded by event count, and a count is not a bound on memory: a
// MongoDB document may be 16MB and a Redis value 512MB, so two thousand events
// is anywhere from a few kilobytes to something that ends the process. The count
// still decides how far ahead the reader may run; this decides how much it may
// hold while doing so.

type budget struct {
	limit int64

	mu    sync.Mutex
	room  *sync.Cond
	held  int64
	drain bool
}

func newBudget(limit int64) *budget {
	b := &budget{limit: limit}
	b.room = sync.NewCond(&b.mu)
	return b
}

// admit charges size without waiting for room.
//
// For an event that continues a source transaction. The applier may not cut a
// batch inside one, so it cannot release anything until the transaction's last
// event arrives -- and if the reader waits here for room before handing that
// event over, neither side can move and replication stops for good. A
// transaction two events long against a budget that fits one reproduces it.
//
// The budget bounds how far the reader runs ahead, and a transaction is the
// smallest unit it can run ahead by; going over on one is the same trade the
// single-event case already makes.
func (b *budget) admit(size int64) {
	if b == nil || b.limit <= 0 || size <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.held += size
}

// acquire waits until size fits, then charges it.
//
// An event larger than the whole budget is admitted alone rather than waited
// on: nothing will ever free enough room for it, and the alternative is a
// reader that stops for good. The budget is a bound on how much is held
// together, not a limit on what one event may be.
func (b *budget) acquire(ctx context.Context, size int64) {
	if b == nil || b.limit <= 0 || size <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	stop := context.AfterFunc(ctx, func() {
		b.mu.Lock()
		b.drain = true
		b.room.Broadcast()
		b.mu.Unlock()
	})
	defer stop()

	for !b.drain && b.held > 0 && b.held+size > b.limit {
		b.room.Wait()
	}
	b.held += size
}

// release gives the room back, which is done when a batch has landed on the
// target and the events in it are no longer held anywhere.
func (b *budget) release(size int64) {
	if b == nil || b.limit <= 0 || size <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	b.held -= size
	if b.held < 0 {
		b.held = 0
	}
	b.room.Broadcast()
}

// heldBytes reports what is charged now, for the metric.
func (b *budget) heldBytes() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.held
}
