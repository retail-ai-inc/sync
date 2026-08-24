// Package pipeline is the one replication loop every engine runs.
//
// It reads a single stream, groups events into batches, applies each batch
// atomically and records where it got to. What each engine supplies is a
// Reader, an Applier and a Snapshotter; the rules that have to be exactly right
// — where a batch may be cut, what may be reordered within one, when the
// position moves — live here, once.
package pipeline

import (
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Limits cap how large a batch may grow before it is applied.
type Limits struct {
	// MaxEvents is the event count a batch aims for. Zero means the default.
	MaxEvents int
	// MaxBytes is the size a batch aims for. Zero means no size limit.
	MaxBytes int
	// MaxTransactionEvents is the point at which a single source transaction is
	// refused rather than buffered further. Zero means the default.
	//
	// A batch may not be cut inside a source transaction, so a transaction
	// larger than memory would otherwise take the process down. Refusing says
	// what happened and leaves the operator a knob; an out-of-memory kill in the
	// middle of applying a payment batch does neither.
	MaxTransactionEvents int
}

const (
	defaultMaxEvents            = 500
	defaultMaxTransactionEvents = 200_000
)

func (l Limits) maxEvents() int {
	if l.MaxEvents > 0 {
		return l.MaxEvents
	}
	return defaultMaxEvents
}

func (l Limits) maxTransactionEvents() int {
	if l.MaxTransactionEvents > 0 {
		return l.MaxTransactionEvents
	}
	return defaultMaxTransactionEvents
}

// batch accumulates events until it may be applied.
type batch struct {
	events []*domain.Event
	bytes  int
	// sinceBoundary counts events held since the last transaction boundary, so
	// a runaway transaction is noticed rather than buffered until the process
	// dies.
	sinceBoundary int
}

func (b *batch) add(e *domain.Event) {
	b.events = append(b.events, e)
	b.bytes += e.Bytes
	b.sinceBoundary++
	if e.EndsTransaction {
		b.sinceBoundary = 0
	}
}

func (b *batch) len() int { return len(b.events) }

// cuttable reports whether the batch may be applied as it stands.
//
// It may not be cut in the middle of a source transaction. A transaction split
// across two batches leaves the target holding part of it — the order without
// its payment — for as long as the second batch takes, and permanently if the
// first batch is the last thing applied before a failover. That state never
// existed at the source, so nothing downstream is written to cope with it.
//
// This is the rule AWS DMS spells BatchApplyPreserveTransaction, and its default
// is on for the same reason.
func (b *batch) cuttable() bool {
	if len(b.events) == 0 {
		return false
	}
	return b.events[len(b.events)-1].EndsTransaction
}

// full reports whether the batch has reached a limit and may be cut.
//
// Reaching a limit is not enough on its own: the batch keeps taking events
// until the transaction it is inside ends.
func (b *batch) full(l Limits) bool {
	if !b.cuttable() {
		return false
	}
	if len(b.events) >= l.maxEvents() {
		return true
	}
	return l.MaxBytes > 0 && b.bytes >= l.MaxBytes
}

// overrunning reports whether one source transaction has grown past what may be
// held, which is a failure rather than a batch to apply.
func (b *batch) overrunning(l Limits) bool {
	return b.sinceBoundary > l.maxTransactionEvents()
}

// take returns the accumulated events and resets the batch.
func (b *batch) take() []*domain.Event {
	events := b.events
	b.events = nil
	b.bytes = 0
	b.sinceBoundary = 0
	return events
}

// standsAlone reports whether an event has to be the only one in its batch.
//
// A schema change cannot share a batch with rows. On MongoDB it cannot run
// inside a transaction at all — the catalogue is not transactional — so a batch
// holding both could not be applied atomically, and applying the two halves
// separately is exactly the torn batch the design forbids. On MySQL a DDL
// commits implicitly, which has the same effect. Giving it a batch of its own
// makes both engines honest about it.
func standsAlone(e *domain.Event) bool {
	return e != nil && e.Op == domain.OpSchema
}

// holdsSchemaChange reports whether the batch is a schema change rather than a
// set of row changes, which decides whether the applier may open a transaction
// for it.
func holdsSchemaChange(events []*domain.Event) bool {
	for _, e := range events {
		if e.Op == domain.OpSchema {
			return true
		}
	}
	return false
}

// runKey identifies the record an event touches, across namespaces.
func runKey(e *domain.Event) (string, bool) {
	if e.Key == "" || e.Op == domain.OpSchema {
		// Nothing to compare, or a change to the shape of the object rather than
		// one record in it. Either way it cannot be reordered against anything.
		return "", false
	}
	return e.NS.String() + "\x00" + e.Key, true
}

// orderedRuns splits a batch into groups that may each be applied in any order
// internally, while the groups themselves are applied in sequence.
//
// Two events touching the same record have to be applied in the order they were
// read: an insert followed by a delete leaves nothing behind, and the same two
// applied the other way round leave the row there. Within one run no record
// appears twice, so the applier is free to parallelise it — which is exactly the
// rule a MongoDB secondary applies when it hands an oplog batch to its writer
// threads: operations on one document go to one thread, everything else may go
// wide.
//
// An event that names no record — a DDL statement — is a barrier: it gets a run
// of its own and nothing after it may join a run that came before it.
func orderedRuns(events []*domain.Event) [][]*domain.Event {
	var runs [][]*domain.Event
	var keys []map[string]struct{}
	// firstOpen is the earliest run that may still take another event. A barrier
	// moves it past every run that exists.
	firstOpen := 0

	for _, event := range events {
		key, ok := runKey(event)
		if !ok {
			runs = append(runs, []*domain.Event{event})
			keys = append(keys, nil)
			firstOpen = len(runs)
			continue
		}

		placed := false
		for i := firstOpen; i < len(runs); i++ {
			if keys[i] == nil {
				continue
			}
			if _, clash := keys[i][key]; clash {
				// An earlier run already holds this record, so this event has to
				// go after it.
				continue
			}
			runs[i] = append(runs[i], event)
			keys[i][key] = struct{}{}
			placed = true
			break
		}
		if !placed {
			runs = append(runs, []*domain.Event{event})
			keys = append(keys, map[string]struct{}{key: {}})
		}
	}
	return runs
}

// applicable drops the events that exist only to prove the link is alive.
//
// A heartbeat is written by the syncer, not the source, and writing it to the
// target would replicate the syncer's own bookkeeping into the payment data.
func applicable(events []*domain.Event) []*domain.Event {
	kept := events[:0:0]
	for _, e := range events {
		if e.Heartbeat {
			continue
		}
		kept = append(kept, e)
	}
	return kept
}
