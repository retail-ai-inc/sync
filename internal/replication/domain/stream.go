package domain

import (
	"context"
	"time"
)

// A replicated change, and the three ports a replication engine implements.
//
// Every engine used to carry its own reading loop, its own batching, its own
// ordering rules and its own idea of when the position may be advanced. Four
// copies of a loop that has to be exactly right is four chances to get it
// wrong, and they had already drifted: MongoDB spooled to disk and advanced its
// token on a timer, MySQL held statements in memory and advanced its offset on
// a different timer, and only one of them cut its batches on source transaction
// boundaries.
//
// These types name what the loop needs so there can be one of it. An engine
// supplies a Reader that turns its log into Events, an Applier that writes them,
// and a Snapshotter for the first copy. Everything between — batching, ordering,
// back pressure, when the position moves — belongs to the runner.

// Namespace identifies one replicated object at the source.
//
// Object is a table for the relational engines and a collection for MongoDB.
type Namespace struct {
	DB     string
	Object string
}

// String renders the namespace as it appears in logs and metrics.
func (ns Namespace) String() string { return ns.DB + "." + ns.Object }

// Op is what happened to one row or document.
type Op uint8

const (
	// OpInsert is a row or document that did not exist before.
	OpInsert Op = iota + 1
	// OpUpdate replaces a row or document that did.
	OpUpdate
	// OpDelete removes one.
	OpDelete
	// OpSchema is a change to the shape of an object rather than its contents:
	// a MySQL DDL statement, or a MongoDB collection being dropped or renamed.
	OpSchema
)

// String names the operation for logs.
func (op Op) String() string {
	switch op {
	case OpInsert:
		return "insert"
	case OpUpdate:
		return "update"
	case OpDelete:
		return "delete"
	case OpSchema:
		return "schema"
	}
	return "unknown"
}

// Position is how far a stream has got, in whatever form the engine resumes
// from: a binlog file and offset with a GTID set, or a change stream resume
// token. The runner never looks inside it.
//
// Payload is empty for the zero value, which means "nothing has been recorded",
// and is what makes the runner take a snapshot before streaming.
type Position struct {
	Payload string
}

// IsZero reports whether anything has been recorded yet.
func (p Position) IsZero() bool { return p.Payload == "" }

// Event is one change read from the source log.
type Event struct {
	NS Namespace
	Op Op

	// Key identifies the row or document within its namespace, as a string that
	// is stable across events. Two events with the same namespace and key touch
	// the same record and must be applied in the order they were read.
	//
	// For MongoDB this is the whole documentKey, not just the _id: on a sharded
	// collection documentKey carries the shard key as well, and an update whose
	// filter omits the shard key cannot be routed to one shard — mongos has to
	// broadcast it to every one of them.
	Key string

	// Payload is what the applier needs to write, in whatever form the engine
	// produced: a statement for MySQL, a write model for MongoDB.
	Payload interface{}

	// Bytes is roughly how large the change is, used to cap a batch by size as
	// well as by count. Zero means the engine does not measure it.
	Bytes int

	// Pos is where the stream stands after this event.
	Pos Position

	// SourceTime is when the source made the change. The applied lag — the
	// number a disaster-recovery setup is judged on — is measured from it.
	SourceTime time.Time

	// EndsTransaction marks the last event of a source transaction.
	//
	// A batch may only be cut where this is true. A source transaction split
	// across two batches leaves the target holding half of it for as long as the
	// second batch takes, and permanently if the first batch is the last thing
	// applied before a failover: the order was written to the source as one
	// atomic act, and the target would show the order without its payment.
	//
	// An engine that cannot tell where transactions end sets this on every
	// event, which makes every event a legal cut point.
	EndsTransaction bool

	// Heartbeat marks an event the syncer produced itself rather than one the
	// source made. It proves the link is alive when nothing is being written,
	// and is not applied to the target.
	Heartbeat bool
}

// Reader turns one source log into a stream of events.
//
// There is exactly one Reader per task, reading one stream: a MySQL server's
// binlog, or a MongoDB deployment's change stream. Opening a stream per table
// or per collection makes the source do the same work over again for each one —
// a change stream cannot use an index, because there is no index on the oplog.
type Reader interface {
	// Open starts reading from a position, or from the current end of the log
	// when the position is zero.
	Open(ctx context.Context, from Position) error
	// Next blocks until an event arrives, the context is cancelled, or the
	// stream cannot continue.
	Next(ctx context.Context) (*Event, error)
	// Close releases the stream. It is safe to call more than once.
	Close() error
}

// Applier writes a batch of events to the target.
//
// Apply has to be idempotent: a batch may be applied twice when the position
// could not be recorded atomically with it. It must also be atomic — either the
// whole batch is visible on the target or none of it is. A partly applied batch
// leaves the target in a state the source was never in, which is the one kind
// of inconsistency a failover cannot recover from and an operator cannot detect.
type Applier interface {
	// Apply writes one batch, given as runs that must be applied in order, and
	// reports whether it also recorded pos as part of the same commit.
	//
	// The whole batch — every run — has to land as one atomic unit. Committing
	// the runs separately would be no better than not batching at all: a failure
	// between two runs leaves the target holding part of a batch, which is the
	// state this design exists to prevent. Within a single run no record appears
	// twice, so an applier is free to parallelise one; between runs it is not.
	//
	// Committing the position with the data is what a MySQL replica does — the
	// applier position lives in an InnoDB table and is updated in the
	// transaction that applies the rows, so the two cannot come apart. An
	// engine that can do the same returns true and the runner records nothing
	// further; one that cannot returns false and the runner records the
	// position afterwards, which is at-least-once and relies on idempotence.
	Apply(ctx context.Context, runs [][]*Event, pos Position) (committed bool, err error)
}

// Retention is a Reader that knows how far back the source's history reaches.
//
// It answers the only question that matters while a task is stopped: how long
// it can stay stopped. Past that point the position it saved is no longer in the
// source's log, and the only way back is copying everything again — which for a
// payment database is measured in hours, and is a decision somebody wants to
// make before the deadline rather than after.
//
// A Reader that cannot find out implements nothing, and no headroom is
// published. A number that is silently wrong is worse than no number.
type Retention interface {
	// Window is how much history the source still holds, counted back from now.
	Window(ctx context.Context) (time.Duration, error)
}

// Snapshotter makes the first copy, for a task that has no position yet.
type Snapshotter interface {
	// Pin records where the stream must resume from. It runs before a single
	// row is copied: reading the position afterwards loses every write made
	// while the copy was running, and the copy of a payment table runs for as
	// long as it runs.
	Pin(ctx context.Context) (Position, error)
	// Copy fills the target from the source, reading at the pinned point.
	Copy(ctx context.Context) error
}
