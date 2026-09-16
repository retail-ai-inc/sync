package domain

import (
	"context"
	"errors"
	"time"
)

// A replicated change, and the three ports a replication engine implements: a
// Reader that turns its log into Events, an Applier that writes them, and a
// Snapshotter for the first copy. Batching, ordering, back pressure and when
// the position moves all belong to the runner, not to the engine.

// Namespace identifies one replicated object: a table for the relational
// engines, a collection for MongoDB.
type Namespace struct {
	DB     string
	Object string
}

func (ns Namespace) String() string { return ns.DB + "." + ns.Object }

type Op uint8

const (
	OpInsert Op = iota + 1
	OpUpdate
	OpDelete
	// OpSchema is a change to the shape of an object rather than its contents:
	// a MySQL DDL statement, or a MongoDB collection being dropped or renamed.
	OpSchema
)

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
// from. The runner never looks inside it. An empty Payload means nothing has
// been recorded, which is what makes the runner take a snapshot first.
type Position struct {
	Payload string
}

func (p Position) IsZero() bool { return p.Payload == "" }

type Event struct {
	NS Namespace
	Op Op

	// Key identifies the record within its namespace. Two events with the same
	// namespace and key must be applied in the order they were read.
	//
	// For MongoDB this is the whole documentKey, not just the _id: on a sharded
	// collection it carries the shard key, and a filter without the shard key
	// cannot be routed to one shard.
	Key string

	// Payload is what the applier writes: a statement for MySQL, a write model
	// for MongoDB.
	Payload interface{}

	// Bytes caps a batch by size as well as by count. Zero means the engine
	// does not measure it.
	Bytes int

	Pos Position

	// SourceTime is when the source made the change. The applied lag is
	// measured from it, and so is every ordering decision that compares the
	// stream against something else -- a re-copy's chunk waits on it. It is the
	// source's own ordering clock, which is not always a wall clock: MongoDB's
	// cluster time counts whole seconds.
	SourceTime time.Time

	// WallTime is the source's wall clock at the change, for sources that report
	// one alongside their ordering clock. Zero when there is none.
	//
	// Only the lag gauges read it. A wall clock can jump and two of them can
	// disagree, so nothing may order by it -- but measuring a sub-second delay
	// against a clock that counts whole seconds reports up to a second of delay
	// that is not there, which is what this exists to avoid.
	WallTime time.Time

	// Landed, when set, is closed once the batch carrying this event has been
	// applied to the target. It is nil for everything the stream produces.
	//
	// It exists for the re-copy, whose progress is stored so an interrupted run
	// resumes where it stopped. Handing a chunk to the queue is not applying it,
	// and recording progress on the hand-over meant a crash between the two lost
	// those rows for good: the next run resumed past them, and the re-copy that
	// was meant to repair the target had quietly skipped part of it.
	Landed chan struct{}

	// EndsTransaction marks the last event of a source transaction, and a batch
	// may only be cut where it is true: a transaction split across two batches
	// shows the target the order without its payment, permanently so if the
	// failover lands in between. An engine that cannot tell where transactions
	// end sets this on every event, making every event a legal cut point.
	EndsTransaction bool

	// Heartbeat marks an event the syncer produced itself. It proves the link
	// is alive when nothing is being written, and is not applied to the target.
	Heartbeat bool
}

// Reader turns one source log into a stream of events.
//
// Exactly one per task: opening a stream per table makes the source repeat the
// same work for each one, and a change stream cannot use an index because there
// is no index on the oplog.
type Reader interface {
	// Open starts reading from a position, or from the current end of the log
	// when the position is zero.
	Open(ctx context.Context, from Position) error
	Next(ctx context.Context) (*Event, error)
	// Close releases the stream. It is safe to call more than once.
	Close() error
}

// CopyBoundary is a Reader that reads the first copy's window differently from
// what follows it, and so needs to be told where that window ended.
//
// Only the pipeline knows: the boundary is the moment the snapshotter returns.
// Redis implements it because a command issued while the copy was running may
// already be inside the copy, so those are applied by re-reading the value
// rather than by replaying the command.
type CopyBoundary interface {
	CopyFinished()
}

// Applier writes a batch of events to the target.
//
// Apply must be idempotent, because a batch may be applied twice when the
// position could not be recorded with it, and atomic — a partly applied batch
// leaves the target in a state the source was never in, which is the one kind
// of inconsistency a failover cannot recover from and nobody can detect.
type Applier interface {
	// Apply writes one batch, given as runs that must be applied in order, and
	// reports whether it also recorded pos in the same commit. Every run has to
	// land as one atomic unit; committing them separately is no better than not
	// batching.
	Apply(ctx context.Context, runs [][]*Event, pos Position) (committed bool, err error)
}

// ErrWindowNotYet separates "cannot answer" from "cannot answer yet". The
// caller gives up permanently on the first refusal, so a source that needs a
// second measurement must not be written off on the strength of the first.
var ErrWindowNotYet = errors.New("the retention window is not known yet")

// Retention is a Reader that knows how far back the source's history reaches,
// which is how long a stopped task may stay stopped before its position is no
// longer in the log and the only way back is copying everything again.
//
// A Reader that cannot find out implements nothing: a number that is silently
// wrong is worse than no number.
type Retention interface {
	Window(ctx context.Context) (time.Duration, error)
}

type Snapshotter interface {
	// Pin records where the stream resumes from. It runs before a single row is
	// copied: reading the position afterwards loses every write made while the
	// copy was running.
	Pin(ctx context.Context) (Position, error)
	Copy(ctx context.Context) error
}

// StaleSweeper removes what the target holds and the source does not.
//
// A copy writes what the source has; it says nothing about what the source no
// longer has. That is nothing to worry about for a first copy, where the
// target starts empty, and it is the whole problem for a copy made to recover
// a position: everything deleted at the source while the task was away is
// still on the target, and no copy will mention it.
//
// A Snapshotter may implement this. One that does not cannot recover a
// position by copying, because the result would be a target holding data the
// source has deleted.
type StaleSweeper interface {
	SweepStale(ctx context.Context) error
}
