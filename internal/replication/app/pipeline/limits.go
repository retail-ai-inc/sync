package pipeline

import "time"

// Tuning is what a deployment has set for the pipeline, so an engine does not
// have to know about the control database.
//
// Every field is zero for "leave the built-in default", which is what a
// deployment that has set nothing holds. An engine's own value still wins: the
// Redis syncer sets FlushInterval from the task, and a task saying something
// is more specific than a global default.
type Tuning struct {
	Limits                Limits
	FlushInterval         time.Duration
	QueueCapacity         int
	QueueBytes            int64
	SnapshotQueueCapacity int
	// CopyBatchRows and StreamAwait belong to the engines rather than to the
	// runner, and are read through CopyBatch and Await. They are here because
	// this is the one place a deployment's tuning is assembled.
	CopyBatchRows int
	StreamAwait   time.Duration
	// WholeDocuments keeps MongoDB attaching the whole document to every
	// update, which is how this replicated before an update could be applied as
	// the fields it touched. It is the way back if a delta ever turns out to be
	// wrong for a collection.
	WholeDocuments bool
	// RecopyOnUnusablePosition lets a task rebuild the target by copying when
	// the source cannot continue from the position it holds. Unlike everything
	// else here it is on unless a deployment turns it off, so it is read
	// through RecopyOnUnusablePosition rather than filled in from a zero.
	RecopyOnUnusablePosition bool
}

// StoredTuning reports it. Nil, or a function returning zeroes, leaves every
// built-in default in place -- which is what every engine had before, since
// none of them set any of this and MaxBytes therefore had no bound at all
// until it was given one.
var StoredTuning func() Tuning

// tuned fills in whatever the caller left at zero from what was stored.
func tuned(o Options) Options {
	if StoredTuning == nil {
		return o
	}
	stored := StoredTuning()

	if o.Limits.MaxEvents == 0 {
		o.Limits.MaxEvents = stored.Limits.MaxEvents
	}
	if o.Limits.MaxBytes == 0 {
		o.Limits.MaxBytes = stored.Limits.MaxBytes
	}
	if o.Limits.MaxTransactionEvents == 0 {
		o.Limits.MaxTransactionEvents = stored.Limits.MaxTransactionEvents
	}
	if o.FlushInterval == 0 {
		o.FlushInterval = stored.FlushInterval
	}
	if o.QueueCapacity == 0 {
		o.QueueCapacity = stored.QueueCapacity
	}
	if o.QueueBytes == 0 {
		o.QueueBytes = stored.QueueBytes
	}
	if o.SnapshotQueueCapacity == 0 {
		o.SnapshotQueueCapacity = stored.SnapshotQueueCapacity
	}
	return o
}

// CopyBatch reports how many rows or documents a first copy should read per
// round trip, and fallback when a deployment has not said. The engines each
// have their own default because what one round trip costs is not the same for
// a document, a row and a key.
func CopyBatch(fallback int) int {
	if StoredTuning == nil {
		return fallback
	}
	if rows := StoredTuning().CopyBatchRows; rows > 0 {
		return rows
	}
	return fallback
}

// Await reports how long a reader may leave a read for changes outstanding
// when there is nothing to return, and fallback when a deployment has not
// said.
func Await(fallback time.Duration) time.Duration {
	if StoredTuning == nil {
		return fallback
	}
	if await := StoredTuning().StreamAwait; await > 0 {
		return await
	}
	return fallback
}

// MongoWholeDocuments reports whether MongoDB updates must be replicated as
// whole documents rather than as the fields they touch.
func MongoWholeDocuments() bool {
	if StoredTuning == nil {
		return false
	}
	return StoredTuning().WholeDocuments
}

// RecopyOnUnusablePosition reports whether a task whose stored position the
// source cannot continue from should copy the source again rather than stop.
//
// It is on by default, because the alternative for a disaster-recovery copy is
// standing still: a Redis source that restarts ends the replication history
// every stored offset belongs to, and a replica frozen for days is worse than
// one rebuilt in minutes. What it costs is a copy, and the target holding two
// versions of the truth while that copy runs.
//
// Off is the older behaviour: the task stops and says what happened. That is
// also what a deployment gets when the settings cannot be read -- rebuilding a
// target is not something to do on a guess.
func RecopyOnUnusablePosition() bool {
	if StoredTuning == nil {
		return true
	}
	return StoredTuning().RecopyOnUnusablePosition
}
