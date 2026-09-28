package domain

// How far the target has been written, for the decision to switch over.
//
// Replication lag answers "how far behind is it" only while the task is
// running: a task that has stopped leaves no lag to read, which is exactly when
// somebody is deciding whether Osaka is safe to promote. The question then is
// not a duration but a position -- everything Tokyo had accepted by the moment
// it went, has Osaka applied it -- and that is answerable from what the target
// durably holds, whether or not anything is still running.

// Progress is one task's answer.
type Progress struct {
	Engine string
	// Shards is one entry for a task with a single stream, and one per shard
	// for a Redis Cluster task. A task is caught up when every shard is.
	Shards []ShardProgress
}

// CaughtUp reports whether every shard has applied everything its source had.
//
// A task with no shards at all is not caught up: it is a task whose position
// could not be read, and answering "yes" to that question is the one wrong
// answer that loses data.
func (p Progress) CaughtUp() bool {
	if len(p.Shards) == 0 {
		return false
	}
	for _, shard := range p.Shards {
		if !shard.CaughtUp {
			return false
		}
	}
	return true
}

// ShardProgress is how far one stream has been applied.
type ShardProgress struct {
	// Shard names the stream, empty for an engine with one.
	Shard string
	// Source is where the source is now, as the engine writes a position.
	Source string
	// Applied is what the target durably holds, read from the target and not
	// from a running task.
	Applied string
	// CaughtUp is whether Applied has reached Source. It is false whenever the
	// two cannot be compared, which is not the same as "behind" -- Comparable
	// says which.
	CaughtUp bool
	// Comparable is whether the two positions can be ordered at all. A MongoDB
	// resume token is opaque, so a task that has one stored and no cluster time
	// beside it can report both positions and not their order.
	Comparable bool
	// BehindBytes is how far behind the target is, where the engine's position
	// is a byte offset. Negative means it is not a byte offset.
	BehindBytes int64
	// Note carries what an operator needs to read the two positions when they
	// cannot be compared.
	Note string
}
