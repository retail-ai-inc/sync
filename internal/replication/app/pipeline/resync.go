package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Re-copying one object without stopping the rest.
//
// Sooner or later a collection or a table is found not to match — a bug, a
// botched migration, a stretch of stream lost before the guarantees below were
// in place. The only remedy used to be clearing the checkpoint, which throws
// away the position for everything and re-copies the lot. On a payment database
// that is hours during which the target is further behind, not closer.
//
// This re-copies one object while the stream keeps running. Debezium does the
// same thing with a pair of watermarks around each chunk and a set of keys to
// subtract; this takes a simpler route that the single ordered queue makes
// available.
//
// The hazard is one thing only: a chunk read at source time T, applied before a
// change the source made *before* T that the stream has not delivered yet. That
// change is older than the chunk, so applying it afterwards puts the record back
// as it was — a silent regression, in the middle of a repair.
//
// The fix is to hold the chunk until the stream has been read past T. Then every
// change older than the chunk is already ahead of it in the queue and is applied
// first, and everything newer arrives behind it and wins, which is what it
// should do. No key bookkeeping, and nothing to get wrong when a chunk is
// retried.

// Chunk is a run of records read from the source, in key order.
type Chunk struct {
	// Events are the records, as upserts the applier can write.
	Events []*domain.Event
	// After is the key to resume from, which is the last key in this chunk.
	After string
	// ReadAt is the source's own clock at the moment the chunk was read.
	// Nothing is applied until the stream has been read past it.
	ReadAt time.Time
	// Done reports that the object has been read to the end.
	Done bool
}

// ChunkReader reads one object in key order. An engine supplies it.
type ChunkReader interface {
	// NextChunk reads the records after a key, at most size of them.
	NextChunk(ctx context.Context, ns domain.Namespace, after string, size int) (Chunk, error)
}

// Resync re-copies one object through the pipeline's own applier, so its writes
// are ordered against the stream's.
type Resync struct {
	NS     domain.Namespace
	Reader ChunkReader
	// ChunkSize is how many records a chunk holds. Zero means the default.
	ChunkSize int
	// Progress records the key reached, so an interrupted re-copy resumes rather
	// than starting again.
	Progress    Checkpoints
	ProgressKey string
}

const defaultChunkSize = 1000

func (r *Resync) chunkSize() int {
	if r.ChunkSize > 0 {
		return r.ChunkSize
	}
	return defaultChunkSize
}

// streamClock reports how far the stream has been read, in the source's own
// time. The runner supplies it.
type streamClock func() time.Time

// run reads the object in chunks and hands each one to emit, holding every chunk
// until the stream has been read past the moment the chunk was read.
func (r *Resync) run(ctx context.Context, read streamClock, emit func([]*domain.Event) error) error {
	after := ""
	if r.Progress != nil {
		stored, err := r.Progress.Load(ctx, r.ProgressKey)
		if err != nil {
			return fmt.Errorf("read the re-copy's progress: %w", err)
		}
		after = stored
	}

	for {
		chunk, err := r.Reader.NextChunk(ctx, r.NS, after, r.chunkSize())
		if err != nil {
			return fmt.Errorf("read a chunk of %s: %w", r.NS, err)
		}

		if len(chunk.Events) > 0 {
			if err := waitForStream(ctx, read, chunk.ReadAt); err != nil {
				return err
			}
			if err := emit(chunk.Events); err != nil {
				return err
			}
		}

		// The progress moves only after the chunk has been handed over, so an
		// interrupted re-copy repeats a chunk rather than skipping one. Repeating
		// is safe: the writes are upserts.
		if chunk.After != "" && r.Progress != nil {
			if err := r.Progress.Save(ctx, r.ProgressKey, chunk.After); err != nil {
				return fmt.Errorf("record the re-copy's progress: %w", err)
			}
		}
		if chunk.After != "" {
			after = chunk.After
		}

		if chunk.Done {
			return nil
		}
		if len(chunk.Events) == 0 && chunk.After == "" {
			// Neither rows nor a new key: reading again would loop for ever.
			return fmt.Errorf("a chunk of %s returned nothing and did not report the "+
				"end of the object", r.NS)
		}
	}
}

// waitForStream blocks until the stream has been read past a moment.
//
// This is the whole of the correctness argument. Once the stream has passed the
// point the chunk was read at, every change older than the chunk is already
// ahead of it in the queue.
func waitForStream(ctx context.Context, read streamClock, readAt time.Time) error {
	if readAt.IsZero() || read == nil {
		// Nothing to compare against. Waiting for ever would be worse than the
		// risk, but proceeding silently would be worse than either.
		return fmt.Errorf("the chunk carries no source timestamp, so there is no way " +
			"to tell whether the stream has passed it; the re-copy cannot be ordered " +
			"against the stream safely")
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	silentSince := time.Now()
	for {
		at := read()
		if !at.IsZero() {
			if !at.Before(readAt) {
				return nil
			}
			// The stream is behind but reporting, so waiting is right: it may be
			// hours behind and catching up is still the correct thing to do.
			silentSince = time.Now()
		} else if time.Since(silentSince) > silenceLimit {
			// The stream has never said where it is. Waiting for ever would
			// leave the repair looking as though it were running, which is worse
			// than saying so — a quiet source used to do exactly this, because
			// its heartbeats carried no timestamp.
			return fmt.Errorf("the stream has not reported its position for %s, so "+
				"there is no way to tell whether it has passed the point this chunk "+
				"was read at; the re-copy cannot be ordered against it", silenceLimit)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// silenceLimit is how long a stream may report nothing at all before a re-copy
// gives up on being able to order itself against it.
var silenceLimit = 2 * time.Minute
