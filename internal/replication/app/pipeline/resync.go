package pipeline

import (
	"context"
	"fmt"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Re-copying one object without stopping the rest. Sooner or later a
// collection or a table is found not to match — a bug, a botched migration, a
// stretch of stream lost before the guarantees below were in place.

type Chunk struct {
	// Events are the records, as upserts the applier can write.
	Events []*domain.Event
	// After is the key to resume from, which is the last key in this chunk.
	After string
	// ReadAt is the source's own clock, no later than any record here was read,
	// or a change the read missed is dated before it and the chunk overwrites it.
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

		// The progress moves only after the chunk has reached the target, so an
		// interrupted re-copy repeats a chunk rather than skipping one. Repeating
		// is safe: the writes are upserts.
		//
		// It used to move once the chunk had been handed to the queue, which is
		// not the same thing and gave the opposite guarantee: a crash between the
		// hand-over and the write lost those rows, because the stored progress
		// had already passed them. emit now returns when the batch has landed.
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
// Once the stream has passed the point the chunk was read at, every change older
// than the chunk is already ahead of it in the queue. The runner holds back every
// newer one (orderedChunks), which is the other half of the ordering.
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
