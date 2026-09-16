package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// One shard's replication connection and the disk behind it, shared by the
// snapshot and the reader: the snapshot's handshake pins the point the first
// copy is taken against, and the copy runs while the connection already fills
// the buffer. Nothing here waits for the target — that separation is the reason
// to relay at all, because a slow target cannot make the source drop this
// replica.
type link struct {
	opts   StreamOptions
	buffer *Buffer
	shard  string
	logger logrus.FieldLogger
	labels metrics.Labels

	// node is a plain connection to the same master, only for reading its own
	// write offset: the replication connection takes no ordinary commands once it
	// is a replica link.
	node goredis.UniversalClient

	mu     sync.Mutex
	stream *Stream

	// durable is the offset written to disk, and so the offset it is honest to
	// acknowledge. Read by the acknowledging goroutine, written by the pump.
	durable atomic.Int64
	// received is how far the stream has been read and written to the buffer.
	received atomic.Int64
	// lagTicks counts acknowledgement ticks, so the source is asked for its own
	// offset every few rather than every one.
	lagTicks int

	// applied is how far the target has been written, kept here so it and durable
	// are published at one instant: published separately, applied routinely
	// appeared ahead of received and the byte lag came out negative.
	applied atomic.Int64

	pumping   sync.WaitGroup
	pumpErr   chan error
	stopPump  context.CancelFunc
	closeOnce sync.Once
}

// start opens the connection, positions it and begins filling the buffer. It
// continues from the end of the disk, never from the applied position: the disk
// holds everything received and the position only what reached the target, so
// resuming from the latter re-sends bytes the disk already has and appends them
// at offsets that mean nothing.
func (l *link) start(ctx context.Context, from streamPosition) (streamPosition, error) {
	resume := from
	if !from.IsZero() {
		switch head := l.buffer.Newest(); {
		case head > from.Offset:
			resume.Offset = head
		case head < from.Offset:
			// The disk is behind what the target has already applied: a volume
			// replaced, or bytes committed to the target that the buffer had not
			// yet sealed. Appending from here would number every later byte
			// (from.Offset - head) too low, and the applier would then read
			// bytes it has already applied as if they were new — silently, at
			// offsets that no longer mean what they say. Everything still held
			// is at or below the applied position, so discarding it is safe.
			if head > 0 && l.logger != nil {
				l.logger.Warnf("[Redis] The buffer for shard %s ends at %d while the "+
					"target has applied up to %d, so the buffered history is being "+
					"discarded and refilled from the applied position", l.shard, head, from.Offset)
			}
			if err := l.buffer.Reset(from.Offset); err != nil {
				return streamPosition{}, err
			}
		}
	}

	stream, err := Dial(ctx, l.opts)
	if err != nil {
		return streamPosition{}, err
	}

	agreed, err := stream.Sync(Point{ReplID: resume.ReplID, Offset: resume.Offset})
	if err != nil {
		stream.Close()
		return streamPosition{}, err
	}

	at := from
	switch {
	case agreed.Full && from.ReplID == "":
		// A first connection. The data set on the wire is thrown away: the first copy
		// uses SCAN and DUMP, so nothing here has to understand a format that changes
		// every release.
		if _, err := stream.SkipRDB(ctx); err != nil {
			stream.Close()
			return streamPosition{}, err
		}
		if err := l.buffer.Reset(agreed.Offset); err != nil {
			stream.Close()
			return streamPosition{}, err
		}
		at = streamPosition{
			ReplID: agreed.ReplID,
			Offset: agreed.Offset,
			Phase:  phaseValue,
		}

	case agreed.Full:
		// The source will not continue from where this task reached; its backlog no
		// longer covers the gap. Emptying the target and copying everything again is
		// the one thing not to do here.
		stream.Close()
		return streamPosition{}, domain.PositionUnusable(
			"the source will not resume shard %s from offset %d and offered to send "+
				"everything again: the history that offset belongs to is not the one "+
				"the source is running. Its replication history lives in memory, so a "+
				"restart of the source ends it, and a bigger repl-backlog-size does not "+
				"help across one. Copying the shard again is the way back, and the "+
				"setting decides whether this task does that on its own",
			l.shard, resume.Offset)

	default:
		// psync2 may hand over a new identifier for the same history.
		at.ReplID = agreed.ReplID
	}

	l.mu.Lock()
	l.stream = stream
	l.mu.Unlock()
	metrics.SetConnected(l.labels, true)

	// Acknowledgements report what is on disk, which is where the connection
	// resumed from rather than what has been applied.
	l.durable.Store(l.buffer.Newest())
	l.received.Store(l.buffer.Newest())
	if at.Offset > 0 {
		l.applied.Store(at.Offset)
	}

	pumpCtx, stop := context.WithCancel(ctx)
	l.stopPump = stop
	l.pumpErr = make(chan error, 1)
	l.pumping.Add(1)
	go func() {
		defer l.pumping.Done()
		err := l.pump(pumpCtx, stream)
		// Waking the readers matters as much as reporting why: a cursor waiting at
		// the end of the buffer is woken by an append, and there will not be another
		// one.
		l.buffer.Seal()
		l.pumpErr <- err
	}()

	l.pumping.Add(1)
	go func() {
		defer l.pumping.Done()
		l.acknowledge(pumpCtx, stream)
	}()
	return at, nil
}

// pump writes what arrives to disk and acknowledges it. The acknowledgement
// reports what is durable, not what is applied: reporting less makes the source
// keep history this side no longer needs, reporting more lets it drop history
// this side still does.
func (l *link) pump(ctx context.Context, stream *Stream) error {
	ticker := time.NewTicker(ackPeriod)
	defer ticker.Stop()

	var unsynced bool
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		cmd, err := stream.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			metrics.CountDisconnect(l.labels)
			metrics.SetConnected(l.labels, false)
			l.log().Warnf("[Redis] Shard %s lost its replication connection at "+
				"offset %d: %v", l.shard, l.buffer.Newest(), err)
			return err
		}
		if err := l.buffer.Append(cmd.Raw); err != nil {
			return err
		}
		unsynced = true
		// Recorded per command, not per flush: it is what the acknowledgement
		// reports, and a value a second stale makes the source keep history this side
		// no longer needs.
		l.received.Store(cmd.End)

		select {
		case <-ticker.C:
			// One flush per tick rather than per command. The source can resend the
			// tail, so a crash costs little, while an fsync per command does not.
			if unsynced {
				if err := l.buffer.Sync(); err != nil {
					return err
				}
				unsynced = false
			}
			l.durable.Store(cmd.End)
		default:
		}
	}
}

// acknowledge reports the durable offset once a second whether or not anything
// arrived, on its own goroutine, which is the point.
func (l *link) acknowledge(ctx context.Context, stream *Stream) {
	ticker := time.NewTicker(ackPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Both numbers from one place at one instant, so subtracting them means
			// something.
			received, applied := l.received.Load(), l.applied.Load()
			metrics.SetStreamOffset(l.labels, received, l.buffer.Held())
			metrics.SetAppliedOffset(l.labels, applied)
			// The same numbers under engine-neutral names, so one dashboard panel covers
			// every engine. What is waiting is the distance between the two offsets,
			// not the size of the buffer: the buffer keeps history on purpose.
			metrics.SetSourcePosition(l.labels, received)
			metrics.SetUnappliedBytes(l.labels, received-applied)
			l.publishSourceLag(ctx)

			if err := stream.Ack(l.durable.Load()); err != nil {
				// The pump will report the connection going away; there is nothing to add
				// from here.
				return
			}
		}
	}
}

func (l *link) log() logrus.FieldLogger { return orDefault(l.logger) }

func (l *link) failure() (error, bool) {
	if l.pumpErr == nil {
		return nil, false
	}
	select {
	case err := <-l.pumpErr:
		return err, true
	default:
		return nil, false
	}
}

func (l *link) cursor(offset int64) (*Cursor, error) {
	cursor, err := l.buffer.Cursor(offset)
	if err != nil {
		if errors.Is(err, ErrTruncated) {
			return nil, domain.Unrecoverable(
				"the buffered stream for shard %s no longer reaches offset %d. Raise "+
					"the buffer's size, or re-copy this shard deliberately",
				l.shard, offset)
		}
		return nil, fmt.Errorf("read the buffered stream for shard %s: %w", l.shard, err)
	}
	return cursor, nil
}

func (l *link) head() int64 { return l.buffer.Newest() }

// close stops the pump and releases the connection. The buffer stays on disk.
func (l *link) close() {
	l.closeOnce.Do(func() {
		if l.stopPump != nil {
			l.stopPump()
		}
		l.mu.Lock()
		stream := l.stream
		l.mu.Unlock()
		if stream != nil {
			_ = stream.Close()
		}
		l.pumping.Wait()
	})
}

// sourceLagPeriod is how often the source is asked for its own offset — less
// often than the once-a-second acknowledgement.
const sourceLagPeriod = 5

// publishSourceLag reports how far the target is behind the source's own write
// offset, the only lag that keeps growing while this process is stuck. Received
// minus applied only describes the backlog already held.
func (l *link) publishSourceLag(ctx context.Context) {
	if l.node == nil {
		return
	}
	l.lagTicks++
	if l.lagTicks%sourceLagPeriod != 0 {
		return
	}
	offset, err := masterOffset(ctx, l.node)
	if err != nil {
		// The source not answering is reported by the connection counters; an unknown
		// lag must not be published as zero.
		return
	}
	if applied := l.applied.Load(); offset >= applied {
		metrics.SetSourceLag(l.labels, offset-applied)
	}
}

func masterOffset(ctx context.Context, node goredis.UniversalClient) (int64, error) {
	info, err := node.Info(ctx, "replication").Result()
	if err != nil {
		return 0, err
	}
	return parseMasterOffset(info)
}

func parseMasterOffset(info string) (int64, error) {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		rest, found := strings.CutPrefix(line, "master_repl_offset:")
		if !found {
			continue
		}
		return strconv.ParseInt(rest, 10, 64)
	}
	return 0, fmt.Errorf("the source did not report master_repl_offset")
}

// appliedOffset reports how far the target has been written, in the source's
// own offsets. The reconciler reads it to decide whether a repair would land
// on top of commands still in flight.
func (l *link) appliedOffset() int64 { return l.applied.Load() }
