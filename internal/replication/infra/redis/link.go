package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// One shard's replication connection, and the disk behind it.
//
// The snapshot and the reader share this. The snapshot opens it — the handshake
// is what pins the point the first copy is taken against — and the copy then runs
// while the connection is already filling the buffer, so the source only has to
// hold history for as long as the handshake, not for as long as the copy.
//
// Nothing on this side waits for the target. That separation is the reason to
// relay at all: a target that is slow, restarting or unreachable cannot make the
// source drop this replica and force a full resync.
type link struct {
	opts   StreamOptions
	buffer *Buffer
	shard  string
	logger logrus.FieldLogger
	labels metrics.Labels

	mu     sync.Mutex
	stream *Stream

	// durable is the offset written to disk, and so the offset it is honest to
	// acknowledge. Read by the acknowledging goroutine, written by the pump.
	durable atomic.Int64

	pumping   sync.WaitGroup
	pumpErr   chan error
	stopPump  context.CancelFunc
	closeOnce sync.Once
}

// start opens the connection, positions it, and begins filling the buffer.
//
// It reports the position the stream now stands at. A zero position asks the
// source for everything, which is what pins the point a first copy is taken
// against; a non-zero one asks it to continue.
//
// The connection continues from the end of the disk, not from the position that
// was applied. Those are two different clocks and confusing them corrupts the
// buffer: the disk holds everything received, the position holds what has been
// written to the target, and the second is always behind. Asking the source to
// resume from the applied position makes it re-send bytes the disk already has,
// and they get appended after the ones already there — the same commands twice,
// at offsets that now mean nothing.
func (l *link) start(ctx context.Context, from streamPosition) (streamPosition, error) {
	resume := from
	if !from.IsZero() {
		switch head := l.buffer.Newest(); {
		case head > from.Offset:
			resume.Offset = head
		case head == 0:
			// Nothing on disk — a replaced volume, or a first run after the
			// position was recorded. Appends have to start where the position
			// says, or every offset after them is wrong.
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
		// A first connection. The data set on the wire is consumed and thrown
		// away: the first copy is taken with SCAN and DUMP instead, so that
		// nothing here has to understand a format that changes every release.
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
		// The source would not continue from where this task had reached: its
		// backlog no longer covers the gap.
		//
		// Emptying the target and copying everything again is the one thing not
		// to do here. It is a window with no disaster recovery copy at all,
		// entered because of a network problem — so the task stops and says what
		// it needs instead.
		stream.Close()
		return streamPosition{}, domain.Unrecoverable(
			"the source will not resume shard %s from offset %d and offered to send "+
				"everything again: its replication backlog no longer reaches back that "+
				"far. Raise repl-backlog-size on the source, or re-copy this shard "+
				"deliberately — this will not empty the target on its own",
			l.shard, resume.Offset)

	default:
		// psync2 may hand over a new identifier for the same history.
		at.ReplID = agreed.ReplID
	}

	l.mu.Lock()
	l.stream = stream
	l.mu.Unlock()

	// Acknowledgements report what is on disk, which is where the connection was
	// resumed from rather than what has been applied.
	l.durable.Store(l.buffer.Newest())

	pumpCtx, stop := context.WithCancel(ctx)
	l.stopPump = stop
	l.pumpErr = make(chan error, 1)
	l.pumping.Add(1)
	go func() {
		defer l.pumping.Done()
		err := l.pump(pumpCtx, stream)
		// Waking the readers matters as much as reporting the reason. A cursor
		// waiting at the end of the buffer is woken by an append, and there will
		// not be another one, so without this the task hangs instead of failing.
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

// pump writes what arrives to disk and acknowledges it.
//
// The acknowledgement reports what is on disk, not what has been applied. Once a
// command is durable here it will be applied, so reporting less would have the
// source keep history this side no longer needs — and in the other direction,
// reporting more would let it drop history this side still does.
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
			l.log().Warnf("[Redis] Shard %s lost its replication connection at "+
				"offset %d: %v", l.shard, l.buffer.Newest(), err)
			return err
		}
		if err := l.buffer.Append(cmd.Raw); err != nil {
			return err
		}
		unsynced = true

		select {
		case <-ticker.C:
			// One flush per tick rather than one per command. The source can
			// always send the tail again, so what a crash costs is bounded and
			// small, while an fsync per command is not.
			if unsynced {
				if err := l.buffer.Sync(); err != nil {
					return err
				}
				unsynced = false
			}
			l.durable.Store(cmd.End)
			metrics.SetStreamOffset(l.labels, cmd.End, l.buffer.Held())
		default:
		}
	}
}

// acknowledge reports the durable offset to the source once a second, whether or
// not anything has arrived.
//
// It runs on its own goroutine, and that is the point. Acknowledging only after
// receiving something looks equivalent and is not: a master that streams its data
// set without a length cannot tell when the replica finished loading it, so it
// waits for an acknowledgement before sending any commands. If the one sent
// straight after the transfer arrives while the master is still finishing — and
// whether it does is a race — the master then waits for the next one, the replica
// waits for a command, and the two wait for each other for good.
//
// The symptom is a connection that is established, online in INFO replication,
// and completely silent. It took a while to find, because it only happened
// sometimes.
func (l *link) acknowledge(ctx context.Context, stream *Stream) {
	ticker := time.NewTicker(ackPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := stream.Ack(l.durable.Load()); err != nil {
				// The pump will report the connection going away; there is
				// nothing useful to add from here.
				return
			}
		}
	}
}

func (l *link) log() logrus.FieldLogger {
	if l.logger != nil {
		return l.logger
	}
	return logrus.StandardLogger()
}

// failure reports why the connection stopped, or nil if it has not.
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

// cursor opens a reader on the buffered stream.
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

// head is how far the buffer has been filled.
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
