package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Turning one shard's buffered stream into events.
//
// The connection and the disk are the link's job; this reads the disk. So a
// restart re-reads from the buffer rather than from the source, and the source is
// only involved again when the buffer cannot reach far enough back.

type Reader struct {
	// Shard names the source master, for logs, metrics and the position's key.
	Shard string
	// Link is the connection and the buffer behind it, shared with the snapshot.
	Link *link
	// Target is asked which key a command touches when the answer is not fixed.
	Target goredis.UniversalClient
	// Commands is the key specification table, read from the target.
	Commands *commandTable

	// Node is a plain connection to this shard's master, for asking how much
	// history it keeps. It is not used to read the stream.
	Node goredis.UniversalClient
	// Configured is the retention window the task was told, for a source that
	// cannot be asked. Zero means measure it.
	Configured time.Duration
	// SourceDB is the database this task replicates. A standalone server
	// interleaves every database into one replication stream, separated by
	// SELECT, so a task that copies one of them has to drop the rest: they
	// would otherwise be applied to the target as though they were its own.
	SourceDB int

	Logger logrus.FieldLogger
	Labels metrics.Labels

	// sampledAt and sampledOffset are the previous measurement of how fast the
	// stream is being written, which is half of the retention window.
	sampledAt     time.Time
	sampledOffset int64
	// backlogBytes is the source's remembered backlog size, and backlogAt when
	// it was read. It is a setting rather than a measurement, so it is not worth
	// a round trip every time the window is worked out.
	backlogBytes int64
	backlogAt    time.Time

	// streamDB is the database the stream is currently in, which SELECT moves.
	// A stream starts in database zero, which is what a master assumes of a
	// replica that has just connected.
	streamDB int

	// position is what was resumed from. Its phase decides whether a change is
	// applied by replaying the command or by re-reading the key's value.
	position streamPosition

	cursor *Cursor
	// offset is where the events handed out have reached.
	offset int64

	// pending holds a MULTI block being read, so a batch is never cut inside one.
	pending []*domain.Event
	inMulti bool
	// ready holds complete units waiting to be handed over.
	ready []*domain.Event

	closeOnce sync.Once
}

func (r *Reader) Open(ctx context.Context, from domain.Position) error {
	position, err := decodePosition(from.Payload)
	if err != nil {
		return err
	}

	if r.Link.pumpErr == nil {
		// Resuming without a first copy: nothing has opened the connection yet.
		position, err = r.Link.start(ctx, position)
		if err != nil {
			return err
		}
	}
	r.position = position
	r.offset = position.Offset

	if r.position.Phase == phaseValue && r.position.ValueUntil <= 0 {
		// How long to keep re-reading values rather than replaying commands. The
		// first copy is taken with SCAN, so it is a smear rather than a point in
		// time, and a command from inside that smear may already be in the copy.
		r.position.ValueUntil = r.Link.head()
		r.logger().Infof("[Redis] Shard %s applies by value up to offset %d, then "+
			"replays commands", r.Shard, r.position.ValueUntil)
	}

	cursor, err := r.Link.cursor(r.offset)
	if err != nil {
		return err
	}
	r.cursor = cursor
	return nil
}

func (r *Reader) logger() logrus.FieldLogger { return orDefault(r.Logger) }

// selectedDB reads the database index out of a SELECT. A stream that says
// SELECT and does not say where is one this cannot follow: guessing would
// silently attribute every command after it to the wrong database.
func selectedDB(cmd *Command) (int, error) {
	if len(cmd.Args) < 2 {
		return 0, domain.Unrecoverable("the source sent SELECT with no database")
	}
	db, err := strconv.Atoi(string(cmd.Args[1]))
	if err != nil || db < 0 {
		return 0, domain.Unrecoverable(
			"the source sent SELECT %q, which is not a database index", cmd.Args[1])
	}
	return db, nil
}

func (r *Reader) Next(ctx context.Context) (*domain.Event, error) {
	for {
		if len(r.ready) > 0 {
			event := r.ready[0]
			r.ready = r.ready[1:]
			return event, nil
		}

		// A failure on the connection is the real reason the stream stopped, so
		// it is preferred over whatever the cursor says about running dry.
		if err, stopped := r.Link.failure(); stopped {
			if err != nil {
				return nil, err
			}
			return nil, io.EOF
		}

		if err := r.take(ctx); err != nil {
			if errors.Is(err, io.EOF) {
				// The buffer has been sealed, which only happens when the
				// connection stopped. Its reason is the useful one.
				if reason, stopped := r.Link.failure(); stopped && reason != nil {
					return nil, reason
				}
			}
			return nil, err
		}
	}
}

func (r *Reader) take(ctx context.Context) error {
	raw, end, err := r.cursor.Next(ctx)
	if err != nil {
		return err
	}
	args, err := parseFrame(raw)
	if err != nil {
		return fmt.Errorf("read a buffered command for shard %s: %w", r.Shard, err)
	}
	cmd := &Command{Args: args, Raw: raw, End: end}
	r.offset = end

	class, key, err := r.Commands.classify(ctx, r.Target, cmd)
	if err != nil {
		return err
	}

	// The stream carries no timestamps, so when a change was made can only be
	// taken as when it was read. The lag this produces therefore measures the
	// buffer and the target, not the link across the region — which is why the
	// byte lag is published alongside it.
	at := time.Now()

	switch class {
	case classHeartbeat:
		r.hand(heartbeatEvent(at))
		return nil

	case classSelect:
		db, err := selectedDB(cmd)
		if err != nil {
			return err
		}
		r.streamDB = db
		return nil

	case classIgnored:
		return nil

	case classRefused:
		return domain.Unrecoverable(
			"the source sent %s for shard %s, which will not be replicated. Emptying "+
				"the target is the one operation that destroys the disaster recovery "+
				"copy, so an accident on the source must not take the copy with it. "+
				"Re-copy this shard deliberately if the source really was emptied",
			cmd.Name(), r.Shard)

	case classTransactionBegin:
		r.inMulti = true
		return nil

	case classTransactionEnd:
		r.inMulti = false
		if len(r.pending) > 0 {
			// The block is complete, so its last event may end a batch.
			last := r.pending[len(r.pending)-1]
			last.EndsTransaction = true
			last.Pos = r.currentPosition()
			r.ready = append(r.ready, r.pending...)
			r.pending = nil
		}
		return nil
	}

	slot := SlotOf(key)
	var event *domain.Event
	if r.position.inValuePhase(end) {
		event = repairEvent(&valueRepair{key: key, db: r.streamDB, slot: slot, offset: end}, at, !r.inMulti)
	} else {
		event = commandEvent(&command{args: args, db: r.streamDB, slot: slot, offset: end}, key, at, !r.inMulti)
	}

	if r.inMulti {
		// A batch may not be cut inside a MULTI block: the source applied it as
		// one act, and half of it on the target is a state the source never had.
		r.pending = append(r.pending, event)
		return nil
	}
	r.hand(event)
	return nil
}

func (r *Reader) hand(event *domain.Event) {
	event.Pos = r.currentPosition()
	r.ready = append(r.ready, event)
}

func (r *Reader) currentPosition() domain.Position {
	position := r.position
	position.Offset = r.offset
	if position.Phase == phaseValue && r.offset >= position.ValueUntil {
		position.Phase = phaseCommand
	}
	payload, err := position.encode()
	if err != nil {
		return domain.Position{}
	}
	return domain.Position{Payload: payload}
}

// Close releases the cursor. The connection belongs to the link.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		if r.cursor != nil {
			_ = r.cursor.Close()
		}
	})
	return nil
}

// parseFrame reads one buffered command back into its arguments.
//
// The buffer holds the bytes exactly as the source sent them, so this is the
// same parse the connection already did. Doing it twice is what lets a restart
// read from disk, where the connection's parse is long gone.
func parseFrame(raw []byte) ([][]byte, error) {
	if len(raw) == 0 || raw[0] != '*' {
		return nil, fmt.Errorf("a buffered frame does not start a command")
	}
	at := 0
	line := func() (string, error) {
		for i := at; i+1 < len(raw); i++ {
			if raw[i] == '\r' && raw[i+1] == '\n' {
				text := string(raw[at:i])
				at = i + 2
				return text, nil
			}
		}
		return "", fmt.Errorf("a buffered frame is missing a line ending")
	}

	header, err := line()
	if err != nil {
		return nil, err
	}
	count, err := atoiStrict(header[1:])
	if err != nil || count < 0 {
		return nil, fmt.Errorf("a buffered frame announced %q arguments", header[1:])
	}

	args := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		size, err := line()
		if err != nil {
			return nil, err
		}
		if len(size) == 0 || size[0] != '$' {
			return nil, fmt.Errorf("a buffered argument announced %q", size)
		}
		length, err := atoiStrict(size[1:])
		if err != nil || length < 0 {
			return nil, fmt.Errorf("a buffered argument announced a length of %q", size[1:])
		}
		if at+length+2 > len(raw) {
			return nil, fmt.Errorf("a buffered argument runs past the end of its frame")
		}
		args = append(args, raw[at:at+length])
		at += length + 2
	}
	return args, nil
}

func atoiStrict(text string) (int, error) {
	if text == "" {
		return 0, fmt.Errorf("empty")
	}
	value := 0
	for i := 0; i < len(text); i++ {
		if text[i] < '0' || text[i] > '9' {
			return 0, fmt.Errorf("not a number: %q", text)
		}
		value = value*10 + int(text[i]-'0')
	}
	return value, nil
}
