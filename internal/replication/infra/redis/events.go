package redis

import (
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// What the reader hands the applier. Two shapes, because the stream is applied
// two different ways.

type command struct {
	args [][]byte
	// slot is which hash slot the command's key belongs to, and so which
	// transaction it is committed in.
	slot int
	// offset is the stream offset after this command. It is what the slot's
	// marker records, and what decides whether the command has been applied.
	offset int64
}

// operation says whether the command removes the key or writes it. Redis
// cannot tell an insert from an update without reading the target first — a
// SET creates or overwrites and the stream does not say which — so every write
// is an update here.
func (c *command) operation() domain.Op {
	if len(c.args) == 0 {
		return domain.OpUpdate
	}
	switch strings.ToUpper(string(c.args[0])) {
	case "DEL", "UNLINK":
		return domain.OpDelete
	}
	return domain.OpUpdate
}

func (c *command) arguments() []interface{} {
	args := make([]interface{}, 0, len(c.args))
	for _, arg := range c.args {
		args = append(args, arg)
	}
	return args
}

func (c *command) bytes() int {
	total := 0
	for _, arg := range c.args {
		total += len(arg)
	}
	return total
}

// valueRepair asks for a key's current value to be copied from the source.
//
// Applying a change this way is safe however many times it happens, which is
// what makes it the right tool for the two places replaying a command is not:
// the smeared window of the first copy, and a key the target has diverged on.
type valueRepair struct {
	key []byte
	// slot is the key's hash slot.
	slot int
	// offset is the stream offset that asked for the repair, or zero when the
	// repair came from somewhere other than the stream.
	offset int64
}

// commandEvent wraps a command as an event for the pipeline.
//
// Every event ends its transaction: a Redis stream has no transactions to
// preserve except the MULTI blocks the reader keeps together itself, so any
// point between events is a legal place to cut a batch.
func commandEvent(cmd *command, key []byte, at time.Time, endsBlock bool) *domain.Event {
	return &domain.Event{
		NS:              domain.Namespace{DB: "0"},
		Op:              cmd.operation(),
		Key:             string(key),
		Payload:         cmd,
		Bytes:           cmd.bytes(),
		Pos:             domain.Position{},
		SourceTime:      at,
		EndsTransaction: endsBlock,
	}
}

func repairEvent(repair *valueRepair, at time.Time, endsBlock bool) *domain.Event {
	return &domain.Event{
		NS:              domain.Namespace{DB: "0"},
		Op:              domain.OpUpdate,
		Key:             string(repair.key),
		Payload:         repair,
		Bytes:           len(repair.key),
		SourceTime:      at,
		EndsTransaction: endsBlock,
	}
}

// heartbeatEvent reports that the link is alive without changing anything.
//
// A stream that delivers nothing looks exactly like a source nobody is writing
// to, which is the one failure monitoring cannot otherwise see. A master pings
// its replicas every ten seconds, so the ping is the proof.
func heartbeatEvent(at time.Time) *domain.Event {
	return &domain.Event{
		NS:              domain.Namespace{DB: "0"},
		Heartbeat:       true,
		SourceTime:      at,
		EndsTransaction: true,
	}
}
