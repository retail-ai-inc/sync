package redis

import (
	"context"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// takeFrames appends the frames to the reader's buffer and takes each one.
func takeFrames(t *testing.T, r *Reader, frames ...[]byte) {
	t.Helper()
	for _, frame := range frames {
		if err := r.Link.buffer.Append(frame); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if r.cursor == nil {
		cursor, err := r.Link.cursor(r.offset)
		if err != nil {
			t.Fatalf("cursor: %v", err)
		}
		r.cursor = cursor
	}
	for range frames {
		if err := r.take(context.Background()); err != nil {
			t.Fatalf("take: %v", err)
		}
	}
}

func offsetOf(t *testing.T, e *domain.Event) int64 {
	t.Helper()
	position, err := decodePosition(e.Pos.Payload)
	if err != nil {
		t.Fatalf("%s carries position %q: %v", e.Key, e.Pos.Payload, err)
	}
	return position.Offset
}

func commandReader(t *testing.T) *Reader {
	t.Helper()
	r := readerOverBuffer(t)
	r.Commands = table()
	r.position = streamPosition{ReplID: "abc", Phase: phaseCommand}
	return r
}

func TestABatchCanOnlyBeCutAtTheEndOfAMultiBlock(t *testing.T) {
	r := commandReader(t)
	block := [][]byte{resp("MULTI"), resp("SET", "a", "1"), resp("SET", "b", "2"), resp("EXEC")}
	after := resp("SET", "c", "3")
	takeFrames(t, r, append(block, after)...)

	var blockEnd int64
	for _, frame := range block {
		blockEnd += int64(len(frame))
	}
	if len(r.pending) != 0 || r.inMulti {
		t.Fatalf("the block is still open after EXEC: %d pending", len(r.pending))
	}
	if len(r.ready) != 3 {
		t.Fatalf("handed over %d events, want a, b and c", len(r.ready))
	}
	for i, want := range []struct {
		key  string
		ends bool
	}{{"a", false}, {"b", true}, {"c", true}} {
		got := r.ready[i]
		if got.Key != want.key || got.EndsTransaction != want.ends {
			// A cut after a, or a position saved there, replays half a transaction after a crash.
			t.Errorf("event %d is %s ending a transaction %v, want %s ending one %v",
				i, got.Key, got.EndsTransaction, want.key, want.ends)
		}
	}
	if got := offsetOf(t, r.ready[1]); got != blockEnd {
		t.Errorf("the block's last event records offset %d, want %d, the end of EXEC", got, blockEnd)
	}
	if got, want := offsetOf(t, r.ready[2]), blockEnd+int64(len(after)); got != want {
		t.Errorf("the command after the block records offset %d, want %d", got, want)
	}
}

func TestACommandReadWhileTheFirstCopyRunsIsAppliedByValue(t *testing.T) {
	r := readerOverBuffer(t)
	r.Commands = table()
	r.Link.pumpErr = make(chan error, 1)
	payload, err := streamPosition{ReplID: "abc", Offset: 0, Phase: phaseValue}.encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := r.Open(context.Background(), domain.Position{Payload: payload}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })

	takeFrames(t, r, resp("SET", "k", "1"))
	// A command from inside the copy replayed over it is applied twice: an INCR doubled, a DEL undone.
	if _, ok := r.ready[0].Payload.(*valueRepair); !ok {
		t.Fatalf("a command read during the copy became %T, want a value repair", r.ready[0].Payload)
	}

	r.CopyFinished()
	takeFrames(t, r, resp("SET", "k", "2"))
	if _, ok := r.ready[1].Payload.(*command); !ok {
		t.Fatalf("a command read after the copy became %T, want a command", r.ready[1].Payload)
	}
	position, err := decodePosition(r.ready[1].Pos.Payload)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if position.Phase != phaseCommand {
		t.Errorf("the position after the copy is in phase %q, want %q", position.Phase, phaseCommand)
	}
}
