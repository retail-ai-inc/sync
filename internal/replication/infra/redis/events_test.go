package redis

import (
	"context"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"strconv"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// A SET creates or overwrites and the stream does not say which, so every
// write is an update.
func TestARemovalIsCountedApartFromAWrite(t *testing.T) {
	for _, c := range []struct {
		name string
		args []string
		want domain.Op
	}{
		{"SET is a write", []string{"SET", "k", "v"}, domain.OpUpdate},
		{"HSET is a write", []string{"HSET", "h", "f", "v"}, domain.OpUpdate},
		{"DEL is a removal", []string{"DEL", "k"}, domain.OpDelete},
		{"UNLINK is a removal", []string{"UNLINK", "k"}, domain.OpDelete},
		{"lower case is still a removal", []string{"del", "k"}, domain.OpDelete},
		{"an empty command is a write", nil, domain.OpUpdate},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := make([][]byte, 0, len(c.args))
			for _, a := range c.args {
				args = append(args, []byte(a))
			}
			cmd := &command{args: args}
			if got := cmd.operation(); got != c.want {
				t.Errorf("operation() = %v, want %v", got, c.want)
			}
		})
	}
}

// A standalone server interleaves every database into one replication stream,
// separated by SELECT. The database has to travel with the command: without it
// every database's writes land in whichever one the target connection happens
// to be on, and a key from database 2 becomes a key of database 1 under the
// same name, indistinguishable from the source having written it there.
func TestEachWriteCarriesTheDatabaseItBelongsTo(t *testing.T) {
	r := readerOverBuffer(t)
	r.Commands = table()
	for _, frame := range [][]byte{
		resp("SELECT", "2"),
		resp("SET", "in-two", "v"),
		resp("SELECT", "1"),
		resp("SET", "in-one", "v"),
	} {
		if err := r.Link.buffer.Append(frame); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	cursor, err := r.Link.cursor(0)
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	r.cursor = cursor

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := r.take(ctx); err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
	}

	got := map[string]string{}
	for _, e := range append(append([]*domain.Event{}, r.ready...), r.pending...) {
		got[e.Key] = e.NS.DB
		if cmd, ok := e.Payload.(*command); ok && strconv.Itoa(cmd.db) != e.NS.DB {
			t.Errorf("%s: command database %d disagrees with the event's %q",
				e.Key, cmd.db, e.NS.DB)
		}
	}
	want := map[string]string{"in-two": "2", "in-one": "1"}
	if len(got) != len(want) {
		t.Fatalf("carried %v, want both writes", got)
	}
	for key, db := range want {
		if got[key] != db {
			t.Errorf("%s is in database %q, want %q", key, got[key], db)
		}
	}
	if r.streamDB != 1 {
		t.Errorf("stream database = %d, want 1", r.streamDB)
	}
}

// The direction lock says which side of a pair is the source. Replicating it
// tells the target it is one, and it is rewritten on every heartbeat, so the
// stream carries it over and over. It used to be missed because the copy
// walked only one database and the lock was in another.
func TestThisToolsOwnKeysAreNotReplicated(t *testing.T) {
	r := readerOverBuffer(t)
	r.Commands = table()
	for _, frame := range [][]byte{
		resp("SELECT", "0"),
		resp("HSET", directionlock.RedisKey, "42", "claim"),
		resp("SET", OffsetKey(7, 42), "12345"),
		resp("SET", "real-data", "v"),
	} {
		if err := r.Link.buffer.Append(frame); err != nil {
			t.Fatalf("append: %v", err)
		}
	}

	cursor, err := r.Link.cursor(0)
	if err != nil {
		t.Fatalf("cursor: %v", err)
	}
	r.cursor = cursor

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := r.take(ctx); err != nil {
			t.Fatalf("take %d: %v", i, err)
		}
	}

	var keys []string
	for _, e := range append(append([]*domain.Event{}, r.ready...), r.pending...) {
		keys = append(keys, e.Key)
	}
	if len(keys) != 1 || keys[0] != "real-data" {
		t.Errorf("carried %v, want only the key that is data", keys)
	}
}
