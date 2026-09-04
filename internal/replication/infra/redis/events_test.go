package redis

import (
	"context"
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
// separated by SELECT. A task carries one of them, and applying another
// database's write would put its key into this target under the same name --
// indistinguishable from the source having written it here.
func TestAWriteFromAnotherDatabaseIsNotCarried(t *testing.T) {
	r := readerOverBuffer(t)
	r.SourceDB = 1
	r.Commands = table()
	if err := r.Link.buffer.Append(resp("SELECT", "2")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := r.Link.buffer.Append(resp("SET", "other", "v")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := r.Link.buffer.Append(resp("SELECT", "1")); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := r.Link.buffer.Append(resp("SET", "mine", "v")); err != nil {
		t.Fatalf("append: %v", err)
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
	if len(keys) != 1 || keys[0] != "mine" {
		t.Errorf("carried %v, want only the key written in database 1", keys)
	}
	if r.streamDB != 1 {
		t.Errorf("stream database = %d, want 1", r.streamDB)
	}
}
