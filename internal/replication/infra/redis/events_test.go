package redis

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// TestARemovalIsCountedApartFromAWrite keeps the one operation Redis can
// actually distinguish visible on its own.
//
// A SET creates or overwrites and the stream does not say which, so every write
// is an update. A removal is knowable — and a delete rate that climbs by itself
// is the shape of an eviction storm or a mistaken FLUSH, which counting it in
// with the writes would hide.
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
