package redis

import (
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
