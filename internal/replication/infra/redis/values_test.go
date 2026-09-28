package redis

import (
	"context"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Copying a key by its value. DUMP and RESTORE treat the value as opaque, so
// one path serialises a string, a hash with per-field expiries, a stream or a
// module type -- and what is written for each is the whole behaviour.
//
// The commands are inspected without a server: a pipeline queues them, and the
// arguments are readable from the commands Exec hands back even when the dial
// fails. That is what the target would have been sent.

// queued reports the commands a value would send to the target.
func queued(t *testing.T, values ...*repairedValue) [][]interface{} {
	t.Helper()

	client := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1", // nothing listens here
		MaxRetries:  -1,
		DialTimeout: 50 * time.Millisecond,
	})
	t.Cleanup(func() { _ = client.Close() })

	pipe := client.Pipeline()
	for _, value := range values {
		value.queue(context.Background(), pipe)
	}
	cmds, _ := pipe.Exec(context.Background())

	out := make([][]interface{}, 0, len(cmds))
	for _, cmd := range cmds {
		out = append(out, cmd.Args())
	}
	return out
}

func TestAValueIsRestoredOverWhateverIsThere(t *testing.T) {
	sent := queued(t, &repairedValue{
		key: []byte("order:1"), payload: []byte("serialised"), ttl: time.Minute,
	})

	if len(sent) != 1 {
		t.Fatalf("sent %d commands, want 1: %v", len(sent), sent)
	}
	args := sent[0]
	if args[0] != "restore" {
		t.Errorf("the command is %v, want restore", args[0])
	}
	if args[1] != "order:1" {
		t.Errorf("the key is %v", args[1])
	}
	// Replace rather than fail: this runs when the target may hold an older
	// version, which is exactly the case RESTORE refuses without it.
	if last := args[len(args)-1]; last != "replace" {
		t.Errorf("the command does not end in replace: %v", args)
	}
}

// TestTheRemainingLifeIsSentNotAnAbsoluteExpiry. A duration lands on the
// target's own clock and is only late by the time the copy took; an absolute
// time computed here would carry this process's clock skew into the target's
// data.
func TestTheRemainingLifeIsSentNotAnAbsoluteExpiry(t *testing.T) {
	sent := queued(t, &repairedValue{
		key: []byte("k"), payload: []byte("v"), ttl: 90 * time.Second,
	})

	args := sent[0]
	if args[2] != int64(90000) {
		t.Errorf("the life sent is %v, want 90000 milliseconds", args[2])
	}
}

// TestAKeyThatDoesNotExpireIsRestoredWithoutOne: zero means keep it for ever,
// and sending a zero TTL is how RESTORE is told that.
func TestAKeyThatDoesNotExpireIsRestoredWithoutOne(t *testing.T) {
	sent := queued(t, &repairedValue{key: []byte("k"), payload: []byte("v")})

	if args := sent[0]; args[2] != int64(0) {
		t.Errorf("the life sent is %v, want 0 for a key that does not expire", args[2])
	}
}

// TestAKeyGoneFromTheSourceIsDeletedFromTheTarget. The source is what the
// target is meant to look like, so a key read back as absent is removed rather
// than left behind -- leaving it is how a deleted key survives in the copy.
func TestAKeyGoneFromTheSourceIsDeletedFromTheTarget(t *testing.T) {
	sent := queued(t, &repairedValue{key: []byte("order:9")})

	if len(sent) != 1 {
		t.Fatalf("sent %d commands, want 1: %v", len(sent), sent)
	}
	args := sent[0]
	if args[0] != "del" {
		t.Errorf("the command is %v, want del", args[0])
	}
	if args[1] != "order:9" {
		t.Errorf("the key is %v", args[1])
	}
}

// TestAnEmptyValueIsStillAValue: a zero-length payload is a key holding the
// empty string, which is not the same as a key that is gone. Treating the two
// alike would delete keys the source still has.
func TestAnEmptyValueIsStillAValue(t *testing.T) {
	sent := queued(t, &repairedValue{key: []byte("k"), payload: []byte{}})

	if args := sent[0]; args[0] != "restore" {
		t.Errorf("an empty value was %v, want restore -- an empty string is not a "+
			"missing key", args[0])
	}
}

func TestSeveralValuesGoOutInOrder(t *testing.T) {
	sent := queued(t,
		&repairedValue{key: []byte("a"), payload: []byte("1")},
		&repairedValue{key: []byte("b")},
		&repairedValue{key: []byte("c"), payload: []byte("3")},
	)

	if len(sent) != 3 {
		t.Fatalf("sent %d commands, want 3", len(sent))
	}
	for i, want := range []string{"restore", "del", "restore"} {
		if sent[i][0] != want {
			t.Errorf("command %d is %v, want %s", i, sent[i][0], want)
		}
	}
}
