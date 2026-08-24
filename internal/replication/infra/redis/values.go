package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Copying a key by its value rather than by the command that changed it.
//
// DUMP and RESTORE are the whole mechanism, and the reason they are worth using
// is that they treat the value as opaque: one code path serialises a string, a
// hash with per-field expiries, a stream, or a module type, and RESTORE with
// REPLACE lands the same result however many times it runs. That idempotence is
// what makes this the safe way to apply a change when replaying a command is not.
//
// The cost is proportional to the key, not to the change: a field changed in a
// large hash moves the whole hash. That is why steady state replays commands and
// only the places that need idempotence use this.

// repairedValue is a key's value, read from the source and ready for the target.
type repairedValue struct {
	key []byte
	// payload is the serialised value, or nil when the source no longer has the
	// key — in which case the target should not either.
	payload []byte
	// ttl is how much life the key has left. Zero means it does not expire.
	ttl time.Duration
}

// readValues fetches the current value and remaining life of each key.
//
// The time left is used rather than an absolute expiry so that no third clock
// enters into it: an absolute time computed here would carry this process's
// clock skew into the target's data, while a duration lands on the target's own
// clock and is only late by the time the copy took. That is the same lateness a
// real replica has.
func readValues(ctx context.Context, source goredis.UniversalClient,
	keys [][]byte) ([]*repairedValue, error) {

	pipe := source.Pipeline()
	dumps := make([]*goredis.StringCmd, len(keys))
	lives := make([]*goredis.DurationCmd, len(keys))
	for i, key := range keys {
		dumps[i] = pipe.Dump(ctx, string(key))
		lives[i] = pipe.PTTL(ctx, string(key))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("read %d values from the source: %w", len(keys), err)
	}

	values := make([]*repairedValue, 0, len(keys))
	for i, key := range keys {
		value := &repairedValue{key: key}

		payload, err := dumps[i].Result()
		switch {
		case err == goredis.Nil:
			// Gone from the source between the change and this read. Deleting it
			// on the target is the right answer either way: the source is what
			// the target is meant to look like.
			values = append(values, value)
			continue
		case err != nil:
			return nil, fmt.Errorf("read the value of %q: %w", key, err)
		}
		value.payload = []byte(payload)

		left, err := lives[i].Result()
		if err != nil && err != goredis.Nil {
			return nil, fmt.Errorf("read the remaining life of %q: %w", key, err)
		}
		// PTTL answers with a negative duration for a key with no expiry, and
		// for one that has gone; the first is the common case and means "keep it
		// for ever".
		if left > 0 {
			value.ttl = left
		}
		values = append(values, value)
	}
	return values, nil
}

// queue adds the write to a transaction.
func (v *repairedValue) queue(ctx context.Context, pipe goredis.Pipeliner) {
	if v.payload == nil {
		pipe.Del(ctx, string(v.key))
		return
	}
	// Replace rather than fail if the key is already there: this runs when the
	// target may hold an older version, which is exactly the case RESTORE
	// refuses without it.
	pipe.RestoreReplace(ctx, string(v.key), v.ttl, string(v.payload))
}
