package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Copying a key by its value rather than by the command that changed it. DUMP
// and RESTORE are the whole mechanism, and the reason they are worth using is
// that they treat the value as opaque: one code path serialises a string, a
// hash with per-field expiries, a stream, or a module type, and RESTORE with
// REPLACE lands the same result however many times it runs.

type repairedValue struct {
	key []byte
	// db is the database the key belongs to, which is where it is written back.
	db int
	// payload is the serialised value, or nil when the source no longer has the
	// key — in which case the target should not either.
	payload []byte
	// ttl is how much life the key has left. Zero means it does not expire.
	ttl time.Duration
}

// readValues fetches the current value and remaining life of each key. The
// time left is used rather than an absolute expiry so that no third clock
// enters into it: an absolute time computed here would carry this process's
// clock skew into the target's data, while a duration lands on the target's
// own clock and is only late by the time the copy took.
func readValues(ctx context.Context, source goredis.UniversalClient,
	keys [][]byte) ([]*repairedValue, error) {

	return readValuesIn(ctx, source, 0, 0, keys)
}

// readValuesIn reads values out of one database of the source. home is the
// database the client is on: the pipeline moves to db and back, so the pooled
// connection is returned as its owner expects to find it.
func readValuesIn(ctx context.Context, source goredis.UniversalClient,
	home, db int, keys [][]byte) ([]*repairedValue, error) {

	pipe := source.Pipeline()
	if db != home {
		pipe.Do(ctx, "select", db)
	}
	dumps := make([]*goredis.StringCmd, len(keys))
	lives := make([]*goredis.DurationCmd, len(keys))
	for i, key := range keys {
		dumps[i] = pipe.Dump(ctx, string(key))
		lives[i] = pipe.PTTL(ctx, string(key))
	}
	if db != home {
		pipe.Do(ctx, "select", home)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("read %d values from the source: %w", len(keys), err)
	}

	values := make([]*repairedValue, 0, len(keys))
	for i, key := range keys {
		value := &repairedValue{key: key, db: db}

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
