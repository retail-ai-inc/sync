package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	goredis "github.com/redis/go-redis/v9"
)

// Where a shard's stream has got to, and where that is written down.
//
// The offset never lives in a place of its own. It is held one per slot, in the
// target, written in the same transaction as the data of that slot — which is
// the only atomic unit a Redis cluster has. Everything else about the position
// is metadata that changes rarely, and lives in a single key.

// streamPosition is the metadata half: which history the offsets belong to, and
// whether the stream is still catching up to the first copy.
type streamPosition struct {
	// ReplID identifies the master's history. An offset from a different history
	// is meaningless, and asking to resume with the wrong one earns a full
	// resync.
	ReplID string `json:"replid"`
	// Offset is where to resume reading the stream from, and what a slot with no
	// marker of its own is taken to have applied up to.
	//
	// It only moves after a batch has landed in every slot it touched, which is
	// what makes both of those readings safe: a batch that failed part way leaves
	// it where it was, so the slots that did not land are read again rather than
	// assumed.
	Offset int64 `json:"offset"`
	// Phase is "value" while the stream is still inside the window the first
	// copy was taken over, and "command" afterwards.
	//
	// The first copy is taken with SCAN, so it is not a point in time: a key read
	// early may have changed before a key read late. Replaying commands over a
	// smear like that would apply a change twice — an INCR from before the key
	// was read, added again. So until the stream has passed the end of the copy,
	// every change is applied by re-reading the key's value instead, which lands
	// the same result however many times it happens.
	Phase string `json:"phase,omitempty"`
	// ValueUntil is the offset the copy finished at, and so where the value
	// phase ends.
	ValueUntil int64 `json:"value_until,omitempty"`
}

const (
	phaseValue   = "value"
	phaseCommand = "command"
)

// IsZero reports whether nothing has been recorded, which asks the source for
// everything.
func (p streamPosition) IsZero() bool { return p.ReplID == "" }

func (p streamPosition) inValuePhase(offset int64) bool {
	return p.Phase == phaseValue && offset < p.ValueUntil
}

func (p streamPosition) encode() (string, error) {
	payload, err := json.Marshal(p)
	if err != nil {
		return "", fmt.Errorf("encode the position: %w", err)
	}
	return string(payload), nil
}

func decodePosition(payload string) (streamPosition, error) {
	var position streamPosition
	if payload == "" {
		return position, nil
	}
	if err := json.Unmarshal([]byte(payload), &position); err != nil {
		return position, fmt.Errorf("read the stored position %q: %w", payload, err)
	}
	if position.ReplID == "" {
		return position, fmt.Errorf("the stored position names no replication id, "+
			"so its offset cannot be trusted: %q", payload)
	}
	return position, nil
}

func metaKey(taskID int, shard string) string {
	return "__sync:pos:" + strconv.Itoa(taskID) + ":" + shard
}

// Checkpoints is the position store for one shard's stream.
//
// Two things are written down, and the difference between them matters. The
// per-slot markers are committed with the data, so they are the only record that
// cannot disagree with what is on the target; they say what to skip. The metadata
// holds a single floor to resume from, advanced only after a batch has landed
// outright, so it is never ahead of the truth.
type Checkpoints struct {
	Target goredis.UniversalClient
	TaskID int
	Shard  string

	// markers caches what was read, so the applier can decide what to skip
	// without asking the target about every command.
	mu      sync.Mutex
	markers []int64
	loaded  bool
}

// Load reads the metadata and the slot markers, and reports where to resume.
//
// The resume point is the offset in the metadata, which is advanced only after a
// batch has landed in every slot it touched. That makes it a floor that is never
// ahead of the truth: if the write of it was lost, or a batch failed part way and
// the process died, it still points at or before the last fully applied batch.
//
// Reading from there re-delivers work to slots that are further ahead, and the
// markers are what let those slots skip it. The markers cannot serve as the
// resume point themselves — a slot written once and never again would hold it at
// that moment for ever.
func (c *Checkpoints) Load(ctx context.Context, _ string) (string, error) {
	payload, err := c.Target.Get(ctx, metaKey(c.TaskID, c.Shard)).Result()
	if err == goredis.Nil {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read the stored position: %w", err)
	}
	position, err := decodePosition(payload)
	if err != nil {
		return "", err
	}

	markers, err := c.readMarkers(ctx, position.Offset)
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	c.markers, c.loaded = markers, true
	c.mu.Unlock()

	return position.encode()
}

// readMarkers fetches every slot's marker, defaulting the ones never written to
// the offset the stream started at.
func (c *Checkpoints) readMarkers(ctx context.Context, start int64) ([]int64, error) {
	pipe := c.Target.Pipeline()
	gets := make([]*goredis.StringCmd, SlotCount)
	for slot := 0; slot < SlotCount; slot++ {
		gets[slot] = pipe.Get(ctx, OffsetKey(slot, c.TaskID))
	}
	// A missing key is a redis.Nil error per command, not a failure of the
	// pipeline, so the batch error is only worth reporting if it is something else.
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("read the slot markers: %w", err)
	}

	markers := make([]int64, SlotCount)
	for slot, get := range gets {
		value, err := get.Result()
		if err == goredis.Nil {
			markers[slot] = start
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read the marker for slot %d: %w", slot, err)
		}
		at, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("the marker for slot %d reads %q", slot, value)
		}
		markers[slot] = at
	}
	return markers, nil
}

// Save writes the metadata.
//
// This is the resume floor, and it is only ever written after a batch has landed
// in every slot it touched. Advancing it while a slot still held unapplied
// commands would let those commands be skipped for good, because a slot with no
// marker of its own is taken to be applied up to this offset — which is only
// true if every batch before it succeeded outright.
func (c *Checkpoints) Save(ctx context.Context, _, payload string) error {
	position, err := decodePosition(payload)
	if err != nil {
		return err
	}
	if err := c.Target.Set(ctx, metaKey(c.TaskID, c.Shard), payload, 0).Err(); err != nil {
		return fmt.Errorf("record the position: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded {
		markers := make([]int64, SlotCount)
		for slot := range markers {
			markers[slot] = position.Offset
		}
		c.markers, c.loaded = markers, true
	}
	return nil
}

// markersFor hands the applier the cache, loading a fresh one if Load was never
// called.
// Refresh forgets the in-memory slot markers so the next batch reads them from
// the target again.
//
// The markers are advanced in memory as each slot lands, which is what lets a
// batch that failed part way be re-applied without repeating the slots that
// did land. That reasoning holds only while a failure means the write did not
// happen. A timeout does not mean that: the transaction can land on the target
// and the reply be lost, leaving the target ahead of what this process believes
// it wrote. Replaying then repeats a command that is not idempotent — measured
// as an RPUSH landing three times too often under packet loss.
//
// The target holds the truth, because the marker is written in the same
// transaction as the data. Re-reading it is what a task restart always did, and
// is what makes retrying in place as safe as restarting.
func (c *Checkpoints) Refresh(ctx context.Context) error {
	c.mu.Lock()
	c.loaded = false
	c.markers = nil
	c.mu.Unlock()
	_, err := c.Load(ctx, "")
	return err
}

func (c *Checkpoints) markersFor(start int64) []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded {
		c.markers = make([]int64, SlotCount)
		for slot := range c.markers {
			c.markers[slot] = start
		}
		c.loaded = true
	}
	return c.markers
}
