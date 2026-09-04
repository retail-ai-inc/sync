// Package directionlock records which way a task replicates, on both databases,
// so the direction cannot silently reverse. Without it: Tokyo falls over, Osaka
// is promoted and takes payments, Tokyo returns and the old task resumes from
// its position — overwriting everything Osaka wrote with older, wrong data, and
// the protocol calls it catching up.
package directionlock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type Role string

const (
	// RoleSource: the task reads changes out of this database.
	RoleSource Role = "source"
	// RoleTarget: the task writes changes into this database.
	RoleTarget Role = "target"
)

// DefaultStaleAfter is how long a claim survives without a heartbeat.
// Comfortably longer than the interval, because a claim expiring under a live
// owner lets another task take the endpoint and both write over each other.
const DefaultStaleAfter = 15 * time.Minute

const HeartbeatInterval = time.Minute

type Claim struct {
	TaskID int  `json:"task_id"`
	Role   Role `json:"role"`
	// Peer identifies the endpoint on the other side, without credentials.
	Peer string `json:"peer"`
	// Owner names the process holding the claim, so a stale one can be traced to
	// something an operator can look at.
	Owner     string    `json:"owner"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (c Claim) Fresh(now time.Time, staleAfter time.Duration) bool {
	return now.Sub(c.UpdatedAt) < staleAfter
}

type Store interface {
	// Claims reports every claim on the endpoint, from every task.
	Claims(ctx context.Context) ([]Claim, error)
	// Put records one claim, replacing that task's previous one.
	Put(ctx context.Context, c Claim) error
	Remove(ctx context.Context, taskID int) error
	// Endpoint describes the database, for error messages. It must not carry
	// credentials.
	Endpoint() string
}

type Guard struct {
	TaskID int
	Source Store
	Target Store

	// StaleAfter overrides DefaultStaleAfter; zero means the default.
	StaleAfter time.Duration
	// Now overrides the clock, for tests.
	Now func() time.Time
	// Owner overrides the process name; empty means the hostname.
	Owner string
}

func (g *Guard) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now().UTC()
}

func (g *Guard) staleAfter() time.Duration {
	if g.StaleAfter > 0 {
		return g.StaleAfter
	}
	return DefaultStaleAfter
}

// owner tells a restarted instance apart from a second one. The hostname by
// default — a restarted pod keeps its name and takes its claim back, a second
// replica is refused; SYNC_INSTANCE overrides it where the hostname is not
// distinctive.
func (g *Guard) owner() string {
	if g.Owner != "" {
		return g.Owner
	}
	if named := strings.TrimSpace(os.Getenv("SYNC_INSTANCE")); named != "" {
		return named
	}
	host, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return host
}

// Conflict is the refusal to replicate, carrying the claim that caused it so an
// operator can see who holds the endpoint and since when.
type Conflict struct {
	Endpoint string
	Existing Claim
	Reason   string
	// Concurrent marks the one conflict that resolves itself: another process
	// running this task, which only needs the other to finish exiting. The
	// direction conflicts need somebody to decide which side is authoritative.
	Concurrent bool
}

func (c *Conflict) Error() string {
	return fmt.Sprintf("refusing to replicate: %s. %s is claimed by task %d as its "+
		"%s of %s, held by %s and last refreshed at %s",
		c.Reason, c.Endpoint, c.Existing.TaskID, c.Existing.Role,
		c.Existing.Peer, c.Existing.Owner, c.Existing.UpdatedAt.Format(time.RFC3339))
}

// concurrentWith reports a claim held by another process running this task.
// Skipping every claim with this task's id, which is what let a crashed task
// reclaim at once, also let two processes run it together — and two writers
// replaying one stream from different offsets eventually apply an older version
// of a record after a newer one, which idempotence does not save.
func (g *Guard) concurrentWith(existing Claim, now time.Time, endpoint string) *Conflict {
	if existing.TaskID != g.TaskID || existing.Owner == g.owner() {
		return nil
	}
	if !existing.Fresh(now, ConcurrentAfter) {
		// The other process has stopped refreshing, so it has gone.
		return nil
	}
	return &Conflict{
		Endpoint:   endpoint,
		Existing:   existing,
		Concurrent: true,
		Reason: "another process is already running this task. Two writers replaying " +
			"one stream from different offsets apply an older version of a record " +
			"after a newer one, which no amount of idempotence undoes",
	}
}

// IsConcurrent reports whether a failure is another process running the same
// task, worth retrying rather than stopping for.
func IsConcurrent(err error) bool {
	var conflict *Conflict
	return errors.As(err, &conflict) && conflict.Concurrent
}

// IsBlocking reports whether a failure is a direction conflict retrying cannot
// resolve. Everything else is transient, the unreachable endpoint most of all:
// reading a claim needs the database the outage took away, and calling that
// permanent stopped replication for good the first time Osaka bounced.
func IsBlocking(err error) bool {
	var conflict *Conflict
	return errors.As(err, &conflict) && !conflict.Concurrent
}

// ConcurrentAfter is how recently another process must have refreshed to count
// as running. Three heartbeats: long enough that a live process has certainly
// refreshed, short enough not to block a genuine handover for the staleness
// window.
const ConcurrentAfter = 3 * HeartbeatInterval

// Acquire checks both endpoints and records this task's direction.
//
// It refuses in three cases, all of which mean two writers would end up fighting
// over the same data:
//
//   - the target is somebody's source, which is what a promoted replica looks
//     like: writing to it would overwrite everything written since the promotion
//   - the source is somebody's target, so this task would read a database that
//     is itself being written by a replication task
//   - the target is already another task's target, from a different source
func (g *Guard) Acquire(ctx context.Context) error {
	now := g.now()

	targetClaims, err := g.Target.Claims(ctx)
	if err != nil {
		return fmt.Errorf("read the direction claims on %s: %w", g.Target.Endpoint(), err)
	}
	for _, existing := range targetClaims {
		if conflict := g.concurrentWith(existing, now, g.Target.Endpoint()); conflict != nil {
			return conflict
		}
		if existing.TaskID == g.TaskID || !existing.Fresh(now, g.staleAfter()) {
			continue
		}
		switch {
		case existing.Role == RoleSource:
			return &Conflict{
				Endpoint: g.Target.Endpoint(),
				Existing: existing,
				Reason: "the target is being replicated out of, which is what a " +
					"promoted replica looks like; writing to it would overwrite " +
					"everything written since the promotion",
			}
		case existing.Peer != g.Source.Endpoint():
			return &Conflict{
				Endpoint: g.Target.Endpoint(),
				Existing: existing,
				Reason: "the target is already being written by another task from a " +
					"different source, and the two would overwrite each other",
			}
		}
	}

	sourceClaims, err := g.Source.Claims(ctx)
	if err != nil {
		return fmt.Errorf("read the direction claims on %s: %w", g.Source.Endpoint(), err)
	}
	for _, existing := range sourceClaims {
		if conflict := g.concurrentWith(existing, now, g.Source.Endpoint()); conflict != nil {
			return conflict
		}
		if existing.TaskID == g.TaskID || !existing.Fresh(now, g.staleAfter()) {
			continue
		}
		if existing.Role == RoleTarget {
			return &Conflict{
				Endpoint: g.Source.Endpoint(),
				Existing: existing,
				Reason: "the source is itself being replicated into, so this task " +
					"would carry another task's writes back to where they came from",
			}
		}
	}

	return g.Heartbeat(ctx)
}

// Heartbeat refreshes both claims, which is what keeps them from going stale
// while the task runs.
// TargetClaim reports this task's claim on the target, encoded the way the
// store writes it.
//
// It exists for the one caller that has to write the claim back itself: a
// replicated FLUSHDB or FLUSHALL empties the database the claim lives in, and
// the transaction that carries the flush restores it in the same breath. The
// heartbeat cannot cover that -- it refreshes a claim rather than noticing one
// has gone, so between the flush and the next tick the target would be
// unclaimed and something else could take it for a source.
func (g *Guard) TargetClaim() (string, error) {
	encoded, err := json.Marshal(Claim{
		TaskID:    g.TaskID,
		Role:      RoleTarget,
		Peer:      g.Source.Endpoint(),
		Owner:     g.owner(),
		UpdatedAt: g.now(),
	})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func (g *Guard) Heartbeat(ctx context.Context) error {
	now := g.now()
	owner := g.owner()

	if err := g.Source.Put(ctx, Claim{
		TaskID:    g.TaskID,
		Role:      RoleSource,
		Peer:      g.Target.Endpoint(),
		Owner:     owner,
		UpdatedAt: now,
	}); err != nil {
		return fmt.Errorf("record the direction claim on %s: %w", g.Source.Endpoint(), err)
	}

	if err := g.Target.Put(ctx, Claim{
		TaskID:    g.TaskID,
		Role:      RoleTarget,
		Peer:      g.Source.Endpoint(),
		Owner:     owner,
		UpdatedAt: now,
	}); err != nil {
		return fmt.Errorf("record the direction claim on %s: %w", g.Target.Endpoint(), err)
	}
	return nil
}

// KeepAlive refreshes the claims until the context is cancelled. A failed
// refresh is logged rather than fatal: a briefly unreachable endpoint is not
// evidence the direction changed, and the claim outlives several missed
// heartbeats.
func (g *Guard) KeepAlive(ctx context.Context, onError func(error)) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := g.Heartbeat(ctx); err != nil && onError != nil {
				onError(err)
			}
		}
	}
}

// Release discards this task's claims, which is what makes a planned failover
// quick rather than a wait on the staleness window. A crash deliberately does
// not release them — a claim outliving a dead process is why the heartbeat
// exists.
func (g *Guard) Release(ctx context.Context) error {
	var firstErr error
	for _, store := range []Store{g.Source, g.Target} {
		if err := store.Remove(ctx, g.TaskID); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("release the direction claim on %s: %w", store.Endpoint(), err)
		}
	}
	return firstErr
}

// releaseTimeout bounds the release, which needs a deadline of its own because
// the task's context is already cancelled by then.
const releaseTimeout = 5 * time.Second

// Warner is the part of a logger this package uses, an interface so the lock
// carries no logging dependency and a test can see what was reported.
type Warner interface {
	Warnf(format string, args ...interface{})
}

// Hold acquires the claims, keeps them refreshed while the context lives, and
// returns the function that gives them up. The order is the part every engine
// got wrong on its own: the heartbeat stops before the release, and the release
// needs a deadline that is not the cancelled one.
func Hold(ctx context.Context, guard *Guard, log Warner, engine string) (release func(), err error) {
	if err := guard.Acquire(ctx); err != nil {
		return nil, err
	}

	heartbeatCtx, stop := context.WithCancel(ctx)
	go guard.KeepAlive(heartbeatCtx, func(err error) {
		if log != nil {
			log.Warnf("[%s] Could not refresh the replication direction claim: %v", engine, err)
		}
	})

	var once sync.Once
	return func() {
		once.Do(func() {
			stop()

			releaseCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
			defer cancel()
			if err := guard.Release(releaseCtx); err != nil && log != nil {
				log.Warnf("[%s] Could not release the replication direction claim: %v", engine, err)
			}
		})
	}, nil
}
