// Package directionlock records which way a task replicates, on both of the
// databases it touches, so the direction cannot silently reverse.
//
// The failure it prevents follows a regional outage: Tokyo goes down, Osaka is
// promoted and takes payments, Tokyo comes back and the Tokyo → Osaka task
// resumes from its stored position — overwriting everything Osaka wrote since
// the promotion with data that is older and wrong. Nothing in the replication
// protocol notices; it looks like catching up.
package directionlock

import (
	"context"
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
//
// It has to be comfortably longer than the heartbeat interval, because a claim
// that expires while its owner is still running is worse than no claim at all:
// another task would take the endpoint and the two would write over each other.
const DefaultStaleAfter = 15 * time.Minute

const HeartbeatInterval = time.Minute

type Claim struct {
	TaskID int  `json:"task_id"`
	Role   Role `json:"role"`
	// Peer identifies the endpoint on the other side, without credentials.
	Peer string `json:"peer"`
	// Owner names the process holding the claim, so a stale one can be traced
	// to something an operator can look at.
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
	// Remove discards one task's claim.
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

// owner names the process holding a claim, which is what tells a restarted
// instance apart from a second one. The hostname is the default: a restarted
// pod keeps its name and may take its own claim back at once, while a second
// replica has a different name and is refused. SYNC_INSTANCE overrides it where
// the hostname is not distinctive — two instances on one machine, or a
// container sharing the host's.
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
	// running this same task. The direction conflicts need somebody to decide
	// which side is authoritative; this one only needs the other process to
	// finish exiting, which is what a rolling update looks like from the new
	// pod's side.
	Concurrent bool
}

func (c *Conflict) Error() string {
	return fmt.Sprintf("refusing to replicate: %s. %s is claimed by task %d as its "+
		"%s of %s, held by %s and last refreshed at %s",
		c.Reason, c.Endpoint, c.Existing.TaskID, c.Existing.Role,
		c.Existing.Peer, c.Existing.Owner, c.Existing.UpdatedAt.Format(time.RFC3339))
}

// concurrentWith reports a claim held by another process running this same
// task. Skipping every claim carrying this task's id — which is what let a
// crashed task take its own claim back at once — also let two processes run it
// at the same time.
//
// Two writers replaying one stream from different offsets eventually apply an
// older version of a record after a newer one, and idempotence does not save
// that: an upsert of the older version is a regression. They agree only once
// both reach the end of the log, so at any moment in between — the moment of a
// failover included — the target can hold stale values.
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
// task, which is worth retrying rather than stopping for.
func IsConcurrent(err error) bool {
	var conflict *Conflict
	return errors.As(err, &conflict) && conflict.Concurrent
}

// IsBlocking reports whether a failure is a direction conflict that no amount
// of retrying resolves, and so must stop the task rather than restart it.
//
// Everything else is transient, the unreachable endpoint most of all: reading a
// claim needs the very database the outage took away, and treating that as
// permanent stopped replication for good the first time Osaka bounced. A
// concurrent claim clears when the other process exits.
func IsBlocking(err error) bool {
	var conflict *Conflict
	return errors.As(err, &conflict) && !conflict.Concurrent
}

// ConcurrentAfter is how recently another process must have refreshed its claim
// for this one to treat it as still running.
//
// Three heartbeats: long enough that a process which is actually alive has
// certainly refreshed within it, short enough that a genuine handover to another
// host is not blocked for the quarter of an hour a direction claim survives.
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
// while the task is running.
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

// KeepAlive refreshes the claims until the context is cancelled.
//
// A failure to refresh is logged by the caller rather than stopping the task:
// the endpoint being briefly unreachable is not evidence the direction has
// changed, and the claim outlives several missed heartbeats.
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
// quick: without it an operator switching Tokyo → Osaka round waits out the
// staleness window. A crash deliberately does not release them — a claim
// outliving the process that died is the whole reason the heartbeat exists.
func (g *Guard) Release(ctx context.Context) error {
	var firstErr error
	for _, store := range []Store{g.Source, g.Target} {
		if err := store.Remove(ctx, g.TaskID); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("release the direction claim on %s: %w", store.Endpoint(), err)
		}
	}
	return firstErr
}

// releaseTimeout bounds the release. It needs a deadline of its own because the
// task's context is already cancelled by the time the release runs — passing
// that one in would abandon every claim on the way out.
const releaseTimeout = 5 * time.Second

// Warner is the part of a logger this package uses. Taking an interface rather
// than logrus keeps the lock free of a logging dependency, and lets a test see
// what was reported.
type Warner interface {
	Warnf(format string, args ...interface{})
}

// Hold acquires the claims, keeps them refreshed for as long as the context
// lives, and returns the function that gives them up. Every engine did this
// itself, in fourteen identical lines each, and the parts that are easy to get
// wrong were the parts being copied: the heartbeat has to stop before the
// release, and the release needs a deadline that is not the cancelled one.
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
