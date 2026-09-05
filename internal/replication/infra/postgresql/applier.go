package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// Writing a batch to the target, in one transaction with the position.
//
// Rows used to be written one statement at a time as they were decoded, with
// the position recorded separately per commit. Two things follow from that and
// neither is small: a busy source cost one round trip per row, and a stop
// between the last write and the position leaves the two disagreeing -- the
// next start replays from a position that is behind what the target holds,
// which is safe only where the writes are idempotent, and on a table with no
// primary key they are not.
type Applier struct {
	DB *sql.DB
	// Checkpoints records the position in the same transaction as the rows, so
	// the two cannot disagree.
	Checkpoints   *checkpoint.SQLStore
	CheckpointKey string
	// Mirror is written after the transaction commits, for a task that asked for
	// the position on disk as well. Best effort by design: the position on the
	// target is the one that is resumed from, and a file that cannot be written
	// must not fail a batch that has already landed.
	Mirror checkpoint.Store
	// Applied is told the position that landed, so the reader can report it to
	// the source. The server discards WAL a standby says it has flushed, so this
	// must be what reached the target and not what was received.
	Applied func(pglogrepl.LSN)
	Logger  logrus.FieldLogger
	Labels  metrics.Labels
}

func (a *Applier) Apply(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	total := 0
	for _, run := range runs {
		total += len(run)
	}
	if total == 0 {
		return false, nil
	}

	started := time.Now()
	namespaces := namespacesOf(runs)

	committed := false
	var trips int
	var commit time.Duration
	err := resilience.RetryDBOperation(ctx, a.Logger,
		fmt.Sprintf("apply %d changes", total),
		func() error {
			var attemptErr error
			committed, trips, commit, attemptErr = a.once(ctx, runs, pos)
			return attemptErr
		})
	if err != nil {
		return false, err
	}

	if committed && a.Mirror != nil {
		if err := a.Mirror.Save(ctx, a.CheckpointKey, pos.Payload); err != nil {
			a.Logger.Warnf("[PostgreSQL] The position is recorded on the target but "+
				"the copy on disk could not be written: %v", err)
		}
	}

	if committed && a.Applied != nil {
		if lsn, _, err := decodeLSN(pos.Payload, ""); err == nil && lsn > 0 {
			a.Applied(lsn)
		}
	}
	metrics.ObserveBatch(a.Labels, time.Since(started), commit, trips, namespaces, total)
	return committed, nil
}

// namespacesOf reports how many distinct tables a batch touches, which is what
// says whether a commit has one table's worth of work to coordinate or several.
func namespacesOf(runs [][]*domain.Event) int {
	seen := map[string]bool{}
	for _, run := range runs {
		for _, event := range run {
			seen[event.NS.String()] = true
		}
	}
	return len(seen)
}

func (a *Applier) once(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (
	bool, int, time.Duration, error) {

	if a.DB == nil {
		return false, 0, 0, fmt.Errorf("no target connection")
	}

	trips := 0
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, trips, 0, err
	}
	// A rollback after a successful commit is a no-op, so this needs no
	// bookkeeping to decide whether it should run.
	defer func() { _ = tx.Rollback() }()

	for _, run := range runs {
		for _, event := range run {
			if event.Heartbeat {
				continue
			}
			stmt, ok := event.Payload.(statement)
			if !ok {
				// Not transient, and retrying would produce it again. Rolling
				// back is right: half a batch is worse than none of it.
				return false, trips, 0, domain.Unrecoverable(
					"a %s event for %s carries a %T rather than a statement, so the "+
						"batch cannot be applied", event.Op, event.NS, event.Payload)
			}
			trips++
			if _, err := tx.ExecContext(ctx, stmt.query, stmt.args...); err != nil {
				return false, trips, 0, fmt.Errorf("%s: %w", stmt.query, err)
			}
		}
	}

	committed := false
	if a.Checkpoints != nil && !pos.IsZero() {
		trips++
		if err := a.Checkpoints.SaveTx(ctx, tx, a.CheckpointKey, pos.Payload); err != nil {
			return false, trips, 0, err
		}
		committed = true
	}

	startedCommit := time.Now()
	if err := tx.Commit(); err != nil {
		return false, trips, 0, err
	}
	return committed, trips, time.Since(startedCommit), nil
}
