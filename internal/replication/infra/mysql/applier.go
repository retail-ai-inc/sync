package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// Applier writes one batch to a MySQL target in a single transaction, and
// records the position in that same transaction.
//
// Committing the two together is what MySQL's own replica does: the applier
// position lives in the InnoDB table mysql.slave_relay_log_info and is updated
// as part of the transaction that applies the rows, so a crash cannot leave the
// position claiming more than the data holds. The alternative — which this
// replaced — commits the data on one connection and the position on another
// every couple of hundred milliseconds, so every unclean stop replays the
// difference. Replaying is safe only because the writes are idempotent, and on a
// table with no primary key they are not idempotent at all.
//
// One transaction for the whole batch also gives the batch the property the
// design depends on: until it commits, no reader on the target sees any of it,
// and if it fails none of it happened. A batch applied in pieces would leave the
// target in a state the source was never in.
type Applier struct {
	DB *sql.DB
	// Checkpoints records the position. When it is nil the runner records the
	// position itself, which is a weaker guarantee and is only for targets that
	// cannot take part in the transaction.
	Checkpoints *checkpoint.SQLStore
	// CheckpointKey names this task's position.
	CheckpointKey string
	Logger        logrus.FieldLogger
}

// Apply writes every run of the batch, then the position, then commits.
func (a *Applier) Apply(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	total := 0
	for _, run := range runs {
		total += len(run)
	}
	if total == 0 {
		return false, nil
	}

	committed := false
	err := resilience.RetryDBOperation(ctx, a.Logger,
		fmt.Sprintf("apply %d changes", total),
		func() error {
			var attemptErr error
			committed, attemptErr = a.once(ctx, runs, pos)
			return attemptErr
		})
	if err != nil {
		return false, err
	}
	return committed, nil
}

// once is one attempt: begin, write, commit or roll all of it back.
func (a *Applier) once(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	if a.DB == nil {
		return false, fmt.Errorf("no target connection")
	}

	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	// A rollback after a successful commit is a no-op, so this needs no
	// bookkeeping to decide whether it should run.
	defer func() { _ = tx.Rollback() }()

	for _, run := range runs {
		for _, event := range run {
			stmt, ok := event.Payload.(statement)
			if !ok {
				// Not a transient failure, and retrying would produce it again.
				// Rolling back is right: half a batch is worse than none of it.
				return false, domain.Unrecoverable(
					"a %s event for %s carries a %T rather than a statement, so the batch "+
						"cannot be applied", event.Op, event.NS, event.Payload)
			}
			if _, err := tx.ExecContext(ctx, stmt.query, stmt.args...); err != nil {
				return false, fmt.Errorf("%s: %w", stmt.query, err)
			}
		}
	}

	committed := false
	if a.Checkpoints != nil && !pos.IsZero() {
		if err := a.Checkpoints.SaveTx(ctx, tx, a.CheckpointKey, pos.Payload); err != nil {
			return false, err
		}
		committed = true
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return committed, nil
}
