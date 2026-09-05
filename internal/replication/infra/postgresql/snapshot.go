package postgresql

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The first copy, and the position it is taken at.
//
// Pin runs before a single row is read. The slot's consistent point is the
// moment the source promises to keep WAL from, so a copy taken after it and a
// stream resumed from it meet exactly: every change made while the copy ran is
// still in the log, and none of them is missed. Reading the position afterwards
// loses every write made in between, which is the whole reason the pipeline
// asks for it first.
type Snapshotter struct {
	Schema *schemaWork
	// ConsistentPoint is where the slot promises WAL from, set when the slot was
	// created or read. Zero means the slot already existed and the stream picks
	// up from the stored position instead.
	ConsistentPoint pglogrepl.LSN
	Source          string

	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels
}

func (s *Snapshotter) Pin(_ context.Context) (domain.Position, error) {
	if s.ConsistentPoint == 0 {
		// Nothing to pin: the slot was already there, so its own position is
		// where the stream resumes and the copy has nothing to align with.
		return domain.Position{}, nil
	}
	payload, err := encodeLSN(s.ConsistentPoint, s.Source)
	if err != nil {
		return domain.Position{}, err
	}
	return domain.Position{Payload: payload}, nil
}

// Copy fills the target's tables from the source, skipping any that already
// hold rows.
//
// Skipping is what makes a restarted task cheap, and it is also why this is not
// a repair: a table with one row in it is left alone. Putting a table back
// wholesale is what a re-copy is for.
func (s *Snapshotter) Copy(ctx context.Context) error {
	pairs := s.pairs()
	if len(pairs) == 0 {
		s.Logger.Warn("[PostgreSQL] This task names no tables, so there is nothing " +
			"to copy and nothing will be replicated")
		return nil
	}

	metrics.SnapshotStarted(s.Labels, len(pairs))
	copied := 0
	for i, pair := range pairs {
		rows, err := s.copyTable(ctx, pair)
		if err != nil {
			metrics.SnapshotFinished(s.Labels, false, 0)
			return err
		}
		copied += rows
		metrics.SnapshotProgress(s.Labels, rows, len(pairs)-i-1, 0)
	}
	metrics.SnapshotFinished(s.Labels, true, 0)
	s.Logger.Infof("[PostgreSQL] The first copy is done: %d rows across %d tables",
		copied, len(pairs))
	return nil
}

// tablePair is one table on each side, with the schema it lives in.
type tablePair struct {
	sourceSchema, sourceTable string
	targetSchema, targetTable string
}

func (p tablePair) source() string { return p.sourceSchema + "." + p.sourceTable }
func (p tablePair) target() string { return p.targetSchema + "." + p.targetTable }

func (s *Snapshotter) pairs() []tablePair {
	var pairs []tablePair
	for _, mapping := range s.Config.Mappings {
		sourceSchema := orPublic(mapping.SourceSchema)
		targetSchema := orPublic(mapping.TargetSchema)
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			target := table.TargetTable
			if target == "" {
				target = table.SourceTable
			}
			pairs = append(pairs, tablePair{
				sourceSchema: sourceSchema, sourceTable: table.SourceTable,
				targetSchema: targetSchema, targetTable: target,
			})
		}
	}
	return pairs
}

func orPublic(schema string) string {
	if schema == "" {
		return "public"
	}
	return schema
}

func (s *Snapshotter) copyTable(ctx context.Context, pair tablePair) (int, error) {
	// A target that cannot be counted is a stop, not a skip. It used to warn and
	// carry on, which meant a table missing from the target -- because the schema
	// preparation failed, or because nobody created it -- left the copy doing
	// nothing and reporting success, and the link then ran with a table that was
	// never filled.
	var held int
	if err := s.Schema.Target.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s", pair.target())).Scan(&held); err != nil {
		return 0, fmt.Errorf("count %s on the target, to see whether it needs "+
			"copying: %w", pair.target(), err)
	}
	if held > 0 {
		s.Logger.Infof("[PostgreSQL] %s already holds %d rows, so it is left alone",
			pair.target(), held)
		return 0, nil
	}

	rows, err := s.Schema.Source.Query(ctx, "SELECT * FROM "+pair.source())
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", pair.source(), err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	names := make([]string, len(fields))
	placeholders := make([]string, len(fields))
	for i, field := range fields {
		names[i] = string(field.Name)
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	// ON CONFLICT DO NOTHING so a re-run over a partly filled table adds what is
	// missing rather than failing on the first row that is already there.
	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		pair.target(), strings.Join(names, ", "), strings.Join(placeholders, ", "))

	tx, err := s.Schema.Target.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	written := 0
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return 0, err
		}
		result, err := tx.ExecContext(ctx, insert, values...)
		if err != nil {
			return 0, fmt.Errorf("write %s: %w", pair.target(), err)
		}
		if affected, _ := result.RowsAffected(); affected > 0 {
			written++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("read %s: %w", pair.source(), err)
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	s.Logger.Infof("[PostgreSQL] Copied %d rows from %s to %s",
		written, pair.source(), pair.target())
	return written, nil
}
