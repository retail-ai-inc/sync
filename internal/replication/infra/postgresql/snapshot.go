package postgresql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// The first copy, and the position it is taken at.
//
// Pin runs before a single row is read. The slot's consistent point is the
// moment the source promises to keep WAL from, so a copy read in the snapshot
// the slot exported at that point and a stream resumed from it meet exactly:
// every change made while the copy ran is still in the log, and none of them is
// missed. Reading the position afterwards loses every write made in between,
// which is the whole reason the pipeline asks for it first.
type Snapshotter struct {
	Schema *schemaWork
	// Snapshot reads the source as of ConsistentPoint. A copy read outside it
	// also holds rows committed after that point, which the stream then repeats.
	Snapshot sourceSnapshot
	// ConsistentPoint is where the slot promises WAL from, set only when this
	// start created the slot.
	ConsistentPoint pglogrepl.LSN
	Source          string

	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels
}

// sourceSnapshot is the source as the slot exported it, open until rolled back.
type sourceSnapshot interface {
	sourceQuerier
	Rollback(ctx context.Context) error
}

var errNoSnapshot = errors.New("no snapshot was taken where the replication " +
	"slot starts, so a copy would overlap its stream")

func (s *Snapshotter) Pin(_ context.Context) (domain.Position, error) {
	if s.ConsistentPoint == 0 || s.Snapshot == nil {
		return domain.Position{}, errNoSnapshot
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
	// Reading the source's own connection instead brings the overlap back.
	if s.Snapshot == nil {
		return errNoSnapshot
	}
	// An open snapshot holds back vacuum on the source for as long as it lasts.
	defer func() { _ = s.Snapshot.Rollback(ctx) }()

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

func readAt(ctx context.Context, source *pgx.Conn, snapshot string) (pgx.Tx, error) {
	tx, err := source.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin the copy's transaction: %w", err)
	}
	if _, err := tx.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+
		strings.ReplaceAll(snapshot, "'", "''")+"'"); err != nil {
		_ = tx.Rollback(ctx)
		return nil, fmt.Errorf("read the source at the replication slot's snapshot %s: %w",
			snapshot, err)
	}
	return tx, nil
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

	// As text, which the target parses back for any type; bytea in binary, since
	// lib/pq encodes a bytea parameter itself and must be handed the bytes.
	rows, err := s.Snapshot.Query(ctx, "SELECT * FROM "+pair.source(),
		pgx.QueryResultFormatsByOID{pgtype.ByteaOID: pgx.BinaryFormatCode})
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", pair.source(), err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	names := make([]string, len(fields))
	quoted := make([]string, len(fields))
	placeholders := make([]string, len(fields))
	for i, field := range fields {
		names[i] = string(field.Name)
		quoted[i] = pq.QuoteIdentifier(names[i])
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	// ON CONFLICT DO NOTHING so a re-run over a partly filled table adds what is
	// missing rather than failing on the first row that is already there.
	insert := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		pair.target(), strings.Join(quoted, ", "), strings.Join(placeholders, ", "))

	// The copied rows must carry the protection the stream gives later changes to them.
	policy := security.FindTableSecurityFromMappings(security.TableRef{
		Schema: pair.sourceSchema, Table: pair.sourceTable, Target: pair.targetTable,
	}, s.Config.Mappings)
	secured := policy.SecurityEnabled && len(policy.FieldSecurity) > 0

	tx, err := s.Schema.Target.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	written := 0
	for rows.Next() {
		decoded, err := rows.Values()
		if err != nil {
			return 0, err
		}
		values := make([]any, len(decoded))
		for i, raw := range rows.RawValues() {
			values[i] = decoded[i]
			if raw != nil && fields[i].DataTypeOID != pgtype.ByteaOID {
				values[i] = string(raw)
			}
		}
		if secured {
			// Decoded, so an int or a timestamp field is protected too. A value the
			// policy leaves as it was keeps the source's text.
			for i := range decoded {
				if processed := security.ProcessValue(decoded[i], names[i], policy); !reflect.DeepEqual(processed, decoded[i]) {
					values[i] = processed
				}
			}
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
