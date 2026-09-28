package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// How far the target has been written, read from the target rather than from a
// running task, so the question can be asked when nothing is running -- which
// is when it is asked.

// Progress reports where the source is and what the target holds.
//
// Preferred by GTID when both sides have one, because a file and an offset only
// mean something on the server that produced them: after a failover they name
// nowhere, and the comparison would be between two unrelated numbers. A GTID
// set is a set of transactions and survives the failover this is all for.
func Progress(ctx context.Context, cfg config.SyncConfig) (domain.Progress, error) {
	source, err := sql.Open("mysql", cfg.SourceConnection)
	if err != nil {
		return domain.Progress{}, fmt.Errorf("connect to the source: %w", err)
	}
	defer source.Close()

	target, err := sql.Open("mysql", cfg.TargetConnection)
	if err != nil {
		return domain.Progress{}, fmt.Errorf("connect to the target: %w", err)
	}
	defer target.Close()

	store := &checkpoint.SQLStore{
		DB:     target,
		Schema: dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection),
		TaskID: cfg.ID,
	}
	payload, err := store.Load(ctx, "")
	if err != nil {
		return domain.Progress{}, fmt.Errorf("read the stored position: %w", err)
	}

	stored := &binlogCheckpoint{}
	if payload != "" {
		if _, err := checkpoint.Decode(payload, stored); err != nil {
			return domain.Progress{}, fmt.Errorf("read the stored position: %w", err)
		}
	}

	head, err := readBinlogHead(ctx, source)
	if err != nil {
		// Tokyo being gone is the condition this endpoint exists for, and the
		// position it reports is kept on the target for that reason. Failing
		// here threw the half that survives the outage away.
		return domain.Progress{
			Engine: cfg.Type,
			Shards: []domain.ShardProgress{appliedOnly(stored, err)},
		}, nil
	}

	return domain.Progress{
		Engine: cfg.Type,
		Shards: []domain.ShardProgress{compareBinlog(ctx, source, head, stored,
			dsn.Endpoint(cfg.Type, cfg.SourceConnection))},
	}, nil
}

// appliedOnly reports what the target holds when the source cannot be asked
// where it is. Not comparable, not caught up -- and not nothing.
func appliedOnly(stored *binlogCheckpoint, why error) domain.ShardProgress {
	applied := describeStored(stored)
	note := fmt.Sprintf("the source could not be reached, so this is what the target "+
		"has applied and not how far behind it is: %v", why)
	if applied == "" {
		note = fmt.Sprintf("the target holds no position for this task, and the source "+
			"could not be reached either: %v", why)
	}
	return domain.ShardProgress{Applied: applied, Note: note}
}

// binlogHead is where the source's binary log ends now.
type binlogHead struct {
	File     string
	Position uint32
	GTIDSet  string
}

func (h binlogHead) describe() string {
	if h.File == "" {
		return h.GTIDSet
	}
	return fmt.Sprintf("%s:%d", h.File, h.Position)
}

func describeStored(stored *binlogCheckpoint) string {
	if stored.Name == "" {
		if stored.GTID != "" {
			return stored.GTID
		}
		return ""
	}
	return fmt.Sprintf("%s:%d", stored.Name, stored.Pos)
}

func compareBinlog(ctx context.Context, source *sql.DB, head binlogHead,
	stored *binlogCheckpoint, sourceEndpoint string) domain.ShardProgress {

	progress := domain.ShardProgress{
		Source:  head.describe(),
		Applied: describeStored(stored),
	}
	if progress.Applied == "" {
		progress.Note = "nothing has been applied to this target for this task"
		return progress
	}

	// GTID first: a set comparison the server itself makes, and the only one
	// that still means anything after the source has failed over.
	if head.GTIDSet != "" && stored.GTID != "" {
		contained, err := gtidSubset(ctx, source, head.GTIDSet, stored.GTID)
		if err == nil {
			progress.Comparable = true
			progress.CaughtUp = contained
			progress.BehindBytes = -1
			return progress
		}
		progress.Note = fmt.Sprintf("the source would not compare the two GTID sets "+
			"(%v), so the comparison below is by file and offset, which only means "+
			"something while the source has not failed over", err)
	}

	if head.File == "" || stored.Name == "" {
		if progress.Note == "" {
			progress.Note = "the two positions are not of the same kind and cannot be ordered"
		}
		return progress
	}

	// A file and an offset recorded against a different server address one
	// server's bytes and nowhere else, so ordering them here would be a
	// comparison of two unrelated numbers. This is the rule the reader applies
	// before resuming, asked here for the same reason.
	if stored.Source != "" && sourceEndpoint != "" && stored.Source != sourceEndpoint {
		progress.Note = fmt.Sprintf("the stored position was recorded against %s and "+
			"names bytes on that server only, so it cannot be ordered against this "+
			"one", stored.Source)
		return progress
	}

	progress.Comparable = true
	switch {
	case stored.Name > head.File:
		progress.Note = "the stored position names a binary log later than the source's " +
			"own, so it belongs to a history this source no longer has"
		progress.Comparable = false
	case stored.Name < head.File:
		progress.BehindBytes = -1
		progress.Note = "behind by more than one binary log file, so the distance is not " +
			"a byte count"
	case uint64(stored.Pos) >= uint64(head.Position):
		progress.CaughtUp = true
	default:
		progress.BehindBytes = int64(head.Position) - int64(stored.Pos)
	}
	return progress
}

// gtidSubset asks whether the source's set is contained in the applied set,
// which is the question "has the target applied everything the source had".
func gtidSubset(ctx context.Context, source *sql.DB, sourceSet, appliedSet string) (bool, error) {
	var contained sql.NullBool
	if err := source.QueryRowContext(ctx,
		"SELECT GTID_SUBSET(?, ?)", sourceSet, appliedSet).Scan(&contained); err != nil {
		return false, err
	}
	if !contained.Valid {
		return false, fmt.Errorf("GTID_SUBSET answered nothing")
	}
	return contained.Bool, nil
}

// readBinlogHead reads where the source's binary log ends.
//
// SHOW MASTER STATUS was removed in MySQL 8.4 and replaced by SHOW BINARY LOG
// STATUS; the columns differ between versions too, so they are read by name.
func readBinlogHead(ctx context.Context, source *sql.DB) (binlogHead, error) {
	var head binlogHead
	var lastErr error
	for _, statement := range []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"} {
		found, err := readStatusRow(ctx, source, statement, &head)
		if err != nil {
			lastErr = preferInformative(lastErr, err)
			continue
		}
		if found {
			return head, nil
		}
		// The statement worked and there was no row: the source has the binary
		// log switched off, which is a real answer and not something to retry
		// with the other spelling.
		return head, fmt.Errorf("the source reported no binary log position, so it " +
			"has the binary log switched off and nothing can be replicated from it")
	}
	return head, fmt.Errorf("read the source's binary log position: %w", lastErr)
}

// unknownStatement reports the server not recognising a spelling. One of the two
// always fails this way, so it says nothing about the source.
func unknownStatement(err error) bool {
	return err != nil && strings.Contains(err.Error(), "error in your SQL syntax")
}

// preferInformative keeps, of two failures, the one worth reporting. Whichever
// spelling was tried last used to win, so on 8.0 a missing REPLICATION CLIENT
// privilege was reported as a syntax error in a statement the server did not
// have, and the operator was sent to look at the wrong thing.
func preferInformative(kept, latest error) error {
	if kept == nil || unknownStatement(kept) {
		return latest
	}
	return kept
}

// parseUint32 reads a binlog offset, which the replication protocol carries in
// four bytes; a value that does not fit is treated as unreadable, not truncated.
func parseUint32(value string) uint32 {
	n, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(n)
}

func readStatusRow(ctx context.Context, source *sql.DB, statement string,
	head *binlogHead) (bool, error) {

	rows, err := source.QueryContext(ctx, statement)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return false, err
	}
	if !rows.Next() {
		return false, rows.Err()
	}

	cells := make([]interface{}, len(names))
	for i := range cells {
		cells[i] = new(sql.NullString)
	}
	if err := rows.Scan(cells...); err != nil {
		return false, err
	}

	for i, name := range names {
		value := cells[i].(*sql.NullString).String
		switch strings.ToLower(name) {
		case "file":
			head.File = value
		case "position":
			head.Position = parseUint32(value)
		case "executed_gtid_set":
			head.GTIDSet = strings.ReplaceAll(value, "\n", "")
		}
	}
	return true, rows.Err()
}
