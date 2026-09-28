package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

// How far behind the source the target is, in the source's own units.
//
// The performance tests judge a load level by whether this number holds
// level or climbs over four minutes, and until now MySQL could not answer it.
// sync_source_position_bytes is the offset the READER has reached inside the
// current binlog file -- not where the file ends -- and it drops to near zero
// at every rotation; sync_applied_position_bytes was declared and set by
// nothing. Subtracting the two, as the test plan did, measured the queue
// inside this process within one file and nothing across a rotation.
//
// Transactions are the unit that survives both a rotation and a failover: the
// source's executed GTID set minus the target's applied set is exactly the
// work still to do, and the server does that subtraction itself.

// backlogEvery is how often the source is asked where its log ends. Two cheap
// statements per poll; the tests take a median over minutes, so seconds of
// staleness do not matter.
const backlogEvery = 15 * time.Second

// watchBacklog polls the source's head and the target's recorded position and
// publishes the distance between them until the context ends.
func (s *Syncer) watchBacklog(ctx context.Context, store *checkpoint.SQLStore, labels metrics.Labels) {
	source, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Debugf("[MySQL] Could not open the source to measure the backlog: %v", err)
		return
	}
	defer source.Close()

	discovery.Poll(ctx, backlogEvery, func() {
		behind, err := measureBacklog(ctx, source, store)
		if err != nil {
			s.logger.Debugf("[MySQL] The backlog could not be measured this time: %v", err)
			return
		}
		behind.publish(labels)
	})
}

// backlog is one measurement of the distance from applied to head.
type backlog struct {
	// transactions is the count of GTIDs the source has executed and the target
	// has not applied; -1 when the two positions carry no GTID to compare.
	transactions int64
	// appliedOffset is the byte offset the target recorded, in its own file.
	appliedOffset int64
	// bytes is head minus applied when both lie in the same binlog file; -1
	// otherwise, because two offsets in different files are not a distance.
	bytes int64
}

func (b backlog) publish(labels metrics.Labels) {
	metrics.SetAppliedPosition(labels, b.appliedOffset)
	if b.transactions >= 0 {
		metrics.SetTransactionsBehind(labels, b.transactions)
	}
}

func measureBacklog(ctx context.Context, source *sql.DB, store *checkpoint.SQLStore) (backlog, error) {
	payload, err := store.Load(ctx, "")
	if err != nil {
		return backlog{}, fmt.Errorf("read the stored position: %w", err)
	}
	if payload == "" {
		return backlog{}, fmt.Errorf("the target holds no position for this task yet")
	}
	applied := &binlogCheckpoint{}
	if _, err := checkpoint.Decode(payload, applied); err != nil {
		return backlog{}, fmt.Errorf("read the stored position: %w", err)
	}

	head, err := readBinlogHead(ctx, source)
	if err != nil {
		return backlog{}, fmt.Errorf("read the source's head: %w", err)
	}
	return distance(ctx, source, head, applied)
}

// distance works out how far applied is behind head.
//
// GTID first, byte offsets only as a fallback: an offset is a position inside
// one file on one server, and the reader refuses to resume from one recorded
// against another server for the same reason it is not compared here.
func distance(ctx context.Context, source *sql.DB, head binlogHead, applied *binlogCheckpoint) (backlog, error) {
	b := backlog{transactions: -1, appliedOffset: int64(applied.Pos), bytes: -1}

	if head.GTIDSet != "" && applied.GTID != "" && applied.Flavor != gomysql.MariaDBFlavor {
		missing, err := gtidSubtract(ctx, source, head.GTIDSet, applied.GTID)
		if err != nil {
			return b, err
		}
		count, err := countGTIDs(missing)
		if err != nil {
			return b, err
		}
		b.transactions = count
	}

	if head.File != "" && head.File == applied.Name && uint64(head.Position) >= uint64(applied.Pos) {
		b.bytes = int64(head.Position) - int64(applied.Pos)
	}
	return b, nil
}

// gtidSubtract asks the source which of its executed transactions the applied
// set lacks. The server does the interval arithmetic, and it is the same
// server whose set is being read, so the answer is in its own terms.
func gtidSubtract(ctx context.Context, source *sql.DB, executed, applied string) (string, error) {
	var missing sql.NullString
	if err := source.QueryRowContext(ctx,
		"SELECT GTID_SUBTRACT(?, ?)", executed, applied).Scan(&missing); err != nil {
		return "", fmt.Errorf("GTID_SUBTRACT: %w", err)
	}
	return missing.String, nil
}

// countGTIDs counts the transactions in a GTID set: every interval of every
// server it names. An empty set is zero -- the target has everything.
func countGTIDs(set string) (int64, error) {
	if set == "" {
		return 0, nil
	}
	parsed, err := gomysql.ParseMysqlGTIDSet(set)
	if err != nil {
		return 0, fmt.Errorf("parse the GTID set %q: %w", set, err)
	}
	var total int64
	for _, server := range parsed.(*gomysql.MysqlGTIDSet).Sets {
		for _, interval := range server.Intervals {
			// Stop is the first GID after the interval, so the count is the plain
			// difference.
			total += interval.Stop - interval.Start
		}
	}
	return total, nil
}
