package mysql

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"github.com/sirupsen/logrus"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type MySQLSyncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger
	// dialect is the flavour the target speaks; the zero value is MySQL.
	dialect dialect
}

// flavour reports the dialect to render statements in, defaulting to MySQL.
func (s *MySQLSyncer) flavour() dialect {
	if s.dialect == "" {
		return dialectMySQL
	}
	return s.dialect
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *MySQLSyncer {
	return &MySQLSyncer{
		cfg:    cfg,
		logger: logger.WithField("sync_task_id", cfg.ID),
	}
}

// Start replicates until the context is cancelled, or until it cannot carry on.
//
// The returned error is what the supervisor decides on: nil or a transient
// failure means try again, an ErrUnrecoverable means stop and tell somebody.
func (s *MySQLSyncer) Start(ctx context.Context) error {
	if err := security.CheckKeyForMappings(s.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	s.logger.Info("[MySQL] Starting synchronization...")

	cfg := canal.NewDefaultConfig()
	if strings.ToLower(s.cfg.Type) == "mariadb" {
		cfg.Flavor = "mariadb"
	} else {
		cfg.Flavor = "mysql"
	}
	cfg.Addr = s.parseAddr(s.cfg.SourceConnection)
	cfg.User, cfg.Password = s.parseUserPassword(s.cfg.SourceConnection)
	cfg.TLSConfig = s.sourceTLS()
	cfg.Dump.ExecutionPath = s.cfg.DumpExecutionPath

	sourceDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	discovering := !s.hasConfiguredTables()

	var includeTables []string
	if discovering {
		// Nothing was listed, so replicate the whole database — including the
		// tables that appear after this point. A table created at the source
		// used simply not to be replicated, with no warning anywhere, which
		// looks exactly like everything working.
		s.logger.Infof("[MySQL] No tables configured; replicating every table in %s, "+
			"including ones created later", sourceDBName)
		includeTables = []string{fmt.Sprintf("%s\\..*", sourceDBName)}
	} else {
		for _, mapping := range s.cfg.Mappings {
			for _, table := range mapping.Tables {
				includeTables = append(includeTables, fmt.Sprintf("%s\\.%s", sourceDBName, table.SourceTable))
			}
		}
	}
	cfg.IncludeTableRegex = includeTables

	if !discovering {
		// The task names its tables, so anything added at the source afterwards
		// is simply absent from the replica. Nothing used to say so.
		go s.warnAboutUnlistedTables(ctx, sourceDBName)
	}

	var c *canal.Canal
	err := resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var e error
		c, e = canal.NewCanal(cfg)
		return e
	})
	if err != nil {
		return fmt.Errorf("connect to the source: %w", err)
	}

	if err := s.checkRowImage(c); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	var targetDB *sql.DB
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		targetDB, connErr = sql.Open("mysql", s.cfg.TargetConnection)
		if connErr != nil {
			return connErr
		}
		return targetDB.PingContext(ctx)
	})
	if err != nil {
		return fmt.Errorf("connect to the target: %w", err)
	}

	// Nothing is read or written until the direction is agreed. A target that
	// has been promoted, or a source that is itself somebody's target, means
	// the pair has been reversed under us and carrying on would overwrite the
	// newer side with the older one.
	releaseGuard, guardErr := s.claimDirection(ctx, targetDB)
	if guardErr != nil {
		// A reversed direction is not something a retry resolves: somebody has
		// to decide which side is authoritative.
		if directionlock.IsConcurrent(guardErr) {
			// Another process is running this task. It resolves itself once that
			// one exits, which is what a rolling update looks like from the new
			// pod's side, so this is retried rather than blocked — being blocked
			// would leave the new pod refusing to work after the old one had
			// gone.
			return fmt.Errorf("%w", guardErr)
		}
		return domain.Unrecoverable("%v", guardErr)
	}
	defer releaseGuard()

	// The stored checkpoint is the authority on whether the copy has been made:
	// a previous run that reached the stream wrote one. Without it the copy runs
	// and its starting coordinates are pinned first, so writes made while it is
	// running are replayed by the stream rather than falling between the two.
	checkpoints := s.checkpointStore(targetDB)
	stored, err := s.loadCheckpoint(ctx, checkpoints)
	if err != nil {
		// "There is no checkpoint" and "the checkpoint could not be read" lead
		// to opposite decisions, and acting on the wrong one either re-copies
		// the whole database or skips whatever was in flight.
		return fmt.Errorf("read the stored checkpoint, without which this task "+
			"cannot safely decide where to resume from: %w", err)
	}
	if stored == nil {
		stored = s.snapshot(ctx, targetDB)
		if stored != nil {
			if payload, encErr := checkpoint.Encode(*stored); encErr == nil {
				if saveErr := checkpoints.Save(ctx, "", payload); saveErr != nil {
					s.logger.Errorf("[MySQL] Failed to store the snapshot checkpoint: %v", saveErr)
				}
			}
		}
	} else {
		s.logger.Infof("[MySQL] Resuming from a stored checkpoint: %+v", *stored)
	}

	h := &MyEventHandler{
		targetDB:          targetDB,
		mappings:          s.cfg.Mappings,
		logger:            s.logger,
		positionSaverPath: s.cfg.MySQLPositionPath,
		canal:             c,
		lastExecError:     0,
		TargetConnection:  s.cfg.TargetConnection,
		flavor:            cfg.Flavor,
		source:            dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		labels:            s.metricLabels(),
		discovering:       discovering,
		sourceDatabase:    sourceDBName,
		checkpoints:       checkpoints,
		checkpointEvery:   checkpointInterval(),
	}
	c.SetEventHandler(h)

	// Add connection health check
	connCheckTicker := time.NewTicker(5 * time.Minute)
	defer connCheckTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-connCheckTicker.C:
				if err := resilience.CheckSQLConnection(ctx, targetDB); err != nil {
					s.logger.Warnf("[MySQL] Target connection check failed: %v", err)
					if newDB, err := resilience.ReopenSQLConnection(ctx, s.logger, s.cfg.TargetConnection, "mysql"); err == nil {
						oldDB := targetDB
						targetDB = newDB
						h.setTargetDB(newDB)

						if oldDB != nil {
							_ = oldDB.Close()
						}
						s.logger.Info("[MySQL] Successfully reconnected to target database")
					}
				}
			}
		}
	}()

	stopped := make(chan error, 1)
	var readerReturned atomic.Bool

	// Stopping the reader is what makes this function's return mean anything.
	// Cancelling the context used to return from here and leave canal reading
	// the binlog and writing to the target for the life of the process: a task
	// restarted by the supervisor ran a second reader beside the first, and a
	// task edited to point somewhere else went on writing to where it used to
	// point. The final position is recorded afterwards, once the handler is
	// quiet, so nothing is applied after the position that names it.
	defer func() {
		c.Close()
		if !readerReturned.Load() {
			select {
			case <-stopped:
			case <-time.After(10 * time.Second):
				s.logger.Warn("[MySQL] The binlog reader did not stop within ten " +
					"seconds of being closed")
			}
		}
		h.recordPendingCheckpoint()
	}()

	go func() {
		switch {
		case stored == nil:
			stopped <- c.Run()
		case stored.gtidSet() != nil:
			// Preferred: the transactions themselves, which stay meaningful
			// across a failover to a different server.
			stopped <- c.StartFromGTID(stored.gtidSet())
		default:
			stopped <- c.RunFrom(stored.position())
		}
	}()

	metrics.SetTaskUp(s.metricLabels(), true)
	defer metrics.SetTaskUp(s.metricLabels(), false)

	select {
	case <-ctx.Done():
		s.logger.Info("[MySQL] Synchronization stopped.")
		return nil

	case runErr := <-stopped:
		readerReturned.Store(true)
		// The stream ended by itself. Whether that is worth retrying is the
		// whole question: it used to be logged and then waited on for a context
		// cancellation that might never come, so the task sat there doing
		// nothing until the process restarted.
		if runErr == nil {
			s.logger.Warn("[MySQL] The binlog stream ended without an error.")
			return nil
		}
		if strings.Contains(runErr.Error(), "context canceled") {
			return nil
		}
		if reason, lost := positionNoLongerAvailable(runErr); lost {
			return domain.Unrecoverable("%s", reason)
		}
		return fmt.Errorf("read the binlog: %w", runErr)
	}
}

// positionNoLongerAvailable reports whether an error says the source has
// discarded the binlog this task would resume from.
//
// Cloud SQL expires binary logs on a retention schedule, so a task stopped for
// longer than that comes back to find its offset gone. Retrying cannot help:
// the bytes are not there, and every attempt fails the same way. What is needed
// is a fresh copy, which somebody has to decide to make — and while nobody
// knows, the replica falls further behind.
func positionNoLongerAvailable(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	text := strings.ToLower(err.Error())
	for _, marker := range []string{
		"could not find first log file name",
		"could not find next log",
		"requested master_log_file",
		"error 1236",
		"binary log is not available",
		"the slave is connecting using change master to master_auto_position",
	} {
		if strings.Contains(text, marker) {
			return "the source no longer has the binlog this task would resume from (" +
				err.Error() + "). Clear the stored checkpoint to take a fresh copy; " +
				"until then nothing is being replicated", true
		}
	}
	return "", false
}

// snapshot copies the source into the target and reports the binlog coordinates
// the stream must resume from.
//
// The coordinates are read before a row is copied, inside the same transaction
// the copy reads through. Reading them afterwards — which is what starting canal
// with no stored position amounts to — loses every write made while the copy was
// running, and the copy of a payment table runs for as long as it runs.
//
// nil means the coordinates could not be pinned. The caller then has no safe
// place to resume from, which is worth saying out loud rather than papering over.
func (s *MySQLSyncer) snapshot(ctx context.Context, targetDB *sql.DB) *binlogCheckpoint {
	sourceDB, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Errorf("[MySQL] Failed to open source DB: %v", err)
		return nil
	}
	defer sourceDB.Close()

	// One pinned connection: the consistent snapshot and every SELECT that reads
	// through it have to be the same session.
	conn, err := sourceDB.Conn(ctx)
	if err != nil {
		s.logger.Errorf("[MySQL] Failed to pin a source connection: %v", err)
		return nil
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		s.logger.Errorf("[MySQL] Failed to open a consistent snapshot: %v", err)
		return nil
	}
	defer func() { _, _ = conn.ExecContext(ctx, "COMMIT") }()

	pinned, err := s.sourceCheckpoint(ctx, conn)
	if err != nil {
		s.logger.Errorf("[MySQL] Failed to read the source binlog coordinates: %v. "+
			"The copy cannot start without them, because every write made while "+
			"it ran would then belong to neither the copy nor the stream", err)
		return nil
	}
	pinned.Source = dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection)
	s.logger.Infof("[MySQL] Snapshot pinned at %+v", *pinned)

	if err := s.doInitialSync(ctx, conn, targetDB); err != nil {
		s.logger.Errorf("[MySQL] %v", err)
		// Without coordinates the caller has nothing safe to resume from, which
		// is exactly the situation an incomplete copy leaves.
		return nil
	}
	return pinned
}

// sourceCheckpoint reads the source's current binlog coordinates.
//
// The column list of SHOW MASTER STATUS has changed across server versions and
// the statement itself was renamed in 8.4, so the result is read by column name
// and the newer spelling is tried when the older one is not recognised.
func (s *MySQLSyncer) sourceCheckpoint(ctx context.Context, conn *sql.Conn) (*binlogCheckpoint, error) {
	var lastErr error
	for _, stmt := range []string{"SHOW MASTER STATUS", "SHOW BINARY LOG STATUS"} {
		cp, err := readBinlogStatus(ctx, conn, stmt)
		if err == nil {
			cp.Flavor = mysql.MySQLFlavor
			if strings.EqualFold(s.cfg.Type, "mariadb") {
				cp.Flavor = mysql.MariaDBFlavor
			}
			return cp, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// readBinlogStatus runs one form of the status statement and maps its columns.
func readBinlogStatus(ctx context.Context, conn *sql.Conn, stmt string) (*binlogCheckpoint, error) {
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("%s returned no row: is the binary log enabled?", stmt)
	}

	cells := make([]sql.NullString, len(columns))
	scan := make([]interface{}, len(columns))
	for i := range cells {
		scan[i] = &cells[i]
	}
	if err := rows.Scan(scan...); err != nil {
		return nil, err
	}

	var cp binlogCheckpoint
	for i, name := range columns {
		switch strings.ToLower(name) {
		case "file":
			cp.Name = cells[i].String
		case "position":
			pos, err := strconv.ParseUint(cells[i].String, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("%s reported position %q: %w", stmt, cells[i].String, err)
			}
			cp.Pos = uint32(pos)
		case "executed_gtid_set":
			cp.GTID = strings.ReplaceAll(cells[i].String, "\n", "")
		}
	}
	if cp.Name == "" {
		return nil, fmt.Errorf("%s reported no binlog file: is the binary log enabled?", stmt)
	}
	return &cp, nil
}

// doInitialSync copies every mapped table, reporting what it could not copy.
//
// Every failure here used to be logged and stepped over, and the caller then
// recorded the position as though the copy had finished. A table whose create
// failed, or whose rows only half arrived, was therefore never copied again: the
// stream carried on from a point that assumed a complete base, and the gap
// stayed for good. The row counts of the tables that did copy looked right.
//
// A failure to copy one table no longer stops the others — copying what can be
// copied is useful — but the caller is told, and must not record the position
// for an incomplete copy.
func (s *MySQLSyncer) doInitialSync(ctx context.Context, sourceDB *sql.Conn, targetDB *sql.DB) error {
	s.logger.Info("[MySQL] Starting the initial full sync...")

	const batchSize = 100
	var failures []string
	fail := func(format string, args ...interface{}) {
		failures = append(failures, fmt.Sprintf(format, args...))
	}
	sourceDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	targetDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection)

	for _, mapping := range s.resolveMappings(ctx, sourceDB, sourceDBName) {
		for _, tableMap := range mapping.Tables {
			exists, errExist := s.targetTableExists(ctx, targetDB, targetDBName, tableMap.TargetTable)
			if errExist != nil {
				fail("could not check whether %s.%s exists: %v", targetDBName, tableMap.TargetTable, errExist)
				continue
			}
			if !exists {
				countQuery := fmt.Sprintf("SHOW INDEX FROM %s.%s", sourceDBName, tableMap.SourceTable)
				rows, err := sourceDB.QueryContext(ctx, countQuery)
				var indexCount int
				if err == nil {
					for rows.Next() {
						indexCount++
					}
					rows.Close()
				}

				if errCreate := s.createTargetTableAndIndexes(ctx, sourceDB, targetDB, sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable); errCreate != nil {
					fail("could not create %s.%s: %v", targetDBName, tableMap.TargetTable, errCreate)
					continue
				}

				countQuery = fmt.Sprintf("SHOW INDEX FROM %s.%s", targetDBName, tableMap.TargetTable)
				rows, err = targetDB.QueryContext(ctx, countQuery)
				var createdCount int
				if err == nil {
					for rows.Next() {
						createdCount++
					}
					rows.Close()
				}

				s.logger.Infof("[MySQL] Created table %s.%s from source %s.%s with %d indexes",
					targetDBName, tableMap.TargetTable, sourceDBName, tableMap.SourceTable, createdCount)
			}

			// A target that already holds rows is not evidence the copy
			// finished: an interrupted copy leaves exactly that. The copy is
			// made of upserts, so re-reading rows it already wrote costs time
			// and nothing else, and it is the only way to fill the gap an
			// interrupted copy left. With no position path there is nothing to
			// remember between runs, so the row count is all there is to go on.
			if s.cfg.MySQLPositionPath == "" {
				targetCountQuery := fmt.Sprintf("SELECT COUNT(1) FROM %s.%s", targetDBName, tableMap.TargetTable)
				var count int
				if errC := targetDB.QueryRow(targetCountQuery).Scan(&count); errC != nil {
					fail("could not count the rows already in %s.%s: %v", targetDBName, tableMap.TargetTable, errC)
					continue
				}
				if count > 0 {
					s.logger.Infof("[MySQL] table %s.%s has %d rows and no position path is configured => skip initial sync", targetDBName, tableMap.TargetTable, count)
					continue
				}
			}

			s.logger.Infof("[MySQL] Doing initial full sync from %s.%s => %s.%s", sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable)

			cols, errCols := s.getTableColumns(ctx, sourceDB, sourceDBName, tableMap.SourceTable)
			if errCols != nil {
				fail("could not read the columns of %s.%s: %v", sourceDBName, tableMap.SourceTable, errCols)
				continue
			}

			selectSQL := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(cols, ","), sourceDBName, tableMap.SourceTable)
			srcRows, errQ := sourceDB.QueryContext(ctx, selectSQL)
			if errQ != nil {
				fail("could not read %s.%s: %v", sourceDBName, tableMap.SourceTable, errQ)
				continue
			}

			insertedCount := 0
			batchRows := make([][]interface{}, 0, batchSize)

			for srcRows.Next() {
				rowValues := make([]interface{}, len(cols))
				valuePtrs := make([]interface{}, len(cols))
				for i := range cols {
					valuePtrs[i] = &rowValues[i]
				}
				if errScan := srcRows.Scan(valuePtrs...); errScan != nil {
					fail("could not read a row of %s.%s: %v", sourceDBName, tableMap.SourceTable, errScan)
					continue
				}
				batchRows = append(batchRows, rowValues)
				if len(batchRows) == batchSize {
					if errB := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows); errB != nil {
						fail("could not write a batch of %s.%s: %v", targetDBName, tableMap.TargetTable, errB)
					} else {
						insertedCount += len(batchRows)
					}
					batchRows = batchRows[:0]
				}
			}
			srcRows.Close()

			if len(batchRows) > 0 {
				if errB2 := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows); errB2 != nil {
					fail("could not write the last batch of %s.%s: %v", targetDBName, tableMap.TargetTable, errB2)
				} else {
					insertedCount += len(batchRows)
				}
			}
			s.logger.Infof("[MySQL] initial sync => %s.%s => %s.%s inserted=%d rows",
				sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable, insertedCount)
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("the initial copy is incomplete, so the stream must not start "+
			"from it: %s", strings.Join(failures, "; "))
	}
	return nil
}

func (s *MySQLSyncer) targetTableExists(ctx context.Context, db *sql.DB, dbName, tableName string) (bool, error) {
	query := "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?"
	var cnt int
	err := db.QueryRowContext(ctx, query, dbName, tableName).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func (s *MySQLSyncer) createTargetTableAndIndexes(
	ctx context.Context,
	sourceDB *sql.Conn, targetDB *sql.DB,
	srcDBName, srcTableName, tgtDBName, tgtTableName string,
) error {
	createStmt, seqs, errGen := s.generateCreateTableSQL(ctx, sourceDB, srcDBName, srcTableName, tgtDBName, tgtTableName)
	if errGen != nil {
		return fmt.Errorf("generateCreateTableSQL fail: %w", errGen)
	}
	for _, seqStmt := range seqs {
		s.logger.Debugf("[MySQL] Creating sequence => %s", seqStmt)
		if _, errExec := targetDB.ExecContext(ctx, seqStmt); errExec != nil {
			s.logger.Warnf("[MySQL] create sequence fail => %v", errExec)
		}
	}

	// Check if table already exists
	exists, err := s.targetTableExists(ctx, targetDB, tgtDBName, tgtTableName)
	if err != nil {
		s.logger.Warnf("[MySQL] Error checking if table exists: %v", err)
	}

	if exists {
		s.logger.Infof("[MySQL] Table %s.%s already exists, skipping creation", tgtDBName, tgtTableName)
		return nil
	}

	s.logger.Infof("[MySQL] Creating table => %s", createStmt)
	if _, errExec := targetDB.ExecContext(ctx, createStmt); errExec != nil {
		return fmt.Errorf("create table fail: %w", errExec)
	}

	s.logger.Infof("[MySQL] Successfully created table %s.%s with indexes", tgtDBName, tgtTableName)
	return nil
}

func (s *MySQLSyncer) generateCreateTableSQL(
	ctx context.Context,
	sourceDB *sql.Conn,
	srcDBName, srcTableName, tgtDBName, tgtTableName string,
) (string, []string, error) {
	var tableName, createSQL string
	showQuery := fmt.Sprintf("SHOW CREATE TABLE %s.%s", srcDBName, srcTableName)
	row := sourceDB.QueryRowContext(ctx, showQuery)
	if err := row.Scan(&tableName, &createSQL); err != nil {
		return "", nil, fmt.Errorf("SHOW CREATE TABLE fail: %w", err)
	}
	oldPrefix := fmt.Sprintf("CREATE TABLE %s.", srcDBName)
	newPrefix := fmt.Sprintf("CREATE TABLE %s.", tgtDBName)
	createSQL = strings.Replace(createSQL, oldPrefix, newPrefix, 1)

	oldTable := fmt.Sprintf("%s.%s", srcDBName, srcTableName)
	newTable := fmt.Sprintf("%s.%s", tgtDBName, tgtTableName)
	createSQL = strings.Replace(createSQL, oldTable, newTable, 1)

	var seqs []string
	return createSQL, seqs, nil
}

func (s *MySQLSyncer) batchInsert(
	ctx context.Context,
	db *sql.DB,
	dbName, tableName string,
	cols []string,
	rows [][]interface{},
) error {
	if len(rows) == 0 {
		return nil
	}

	tableSecurity := security.FindTableSecurityFromMappings(tableName, s.cfg.Mappings)

	s.logger.Debugf("[MySQL] Table=%s security configuration: enabled=%v, rules=%d",
		tableName, tableSecurity.SecurityEnabled, len(tableSecurity.FieldSecurity))

	if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
		// A copy, not the caller's rows. The masking used to be applied in place,
		// so the snapshot reader's own buffer came back holding asterisks — the
		// binlog path builds a new slice for exactly this reason, and the two
		// halves of one syncer disagreeing about whether a shared slice may be
		// written to is the kind of thing that holds for years and then does not.
		masked := make([][]interface{}, len(rows))
		for i, row := range rows {
			out := make([]interface{}, len(row))
			for j, val := range row {
				if j < len(cols) {
					out[j] = security.ProcessValue(val, cols[j], tableSecurity)
					continue
				}
				out[j] = val
			}
			masked[i] = out
		}
		rows = masked
	}

	// The snapshot is idempotent for the same reason the change stream is: a
	// copy that is interrupted and resumed re-reads rows it already wrote.
	insertSQL := upsertStatement(s.flavour(), dbName, tableName, cols, len(rows))

	var args []interface{}
	for _, rowData := range rows {
		args = append(args, rowData...)
	}
	res, err := db.ExecContext(ctx, insertSQL, args...)
	if err != nil {
		return fmt.Errorf("batchInsert Exec => %w", err)
	}
	ra, _ := res.RowsAffected()
	s.logger.Infof("[MySQL][BULK-INSERT] table=%s.%s insertedRows=%d", dbName, tableName, ra)
	return nil
}

// dialect names the SQL flavour the write side speaks.
//
// Production targets are MySQL. The unit suite drives the handler against
// SQLite, which spells an idempotent insert differently, so the statement
// builders take the flavour rather than assuming one.
type dialect string

const (
	dialectMySQL  dialect = "mysql"
	dialectSQLite dialect = "sqlite"
)

// upsertStatement renders an idempotent multi-row insert for d.
//
// Replication is at-least-once: the binlog position is persisted periodically,
// so a restart replays whatever came after the last write. A plain INSERT turns
// that replay into a duplicate-key error, and a duplicate-key error is not a
// connection failure, so RetryDBOperation gives up on it immediately and the
// row is dropped. Every insert this syncer emits therefore has to be an upsert.
func upsertStatement(d dialect, dbName, table string, cols []string, rowCount int) string {
	if rowCount < 1 {
		rowCount = 1
	}

	placeholder := "(" + strings.Join(makeQuestionMarks(len(cols)), ",") + ")"
	values := make([]string, rowCount)
	for i := range values {
		values[i] = placeholder
	}

	if d == dialectSQLite {
		// SQLite's ON CONFLICT clause needs a conflict target, which the binlog
		// does not always give us, so use the form that needs none.
		return fmt.Sprintf("INSERT OR REPLACE INTO %s.%s (%s) VALUES %s",
			dbName, table, strings.Join(cols, ", "), strings.Join(values, ", "))
	}

	assignments := make([]string, len(cols))
	for i, c := range cols {
		assignments[i] = fmt.Sprintf("%s = VALUES(%s)", c, c)
	}
	return fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		dbName, table, strings.Join(cols, ", "), strings.Join(values, ", "),
		strings.Join(assignments, ", "))
}

func makeQuestionMarks(n int) []string {
	res := make([]string, n)
	for i := 0; i < n; i++ {
		res[i] = "?"
	}
	return res
}

// binlogCheckpoint is what the position file holds.
//
// The file-and-offset pair only means something on the server that produced it.
// After a Cloud SQL failover the new primary has its own binlog files, and an
// offset taken from the old one points at unrelated bytes — the syncer either
// fails to start or, worse, resumes from the wrong place. A GTID set names the
// transactions themselves and survives the failover, so it is what a resumed
// task prefers.
//
// Name and Pos keep the capitalised spelling mysql.Position marshals to, so a
// file written before GTIDs were recorded still loads.
type binlogCheckpoint struct {
	Name   string `json:"Name"`
	Pos    uint32 `json:"Pos"`
	GTID   string `json:"gtid,omitempty"`
	Flavor string `json:"flavor,omitempty"`
	// Source names the server the offset belongs to, without credentials. A
	// file and offset mean nothing anywhere else: read against a different
	// server they address unrelated bytes, and the read succeeds, so the task
	// resumes from somewhere arbitrary with no error to show for it. That is
	// reachable by repointing a task at another source, and by a task id being
	// reused after the configuration database is restored from a backup.
	Source string `json:"source,omitempty"`
}

// position reports the file-and-offset half of the checkpoint.
func (c *binlogCheckpoint) position() mysql.Position {
	return mysql.Position{Name: c.Name, Pos: c.Pos}
}

// gtidSet parses the recorded GTID set, reporting nil when there is none or it
// cannot be read. A checkpoint written by an older build has no GTID set and
// falls back to the offset.
func (c *binlogCheckpoint) gtidSet() mysql.GTIDSet {
	if c.GTID == "" {
		return nil
	}
	flavor := c.Flavor
	if flavor == "" {
		flavor = mysql.MySQLFlavor
	}
	set, err := mysql.ParseGTIDSet(flavor, c.GTID)
	if err != nil {
		return nil
	}
	return set
}

// checkpointStore is where this task records its offset.
//
// It writes to the target database as well as the configured file. The file
// alone was the problem: the syncer runs beside the source, so the outage this
// setup exists to survive takes the record of what has been applied with it,
// and a replacement started in the other region has no way to find out where to
// resume from.
func (s *MySQLSyncer) checkpointStore(targetDB *sql.DB) checkpoint.Store {
	stores := []checkpoint.Store{
		&checkpoint.SQLStore{
			DB:     targetDB,
			Schema: dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection),
			TaskID: s.cfg.ID,
		},
	}
	if s.cfg.MySQLPositionPath != "" {
		stores = append(stores, &checkpoint.FileStore{Path: s.cfg.MySQLPositionPath})
	}
	return &checkpoint.Layered{
		Stores:  stores,
		OnError: func(err error) { s.logger.Warnf("[MySQL] Checkpoint store: %v", err) },
	}
}

// loadCheckpoint reads the stored position, reporting nil when there is none.
func (s *MySQLSyncer) loadCheckpoint(ctx context.Context, store checkpoint.Store) (*binlogCheckpoint, error) {
	payload, err := store.Load(ctx, "")
	if err != nil {
		return nil, err
	}

	var cp binlogCheckpoint
	found, err := checkpoint.Decode(payload, &cp)
	if err != nil {
		return nil, fmt.Errorf("read the stored checkpoint: %w", err)
	}
	if !found || cp.Name == "" {
		return nil, nil
	}

	// A checkpoint written against another server is worse than none: the offset
	// would be read against data it does not describe, successfully, and the
	// task would resume from somewhere arbitrary.
	if want := dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection); cp.Source != "" && cp.Source != want {
		s.logger.Warnf("[MySQL] Ignoring a checkpoint recorded against %s: this task "+
			"reads %s, and a binlog offset means nothing on another server. The copy "+
			"will be made again.", cp.Source, want)
		return nil, nil
	}
	return &cp, nil
}

// loadBinlogPosition reports the file-and-offset pair recorded in a file, for
// callers that only need that half.
func (s *MySQLSyncer) loadBinlogPosition(path string) *mysql.Position {
	cp, err := s.loadCheckpoint(context.Background(), &checkpoint.FileStore{Path: path})
	if err != nil || cp == nil {
		return nil
	}
	pos := cp.position()
	return &pos
}

// parseAddr reports the host and port canal should dial.
//
// The driver's own parser is used rather than splitting on punctuation: "@"
// and ":" are both legal inside a password, and Cloud SQL generates passwords
// that contain them. Splitting by hand recovered the wrong credentials and the
// only symptom was an authentication failure that named neither.
func (s *MySQLSyncer) parseAddr(dsn string) string {
	// An empty DSN parses into the driver's defaults, which would have canal
	// quietly dial 127.0.0.1:3306 instead of the configured source.
	if dsn == "" {
		s.logger.Error("[MySQL] No source connection configured")
		return ""
	}
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		s.logger.Errorf("[MySQL] Invalid DSN => %v", err)
		return ""
	}
	if cfg.Net != "tcp" {
		s.logger.Errorf("[MySQL] Replication needs a tcp DSN, got net=%q", cfg.Net)
		return ""
	}
	return cfg.Addr
}

// sourceTLS reports the TLS settings the binlog connection should use.
//
// canal opens its own connection rather than going through database/sql, so the
// tls parameter in the DSN does not reach it: without this the row events would
// cross the region in the clear even when every other connection is encrypted.
// A DSN asking for "preferred" gets no TLS here, because canal has no way to
// negotiate and fall back — asking for it unconditionally would break a server
// that has no certificate.
func (s *MySQLSyncer) sourceTLS() *tls.Config {
	cfg, err := mysqldriver.ParseDSN(s.cfg.SourceConnection)
	if err != nil {
		return nil
	}
	switch strings.ToLower(cfg.TLSConfig) {
	case "true":
		host, _, splitErr := net.SplitHostPort(cfg.Addr)
		if splitErr != nil {
			host = cfg.Addr
		}
		return &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	case "skip-verify":
		return &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	}
	return nil
}

// parseUserPassword recovers the credentials canal should authenticate with.
func (s *MySQLSyncer) parseUserPassword(dsn string) (string, string) {
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		s.logger.Errorf("[MySQL] Invalid DSN => %v", err)
		return "", ""
	}
	return cfg.User, cfg.Passwd
}

func (s *MySQLSyncer) getTableColumns(ctx context.Context, db *sql.Conn, database, table string) ([]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM %s.%s", database, table)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var field, typeStr, nullStr, keyStr, defaultStr, extraStr sql.NullString
		if err := rows.Scan(&field, &typeStr, &nullStr, &keyStr, &defaultStr, &extraStr); err != nil {
			return nil, fmt.Errorf("failed to scan columns info from %s.%s: %v", database, table, err)
		}
		if field.Valid {
			cols = append(cols, field.String)
		} else {
			return nil, fmt.Errorf("invalid column name for %s.%s", database, table)
		}
	}
	return cols, nil
}

type MyEventHandler struct {
	canal.DummyEventHandler
	// mu guards targetDB and pending. Row events arrive on canal's goroutine
	// while the health check may replace the connection from its own.
	mu           sync.Mutex
	pending      []statement
	pendingLimit int
	targetDB     *sql.DB

	mappings          []config.DatabaseMapping
	logger            logrus.FieldLogger
	positionSaverPath string
	// checkpoints is where the offset is recorded. It writes to the target
	// database as well as the local file, so a syncer replaced in the other
	// region can find out where to resume from.
	checkpoints checkpoint.Store
	// lastCheckpointAt is when the offset was last recorded, and checkpointEvery
	// is how often it may be. canal reports a synced position once per source
	// transaction, and recording every one of them meant a round trip and an
	// fsync on the target for every transaction replicated: measured against
	// MySQL 8.1, that held the apply rate to 47 rows a second where the target
	// itself accepted 177. Recording less often costs nothing but a replay of
	// the interval after an unclean stop, and replaying is already safe — the
	// applied statements are idempotent, which is what makes an interrupted
	// batch recoverable at all.
	lastCheckpointAt time.Time
	checkpointEvery  time.Duration
	// pendingCheckpoint is the most recent position not yet recorded, so a clean
	// stop can record it and start from where it actually got to.
	pendingCheckpoint string

	canal         *canal.Canal
	lastExecError int32
	// keylessTables names the tables already reported as having no primary key,
	// so the warning appears once rather than once per row.
	keylessTables    map[string]bool
	TargetConnection string
	// flavor names the server dialect the recorded GTID set belongs to, so a
	// MariaDB set is not read back as a MySQL one.
	flavor string
	// source names the server the offsets belong to, without credentials.
	source string
	// labels identify this task in the metrics.
	labels metrics.Labels
	// discovering means the task listed no tables, so every table it sees is
	// replicated under its own name.
	discovering bool
	// sourceDatabase is the database on the source this task reads. DDL names a
	// database of its own, and without this there was nothing to compare it
	// against: see replicatesSchema.
	sourceDatabase string
	// sourceEventAt is when the source made the change the buffer is holding.
	// The applied lag is measured from it, which is the number a
	// disaster-recovery setup is judged on.
	sourceEventAt time.Time
	// dialect is the flavour the target speaks. The zero value is MySQL, so a
	// handler built without naming one behaves as production does.
	dialect dialect
	// sink diverts the statements this handler builds instead of buffering them
	// for its own flush. The single-stream reader sets it so that one piece of
	// conversion code serves both the old path and the pipeline.
	sink func(*statement) error
	// allowKeyless lets a table with no primary key be replicated on a
	// best-effort basis: inserts arrive, updates and deletes do not. It is off
	// by default because the two sides then drift apart silently, which is not a
	// thing payment data may do. An operator who knows what they are accepting
	// turns it on deliberately.
	allowKeyless bool
}

// flavour reports the dialect to render statements in, defaulting to MySQL.
func (h *MyEventHandler) flavour() dialect {
	if h.dialect == "" {
		return dialectMySQL
	}
	return h.dialect
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	table := e.Table
	sourceDB := table.Schema
	tableName := table.Name

	targetDBName := dsn.GetDatabaseName("mysql", h.TargetConnection)
	targets := h.targetsFor(sourceDB, tableName)
	if len(targets) == 0 {
		if !h.discovering || discovery.IsInternal(tableName) {
			h.logger.Debugf("[MySQL] No mapping found for source table %s.%s => skip event", sourceDB, tableName)
			return nil
		}
		// The task lists no tables, so everything is replicated under its own
		// name — including tables created after the task started.
		targets = []string{tableName}
	}

	columnNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		columnNames[i] = col.Name
	}

	if e.Header != nil && e.Header.Timestamp > 0 {
		at := time.Unix(int64(e.Header.Timestamp), 0)
		h.mu.Lock()
		h.sourceEventAt = at
		h.mu.Unlock()
		metrics.SetReadLag(h.labels, time.Since(at).Seconds())
	}

	var firstErr error
	fail := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Every mapping that names this table, not just the first. A table listed
	// twice — the way a fan-out to two targets is spelled — used to stop at the
	// first match, so the second target silently received nothing.
	// build renders one statement and hands it on, keeping the first failure.
	build := func(op, targetTable string, newRow, oldRow []interface{}) {
		stmt, err := h.buildStatement(op, targetDBName, targetTable, columnNames, table, newRow, oldRow)
		if err != nil {
			fail(err)
			return
		}
		fail(h.enqueue(stmt))
	}

	for _, targetTableName := range targets {
		switch e.Action {
		case canal.InsertAction:
			for _, row := range e.Rows {
				build("INSERT", targetTableName, row, nil)
			}
		case canal.UpdateAction:
			// An update event carries the rows in before/after pairs. Reading
			// e.Rows[i+1] on trust panicked on an odd count, and a panic in the
			// canal callback takes the process with it.
			if len(e.Rows)%2 != 0 {
				return fmt.Errorf("an update event for %s.%s carries %d rows, which "+
					"is not a whole number of before/after pairs", sourceDB, tableName, len(e.Rows))
			}
			for i := 0; i+1 < len(e.Rows); i += 2 {
				build("UPDATE", targetTableName, e.Rows[i+1], e.Rows[i])
			}
		case canal.DeleteAction:
			for _, row := range e.Rows {
				build("DELETE", targetTableName, row, nil)
			}
		default:
			// This used to be a warning, so an action the library grew later was
			// dropped with a log line nobody reads and the two sides diverged.
			// A change this does not understand is a change it must not claim to
			// have replicated.
			return domain.Unrecoverable(
				"a %q event on %s.%s is not one this knows how to replicate; it knows "+
					"inserts, updates and deletes. Replication has stopped rather than "+
					"skip a change", e.Action, sourceDB, tableName)
		}
	}
	return firstErr
}

// targetsFor reports the target tables one source table is replicated to.
//
// The lookup used to compare the table name alone and to stop at the first
// match. A task reading from more than one source database therefore sent both
// databases' "orders" to whichever target was listed first — the schema was read
// off the event and then only used for logging.
func (h *MyEventHandler) targetsFor(sourceDB, sourceTable string) []string {
	var targets []string

	for _, mapping := range h.mappings {
		// A mapping that does not name a source database matches any of them,
		// which is what a single-database task looks like.
		if mapping.SourceDatabase != "" && !strings.EqualFold(mapping.SourceDatabase, sourceDB) {
			continue
		}
		for _, tableMap := range mapping.Tables {
			if strings.EqualFold(tableMap.SourceTable, sourceTable) {
				target := tableMap.TargetTable
				if target == "" {
					target = tableMap.SourceTable
				}
				targets = append(targets, target)
			}
		}
	}
	return targets
}

// statement is one DML the target has to run, already rendered with its
// arguments, in the order the source produced it.
type statement struct {
	query string
	args  []interface{}
}

// maxPendingStatements caps the transaction buffer. A source transaction larger
// than this is split across more than one target transaction: atomicity is lost
// for that transaction and the split is logged, but the alternative is holding
// an unbounded stretch of the binlog in memory.
const maxPendingStatements = 10000

// buildStatement renders one row event, or reports nil when the event has
// nothing the target can apply — an update or delete on a table with no primary
// key, which cannot be addressed on the target at all.
func (h *MyEventHandler) buildStatement(
	opType, tgtDB, tgtTable string,
	cols []string,
	table *schema.Table,
	newRow []interface{},
	oldRow []interface{},
) (*statement, error) {
	tableSecurity := security.FindTableSecurityFromMappings(tgtTable, h.mappings)
	secured := tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0

	// process applies the field security policy to a row, leaving it alone when
	// the table has none.
	process := func(row []interface{}) []interface{} {
		if !secured {
			return row
		}
		out := make([]interface{}, len(row))
		for i, val := range row {
			if i < len(cols) {
				out[i] = security.ProcessValue(val, cols[i], tableSecurity)
			} else {
				out[i] = val
			}
		}
		return out
	}

	switch opType {
	case "INSERT":
		return &statement{
			query: upsertStatement(h.flavour(), tgtDB, tgtTable, cols, 1),
			args:  process(newRow),
		}, nil

	case "UPDATE":
		if len(table.PKColumns) == 0 {
			if !h.allowKeyless {
				return nil, h.refuseKeyless(table.Schema, table.Name)
			}
			h.warnAboutMissingKey(tgtDB, tgtTable)
			return nil, nil
		}
		setClauses := make([]string, len(cols))
		for i, colName := range cols {
			setClauses[i] = fmt.Sprintf("%s = ?", colName)
		}
		var whereClauses []string
		args := process(newRow)
		for _, pkIndex := range table.PKColumns {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", cols[pkIndex]))
			args = append(args, oldRow[pkIndex])
		}
		return &statement{
			query: fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
				tgtDB, tgtTable,
				strings.Join(setClauses, ", "),
				strings.Join(whereClauses, " AND ")),
			args: args,
		}, nil

	case "DELETE":
		if len(table.PKColumns) == 0 {
			if !h.allowKeyless {
				return nil, h.refuseKeyless(table.Schema, table.Name)
			}
			h.warnAboutMissingKey(tgtDB, tgtTable)
			return nil, nil
		}
		var whereClauses []string
		var args []interface{}
		for _, pkIndex := range table.PKColumns {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", cols[pkIndex]))
			args = append(args, newRow[pkIndex])
		}
		return &statement{
			query: fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
				tgtDB, tgtTable, strings.Join(whereClauses, " AND ")),
			args: args,
		}, nil
	}
	return nil, nil
}

// refuseKeyless stops replication for a table whose rows cannot be addressed.
//
// Carrying on replicates the inserts and drops the updates and the deletes, so
// the target accumulates rows the source has since changed or removed — and
// nothing downstream can tell. The row counts even agree for a while. MySQL's
// own answer to this is sql_require_primary_key, and Group Replication refuses
// such a table outright; for payment data that is the right severity.
func (h *MyEventHandler) refuseKeyless(db, table string) error {
	return domain.Unrecoverable(
		"%s.%s has no primary key, so its updates and deletes cannot be addressed on "+
			"the target and the two sides would drift apart with nothing to show it. "+
			"Add a primary key to the table. If a partial copy is genuinely acceptable, "+
			"set allowKeyless on the task to replicate its inserts only",
		db, table)
}

// warnAboutMissingKey reports a table whose rows cannot be addressed on the
// target, once rather than once per event.
//
// Skipping the update or the delete is right — the alternative is matching on
// every column and rewriting whatever happens to look the same — but it means
// the table is only half replicated: inserts arrive and nothing else does, so
// the target accumulates rows the source has changed or removed. That is worth
// one clear line, not a debug entry per row.
func (h *MyEventHandler) warnAboutMissingKey(db, table string) {
	name := db + "." + table

	h.mu.Lock()
	if h.keylessTables == nil {
		h.keylessTables = map[string]bool{}
	}
	warned := h.keylessTables[name]
	h.keylessTables[name] = true
	h.mu.Unlock()

	if warned {
		return
	}
	h.logger.Warnf("[MySQL] %s has no primary key, so its updates and deletes "+
		"cannot be addressed on the target. Only inserts are being replicated, and "+
		"the two sides will drift apart until it has one.", name)
}

// enqueue adds a statement to the open transaction, flushing early when the
// buffer has grown past what is safe to hold.
func (h *MyEventHandler) enqueue(stmt *statement) error {
	if stmt == nil {
		return nil
	}
	if h.sink != nil {
		return h.sink(stmt)
	}

	h.mu.Lock()
	h.pending = append(h.pending, *stmt)
	overflow := len(h.pending) >= h.maxPending()
	h.mu.Unlock()

	if !overflow {
		return nil
	}
	h.logger.Warnf("[MySQL] Source transaction exceeds %d statements; applying it "+
		"in more than one target transaction", h.maxPending())
	return h.flush()
}

// maxPending reports the buffer cap. The zero value means the package default;
// the field exists so a test can reach the cap without building ten thousand
// statements.
func (h *MyEventHandler) maxPending() int {
	if h.pendingLimit > 0 {
		return h.pendingLimit
	}
	return maxPendingStatements
}

// flush applies the buffered statements as one transaction on the target.
//
// Either every statement of a source transaction lands or none of it does. A
// reader on the target can otherwise observe a state that never existed at the
// source, which for a payment ledger means a debit without its matching credit.
func (h *MyEventHandler) flush() error {
	h.mu.Lock()
	pending := h.pending
	h.pending = nil
	db := h.targetDB
	eventAt := h.sourceEventAt
	h.mu.Unlock()

	if len(pending) == 0 {
		return nil
	}
	if db == nil {
		return fmt.Errorf("apply %d statements: no target connection", len(pending))
	}

	err := resilience.RetryDBOperation(context.Background(), h.logger,
		fmt.Sprintf("apply %d statements", len(pending)),
		func() error {
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			for _, stmt := range pending {
				if _, err := tx.Exec(stmt.query, stmt.args...); err != nil {
					_ = tx.Rollback()
					return fmt.Errorf("%s: %w", stmt.query, err)
				}
			}
			return tx.Commit()
		})

	if err != nil {
		h.logger.Errorf("[MySQL] Failed to apply a source transaction of %d "+
			"statements: %v", len(pending), err)
		atomic.StoreInt32(&h.lastExecError, 1)
		metrics.Failed(h.labels, len(pending))
		return fmt.Errorf("apply source transaction: %w", err)
	}

	metrics.Applied(h.labels, len(pending))
	if !eventAt.IsZero() {
		metrics.SetLag(h.labels, time.Since(eventAt).Seconds())
	}
	h.logger.Debugf("[MySQL] Applied a source transaction of %d statements", len(pending))
	return nil
}

// defaultCheckpointInterval is how often the binlog position is recorded.
//
// canal reports a synced position once per source transaction. Recording each
// one costs a round trip and an fsync on the target per transaction replicated,
// which is most of the cost of replicating a payment ledger where every payment
// is its own transaction. What the interval buys back is bounded: after an
// unclean stop, replication replays at most this much, and replaying is safe
// because the statements are idempotent.
const defaultCheckpointInterval = 200 * time.Millisecond

// checkpointInterval reports the interval, which SYNC_MYSQL_CHECKPOINT_INTERVAL
// overrides with any duration Go can parse. Zero records every position, which
// is the old behaviour and is available for anyone who wants it.
func checkpointInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv("SYNC_MYSQL_CHECKPOINT_INTERVAL"))
	if raw == "" {
		return defaultCheckpointInterval
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed < 0 {
		return defaultCheckpointInterval
	}
	return parsed
}

// OnXID marks the end of a source transaction, which is where the buffered
// statements are applied.
func (h *MyEventHandler) OnXID(*replication.EventHeader, mysql.Position) error {
	return h.flush()
}

// setTargetDB swaps the connection the handler writes through. The health check
// reconnects from its own goroutine, so the field needs the same lock the
// statement buffer uses.
func (h *MyEventHandler) setTargetDB(db *sql.DB) {
	h.mu.Lock()
	h.targetDB = db
	h.mu.Unlock()
}

func (h *MyEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// A source that never sends XID events — a non-transactional engine, or a
	// stream that stops mid-transaction — would otherwise leave rows buffered
	// indefinitely. This is the periodic checkpoint, so drain here too. The
	// failure is already recorded on the handler, and the guard below keeps the
	// offset where it is.
	_ = h.flush()

	if h.checkpoints == nil {
		return nil
	}

	// The offset is only meaningful if everything before it reached the target.
	// Once a statement has failed the flag stays raised for the life of the
	// handler, so the stored position never moves past the loss and a restart
	// replays from the last offset that was fully applied.
	if atomic.LoadInt32(&h.lastExecError) != 0 {
		h.logger.Warnf("[MySQL] Not writing position %v: an earlier statement "+
			"failed to apply, so replication must resume from the stored offset", pos)
		return nil
	}

	h.logger.Debugf("[MySQL] Syncing position: %v, force: %v", pos, force)

	cp := binlogCheckpoint{Name: pos.Name, Pos: pos.Pos, Source: h.source}
	if set != nil {
		cp.GTID = set.String()
		cp.Flavor = h.flavor
	}

	payload, err := checkpoint.Encode(cp)
	if err != nil {
		h.logger.Errorf("[MySQL] Failed to marshal position: %v", err)
		return err
	}

	// force means canal is at a boundary it wants recorded — a rotate, or a
	// stop — and those are rare enough to honour immediately.
	if !force && h.checkpointEvery > 0 && time.Since(h.lastCheckpointAt) < h.checkpointEvery {
		h.pendingCheckpoint = payload
		return nil
	}
	if err := h.recordCheckpoint(payload); err != nil {
		return err
	}

	h.logger.Debugf("[MySQL] Recorded binlog position: %v", pos)
	return nil
}

// recordCheckpoint writes one position and remembers when.
func (h *MyEventHandler) recordCheckpoint(payload string) error {
	if err := h.checkpoints.Save(context.Background(), "", payload); err != nil {
		h.logger.Errorf("[MySQL] Failed to record the position: %v", err)
		return err
	}
	h.lastCheckpointAt = time.Now()
	h.pendingCheckpoint = ""
	return nil
}

// recordPendingCheckpoint writes the position the throttle was holding back.
//
// It runs on a clean stop, so an orderly restart resumes from where replication
// actually got to rather than replaying the last interval. An unclean stop
// replays it, which is safe and is the whole reason the interval is affordable.
func (h *MyEventHandler) recordPendingCheckpoint() {
	if h.checkpoints == nil || h.pendingCheckpoint == "" {
		return
	}
	if atomic.LoadInt32(&h.lastExecError) != 0 {
		return
	}
	if err := h.recordCheckpoint(h.pendingCheckpoint); err != nil {
		h.logger.Warnf("[MySQL] Could not record the final position: %v", err)
	}
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

// claimDirection records which way this task replicates, on both databases, and
// keeps the claims refreshed for as long as it runs.
//
// The returned function stops the refresh. A failure here stops the task: the
// direction not being agreed is exactly the situation where carrying on
// destroys data.
func (s *MySQLSyncer) claimDirection(ctx context.Context, targetDB *sql.DB) (func(), error) {
	sourceDB, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		return nil, fmt.Errorf("open the source to claim the replication direction: %w", err)
	}

	guard := &directionlock.Guard{
		TaskID: s.cfg.ID,
		Source: &directionlock.SQLStore{
			DB:      sourceDB,
			Schema:  dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection),
			Address: dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		},
		Target: &directionlock.SQLStore{
			DB:      targetDB,
			Schema:  dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection),
			Address: dsn.Endpoint(s.cfg.Type, s.cfg.TargetConnection),
		},
	}

	release, err := directionlock.Hold(ctx, guard, s.logger, "MySQL")
	if err != nil {
		_ = sourceDB.Close()
		return nil, err
	}
	// The connection opened for the claim is this function's own, so it closes
	// with the claim rather than living as long as the syncer.
	return func() {
		release()
		_ = sourceDB.Close()
	}, nil
}

// metricLabels identify this task in the metrics. The endpoints are named
// without their credentials, because the exposition is scraped and stored.
func (s *MySQLSyncer) metricLabels() metrics.Labels {
	return metrics.Labels{
		"task":   strconv.Itoa(s.cfg.ID),
		"engine": s.cfg.Type,
		"source": dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		"target": dsn.Endpoint(s.cfg.Type, s.cfg.TargetConnection),
	}
}

// hasConfiguredTables reports whether the task names any table at all. The
// configuration loader inserts a mapping with an empty table list for a task
// that has none, so the check has to look past the mapping itself.
func (s *MySQLSyncer) hasConfiguredTables() bool {
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable != "" {
				return true
			}
		}
	}
	return false
}

// unlistedScanEvery is how often a task that names its tables is compared
// against what the source actually holds.
const unlistedScanEvery = 5 * time.Minute

// warnAboutUnlistedTables reports the tables the source has and this task does
// not replicate.
//
// It does not start replicating them: a task that names its tables means it, and
// quietly widening the scope would be worse than the gap. What it does is make
// the gap visible, because the alternative is finding out during a failover that
// the copy is missing a table nobody added to the task.
func (s *MySQLSyncer) warnAboutUnlistedTables(ctx context.Context, sourceDBName string) {
	listed := map[string]bool{}
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable != "" {
				listed[strings.ToLower(table.SourceTable)] = true
			}
		}
	}

	source, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Debugf("[MySQL] Could not open the source to check which tables it "+
			"holds: %v", err)
		return
	}
	defer source.Close()

	warned := map[string]bool{}
	scan := func() {
		tables, err := discovery.MySQLTables(ctx, source, sourceDBName)
		if err != nil {
			s.logger.Debugf("[MySQL] Could not list the tables in %s: %v", sourceDBName, err)
			return
		}
		missing := discovery.Unlisted(listed, warned, tables)
		if len(missing) == 0 {
			return
		}
		s.logger.Warnf("[MySQL] %s holds %d tables this task does not replicate: %v. "+
			"They are not in the disaster-recovery copy. Add them to the task, or "+
			"remove every table from it to replicate the database as a whole.",
			sourceDBName, len(missing), missing)
		metrics.SetUnreplicated(s.metricLabels(), float64(len(warned)))
	}

	discovery.Poll(ctx, unlistedScanEvery, scan)
}

// resolveMappings reports the tables to copy, discovering them from the source
// when the task lists none.
func (s *MySQLSyncer) resolveMappings(ctx context.Context, conn *sql.Conn, sourceDBName string) []config.DatabaseMapping {
	if s.hasConfiguredTables() {
		return s.cfg.Mappings
	}

	tables, err := discovery.MySQLTables(ctx, conn, sourceDBName)
	if err != nil {
		s.logger.Errorf("[MySQL] Could not discover the tables in %s, so the initial "+
			"copy has nothing to do: %v", sourceDBName, err)
		return nil
	}

	mapped := make([]config.TableMapping, 0, len(tables))
	for _, table := range tables {
		mapped = append(mapped, config.TableMapping{SourceTable: table, TargetTable: table})
	}
	s.logger.Infof("[MySQL] Discovered %d tables in %s", len(mapped), sourceDBName)
	return []config.DatabaseMapping{{Tables: mapped}}
}

// checkRowImage refuses a source whose binlog does not carry whole rows.
//
// canal checks binlog_format for itself, but not binlog_row_image: that check is
// an exported method it never calls. MariaDB has no such setting and always logs
// the whole row, so it is only asked of MySQL.
func (s *MySQLSyncer) checkRowImage(c *canal.Canal) error {
	if strings.EqualFold(s.cfg.Type, "mariadb") {
		return nil
	}

	result, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'`)
	if err != nil {
		return fmt.Errorf("read binlog_row_image: %w", err)
	}
	image, _ := result.GetString(0, 1)
	return requireFullRowImage(image)
}

// requireFullRowImage reports why a binlog row image cannot be replicated from.
//
// With anything other than FULL the binlog carries only the columns that changed
// plus the primary key, and the driver fills the rest of the row with nils. The
// UPDATE this syncer builds sets every column, so those nils are written as NULL
// over values that never changed — silently, with the target looking perfectly
// healthy and the row counts matching.
//
// There is no way to tell such a nil from a column that really is NULL, so this
// cannot be worked around by writing fewer columns. It has to be refused.
func requireFullRowImage(image string) error {
	switch {
	case image == "":
		// Servers before 5.6 have no such setting and always log whole rows.
		return nil
	case strings.EqualFold(image, "FULL"):
		return nil
	default:
		return fmt.Errorf("the source logs %s binlog row images, which carry only the "+
			"columns that changed. Every other column would be written to the target "+
			"as NULL, with nothing to show that it happened. Set binlog_row_image=FULL "+
			"on the source", image)
	}
}
