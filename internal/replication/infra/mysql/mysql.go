package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"github.com/sirupsen/logrus"
)

type MySQLSyncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger
	// dialect is the flavour the target speaks; the zero value is MySQL.
	dialect dialect
}

func (s *MySQLSyncer) flavour() dialect {
	if s.dialect == "" {
		return dialectMySQL
	}
	return s.dialect
}

// positionNoLongerAvailable reports whether an error says the source has
// discarded the binlog this task would resume from. Cloud SQL expires binary
// logs on a schedule, so a task stopped for longer comes back to find its
// offset gone. Retrying cannot help — the bytes are not there — and what is
// needed is a fresh copy, which somebody has to decide to make.
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

// defaultCopyBatch is how many rows move per round trip when a deployment has
// not said otherwise.
const defaultCopyBatch = 100

// doInitialSync copies every mapped table, reporting what it could not copy.
// Every failure here used to be logged and stepped over, and the caller then
// recorded the position as though the copy had finished.
func (s *MySQLSyncer) doInitialSync(ctx context.Context, sourceDB *sql.Conn, targetDB *sql.DB) error {
	s.logger.Info("[MySQL] Starting the initial full sync...")

	batchSize := pipeline.CopyBatch(defaultCopyBatch)
	var failures []string
	fail := func(format string, args ...interface{}) {
		failures = append(failures, fmt.Sprintf(format, args...))
	}
	sourceDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	targetDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection)

	mappings, err := s.resolveMappings(ctx, sourceDB, sourceDBName)
	if err != nil {
		return err
	}
	remainingTables := 0
	for _, mapping := range mappings {
		remainingTables += len(mapping.Tables)
	}
	copyStarted := time.Now()
	metrics.SnapshotStarted(s.metricLabels(), remainingTables)

	for _, mapping := range mappings {
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

			// A target that already holds rows is not evidence the copy finished:
			// an interrupted copy leaves exactly that, and skipping on it left the
			// rows the copy had not reached missing for good -- the stream starts
			// after them, so nothing fills the gap.
			//
			// Whether a copy is owed at all is decided once, from the position
			// stored on the target, in Runner.startingPoint. By the time this runs
			// that decision has been made; second-guessing it here could only
			// overrule it wrongly. The copy is made of upserts, so re-reading rows
			// it already wrote costs time and nothing else.

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
						// Debezium: RowsScanned. Reported per batch rather than
						// per table, so a copy of one very large table still
						// shows movement instead of looking hung.
						metrics.SnapshotProgress(s.metricLabels(), len(batchRows),
							remainingTables, time.Since(copyStarted).Seconds())
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
			remainingTables--
			metrics.SnapshotProgress(s.metricLabels(), 0, remainingTables,
				time.Since(copyStarted).Seconds())
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

// upsertStatement renders an idempotent multi-row insert for d. Replication is
// at-least-once: the binlog position is persisted periodically, so a restart
// replays whatever came after the last write.
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

// binlogCheckpoint is what the position file holds. The file-and-offset pair
// only means something on the server that produced it.
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

// generatedColumn reports whether SHOW COLUMNS says the server computes this
// column rather than storing what it is given.
//
// DEFAULT_GENERATED is not that: it marks a default expression, and the column
// still takes a value, so the word alone cannot be the test. MariaDB spells the
// same property VIRTUAL and PERSISTENT.
func generatedColumn(extra string) bool {
	rest := strings.ToUpper(strings.TrimSpace(extra))
	rest = strings.ReplaceAll(rest, "DEFAULT_GENERATED", "")
	return strings.Contains(rest, "GENERATED") ||
		strings.Contains(rest, "VIRTUAL") ||
		strings.Contains(rest, "PERSISTENT")
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
		if !field.Valid {
			return nil, fmt.Errorf("invalid column name for %s.%s", database, table)
		}
		// A generated column is computed by the server, which refuses a write that
		// supplies one. Sending every column the source had made any table holding
		// one impossible to copy at all: the whole table failed, and with it every
		// table whose foreign key pointed at it.
		if generatedColumn(extraStr.String) {
			continue
		}
		cols = append(cols, field.String)
	}
	return cols, nil
}

// MyEventHandler renders binlog row events into target statements. It was
// canal's event handler as well, which is why it embedded DummyEventHandler;
// Reader is the handler now, so the embedding is gone — with it in place a call
// to a callback this type no longer implements resolved to canal's no-op
// instead of failing to compile.
type MyEventHandler struct {
	// mu guards keylessTables, written from canal's goroutine.
	mu sync.Mutex

	mappings []config.DatabaseMapping
	logger   logrus.FieldLogger
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
	// dialect is the flavour the target speaks. The zero value is MySQL, so a
	// handler built without naming one behaves as production does.
	dialect dialect
	// sink takes the statements this handler renders. It is the only way out:
	// the handler used to buffer and apply them itself, which is the pipeline's
	// job now.
	sink func(*statement) error
	// allowKeyless lets a table with no primary key be replicated on a
	// best-effort basis: inserts arrive, updates and deletes do not. It is off
	// by default because the two sides then drift apart silently, which is not a
	// thing payment data may do.
	allowKeyless bool
}

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

	// The columns a statement may set, by their index in the full row. A
	// generated column is computed by the server and refuses a write, so it is
	// left out of the column list and out of the arguments -- while the indexes
	// stay whole, because the primary key is addressed by position in the full
	// row and renumbering it here would address the wrong column.
	writable := writableColumns(table)
	writableCols := pick(cols, writable)

	switch opType {
	case "INSERT":
		return &statement{
			query: upsertStatement(h.flavour(), tgtDB, tgtTable, writableCols, 1),
			args:  pick(process(newRow), writable),
		}, nil

	case "UPDATE":
		if len(table.PKColumns) == 0 {
			if !h.allowKeyless {
				return nil, h.refuseKeyless(table.Schema, table.Name)
			}
			h.warnAboutMissingKey(tgtDB, tgtTable)
			return nil, nil
		}
		setClauses := make([]string, len(writableCols))
		for i, colName := range writableCols {
			setClauses[i] = fmt.Sprintf("%s = ?", colName)
		}
		var whereClauses []string
		args := pick(process(newRow), writable)
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

// writableColumns lists the columns of a table that a statement may set, by
// their index in the row the binlog carries.
func writableColumns(table *schema.Table) []int {
	keep := make([]int, 0, len(table.Columns))
	for i, col := range table.Columns {
		if col.IsVirtual || col.IsStored {
			continue
		}
		keep = append(keep, i)
	}
	return keep
}

// pick projects a column list or a row onto the given indexes.
func pick[T any](all []T, indexes []int) []T {
	out := make([]T, 0, len(indexes))
	for _, i := range indexes {
		if i < len(all) {
			out = append(out, all[i])
		}
	}
	return out
}

// refuseKeyless stops replication for a table whose rows cannot be addressed.
// Carrying on replicates the inserts and drops the updates and the deletes, so
// the target accumulates rows the source has since changed or removed — and
// nothing downstream can tell.
func (h *MyEventHandler) refuseKeyless(db, table string) error {
	return domain.Unrecoverable(
		"%s.%s has no primary key, so its updates and deletes cannot be addressed on "+
			"the target and the two sides would drift apart with nothing to show it. "+
			"Add a primary key to the table. If a partial copy is genuinely acceptable, "+
			"set allowKeyless on the task to replicate its inserts only",
		db, table)
}

// warnAboutMissingKey reports a table whose rows cannot be addressed on the
// target, once rather than once per event. Skipping the update or the delete
// is right — the alternative is matching on every column and rewriting
// whatever happens to look the same — but it means the table is only half
// replicated: inserts arrive and nothing else does, so the target accumulates
// rows the source has changed or removed.
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
// enqueue hands a rendered statement to whoever asked for it. The sink is the
// only path: this used to buffer and apply the statements itself, which is what
// the pipeline's applier does now.
func (h *MyEventHandler) enqueue(stmt *statement) error {
	if stmt == nil {
		return nil
	}
	if h.sink == nil {
		return fmt.Errorf("no sink is set, so the statement for %s would be dropped",
			h.sourceDatabase)
	}
	return h.sink(stmt)
}

// defaultCheckpointInterval is how often the binlog position is recorded.
// canal reports a synced position once per source transaction. Recording each
// one costs a round trip and an fsync on the target per transaction
// replicated, which is most of the cost of replicating a payment ledger where
// every payment is its own transaction.
const defaultCheckpointInterval = 200 * time.Millisecond

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
	// Task and engine, and nothing else. The endpoints used to be labels here,
	// which put this syncer's snapshot series beside the pipeline's rather than
	// on it: two series per metric per task, the pipeline's left holding the
	// zero it starts from, and a dashboard querying by task getting both. The
	// endpoints belong on the info series, where a slow string costs one sample
	// instead of multiplying every series that carries it.
	return metrics.Labels{
		"task":   strconv.Itoa(s.cfg.ID),
		"engine": s.cfg.Type,
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

// resolveMappings reports the tables to copy, discovering them from the source
// when the task lists none.
func (s *MySQLSyncer) resolveMappings(ctx context.Context, conn *sql.Conn, sourceDBName string) ([]config.DatabaseMapping, error) {
	if s.hasConfiguredTables() {
		return s.cfg.Mappings, nil
	}

	// Reported rather than logged. An empty list here is indistinguishable from
	// a database with no tables: the copy did nothing, said it had succeeded,
	// and the position was stored -- so the stream started after data that had
	// never been copied and nothing would ever fill it in.
	tables, err := discovery.MySQLTables(ctx, conn, sourceDBName)
	if err != nil {
		return nil, fmt.Errorf("discover the tables in %s: %w", sourceDBName, err)
	}

	mapped := make([]config.TableMapping, 0, len(tables))
	for _, table := range tables {
		mapped = append(mapped, config.TableMapping{SourceTable: table, TargetTable: table})
	}
	s.logger.Infof("[MySQL] Discovered %d tables in %s", len(mapped), sourceDBName)
	return []config.DatabaseMapping{{Tables: mapped}}, nil
}

// requireFullRowImage reports why a binlog row image cannot be replicated
// from. With anything other than FULL the binlog carries only the columns that
// changed plus the primary key, and the driver fills the rest of the row with
// nils.
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
