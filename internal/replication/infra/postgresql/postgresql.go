package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/lib/pq"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"github.com/sirupsen/logrus"
)

type replicationState struct {
	inStream        bool
	processMessages bool
	relations       map[uint32]*pglogrepl.RelationMessageV2

	lastReceivedLSN pglogrepl.LSN
	currentTxLSN    pglogrepl.LSN
	lastWrittenLSN  pglogrepl.LSN

	replicaConn *sql.DB
}

type PostgreSQLSyncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger

	sourceConnNormal *pgx.Conn
	sourceConnRepl   *pgconn.PgConn
	targetDB         *sql.DB

	repSlot          string
	outputPlugin     string
	publicationNames string
	currentLsn       pglogrepl.LSN

	state replicationState

	lastExecError int32

	// checkpoints is where the replication position is recorded. It writes to
	// the target database as well as the local file, so a syncer replaced in the
	// other region can find out where to resume from.
	checkpoints checkpoint.Store
}

func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *PostgreSQLSyncer {
	return &PostgreSQLSyncer{
		cfg:    cfg,
		logger: logger.WithField("sync_task_id", cfg.ID),
	}
}

// Start begins the synchronization process
// Start replicates until the context is cancelled, or until it cannot carry on.
//
// The returned error is what the supervisor decides on: nil or a transient
// failure means try again, an ErrUnrecoverable means stop and tell somebody.
func (s *PostgreSQLSyncer) Start(ctx context.Context) error {
	var err error

	if err := security.CheckKeyForMappings(s.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	s.logger.Info("[PostgreSQL] Starting synchronization...")

	// Connect normal
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		s.sourceConnNormal, connErr = pgx.Connect(ctx, s.cfg.SourceConnection)
		return connErr
	})
	if err != nil {
		return fmt.Errorf("connect to the source: %w", err)
	}
	defer s.sourceConnNormal.Close(ctx)

	// Connect replication
	replDSN, err := s.buildReplicationDSN(s.cfg.SourceConnection)
	if err != nil {
		return domain.Unrecoverable("build the replication DSN: %v", err)
	}
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		s.sourceConnRepl, connErr = pgconn.Connect(ctx, replDSN)
		return connErr
	})
	if err != nil {
		return fmt.Errorf("open a replication connection to the source: %w", err)
	}
	defer s.sourceConnRepl.Close(ctx)

	// Connect target
	s.targetDB, err = sql.Open("postgres", s.cfg.TargetConnection)
	if err != nil {
		return fmt.Errorf("open the target: %w", err)
	}
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		return s.targetDB.PingContext(ctx)
	})
	if err != nil {
		return fmt.Errorf("connect to the target: %w", err)
	}
	defer s.targetDB.Close()

	// Nothing is read or written until the direction is agreed. A target that
	// has been promoted, or a source that is itself somebody's target, means the
	// pair has been reversed under us and carrying on would overwrite the newer
	// side with the older one.
	releaseGuard, guardErr := s.claimDirection(ctx)
	if guardErr != nil {
		if directionlock.IsBlocking(guardErr) {
			return domain.Unrecoverable("%v", guardErr)
		}
		// Anything else is transient and the task is restarted for it: another
		// process still finishing its shutdown, or an endpoint that is briefly
		// unreachable — the guard reads its claims from the databases, so an
		// outage on either side fails it while the outage lasts.
		return fmt.Errorf("%w", guardErr)
	}
	defer releaseGuard()

	s.checkpoints = s.checkpointStore()
	metrics.SetTaskInfo(s.metricLabels(),
		dsn.Endpoint("postgresql", s.cfg.SourceConnection),
		dsn.Endpoint("postgresql", s.cfg.TargetConnection))
	metrics.SetTaskUp(s.metricLabels(), true)
	defer metrics.SetTaskUp(s.metricLabels(), false)

	s.repSlot = s.cfg.PGReplicationSlot()
	s.outputPlugin = s.cfg.PGPlugin()
	s.publicationNames = s.cfg.PGPublicationNames
	if s.repSlot == "" || s.outputPlugin == "" {
		return domain.Unrecoverable("this task specifies no pg_replication_slot or " +
			"pg_plugin, so there is nothing to read changes from")
	}

	s.state = replicationState{
		inStream:        false,
		processMessages: false,
		relations:       make(map[uint32]*pglogrepl.RelationMessageV2),
		lastReceivedLSN: 0,
		currentTxLSN:    0,
		lastWrittenLSN:  0,
		replicaConn:     s.targetDB,
	}
	atomic.StoreInt32(&s.lastExecError, 0)

	err = s.ensureReplicationSlot(ctx)
	if err != nil {
		return fmt.Errorf("prepare the replication slot: %w", err)
	}

	if stored, errLoad := s.loadStoredLSN(ctx); errLoad != nil {
		return fmt.Errorf("read the stored replication position: %w", errLoad)
	} else if stored > 0 {
		s.logger.Infof("[PostgreSQL] Resuming from LSN %X", stored)
		s.currentLsn = stored
		s.state.lastWrittenLSN = stored
	}

	if err := s.prepareTargetSchema(ctx); err != nil {
		s.logger.Warnf("[PostgreSQL] prepareTargetSchema error: %v", err)
	}

	err = s.doInitialSync(ctx)
	if err != nil {
		return fmt.Errorf("make the initial copy: %w", err)
	}
	s.logger.Info("[PostgreSQL] Initial full sync done.")

	err = s.startLogicalReplication(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("read the replication stream: %w", err)
	}

	s.logger.Info("[PostgreSQL] Synchronization tasks completed.")
	return nil
}

// buildReplicationDSN constructs replication DSN buildReplicationDSN turns the
// task's connection string into one that opens a replication connection. libpq
// accepts two forms, and only the URL one used to be handled: the keyword form
// ("host=x dbname=y") parses as a relative path rather than failing, so the
// replication parameter was appended as a query string onto something that has
// no query string and the connection was refused with an error naming neither.
func (s *PostgreSQLSyncer) buildReplicationDSN(normalDSN string) (string, error) {
	trimmed := strings.TrimSpace(normalDSN)
	if trimmed == "" {
		return "", fmt.Errorf("the source connection string is empty")
	}

	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		if !strings.Contains(trimmed, "=") {
			return "", fmt.Errorf("%q is neither a postgres:// URL nor a keyword "+
				"connection string", normalDSN)
		}
		return replicationKeywords(trimmed), nil
	}

	u, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// replicationKeywords sets replication=database in a keyword connection string,
// replacing any value already there.
func replicationKeywords(dsn string) string {
	var kept []string
	for _, field := range strings.Fields(dsn) {
		if key, _, found := strings.Cut(field, "="); found && key == "replication" {
			continue
		}
		kept = append(kept, field)
	}
	return strings.Join(append(kept, "replication=database"), " ")
}

func (s *PostgreSQLSyncer) ensureReplicationSlot(ctx context.Context) error {
	info, err := pglogrepl.IdentifySystem(ctx, s.sourceConnRepl)
	if err != nil {
		return fmt.Errorf("IdentifySystem failed: %w", err)
	}
	s.logger.Infof("[PostgreSQL] IdentifySystem => systemID=%s, timeline=%d, xLogPos=%X",
		info.SystemID, info.Timeline, info.XLogPos)

	slot, err := pglogrepl.CreateReplicationSlot(
		ctx, s.sourceConnRepl, s.repSlot, s.outputPlugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false},
	)
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("CreateReplicationSlot failed: %w", err)
		}
		s.logger.Infof("[PostgreSQL] Replication slot %s already exists, will use existing slot.", s.repSlot)
		// Deliberately not info.XLogPos. That is where the server is writing *now*,
		// and this branch is the restart case: setting it here told the server to
		// stream from the present moment, so everything committed while the syncer
		// was down was skipped and no later message ever carried it.
		return nil
	}
	lsn, err2 := pglogrepl.ParseLSN(slot.ConsistentPoint)
	if err2 != nil {
		return fmt.Errorf("ParseLSN => %w", err2)
	}
	s.logger.Infof("[PostgreSQL] Created replication slot %s at LSN %X", s.repSlot, lsn)
	if s.currentLsn == 0 {
		s.currentLsn = lsn
	}
	return nil
}

func (s *PostgreSQLSyncer) prepareTargetSchema(ctx context.Context) error {
	for _, dbmap := range s.cfg.Mappings {
		srcSchema := dbmap.SourceSchema
		if srcSchema == "" {
			srcSchema = "public"
		}
		tgtSchema := dbmap.TargetSchema
		if tgtSchema == "" {
			tgtSchema = "public"
		}
		if len(dbmap.Tables) > 0 {
			for _, tbl := range dbmap.Tables {
				exist, errCheck := s.checkTableExist(ctx, tgtSchema, tbl.TargetTable)
				if errCheck != nil {
					s.logger.Warnf("[PostgreSQL] check table exist error: %v", errCheck)
					continue
				}
				if !exist {
					createSQL, seqs, errGen := s.generateCreateTableSQL(ctx, srcSchema, tbl.SourceTable, tgtSchema, tbl.TargetTable)
					if errGen != nil {
						s.logger.Warnf("[PostgreSQL] generateCreateTableSQL fail => %v", errGen)
						continue
					}
					for _, seqSQL := range seqs {
						s.logger.Debugf("[PostgreSQL] Creating sequence => %s", seqSQL)
						if _, errSeq := s.targetDB.ExecContext(ctx, seqSQL); errSeq != nil {
							s.logger.Warnf("[PostgreSQL] Create sequence fail => %v", errSeq)
							continue
						}
					}
					s.logger.Infof("[PostgreSQL] Creating table => %s", createSQL)
					if _, err2 := s.targetDB.ExecContext(ctx, createSQL); err2 != nil {
						s.logger.Errorf("[PostgreSQL] Create table fail => %v", err2)
						continue
					}
					if err3 := s.copyIndexes(ctx, srcSchema, tbl.SourceTable, tgtSchema, tbl.TargetTable); err3 != nil {
						s.logger.Warnf("[PostgreSQL] copyIndexes fail => %v", err3)
					} else {
						s.logger.Infof("[PostgreSQL] Created table and indexes for %s.%s", tgtSchema, tbl.TargetTable)
					}
				}
			}
		} else {
			s.logger.Warn("[PostgreSQL] Table mappings are empty, skipping processing")
			continue
		}
	}
	return nil
}

func (s *PostgreSQLSyncer) generateCreateTableSQL(
	ctx context.Context,
	srcSchema, srcTable, tgtSchema, tgtTable string,
) (createTableSQL string, sequences []string, err error) {

	query := `
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM information_schema.columns
WHERE table_schema=$1
  AND table_name=$2
ORDER BY ordinal_position
`
	rows, errQ := s.sourceConnNormal.Query(ctx, query, srcSchema, srcTable)
	if errQ != nil {
		return "", nil, fmt.Errorf("query source table columns fail: %w", errQ)
	}
	defer rows.Close()

	var columns []string
	sequencesMap := make(map[string]bool)

	for rows.Next() {
		var (
			columnName    string
			dataType      string
			isNullable    string
			columnDefault sql.NullString
			charMaxLen    sql.NullInt64
			numPrecision  sql.NullInt64
			numScale      sql.NullInt64
		)
		if errScan := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault,
			&charMaxLen, &numPrecision, &numScale); errScan != nil {
			return "", nil, fmt.Errorf("scan column info fail: %w", errScan)
		}

		colDef := fmt.Sprintf("%s %s", columnName, dataType)

		if (dataType == "character varying" || dataType == "varchar" ||
			dataType == "character" || dataType == "char") && charMaxLen.Valid {
			colDef += fmt.Sprintf("(%d)", charMaxLen.Int64)
		} else if (dataType == "numeric" || dataType == "decimal") && numPrecision.Valid {
			colDef += fmt.Sprintf("(%d", numPrecision.Int64)
			if numScale.Valid {
				colDef += fmt.Sprintf(",%d", numScale.Int64)
			}
			colDef += ")"
		}

		if columnDefault.Valid {
			colDef += fmt.Sprintf(" DEFAULT %s", columnDefault.String)
			if strings.Contains(columnDefault.String, "nextval(") {
				seqName := extractSequenceName(columnDefault.String)
				if seqName != "" {
					sequencesMap[seqName] = true
				}
			}
		}
		if isNullable == "NO" {
			colDef += " NOT NULL"
		}
		columns = append(columns, colDef)
	}
	if errClose := rows.Err(); errClose != nil {
		return "", nil, fmt.Errorf("iterate columns fail: %w", errClose)
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
  %s
);`, tgtSchema, tgtTable, strings.Join(columns, ",\n  "))

	var seqSlice []string
	for seqName := range sequencesMap {
		seq := fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS "%s"`, seqName)
		seqSlice = append(seqSlice, seq)
	}
	return createSQL, seqSlice, nil
}

func extractSequenceName(defaultVal string) string {
	reg := regexp.MustCompile(`nextval\('([^']+)'::regclass\)`)
	matches := reg.FindStringSubmatch(defaultVal)
	if len(matches) == 2 {
		return matches[1]
	}
	return ""
}

func (s *PostgreSQLSyncer) checkTableExist(ctx context.Context, schemaName, tableName string) (bool, error) {
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2`
	var cnt int
	err := s.targetDB.QueryRowContext(ctx, query, schemaName, tableName).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func (s *PostgreSQLSyncer) copyIndexes(ctx context.Context, srcSchema, srcTable, tgtSchema, tgtTable string) error {
	sqlIdx := `
	SELECT indexname, indexdef
	FROM pg_indexes
	WHERE schemaname=$1 
	  AND tablename=$2
	`
	rows, err := s.sourceConnNormal.Query(ctx, sqlIdx, srcSchema, srcTable)
	if err != nil {
		return fmt.Errorf("query source indexes fail: %w", err)
	}
	defer rows.Close()

	// First check existing indexes in target
	sqlExistingIdx := `
	SELECT indexname 
	FROM pg_indexes
	WHERE schemaname=$1 
	  AND tablename=$2
	`
	existingRows, err := s.targetDB.QueryContext(ctx, sqlExistingIdx, tgtSchema, tgtTable)
	if err != nil {
		s.logger.Warnf("[PostgreSQL] Failed to query existing indexes: %v", err)
		// Continue anyway to attempt creation
	}

	existingIndexes := make(map[string]bool)
	if existingRows != nil {
		for existingRows.Next() {
			var idxName string
			if err := existingRows.Scan(&idxName); err == nil {
				existingIndexes[idxName] = true
			}
		}
		existingRows.Close()
	}

	indexesCreated := 0
	indexesSkipped := 0

	for rows.Next() {
		var idxName, idxDef string
		if err2 := rows.Scan(&idxName, &idxDef); err2 != nil {
			return fmt.Errorf("scan idx fail: %w", err2)
		}

		newIdxDef := idxDef
		oldName := fmt.Sprintf("%s.%s", srcSchema, srcTable)
		newName := fmt.Sprintf("%s.%s", tgtSchema, tgtTable)
		newIdxDef = strings.ReplaceAll(newIdxDef, oldName, newName)
		newIdxName := fmt.Sprintf("%s_%s", tgtTable, idxName)
		newIdxDef = strings.Replace(newIdxDef, idxName, newIdxName, 1)

		if existingIndexes[newIdxName] {
			s.logger.Debugf("[PostgreSQL] Index %s already exists, skipping", newIdxName)
			indexesSkipped++
			continue
		}

		s.logger.Debugf("[PostgreSQL] Creating index => %s", newIdxDef)
		_, errExec := s.targetDB.ExecContext(ctx, newIdxDef)
		if errExec != nil {
			if strings.Contains(errExec.Error(), "already exists") {
				s.logger.Debugf("[PostgreSQL] Index %s already exists", newIdxName)
				indexesSkipped++
			} else {
				s.logger.Warnf("[PostgreSQL] Create index %s fail => %v", newIdxName, errExec)
			}
		} else {
			s.logger.Infof("[PostgreSQL] Successfully created index %s", newIdxName)
			indexesCreated++
		}
	}

	s.logger.Infof("[PostgreSQL] Index creation summary for %s.%s: created=%d, skipped=%d",
		tgtSchema, tgtTable, indexesCreated, indexesSkipped)

	return rows.Err()
}

func (s *PostgreSQLSyncer) doInitialSync(ctx context.Context) error {
	for _, dbmap := range s.cfg.Mappings {
		srcSchema := dbmap.SourceSchema
		if srcSchema == "" {
			srcSchema = "public"
		}
		tgtSchema := dbmap.TargetSchema
		if tgtSchema == "" {
			tgtSchema = "public"
		}
		if len(dbmap.Tables) > 0 {
			for _, tbl := range dbmap.Tables {
				checkSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", tgtSchema, tbl.TargetTable)
				var cnt int
				if err := s.targetDB.QueryRow(checkSQL).Scan(&cnt); err != nil {
					s.logger.Warnf("[PostgreSQL] Could not check table %s.%s: %v", tgtSchema, tbl.TargetTable, err)
					continue
				}
				if cnt > 0 {
					s.logger.Infof("[PostgreSQL] Table %s.%s has %d rows, skip initial sync", tgtSchema, tbl.TargetTable, cnt)
					continue
				}

				selectSQL := fmt.Sprintf("SELECT * FROM %s.%s", srcSchema, tbl.SourceTable)
				rows, errQ := s.sourceConnNormal.Query(ctx, selectSQL)
				if errQ != nil {
					return fmt.Errorf("query source %s.%s => %w", srcSchema, tbl.SourceTable, errQ)
				}
				fds := rows.FieldDescriptions()
				colNames := make([]string, len(fds))
				phArr := make([]string, len(fds))
				for i, fd := range fds {
					colNames[i] = string(fd.Name)
					phArr[i] = fmt.Sprintf("$%d", i+1)
				}
				insertSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
					tgtSchema, tbl.TargetTable,
					strings.Join(colNames, ", "),
					strings.Join(phArr, ", "),
				)

				tx, errB := s.targetDB.Begin()
				if errB != nil {
					rows.Close()
					return errB
				}
				count := 0
				for rows.Next() {
					vals, errVal := rows.Values()
					if errVal != nil {
						_ = tx.Rollback()
						rows.Close()
						return errVal
					}
					res, errExec := tx.Exec(insertSQL, vals...)
					if errExec != nil {
						_ = tx.Rollback()
						rows.Close()
						return errExec
					}
					af, _ := res.RowsAffected()
					if af > 0 {
						count++
					}
				}
				rows.Close()
				if errRows := rows.Err(); errRows != nil {
					_ = tx.Rollback()
					return errRows
				}
				if cErr := tx.Commit(); cErr != nil {
					return cErr
				}
				s.logger.Infof("[PostgreSQL] Initial sync => %s.%s => %s.%s, inserted %d rows",
					srcSchema, tbl.SourceTable, tgtSchema, tbl.TargetTable, count)
			}
		} else {
			s.logger.Warn("[PostgreSQL] Table mappings are empty, skipping processing")
			continue
		}
	}
	return nil
}

func (s *PostgreSQLSyncer) startLogicalReplication(ctx context.Context) error {
	if s.publicationNames == "" {
		s.publicationNames = "mypub"
		s.logger.Warn("[PostgreSQL] No publication name set, default to 'mypub'")
	}
	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.publicationNames),
		},
	}
	if err := pglogrepl.StartReplication(ctx, s.sourceConnRepl, s.repSlot, s.currentLsn, opts); err != nil {
		return err
	}

	connCheckTicker := time.NewTicker(5 * time.Minute)
	defer connCheckTicker.Stop()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-connCheckTicker.C:
				if err := resilience.CheckSQLConnection(ctx, s.targetDB); err != nil {
					s.logger.Warnf("[PostgreSQL] Target connection check failed: %v", err)

					if newDB, err := resilience.ReopenSQLConnection(ctx, s.logger, s.cfg.TargetConnection, "postgres"); err == nil {
						oldDB := s.targetDB
						s.targetDB = newDB
						s.state.replicaConn = newDB

						if oldDB != nil {
							_ = oldDB.Close()
						}
						s.logger.Info("[PostgreSQL] Successfully reconnected to target database")
					}
				}
			}
		}
	}()

	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if upErr := s.confirmProgress(ctx); upErr != nil {
				s.logger.Warnf("[PostgreSQL] SendStandbyStatusUpdate fail: %v", upErr)
			}
		default:
			ctx2, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
			rawMsg, rErr := s.sourceConnRepl.ReceiveMessage(ctx2)
			cancel()
			if rErr != nil {
				if strings.Contains(rErr.Error(), "context canceled") || pgconn.Timeout(rErr) {
					continue
				}
				s.logger.Errorf("[PostgreSQL] ReceiveMessage error: %v", rErr)
				return rErr
			}
			if errResp, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				return fmt.Errorf("WAL error: %+v", errResp)
			}
			cd, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				continue
			}
			switch cd.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, pErr := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
				if pErr != nil {
					s.logger.Errorf("[PostgreSQL] ParsePrimaryKeepaliveMessage error: %v", pErr)
					continue
				}
				if pkm.ServerWALEnd > s.currentLsn {
					s.currentLsn = pkm.ServerWALEnd
				}
				if pkm.ReplyRequested {
					_ = s.confirmProgress(ctx)
				}

			case pglogrepl.XLogDataByteID:
				xld, xErr := pglogrepl.ParseXLogData(cd.Data[1:])
				if xErr != nil {
					s.logger.Errorf("[PostgreSQL] ParseXLogData error: %v", xErr)
					continue
				}
				committed, procErr := s.processMessage(xld, &s.state)
				if procErr != nil {
					// Carrying on here is how a row that never reached the target
					// was left behind while the stream moved past it. The task
					// stops; the supervisor decides whether to start it again, and
					// it resumes from the last position everything was applied at.
					s.logger.Errorf("[PostgreSQL] processMessage error: %v", procErr)
					return procErr
				}
				if committed {
					if atomic.LoadInt32(&s.lastExecError) == 0 {
						s.state.lastWrittenLSN = s.state.currentTxLSN
						s.logger.Debugf("[PostgreSQL] Commit => LSN %s saved", s.state.lastWrittenLSN)
						if fErr := s.recordLSN(ctx, s.state.lastWrittenLSN); fErr != nil {
							s.logger.Errorf("[PostgreSQL] Could not record the position: %v", fErr)
						}
						metrics.Applied(s.metricLabels(), 1)
					} else {
						metrics.Failed(s.metricLabels(), 1)
						s.logger.Warn("[PostgreSQL] Commit => skip writing LSN because lastExecError != 0")
					}
				}
				s.currentLsn = xld.ServerWALEnd

			default:
				s.logger.Debugf("[PostgreSQL] Unknown message byte: %v", cd.Data[0])
			}
		}
	}
}

// confirmProgress tells the source how far this syncer has got. The three
// positions are not the same thing, and reporting one number for all of them
// is what made them dangerous: the server discards WAL the standby has
// flushed, so confirming everything *received* let it recycle segments
// carrying changes that had not been applied to the target yet.
func (s *PostgreSQLSyncer) confirmProgress(ctx context.Context) error {
	applied := s.state.lastWrittenLSN
	if applied > s.currentLsn {
		applied = s.currentLsn
	}
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.sourceConnRepl, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: s.currentLsn,
		WALFlushPosition: applied,
		WALApplyPosition: applied,
		ReplyRequested:   false,
	})
}

func (s *PostgreSQLSyncer) processMessage(xld pglogrepl.XLogData, state *replicationState) (bool, error) {
	walData := xld.WALData
	logicalMsg, err := pglogrepl.ParseV2(walData, state.inStream)
	if err != nil {
		return false, fmt.Errorf("ParseV2 fail: %w", err)
	}
	state.lastReceivedLSN = xld.ServerWALEnd

	switch typed := logicalMsg.(type) {
	case *pglogrepl.RelationMessageV2:
		state.relations[typed.RelationID] = typed

	case *pglogrepl.BeginMessage:
		// The comparison is >=, not >. A transaction whose end LSN is exactly the
		// recorded position has already been applied in full; replaying it
		// re-inserts rows that are there and re-deletes rows that are not.
		if state.lastWrittenLSN >= typed.FinalLSN {
			s.logger.Debugf("[PostgreSQL] Stale begin => lastWrittenLSN=%s > msgLSN=%s", state.lastWrittenLSN, typed.FinalLSN)
			state.processMessages = false
			return false, nil
		}
		state.processMessages = true
		state.currentTxLSN = typed.FinalLSN
		s.logger.Debug("[PostgreSQL][BEGIN] Start transaction")

	case *pglogrepl.CommitMessage:
		s.logger.Debug("[PostgreSQL][COMMIT] Transaction commit")
		state.processMessages = false
		return true, nil

	case *pglogrepl.InsertMessageV2:
		if !state.processMessages {

			return false, nil
		}
		return s.handleInsert(typed, state)

	case *pglogrepl.UpdateMessageV2:
		if !state.processMessages {

			return false, nil
		}
		return s.handleUpdate(typed, state)

	case *pglogrepl.DeleteMessageV2:
		if !state.processMessages {

			return false, nil
		}
		return s.handleDelete(typed, state)

	case *pglogrepl.TruncateMessageV2:
		// A TRUNCATE at the source is not replicated onto the target, for the
		// same reason a DROP is not: the disaster-recovery copy is the only thing
		// left to recover from, and a mistaken truncate would take it too.
		// Ignoring it silently is not an option either — the target would go on
		// holding rows the source no longer has, and nothing would ever say so.
		return false, domain.Unrecoverable("the source truncated a replicated "+
			"table (relations %v). Replication has stopped: truncating the target "+
			"is not something this will do on its own. Truncate it by hand and "+
			"restart the task, or make the copy again.", typed.RelationIDs)

	default:
		s.logger.Debugf("[PostgreSQL] Unhandled message => %T", typed)
	}
	return false, nil
}

func (s *PostgreSQLSyncer) handleInsert(
	msg *pglogrepl.InsertMessageV2,
	st *replicationState,
) (bool, error) {
	rel, ok := st.relations[msg.RelationID]
	if !ok || rel == nil {
		s.logger.Warnf("[PostgreSQL][INSERT] Unknown relationID=%d => skip", msg.RelationID)
		return false, nil
	}
	if msg.Tuple == nil {
		s.logger.Debugf("[PostgreSQL][INSERT] newTuple is nil => skip, relID=%d", msg.RelationID)
		return false, nil
	}

	table := security.FindTableSecurityFromMappings(rel.RelationName, s.cfg.Mappings)

	query, args, err := buildInsert(rel, msg.Tuple, table)
	if err != nil {
		return false, err
	}
	return false, s.replicateQuery(st.replicaConn, query, args, "INSERT", relationName(rel))
}

func (s *PostgreSQLSyncer) handleUpdate(
	msg *pglogrepl.UpdateMessageV2,
	st *replicationState,
) (bool, error) {
	rel, ok := st.relations[msg.RelationID]
	if !ok || rel == nil {
		s.logger.Warnf("[PostgreSQL][UPDATE] Unknown relationID=%d => skip", msg.RelationID)
		return false, nil
	}
	if msg.NewTuple == nil {
		s.logger.Debugf("[PostgreSQL][UPDATE] newTuple is nil => skip, relID=%d", msg.RelationID)
		return false, nil
	}

	table := security.FindTableSecurityFromMappings(rel.RelationName, s.cfg.Mappings)

	query, args, err := buildUpdate(rel, msg.OldTuple, msg.NewTuple, s.keyColumns(rel), table)
	if err != nil {
		return false, err
	}
	return false, s.replicateQuery(st.replicaConn, query, args, "UPDATE", relationName(rel))
}

func (s *PostgreSQLSyncer) handleDelete(
	msg *pglogrepl.DeleteMessageV2,
	st *replicationState,
) (bool, error) {
	rel, ok := st.relations[msg.RelationID]
	if !ok || rel == nil {
		s.logger.Warnf("[PostgreSQL][DELETE] Unknown relationID=%d => skip", msg.RelationID)
		return false, nil
	}
	if msg.OldTuple == nil {
		s.logger.Debugf("[PostgreSQL][DELETE] oldTuple is nil => skip, relID=%d", msg.RelationID)
		return false, nil
	}

	query, args, err := buildDelete(rel, msg.OldTuple, s.keyColumns(rel))
	if err != nil {
		return false, err
	}
	return false, s.replicateQuery(st.replicaConn, query, args, "DELETE", relationName(rel))
}

func relationName(rel *pglogrepl.RelationMessageV2) string {
	return rel.Namespace + "." + rel.RelationName
}

// keyColumns reports the primary key of a replicated table, or nothing when it
// has none or the source cannot be asked. It used to call straight into
// getPrimaryKeyColumns, which reads through the source connection: one DELETE
// while the source was down dereferenced a nil connection and took the whole
// process with it.
func (s *PostgreSQLSyncer) keyColumns(rel *pglogrepl.RelationMessageV2) []string {
	if s.sourceConnNormal == nil {
		s.logger.Warnf("[PostgreSQL] The source connection is gone, so %s is "+
			"addressed by every column rather than by its key", relationName(rel))
		return nil
	}

	keys, err := s.getPrimaryKeyColumns(rel.Namespace, rel.RelationName)
	if err != nil {
		s.logger.Warnf("[PostgreSQL] Could not read the primary key of %s, so it is "+
			"addressed by every column: %v", relationName(rel), err)
		return nil
	}
	return keys
}

func (s *PostgreSQLSyncer) getPrimaryKeyColumns(schema, tableName string) ([]string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = ($1 || '.' || $2)::regclass
		AND i.indisprimary
	`

	var primaryKeys []string
	rows, err := s.sourceConnNormal.Query(context.Background(), query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return primaryKeys, nil
}

// replicateQuery executes replication queries. A statement that failed used to
// be forgiven by the next one that succeeded: the flag the commit path reads
// was cleared on every success, including the next statement of the same
// transaction.
func (s *PostgreSQLSyncer) replicateQuery(db *sql.DB, query string, args []interface{}, opType, tableName string) error {
	s.logger.Debugf("[PostgreSQL][%s] table=%s query=%s", opType, tableName, query)

	err := resilience.RetryDBOperation(context.Background(), s.logger,
		fmt.Sprintf("%s on %s", opType, tableName),
		func() error {
			res, err := db.Exec(query, args...)
			if err != nil {
				return err
			}

			rowsAff, _ := res.RowsAffected()
			if rowsAff == 0 && (opType == "UPDATE" || opType == "DELETE") {
				// The row the source changed is not on the target. Saying so is
				// the only way this shows up at all: the statement succeeded.
				s.logger.Warnf("[PostgreSQL][%s] table=%s matched no row on the target",
					opType, tableName)
			}
			s.logger.Debugf("[PostgreSQL][%s] table=%s rowsAffected=%d", opType, tableName, rowsAff)
			return nil
		})

	if err != nil {
		s.logger.Errorf("[PostgreSQL][%s] table=%s error=%v", opType, tableName, err)
		atomic.StoreInt32(&s.lastExecError, 1)
	}

	return err
}

func parseLSNFromString(lsnStr string) (pglogrepl.LSN, error) {
	lsnStr = strings.TrimSpace(lsnStr)
	if lsnStr == "" {
		return 0, fmt.Errorf("empty LSN string")
	}

	parts := strings.Split(lsnStr, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN format: %s", lsnStr)
	}

	if parts[0] == "" || parts[1] == "" {
		return 0, fmt.Errorf("invalid LSN format (empty part): %s", lsnStr)
	}

	hi, err := hexStrToUint32(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid high bits in LSN %s: %w", lsnStr, err)
	}
	lo, err2 := hexStrToUint32(parts[1])
	if err2 != nil {
		return 0, fmt.Errorf("invalid low bits in LSN %s: %w", lsnStr, err2)
	}
	return pglogrepl.LSN(uint64(hi)<<32 + uint64(lo)), nil
}

func hexStrToUint32(s string) (uint32, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty hex string")
	}
	val, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(val), nil
}

// claimDirection records which way this task replicates, on both databases, and
// keeps the claims refreshed for as long as it runs.
//
// It opens a second connection to the source through database/sql, because the
// two the replication path holds are a pgx connection and a replication
// connection, and neither takes ordinary queries the way the claim store does.
func (s *PostgreSQLSyncer) claimDirection(ctx context.Context) (func(), error) {
	sourceDB, err := sql.Open("postgres", s.cfg.SourceConnection)
	if err != nil {
		return nil, fmt.Errorf("open the source to claim the replication direction: %w", err)
	}

	guard := &directionlock.Guard{
		TaskID: s.cfg.ID,
		Source: &directionlock.SQLStore{
			DB:                   sourceDB,
			Address:              dsn.Endpoint("postgresql", s.cfg.SourceConnection),
			NumberedPlaceholders: true,
		},
		Target: &directionlock.SQLStore{
			DB:                   s.targetDB,
			Address:              dsn.Endpoint("postgresql", s.cfg.TargetConnection),
			NumberedPlaceholders: true,
		},
	}

	release, err := directionlock.Hold(ctx, guard, s.logger, "PostgreSQL")
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

// checkpointStore is where this task records its replication position. It
// writes to the target database as well as the configured file.
func (s *PostgreSQLSyncer) checkpointStore() checkpoint.Store {
	stores := []checkpoint.Store{
		&checkpoint.SQLStore{
			DB:                   s.targetDB,
			TaskID:               s.cfg.ID,
			NumberedPlaceholders: true,
		},
	}
	if s.cfg.PGPositionPath != "" {
		stores = append(stores, &checkpoint.FileStore{Path: s.cfg.PGPositionPath})
	}
	return &checkpoint.Layered{
		Stores:  stores,
		OnError: func(err error) { s.logger.Warnf("[PostgreSQL] Checkpoint store: %v", err) },
	}
}

type walCheckpoint struct {
	LSN string `json:"lsn"`
	// Source names the server the position belongs to, without credentials. An
	// LSN means nothing on another server: read there it addresses unrelated WAL,
	// and the read succeeds.
	Source string `json:"source,omitempty"`
}

func (s *PostgreSQLSyncer) loadStoredLSN(ctx context.Context) (pglogrepl.LSN, error) {
	payload, err := s.checkpoints.Load(ctx, "")
	if err != nil {
		return 0, err
	}
	if payload == "" {
		return 0, nil
	}

	// A file written by an older build holds the LSN as plain text rather than a
	// document, so both forms are read.
	var cp walCheckpoint
	if found, decodeErr := checkpoint.Decode(payload, &cp); decodeErr != nil || !found {
		return parseLSNFromString(strings.TrimSpace(payload))
	}

	if want := dsn.Endpoint("postgresql", s.cfg.SourceConnection); cp.Source != "" && cp.Source != want {
		s.logger.Warnf("[PostgreSQL] Ignoring a position recorded against %s: this task "+
			"reads %s, and an LSN means nothing on another server. The copy will be "+
			"made again.", cp.Source, want)
		return 0, nil
	}
	return parseLSNFromString(cp.LSN)
}

func (s *PostgreSQLSyncer) recordLSN(ctx context.Context, lsn pglogrepl.LSN) error {
	payload, err := checkpoint.Encode(walCheckpoint{
		LSN:    lsn.String(),
		Source: dsn.Endpoint("postgresql", s.cfg.SourceConnection),
	})
	if err != nil {
		return err
	}
	return s.checkpoints.Save(ctx, "", payload)
}

// metricLabels identify this task in the metrics. The endpoints are named
// without their credentials, because the exposition is scraped and stored.
func (s *PostgreSQLSyncer) metricLabels() metrics.Labels {
	// Task and engine, and nothing else, for the reason given on the MySQL
	// syncer's copy of this: endpoint labels split a task's series in two.
	return metrics.Labels{
		"task":   strconv.Itoa(s.cfg.ID),
		"engine": "postgresql",
	}
}
