package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
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
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/syncer/security"
	"github.com/retail-ai-inc/sync/pkg/utils"
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
}

func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *PostgreSQLSyncer {
	return &PostgreSQLSyncer{
		cfg:    cfg,
		logger: logger.WithField("sync_task_id", cfg.ID),
	}
}

func (s *PostgreSQLSyncer) Start(ctx context.Context) {
	var err error

	s.logger.Info("[PostgreSQL] Starting synchronization...")

	// Connect normal
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		s.sourceConnNormal, connErr = pgx.Connect(ctx, s.cfg.SourceConnection)
		return connErr
	})
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Failed to connect to source (normal) after retries: %v", err)
		return
	}
	defer s.sourceConnNormal.Close(ctx)

	// Connect replication
	replDSN, err := s.buildReplicationDSN(s.cfg.SourceConnection)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Failed to build replication DSN: %v", err)
		return
	}
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		s.sourceConnRepl, connErr = pgconn.Connect(ctx, replDSN)
		return connErr
	})
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Failed to connect to source (replication) after retries: %v", err)
		return
	}
	defer s.sourceConnRepl.Close(ctx)

	// Connect target
	s.targetDB, err = sql.Open("postgres", s.cfg.TargetConnection)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Failed to open target DB: %v", err)
		return
	}
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		return s.targetDB.PingContext(ctx)
	})
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Failed to connect to target DB after retries: %v", err)
		return
	}
	defer s.targetDB.Close()

	s.repSlot = s.cfg.PGReplicationSlot()
	s.outputPlugin = s.cfg.PGPlugin()
	s.publicationNames = s.cfg.PGPublicationNames
	if s.repSlot == "" || s.outputPlugin == "" {
		s.logger.Error("[PostgreSQL] Must specify pg_replication_slot and pg_plugin in config")
		return
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
		s.logger.Errorf("[PostgreSQL] ensureReplicationSlot error: %v", err)
		return
	}

	if s.cfg.PGPositionPath != "" {
		lsnFromFile, errLoad := s.loadPosition(s.cfg.PGPositionPath)
		if errLoad == nil && lsnFromFile > 0 {
			s.logger.Infof("[PostgreSQL] Loaded last LSN from file: %X", lsnFromFile)
			s.currentLsn = lsnFromFile
			s.state.lastWrittenLSN = lsnFromFile
		}
	}

	if err := s.prepareTargetSchema(ctx); err != nil {
		s.logger.Warnf("[PostgreSQL] prepareTargetSchema error: %v", err)
	}

	// Perform initial sync
	err = s.doInitialSync(ctx)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] doInitialSync failed after retries: %v", err)
		return
	}
	s.logger.Info("[PostgreSQL] Initial full sync done.")

	// Start logical replication
	err = s.startLogicalReplication(ctx)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Logical replication failed: %v", err)
		return
	}

	s.logger.Info("[PostgreSQL] Synchronization tasks completed.")
}

func (s *PostgreSQLSyncer) buildReplicationDSN(normalDSN string) (string, error) {
	u, err := url.Parse(normalDSN)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("replication", "database")
	u.RawQuery = q.Encode()
	return u.String(), nil
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
		if s.currentLsn == 0 {
			s.currentLsn = info.XLogPos
		}
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

		s.logger.Debugf("[PostgreSQL] Creating index => %s", newIdxDef)
		_, errExec := s.targetDB.ExecContext(ctx, newIdxDef)
		if errExec != nil && !strings.Contains(errExec.Error(), "already exists") {
			s.logger.Warnf("[PostgreSQL] Create index fail => %v", errExec)
		}
	}
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

	ticker := time.NewTicker(8 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			upErr := pglogrepl.SendStandbyStatusUpdate(ctx, s.sourceConnRepl, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: s.currentLsn,
				ReplyRequested:   false,
			})
			if upErr != nil {
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
					_ = pglogrepl.SendStandbyStatusUpdate(ctx, s.sourceConnRepl, pglogrepl.StandbyStatusUpdate{
						WALWritePosition: s.currentLsn,
						ReplyRequested:   false,
					})
				}

			case pglogrepl.XLogDataByteID:
				xld, xErr := pglogrepl.ParseXLogData(cd.Data[1:])
				if xErr != nil {
					s.logger.Errorf("[PostgreSQL] ParseXLogData error: %v", xErr)
					continue
				}
				committed, procErr := s.processMessage(xld, &s.state)
				if procErr != nil {
					s.logger.Errorf("[PostgreSQL] processMessage error: %v", procErr)
					continue
				}
				if committed {
					if atomic.LoadInt32(&s.lastExecError) == 0 {
						s.state.lastWrittenLSN = s.state.currentTxLSN
						s.logger.Debugf("[PostgreSQL] Commit => LSN %s saved", s.state.lastWrittenLSN)
						if s.cfg.PGPositionPath != "" {
							if fErr := s.writeWALPosition(s.state.lastWrittenLSN); fErr != nil {
								s.logger.Errorf("[PostgreSQL] writeWALPosition fail: %v", fErr)
							}
						}
					} else {
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
		if state.lastWrittenLSN > typed.FinalLSN {
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
			s.logger.Debug("[PostgreSQL][INSERT] Stale insert => ignoring")
			return false, nil
		}
		return s.handleInsert(typed, state)

	case *pglogrepl.UpdateMessageV2:
		if !state.processMessages {
			s.logger.Debug("[PostgreSQL][UPDATE] Stale update => ignoring")
			return false, nil
		}
		return s.handleUpdate(typed, state)

	case *pglogrepl.DeleteMessageV2:
		if !state.processMessages {
			s.logger.Debug("[PostgreSQL][DELETE] Stale delete => ignoring")
			return false, nil
		}
		return s.handleDelete(typed, state)

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

	// Get table security configuration
	tableSecurity := security.FindTableSecurityFromMappings(rel.RelationName, s.cfg.Mappings)

	// Print security configuration
	s.logger.Debugf("[PostgreSQL] Table=%s security configuration: enabled=%v, rules=%d",
		rel.RelationName, tableSecurity.SecurityEnabled, len(tableSecurity.FieldSecurity))

	colNames := make([]string, len(msg.Tuple.Columns))
	colValues := make([]string, len(msg.Tuple.Columns))

	// Print data before processing
	if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
		s.logger.Debugf("[PostgreSQL][INSERT] Data before processing: table=%s", rel.RelationName)
		for i, col := range msg.Tuple.Columns {
			if i < len(rel.Columns) {
				colName := rel.Columns[i].Name
				colNames[i] = colName
				s.logger.Debugf("  Column[%d]: %s = %v", i, colName, string(col.Data))
			}
		}
	}

	for i, col := range msg.Tuple.Columns {
		if i >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[i].Name
		colNames[i] = colName

		// Apply security processing
		var value string
		switch col.DataType {
		case 'n':
			value = "NULL"
		case 't':
			value = string(col.Data)
			// Apply security processing to values
			if tableSecurity.SecurityEnabled {
				processed := security.ProcessValue(value, colName, tableSecurity)
				if processedStr, ok := processed.(string); ok {
					value = processedStr
				}
			}
			value = "'" + strings.ReplaceAll(value, "'", "''") + "'"
		default:
			value = "NULL"
		}
		colValues[i] = value
	}

	// Print data after processing
	if tableSecurity.SecurityEnabled && len(tableSecurity.FieldSecurity) > 0 {
		s.logger.Debugf("[PostgreSQL][INSERT] Data after processing: table=%s", rel.RelationName)
		for i, value := range colValues {
			if i < len(colNames) {
				s.logger.Debugf("  Column[%d]: %s = %v", i, colNames[i], value)
			}
		}
	}

	sqlStr := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		rel.Namespace,
		rel.RelationName,
		strings.Join(colNames, ", "),
		strings.Join(colValues, ", "),
	)
	err := s.replicateQuery(st.replicaConn, sqlStr, "INSERT", fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName))
	return false, err
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

	var setClauses []string
	for idx, col := range msg.NewTuple.Columns {
		if idx >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n':
			setClauses = append(setClauses, fmt.Sprintf("%s=NULL", colName))
		case 't':
			val := strings.ReplaceAll(string(col.Data), "'", "''")
			setClauses = append(setClauses, fmt.Sprintf("%s='%s'", colName, val))
		default:
			setClauses = append(setClauses, fmt.Sprintf("%s=NULL", colName))
		}
	}

	var whereClauses []string
	if msg.OldTuple == nil {
		whereClauses = s.buildWhereClausesFromPK(rel, msg.NewTuple.Columns)
	} else {
		whereClauses = s.buildWhereClausesFromPK(rel, msg.OldTuple.Columns)
	}
	if len(whereClauses) == 0 {
		s.logger.Debugf("[PostgreSQL][UPDATE] No PK => skip, relID=%d", msg.RelationID)
		return false, nil
	}

	sqlStr := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		rel.Namespace,
		rel.RelationName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	err := s.replicateQuery(st.replicaConn, sqlStr, "UPDATE", fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName))
	return false, err
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

	primaryKeys, err := s.getPrimaryKeyColumns(rel.Namespace, rel.RelationName)
	if err != nil {
		s.logger.Warnf("[PostgreSQL][DELETE] Failed to get primary keys: %v", err)
		return s.handleDeleteWithAllColumns(msg, st, rel)
	}

	if len(primaryKeys) == 0 {
		s.logger.Debugf("[PostgreSQL][DELETE] No primary keys found, using all columns")
		return s.handleDeleteWithAllColumns(msg, st, rel)
	}

	var whereClauses []string
	for idx, col := range msg.OldTuple.Columns {
		if idx >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[idx].Name

		isPK := false
		for _, pk := range primaryKeys {
			if pk == colName {
				isPK = true
				break
			}
		}

		if !isPK {
			continue
		}

		switch col.DataType {
		case 'n':
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", colName))
		case 't':
			val := strings.ReplaceAll(string(col.Data), "'", "''")
			whereClauses = append(whereClauses, fmt.Sprintf("%s='%s'", colName, val))
		default:
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", colName))
		}
	}

	if len(whereClauses) == 0 {
		s.logger.Warnf("[PostgreSQL][DELETE] No WHERE conditions could be built with primary keys, using all columns")
		return s.handleDeleteWithAllColumns(msg, st, rel)
	}

	sqlStr := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		rel.Namespace,
		rel.RelationName,
		strings.Join(whereClauses, " AND "),
	)
	err = s.replicateQuery(st.replicaConn, sqlStr, "DELETE", fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName))
	return false, err
}

func (s *PostgreSQLSyncer) handleDeleteWithAllColumns(
	msg *pglogrepl.DeleteMessageV2,
	st *replicationState,
	rel *pglogrepl.RelationMessageV2,
) (bool, error) {
	var whereClauses []string
	for idx, col := range msg.OldTuple.Columns {
		if idx >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n':
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", colName))
		case 't':
			val := strings.ReplaceAll(string(col.Data), "'", "''")
			whereClauses = append(whereClauses, fmt.Sprintf("%s='%s'", colName, val))
		default:
			whereClauses = append(whereClauses, fmt.Sprintf("%s IS NULL", colName))
		}
	}
	if len(whereClauses) == 0 {
		return false, nil
	}
	sqlStr := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		rel.Namespace,
		rel.RelationName,
		strings.Join(whereClauses, " AND "),
	)
	err := s.replicateQuery(st.replicaConn, sqlStr, "DELETE", fmt.Sprintf("%s.%s", rel.Namespace, rel.RelationName))
	return false, err
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

func (s *PostgreSQLSyncer) buildWhereClausesFromPK(
	rel *pglogrepl.RelationMessageV2,
	cols []*pglogrepl.TupleDataColumn,
) []string {
	var clauses []string
	if rel == nil || len(rel.Columns) == 0 {
		return clauses
	}
	for idx, col := range cols {
		if idx >= len(rel.Columns) {
			continue
		}
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n':
			clauses = append(clauses, fmt.Sprintf("%s IS NULL", colName))
		case 't':
			val := strings.ReplaceAll(string(col.Data), "'", "''")
			clauses = append(clauses, fmt.Sprintf("%s='%s'", colName, val))
		default:
			clauses = append(clauses, fmt.Sprintf("%s IS NULL", colName))
		}
	}
	return clauses
}

func (s *PostgreSQLSyncer) replicateQuery(db *sql.DB, query, opType, tableName string) error {
	s.logger.Debugf("[PostgreSQL][%s] table=%s query=%s", opType, tableName, query)

	res, err := db.Exec(query)
	if err != nil {
		s.logger.Errorf("[PostgreSQL][%s] table=%s error=%v", opType, tableName, err)
		atomic.StoreInt32(&s.lastExecError, 1)
		return err
	}
	atomic.StoreInt32(&s.lastExecError, 0)

	rowsAff, _ := res.RowsAffected()
	s.logger.Infof("[PostgreSQL][%s] table=%s rowsAffected=%d", opType, tableName, rowsAff)
	return nil
}

func (s *PostgreSQLSyncer) loadPosition(path string) (pglogrepl.LSN, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	str := strings.TrimSpace(string(data))
	if len(str) < 3 {
		return 0, fmt.Errorf("empty position file")
	}
	return parseLSNFromString(str)
}

func (s *PostgreSQLSyncer) writeWALPosition(lsn pglogrepl.LSN) error {
	path := s.cfg.PGPositionPath
	if path == "" {
		return nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(lsn.String()), 0644)
}

func parseLSNFromString(lsnStr string) (pglogrepl.LSN, error) {
	parts := strings.Split(lsnStr, "/")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid LSN format: %s", lsnStr)
	}
	hi, err := hexStrToUint32(parts[0])
	if err != nil {
		return 0, err
	}
	lo, err2 := hexStrToUint32(parts[1])
	if err2 != nil {
		return 0, err2
	}
	return pglogrepl.LSN(uint64(hi)<<32 + uint64(lo)), nil
}

func hexStrToUint32(s string) (uint32, error) {
	val, err := strconv.ParseUint(s, 16, 32)
	if err != nil {
		return 0, err
	}
	return uint32(val), nil
}
