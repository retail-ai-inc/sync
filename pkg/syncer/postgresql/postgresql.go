package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
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
	logger *logrus.Logger

	sourceConnNormal *pgx.Conn
	sourceConnRepl   *pgconn.PgConn
	targetDB         *sql.DB

	repSlot          string
	outputPlugin     string
	publicationNames string
	currentLsn       pglogrepl.LSN

	state replicationState

	// lastExecError is set to 1 if replicateQuery fails
	lastExecError int32
}

func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *PostgreSQLSyncer {
	return &PostgreSQLSyncer{
		cfg:    cfg,
		logger: logger,
	}
}

func (s *PostgreSQLSyncer) Start(ctx context.Context) {
	var err error

	s.sourceConnNormal, err = pgx.Connect(ctx, s.cfg.SourceConnection)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to connect to source (normal): %v", err)
	}
	defer s.sourceConnNormal.Close(ctx)

	replDSN, err := s.buildReplicationDSN(s.cfg.SourceConnection)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to build replication DSN: %v", err)
	}
	s.sourceConnRepl, err = pgconn.Connect(ctx, replDSN)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to connect to source (replication): %v", err)
	}
	defer s.sourceConnRepl.Close(ctx)

	s.targetDB, err = sql.Open("postgres", s.cfg.TargetConnection)
	if err != nil {
		s.logger.Fatalf("[PostgreSQL] Failed to connect to target: %v", err)
	}
	defer s.targetDB.Close()

	s.repSlot = s.cfg.PGReplicationSlot()
	s.outputPlugin = s.cfg.PGPlugin()
	s.publicationNames = s.cfg.PGPublicationNames
	if s.repSlot == "" || s.outputPlugin == "" {
		s.logger.Fatalf("[PostgreSQL] Must specify pg_replication_slot and pg_plugin")
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
		s.logger.Fatalf("[PostgreSQL] Failed to ensure replication slot: %v", err)
	}

	if s.cfg.PGPositionPath != "" {
		lsnFromFile, err := s.loadPosition(s.cfg.PGPositionPath)
		if err == nil && lsnFromFile > 0 {
			s.logger.Infof("[PostgreSQL] Loaded last LSN from file: %X", lsnFromFile)
			s.currentLsn = lsnFromFile
			s.state.lastWrittenLSN = lsnFromFile
		}
	}

	err = s.initialSync(ctx)
	if err != nil {
		s.logger.Errorf("[PostgreSQL] Initial sync failed: %v", err)
	}

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
		}else {
			s.logger.Infof("[PostgreSQL] Replication slot %s already exists. Using XLogPos as start if not loaded.", s.repSlot)
			if s.currentLsn == 0 {
				s.currentLsn = info.XLogPos
			}
			return nil
		}
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

func (s *PostgreSQLSyncer) initialSync(ctx context.Context) error {
	s.logger.Info("[PostgreSQL] Starting initial full sync")

	for _, dbmap := range s.cfg.Mappings {
		srcSchema := dbmap.SourceSchema
		if srcSchema == "" {
			srcSchema = "public"
		}
		tgtSchema := dbmap.TargetSchema
		if tgtSchema == "" {
			tgtSchema = "public"
		}
		for _, tbl := range dbmap.Tables {
			checkSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", tgtSchema, tbl.TargetTable)
			var cnt int
			if err := s.targetDB.QueryRow(checkSQL).Scan(&cnt); err != nil {
				s.logger.Errorf("[PostgreSQL] Could not check %s.%s: %v", tgtSchema, tbl.TargetTable, err)
				continue
			}
			if cnt > 0 {
				s.logger.Infof("[PostgreSQL] Table %s.%s has %d rows, skip initial sync", tgtSchema, tbl.TargetTable, cnt)
				continue
			}

			selectSQL := fmt.Sprintf("SELECT * FROM %s.%s", srcSchema, tbl.SourceTable)
			rows, err := s.sourceConnNormal.Query(ctx, selectSQL)
			if err != nil {
				return fmt.Errorf("query source %s.%s => %w", srcSchema, tbl.SourceTable, err)
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

			tx, err2 := s.targetDB.Begin()
			if err2 != nil {
				rows.Close()
				return err2
			}

			count := 0
			for rows.Next() {
				vals, errVal := rows.Values()
				if errVal != nil {
					_ = tx.Rollback()
					rows.Close()
					return errVal
				}
				if _, errExec := tx.Exec(insertSQL, vals...); errExec != nil {
					_ = tx.Rollback()
					rows.Close()
					return errExec
				}
				count++
			}
			rows.Close()
			if errClose := rows.Err(); errClose != nil {
				_ = tx.Rollback()
				return errClose
			}
			if cErr := tx.Commit(); cErr != nil {
				return cErr
			}
			s.logger.Infof("[PostgreSQL] Initial sync => %s.%s => %s.%s, inserted %d rows",
				srcSchema, tbl.SourceTable, tgtSchema, tbl.TargetTable, count)
		}
	}
	s.logger.Info("[PostgreSQL] Initial full sync done.")
	return nil
}

func (s *PostgreSQLSyncer) startLogicalReplication(ctx context.Context) error {
	if s.publicationNames == "" {
		s.publicationNames = "mypub"
		s.logger.Warn("[PostgreSQL] No publication name set, using 'mypub'")
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
				s.logger.Errorf("[PostgreSQL] SendStandbyStatusUpdate fail: %v", upErr)
			}
		default:
			ctx2, cancel := context.WithDeadline(ctx, time.Now().Add(1*time.Second))
			rawMsg, rErr := s.sourceConnRepl.ReceiveMessage(ctx2)
			cancel()
			if rErr != nil {
				if strings.Contains(rErr.Error(), "context canceled") {
					s.logger.Warnf("[PostgreSQL] context canceled, normal exit: %v", rErr)
					return nil
				}
				if pgconn.Timeout(rErr) {
					continue
				}
				return fmt.Errorf("ReceiveMessage fail: %w", rErr)
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
					return pErr
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
					return xErr
				}
				committed, procErr := s.processMessage(xld, &s.state)
				if procErr != nil {
					s.logger.Errorf("[PostgreSQL] processMessage error: %v", procErr)
					continue
				}
				if committed {
					// Only if lastExecError == 0 do we save LSN
					if atomic.LoadInt32(&s.lastExecError) == 0 {
						s.state.lastWrittenLSN = s.state.currentTxLSN
						s.logger.Infof("[PostgreSQL] Commit => writing LSN %s", s.state.lastWrittenLSN)
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
				s.logger.Warnf("[PostgreSQL] Unknown message byte: %v", cd.Data[0])
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

	s.logger.Debugf("[PostgreSQL] XLogData => WALStart %s, ServerWALEnd %s, ServerTime %s, MessageType=%T",
		xld.WALStart, xld.ServerWALEnd, xld.ServerTime, logicalMsg)

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
		s.logger.Debugf("[PostgreSQL] Begin => %v", typed)
		_ = s.replicateQuery(state.replicaConn, "START TRANSACTION")

	case *pglogrepl.CommitMessage:
		s.logger.Debugf("[PostgreSQL] Commit => %v", typed)
		_ = s.replicateQuery(state.replicaConn, "COMMIT")
		state.processMessages = false
		return true, nil

	case *pglogrepl.InsertMessageV2:
		if !state.processMessages {
			s.logger.Debugf("[PostgreSQL] Stale insert => ignoring")
			return false, nil
		}
		return s.handleInsertV2(typed, state)

	case *pglogrepl.UpdateMessageV2:
		if !state.processMessages {
			return false, nil
		}
		return s.handleUpdateV2(typed, state)

	case *pglogrepl.DeleteMessageV2:
		if !state.processMessages {
			return false, nil
		}
		return s.handleDeleteV2(typed, state)

	default:
		s.logger.Debugf("[PostgreSQL] Unhandled message => %T", typed)
	}
	return false, nil
}

func (s *PostgreSQLSyncer) handleInsertV2(
	msg *pglogrepl.InsertMessageV2,
	st *replicationState,
) (bool, error) {
	rel, ok := st.relations[msg.RelationID]
	if !ok || rel == nil {
		log.Printf("[PostgreSQL] Unknown or nil relation for Insert, relationID=%d", msg.RelationID)
		return false, nil
	}
	if msg.Tuple == nil {
		s.logger.Warnf("[PostgreSQL] InsertMessageV2 => msg.Tuple is nil => skip, relationID=%d", msg.RelationID)
		return false, nil
	}
	if len(rel.Columns) == 0 {
		s.logger.Warnf("[PostgreSQL] relationID=%d has no columns => skip insert", msg.RelationID)
		return false, nil
	}

	var colNames, colVals []string
	for idx, col := range msg.Tuple.Columns {
		if idx >= len(rel.Columns) {
			s.logger.Warnf("[PostgreSQL] Insert col idx=%d out of range for relID=%d", idx, msg.RelationID)
			continue
		}
		colName := rel.Columns[idx].Name
		switch col.DataType {
		case 'n':
			colVals = append(colVals, "NULL")
		case 't':
			val := strings.ReplaceAll(string(col.Data), "'", "''")
			colVals = append(colVals, fmt.Sprintf("'%s'", val))
		default:
			colVals = append(colVals, "NULL")
		}
		colNames = append(colNames, colName)
	}
	if len(colNames) == 0 {
		s.logger.Debugf("[PostgreSQL] Insert => no columns => skip")
		return false, nil
	}

	sqlStr := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT DO NOTHING",
		rel.Namespace, rel.RelationName,
		strings.Join(colNames, ", "),
		strings.Join(colVals, ", "),
	)
	s.logger.Debugf("[PostgreSQL] [INSERT] SQL => %s", sqlStr)
	err := s.replicateQuery(st.replicaConn, sqlStr)
	return false, err
}

func (s *PostgreSQLSyncer) handleUpdateV2(
	msg *pglogrepl.UpdateMessageV2,
	st *replicationState,
) (bool, error) {
	rel, ok := st.relations[msg.RelationID]
	if !ok || rel == nil {
		log.Printf("[PostgreSQL] Unknown or nil relation for Update, relationID=%d", msg.RelationID)
		return false, nil
	}
	if len(rel.Columns) == 0 {
		s.logger.Warnf("[PostgreSQL] relationID=%d has no columns => skip update", msg.RelationID)
		return false, nil
	}

	if msg.NewTuple == nil {
		s.logger.Warnf("[PostgreSQL] UpdateMessageV2 => newTuple is nil, relationID=%d => skip update", msg.RelationID)
		return false, nil
	}

	// Prepare set clauses
	var setClauses []string
	for idx, col := range msg.NewTuple.Columns {
		if idx >= len(rel.Columns) {
			s.logger.Warnf("[PostgreSQL] Update new col idx=%d out of range for relID=%d", idx, msg.RelationID)
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

	// Prepare where from oldTuple if present, otherwise fallback to newTuple
	var whereClauses []string
	if msg.OldTuple == nil {
		s.logger.Warnf("[PostgreSQL] UpdateMessageV2 => oldTuple is nil, relationID=%d => using newTuple for PK", msg.RelationID)
		whereClauses = s.buildWhereClausesFromPK(rel, msg.NewTuple.Columns)
	} else {
		whereClauses = s.buildWhereClausesFromPK(rel, msg.OldTuple.Columns)
	}
	if len(whereClauses) == 0 {
		s.logger.Debugf("[PostgreSQL] Update => no PK => skip update, relID=%d", msg.RelationID)
		return false, nil
	}

	sqlStr := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		rel.Namespace,
		rel.RelationName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "),
	)
	s.logger.Debugf("[PostgreSQL] [UPDATE] SQL => %s", sqlStr)
	err := s.replicateQuery(st.replicaConn, sqlStr)
	return false, err
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

func (s *PostgreSQLSyncer) handleDeleteV2(
	msg *pglogrepl.DeleteMessageV2,
	st *replicationState,
) (bool, error) {
	rel, ok := st.relations[msg.RelationID]
	if !ok || rel == nil {
		log.Printf("[PostgreSQL] Unknown or nil relation for Delete, relationID=%d", msg.RelationID)
		return false, nil
	}
	if len(rel.Columns) == 0 {
		s.logger.Warnf("[PostgreSQL] relationID=%d has no columns => skip delete", msg.RelationID)
		return false, nil
	}
	if msg.OldTuple == nil {
		s.logger.Warnf("[PostgreSQL] DeleteMessageV2 => oldTuple is nil => skip, relationID=%d", msg.RelationID)
		return false, nil
	}

	var whereClauses []string
	for idx, col := range msg.OldTuple.Columns {
		if idx >= len(rel.Columns) {
			s.logger.Warnf("[PostgreSQL] Delete old col idx=%d out of range for relID=%d", idx, msg.RelationID)
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
		s.logger.Debugf("[PostgreSQL] Delete => no old key => skip")
		return false, nil
	}
	sqlStr := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		rel.Namespace,
		rel.RelationName,
		strings.Join(whereClauses, " AND "),
	)
	s.logger.Debugf("[PostgreSQL] [DELETE] SQL => %s", sqlStr)
	err := s.replicateQuery(st.replicaConn, sqlStr)
	return false, err
}

func (s *PostgreSQLSyncer) replicateQuery(db *sql.DB, query string) error {
	s.logger.Debugf("[PostgreSQL] replicateQuery => %s", query)
	_, err := db.Exec(query)
	if err != nil {
		s.logger.Warnf("[PostgreSQL] replicateQuery fail => ignoring => %s => error: %v", query, err)
		atomic.StoreInt32(&s.lastExecError, 1)
	}
	return err
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
