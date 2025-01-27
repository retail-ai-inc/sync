package mariadb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

type MariaDBSyncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger
}

func NewMariaDBSyncer(cfg config.SyncConfig, logger *logrus.Logger) *MariaDBSyncer {
	return &MariaDBSyncer{
		cfg:    cfg,
		logger: logger.WithField("sync_task_id", cfg.ID),
	}
}

func (s *MariaDBSyncer) Start(ctx context.Context) {
	s.logger.Info("[MariaDB] Starting synchronization...")

	cfg := canal.NewDefaultConfig()
	cfg.Addr = s.parseAddr(s.cfg.SourceConnection)
	cfg.User, cfg.Password = s.parseUserPassword(s.cfg.SourceConnection)
	cfg.Dump.ExecutionPath = s.cfg.DumpExecutionPath

	var includeTables []string
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			includeTables = append(includeTables, fmt.Sprintf("%s\\.%s", mapping.SourceDatabase, table.SourceTable))
		}
	}
	cfg.IncludeTableRegex = includeTables

	c, err := canal.NewCanal(cfg)
	if err != nil {
		s.logger.Errorf("[MariaDB] Failed to create canal: %v", err)
		return
	}

	targetDB, err := sql.Open("mysql", s.cfg.TargetConnection)
	if err != nil {
		s.logger.Errorf("[MariaDB] Failed to connect to target: %v", err)
		return
	}

	s.doInitialFullSyncIfNeeded(ctx, c, targetDB)

	h := &MariaDBEventHandler{
		targetDB:          targetDB,
		mappings:          s.cfg.Mappings,
		logger:            s.logger,
		positionSaverPath: s.cfg.MySQLPositionPath,
		canal:             c,
		lastExecError:     0,
	}
	c.SetEventHandler(h)

	var startPos *mysql.Position
	if s.cfg.MySQLPositionPath != "" {
		startPos = s.loadBinlogPosition(s.cfg.MySQLPositionPath)
		if startPos != nil {
			s.logger.Infof("[MariaDB] Starting canal from saved position: %v", *startPos)
		}
	}

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pos := c.SyncedPosition()
				if atomic.LoadInt32(&h.lastExecError) == 0 {
					data, errPos := json.Marshal(pos)
					if errPos != nil {
						s.logger.Errorf("[MariaDB] Failed to marshal binlog position: %v", errPos)
						continue
					}
					if h.positionSaverPath != "" {
						positionDir := filepath.Dir(h.positionSaverPath)
						if errMk := os.MkdirAll(positionDir, os.ModePerm); errMk != nil {
							s.logger.Errorf("[MariaDB] Failed to create directory for binlog position %s: %v", h.positionSaverPath, errMk)
							continue
						}
						if errWrite := os.WriteFile(h.positionSaverPath, data, 0644); errWrite != nil {
							s.logger.Errorf("[MariaDB] Failed to write binlog position to %s: %v", h.positionSaverPath, errWrite)
						} else {
							s.logger.Debugf("[MariaDB] Timer: saved position %v", pos)
						}
					}
				} else {
					s.logger.Warn("[MariaDB] Timer: lastExecError != 0, skip saving binlog position")
				}
			}
		}
	}()

	go func() {
		var runErr error
		if startPos != nil {
			runErr = c.RunFrom(*startPos)
		} else {
			runErr = c.Run()
		}
		if runErr != nil {
			if strings.Contains(runErr.Error(), "context canceled") {
				s.logger.Warnf("[MariaDB] Canal run context canceled: %v", runErr)
			} else {
				s.logger.Errorf("[MariaDB] Failed to run canal: %v", runErr)
			}
		}
	}()

	<-ctx.Done()
	s.logger.Info("[MariaDB] Synchronization stopped.")
}

func (s *MariaDBSyncer) doInitialFullSyncIfNeeded(ctx context.Context, c *canal.Canal, targetDB *sql.DB) {
	s.logger.Info("[MariaDB] Checking if initial full sync is needed...")
	sourceDB, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Errorf("[MariaDB] Failed to open source DB for initial sync: %v", err)
		return
	}
	defer sourceDB.Close()

	const batchSize = 100
	for _, mapping := range s.cfg.Mappings {
		sourceDBName := mapping.SourceDatabase
		targetDBName := mapping.TargetDatabase
		for _, tableMap := range mapping.Tables {
			exists, errExist := s.targetTableExists(ctx, targetDB, targetDBName, tableMap.TargetTable)
			if errExist != nil {
				s.logger.Errorf("[MariaDB] Could not check if target table %s.%s exists: %v",
					targetDBName, tableMap.TargetTable, errExist)
				continue
			}
			if !exists {
				errCreate := s.createTargetTableAndIndexes(ctx, sourceDB, targetDB,
					sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable)
				if errCreate != nil {
					s.logger.Errorf("[MariaDB] Failed to create target table %s.%s: %v",
						targetDBName, tableMap.TargetTable, errCreate)
					continue
				}
				s.logger.Infof("[MariaDB] Created table %s.%s", targetDBName, tableMap.TargetTable)
			}

			targetCountQuery := fmt.Sprintf("SELECT COUNT(1) FROM %s.%s", targetDBName, tableMap.TargetTable)
			var count int
			if errC := targetDB.QueryRow(targetCountQuery).Scan(&count); errC != nil {
				s.logger.Errorf("[MariaDB] Could not check if table %s.%s is empty: %v",
					targetDBName, tableMap.TargetTable, errC)
				continue
			}
			if count > 0 {
				s.logger.Infof("[MariaDB] Table %s.%s has %d rows, skip initial sync", targetDBName, tableMap.TargetTable, count)
				continue
			}

			s.logger.Infof("[MariaDB] Table %s.%s is empty, doing initial full sync from %s.%s",
				targetDBName, tableMap.TargetTable, sourceDBName, tableMap.SourceTable)

			cols, errCols := s.getColumnsOfTable(ctx, sourceDB, sourceDBName, tableMap.SourceTable)
			if errCols != nil {
				s.logger.Errorf("[MariaDB] Failed to get columns of %s.%s: %v",
					sourceDBName, tableMap.SourceTable, errCols)
				continue
			}

			selectSQL := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(cols, ","), sourceDBName, tableMap.SourceTable)
			srcRows, errQ := sourceDB.QueryContext(ctx, selectSQL)
			if errQ != nil {
				s.logger.Errorf("[MariaDB] Failed to query %s.%s: %v", sourceDBName, tableMap.SourceTable, errQ)
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
					s.logger.Errorf("[MariaDB] Failed to scan row from %s.%s: %v",
						sourceDBName, tableMap.SourceTable, errScan)
					continue
				}
				batchRows = append(batchRows, rowValues)
				if len(batchRows) == batchSize {
					if errB := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows); errB != nil {
						s.logger.Errorf("[MariaDB] Batch insert failed: %v", errB)
					} else {
						insertedCount += len(batchRows)
					}
					batchRows = batchRows[:0]
				}
			}
			srcRows.Close()

			if len(batchRows) > 0 {
				if errB2 := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows); errB2 != nil {
					s.logger.Errorf("[MariaDB] Last batch insert failed: %v", errB2)
				} else {
					insertedCount += len(batchRows)
				}
			}
			s.logger.Infof("[MariaDB] Initial sync for %s.%s -> %s.%s: inserted %d rows",
				sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable, insertedCount)
		}
	}
}

func (s *MariaDBSyncer) targetTableExists(ctx context.Context, db *sql.DB, dbName, tableName string) (bool, error) {
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?`
	var cnt int
	err := db.QueryRowContext(ctx, query, dbName, tableName).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func (s *MariaDBSyncer) createTargetTableAndIndexes(
	ctx context.Context,
	sourceDB, targetDB *sql.DB,
	srcDBName, srcTableName, tgtDBName, tgtTableName string,
) error {
	createStmt, seqs, errGen := s.generateCreateTableSQL(ctx, sourceDB, srcDBName, srcTableName, tgtDBName, tgtTableName)
	if errGen != nil {
		return fmt.Errorf("generateCreateTableSQL fail: %w", errGen)
	}

	for _, seqStmt := range seqs {
		s.logger.Debugf("[MariaDB] Creating sequence => %s", seqStmt)
		if _, errE := targetDB.ExecContext(ctx, seqStmt); errE != nil {
			s.logger.Warnf("[MariaDB] Create sequence fail => %v", errE)
		}
	}

	s.logger.Infof("[MariaDB] Creating table => %s", createStmt)
	if _, errExec := targetDB.ExecContext(ctx, createStmt); errExec != nil {
		return fmt.Errorf("create table fail: %w", errExec)
	}
	return nil
}

func (s *MariaDBSyncer) generateCreateTableSQL(
	ctx context.Context,
	sourceDB *sql.DB,
	srcDBName, srcTableName, tgtDBName, tgtTableName string,
) (string, []string, error) {
	var tableName, createSQL string
	showQuery := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", srcDBName, srcTableName)
	row := sourceDB.QueryRowContext(ctx, showQuery)
	if err := row.Scan(&tableName, &createSQL); err != nil {
		return "", nil, fmt.Errorf("SHOW CREATE TABLE fail: %w", err)
	}

	oldPrefix := fmt.Sprintf("CREATE TABLE `%s`.", srcDBName)
	newPrefix := fmt.Sprintf("CREATE TABLE `%s`.", tgtDBName)
	createSQL = strings.Replace(createSQL, oldPrefix, newPrefix, 1)

	oldTable := fmt.Sprintf("`%s`.`%s`", srcDBName, srcTableName)
	newTable := fmt.Sprintf("`%s`.`%s`", tgtDBName, tgtTableName)
	createSQL = strings.Replace(createSQL, oldTable, newTable, 1)

	var seqs []string
	return createSQL, seqs, nil
}

func (s *MariaDBSyncer) batchInsert(
	ctx context.Context,
	db *sql.DB,
	dbName, tableName string,
	cols []string,
	rows [][]interface{},
) error {
	if len(rows) == 0 {
		return nil
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES",
		dbName,
		tableName,
		strings.Join(cols, ", "))

	singleRowPlaceholder := fmt.Sprintf("(%s)", strings.Join(makeQuestionMarks(len(cols)), ","))
	var allPlaceholder []string
	for range rows {
		allPlaceholder = append(allPlaceholder, singleRowPlaceholder)
	}
	insertSQL = insertSQL + " " + strings.Join(allPlaceholder, ", ")

	var args []interface{}
	for _, rowData := range rows {
		args = append(args, rowData...)
	}
	res, err := db.ExecContext(ctx, insertSQL, args...)
	if err != nil {
		return fmt.Errorf("batchInsert Exec failed: %w", err)
	}
	af, _ := res.RowsAffected()
	s.logger.Infof("[MariaDB][BULK-INSERT] table=%s.%s insertedRows=%d", dbName, tableName, af)
	return nil
}

func makeQuestionMarks(n int) []string {
	res := make([]string, n)
	for i := 0; i < n; i++ {
		res[i] = "?"
	}
	return res
}

func (s *MariaDBSyncer) loadBinlogPosition(path string) *mysql.Position {
	positionDir := filepath.Dir(path)
	if err := os.MkdirAll(positionDir, os.ModePerm); err != nil {
		s.logger.Warnf("[MariaDB] Failed to create dir for position file %s: %v", path, err)
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		s.logger.Infof("[MariaDB] No previous binlog position file at %s: %v", path, err)
		return nil
	}
	if len(data) <= 1 {
		s.logger.Infof("[MariaDB] Binlog position file at %s is empty", path)
		return nil
	}
	var pos mysql.Position
	if errUnmarshal := json.Unmarshal(data, &pos); errUnmarshal != nil {
		s.logger.Errorf("[MariaDB] Failed to unmarshal binlog position from %s: %v", path, errUnmarshal)
		return nil
	}
	return &pos
}

func (s *MariaDBSyncer) parseAddr(dsn string) string {
	parts := strings.Split(dsn, "@tcp(")
	if len(parts) < 2 {
		s.logger.Errorf("[MariaDB] Invalid DSN format: %s", dsn)
		return ""
	}
	addr := strings.Split(parts[1], ")")[0]
	return addr
}

func (s *MariaDBSyncer) parseUserPassword(dsn string) (string, string) {
	parts := strings.Split(dsn, "@")
	if len(parts) < 2 {
		return "", ""
	}
	userInfo := parts[0]
	userParts := strings.Split(userInfo, ":")
	if len(userParts) < 2 {
		return "", ""
	}
	return userParts[0], userParts[1]
}

// ---------------------------------------------------------------------------------------
func (s *MariaDBSyncer) getColumnsOfTable(ctx context.Context, db *sql.DB, database, table string) ([]string, error) {
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
			return nil, fmt.Errorf("failed to scan columns info from table %s.%s: %v", database, table, err)
		}
		if field.Valid {
			cols = append(cols, field.String)
		} else {
			return nil, fmt.Errorf("invalid column name for table %s.%s", database, table)
		}
	}
	return cols, nil
}


type MariaDBEventHandler struct {
	canal.DummyEventHandler
	targetDB          *sql.DB
	mappings          []config.DatabaseMapping
	logger            logrus.FieldLogger
	positionSaverPath string
	canal             *canal.Canal
	lastExecError     int32
}

func (h *MariaDBEventHandler) OnRow(e *canal.RowsEvent) error {
	table := e.Table
	sourceDB := table.Schema
	tableName := table.Name

	var targetDBName, targetTableName string
	found := false
	for _, mapping := range h.mappings {
		if mapping.SourceDatabase == sourceDB {
			for _, tableMap := range mapping.Tables {
				if tableMap.SourceTable == tableName {
					targetDBName = mapping.TargetDatabase
					targetTableName = tableMap.TargetTable
					found = true
					break
				}
			}
		}
		if found {
			break
		}
	}
	if !found {
		h.logger.Debugf("[MariaDB] No mapping found for source table %s.%s => skipping event", sourceDB, tableName)
		return nil
	}

	columnNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		columnNames[i] = col.Name
	}

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			h.handleDML("INSERT", sourceDB, tableName, targetDBName, targetTableName, columnNames, table, row, nil)
		}
	case canal.UpdateAction:
		for i := 0; i < len(e.Rows); i += 2 {
			oldRow := e.Rows[i]
			newRow := e.Rows[i+1]
			h.handleDML("UPDATE", sourceDB, tableName, targetDBName, targetTableName, columnNames, table, newRow, oldRow)
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			h.handleDML("DELETE", sourceDB, tableName, targetDBName, targetTableName, columnNames, table, row, nil)
		}
	}
	return nil
}

func (h *MariaDBEventHandler) handleDML(
	opType, srcDB, srcTable, tgtDB, tgtTable string,
	cols []string,
	table *schema.Table,
	newRow []interface{},
	oldRow []interface{},
) {
	switch opType {
	case "INSERT":
		placeholders := make([]string, len(cols))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			tgtDB, tgtTable,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "))
		res, err := h.targetDB.Exec(query, newRow...)
		if err != nil {
			h.logger.Errorf("[MariaDB][INSERT] table=%s.%s error=%v query=%s", tgtDB, tgtTable, err, query)
			atomic.StoreInt32(&h.lastExecError, 1)
		} else {
			ra, _ := res.RowsAffected()
			h.logger.Infof("[MariaDB][INSERT] table=%s.%s rowsAffected=%d query=%s", tgtDB, tgtTable, ra, query)
			atomic.StoreInt32(&h.lastExecError, 0)
		}
	case "UPDATE":
		setClauses := make([]string, len(cols))
		for i, colName := range cols {
			setClauses[i] = fmt.Sprintf("%s = ?", colName)
		}
		whereClauses := []string{}
		whereVals := []interface{}{}
		for _, pkIndex := range table.PKColumns {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", cols[pkIndex]))
			whereVals = append(whereVals, oldRow[pkIndex])
		}
		if len(whereClauses) == 0 {
			h.logger.Warnf("[MariaDB][UPDATE] table=%s.%s no PK, skip", tgtDB, tgtTable)
			return
		}
		query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
			tgtDB, tgtTable,
			strings.Join(setClauses, ", "),
			strings.Join(whereClauses, " AND "))
		args := append(newRow, whereVals...)
		res, err := h.targetDB.Exec(query, args...)
		if err != nil {
			h.logger.Errorf("[MariaDB][UPDATE] table=%s.%s error=%v query=%s", tgtDB, tgtTable, err, query)
			atomic.StoreInt32(&h.lastExecError, 1)
		} else {
			ra, _ := res.RowsAffected()
			h.logger.Infof("[MariaDB][UPDATE] table=%s.%s rowsAffected=%d query=%s", tgtDB, tgtTable, ra, query)
			atomic.StoreInt32(&h.lastExecError, 0)
		}
	case "DELETE":
		whereClauses := []string{}
		whereVals := []interface{}{}
		for _, pkIndex := range table.PKColumns {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", cols[pkIndex]))
			whereVals = append(whereVals, newRow[pkIndex])
		}
		if len(whereClauses) == 0 {
			h.logger.Warnf("[MariaDB][DELETE] table=%s.%s no PK, skip", tgtDB, tgtTable)
			return
		}
		query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s", tgtDB, tgtTable, strings.Join(whereClauses, " AND "))
		res, err := h.targetDB.Exec(query, whereVals...)
		if err != nil {
			h.logger.Errorf("[MariaDB][DELETE] table=%s.%s error=%v query=%s", tgtDB, tgtTable, err, query)
			atomic.StoreInt32(&h.lastExecError, 1)
		} else {
			ra, _ := res.RowsAffected()
			h.logger.Infof("[MariaDB][DELETE] table=%s.%s rowsAffected=%d query=%s", tgtDB, tgtTable, ra, query)
			atomic.StoreInt32(&h.lastExecError, 0)
		}
	}
}

func (h *MariaDBEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, gs mysql.GTIDSet, force bool) error {
	return nil
}

func (h *MariaDBEventHandler) String() string {
	return "MariaDBEventHandler"
}
