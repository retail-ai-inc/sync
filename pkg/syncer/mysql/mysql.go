package mysql

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

// MySQLSyncer is the synchronizer for MySQL
type MySQLSyncer struct {
	cfg    config.SyncConfig
	logger *logrus.Logger
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *MySQLSyncer {
	return &MySQLSyncer{
		cfg:    cfg,
		logger: logger,
	}
}

func (s *MySQLSyncer) Start(ctx context.Context) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = s.parseAddr(s.cfg.SourceConnection)
	cfg.User, cfg.Password = s.parseUserPassword(s.cfg.SourceConnection)
	cfg.Dump.ExecutionPath = s.cfg.DumpExecutionPath

	includeTables := []string{}
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			includeTables = append(includeTables, fmt.Sprintf("%s\\.%s", mapping.SourceDatabase, table.SourceTable))
		}
	}
	cfg.IncludeTableRegex = includeTables

	c, err := canal.NewCanal(cfg)
	if err != nil {
		s.logger.Fatalf("[MySQL] Failed to create canal: %v", err)
	}

	targetDB, err := sql.Open("mysql", s.cfg.TargetConnection)
	if err != nil {
		s.logger.Fatalf("[MySQL] Failed to connect to target: %v", err)
	}

	s.doInitialFullSyncIfNeeded(ctx, c, targetDB)

	h := &MyEventHandler{
		targetDB:          targetDB,
		mappings:          s.cfg.Mappings,
		logger:            s.logger,
		positionSaverPath: s.cfg.MySQLPositionPath,
		canal:             c,
		lastExecError:     0, // 0=ok, 1=error
	}
	c.SetEventHandler(h)

	var startPos *mysql.Position
	if s.cfg.MySQLPositionPath != "" {
		startPos = s.loadBinlogPosition(s.cfg.MySQLPositionPath)
		if startPos != nil {
			s.logger.Infof("[MySQL] Starting canal from saved position: %v", *startPos)
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
					data, err := json.Marshal(pos)
					if err != nil {
						s.logger.Errorf("[MySQL] Failed to marshal binlog position: %v", err)
						continue
					}
					if h.positionSaverPath != "" {
						positionDir := filepath.Dir(h.positionSaverPath)
						if err := os.MkdirAll(positionDir, os.ModePerm); err != nil {
							s.logger.Errorf("[MySQL] Failed to create directory for position file %s: %v", h.positionSaverPath, err)
							continue
						}
						if err := os.WriteFile(h.positionSaverPath, data, 0644); err != nil {
							s.logger.Errorf("[MySQL] Failed to write binlog position to %s: %v", h.positionSaverPath, err)
						} else {
							s.logger.Debugf("[MySQL] Timer: saved position %v", pos)
						}
					}
				} else {
					s.logger.Warn("[MySQL] Timer: lastExecError != 0, skip saving binlog position")
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
				s.logger.Warnf("[MySQL] canal run context canceled, normal exit: %v", runErr)
			} else {
				s.logger.Fatalf("[MySQL] Failed to run canal: %v", runErr)
			}
		}
	}()

	<-ctx.Done()
	s.logger.Info("[MySQL] Synchronization stopped.")
}

func (s *MySQLSyncer) doInitialFullSyncIfNeeded(ctx context.Context, c *canal.Canal, targetDB *sql.DB) {
	sourceDB, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Fatalf("[MySQL] Failed to open source DB: %v", err)
	}
	defer sourceDB.Close()

	const batchSize = 100

	for _, mapping := range s.cfg.Mappings {
		sourceDBName := mapping.SourceDatabase
		targetDBName := mapping.TargetDatabase

		for _, tableMap := range mapping.Tables {
			targetCountQuery := fmt.Sprintf("SELECT COUNT(1) FROM %s.%s", targetDBName, tableMap.TargetTable)
			var count int
			if err := targetDB.QueryRow(targetCountQuery).Scan(&count); err != nil {
				s.logger.Errorf("[MySQL] Could not check if target table %s.%s is empty: %v",
					targetDBName, tableMap.TargetTable, err)
				continue
			}
			if count > 0 {
				s.logger.Infof("[MySQL] Target table %s.%s has %d rows, skipping initial sync", targetDBName, tableMap.TargetTable, count)
				continue
			}

			s.logger.Infof("[MySQL] Target table %s.%s is empty, doing initial full sync from %s.%s",
				targetDBName, tableMap.TargetTable, sourceDBName, tableMap.SourceTable)

			cols, err := s.getColumnsOfTable(ctx, sourceDB, sourceDBName, tableMap.SourceTable)
			if err != nil {
				s.logger.Errorf("[MySQL] Failed to get columns of %s.%s: %v", sourceDBName, tableMap.SourceTable, err)
				continue
			}

			selectSQL := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(cols, ","), sourceDBName, tableMap.SourceTable)
			srcRows, err := sourceDB.QueryContext(ctx, selectSQL)
			if err != nil {
				s.logger.Errorf("[MySQL] Failed to query %s.%s: %v", sourceDBName, tableMap.SourceTable, err)
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
				if err := srcRows.Scan(valuePtrs...); err != nil {
					s.logger.Errorf("[MySQL] Failed to scan row from %s.%s: %v", sourceDBName, tableMap.SourceTable, err)
					continue
				}

				batchRows = append(batchRows, rowValues)
				if len(batchRows) == batchSize {
					err := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows)
					if err != nil {
						s.logger.Errorf("[MySQL] Batch insert failed: %v", err)
					} else {
						insertedCount += len(batchRows)
					}
					batchRows = batchRows[:0]
				}
			}
			srcRows.Close()

			if len(batchRows) > 0 {
				err := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows)
				if err != nil {
					s.logger.Errorf("[MySQL] Last batch insert failed: %v", err)
				} else {
					insertedCount += len(batchRows)
				}
			}

			s.logger.Infof("[MySQL] Initial sync done for %s.%s -> %s.%s, inserted %d rows",
				sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable, insertedCount)
		}
	}
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

	_, err := db.ExecContext(ctx, insertSQL, args...)
	if err != nil {
		return fmt.Errorf("batchInsert Exec failed: %w", err)
	}
	return nil
}

func makeQuestionMarks(n int) []string {
	res := make([]string, n)
	for i := 0; i < n; i++ {
		res[i] = "?"
	}
	return res
}

func (s *MySQLSyncer) loadBinlogPosition(path string) *mysql.Position {
	positionDir := filepath.Dir(path)
	if err := os.MkdirAll(positionDir, os.ModePerm); err != nil {
		s.logger.Errorf("[MySQL] Failed to create directory for position file %s: %v", path, err)
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		s.logger.Infof("[MySQL] No previous binlog position file at %s: %v", path, err)
		return nil
	}
	if len(data) <= 1 {
		s.logger.Infof("[MySQL] Binlog position file for %s is empty", path)
		return nil
	}
	var pos mysql.Position
	if err := json.Unmarshal(data, &pos); err != nil {
		s.logger.Errorf("[MySQL] Failed to unmarshal binlog position from %s: %v", path, err)
		return nil
	}
	return &pos
}

func (s *MySQLSyncer) parseAddr(dsn string) string {
	parts := strings.Split(dsn, "@tcp(")
	if len(parts) < 2 {
		s.logger.Fatalf("[MySQL] Invalid DSN format: %s", dsn)
	}
	addr := strings.Split(parts[1], ")")[0]
	return addr
}

func (s *MySQLSyncer) parseUserPassword(dsn string) (string, string) {
	parts := strings.Split(dsn, "@")
	if len(parts) < 2 {
		s.logger.Fatalf("[MySQL] Invalid DSN format: %s", dsn)
	}
	userInfo := parts[0]
	userParts := strings.Split(userInfo, ":")
	if len(userParts) < 2 {
		s.logger.Fatalf("[MySQL] Invalid DSN user info: %s", userInfo)
	}
	return userParts[0], userParts[1]
}

func (s *MySQLSyncer) getColumnsOfTable(ctx context.Context, db *sql.DB, database, table string) ([]string, error) {
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

// ------------------ Custom Event Handler ------------------

type MyEventHandler struct {
	canal.DummyEventHandler
	targetDB          *sql.DB
	mappings          []config.DatabaseMapping
	logger            *logrus.Logger
	positionSaverPath string
	canal             *canal.Canal

	// 0=ok, 1=error
	lastExecError int32
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
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
		h.logger.Warnf("[MySQL] No mapping found for source table %s.%s", sourceDB, tableName)
		return nil
	}

	columnNames := make([]string, len(table.Columns))
	for i, col := range table.Columns {
		columnNames[i] = col.Name
	}

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			h.handleInsert(sourceDB, tableName, targetDBName, targetTableName, columnNames, row)
		}
	case canal.UpdateAction:
		for i := 0; i < len(e.Rows); i += 2 {
			oldRow := e.Rows[i]
			newRow := e.Rows[i+1]
			h.handleUpdate(sourceDB, tableName, targetDBName, targetTableName, columnNames, table, oldRow, newRow)
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			h.handleDelete(sourceDB, tableName, targetDBName, targetTableName, columnNames, table, row)
		}
	}
	return nil
}

func (h *MyEventHandler) handleInsert(
	sourceDB, sourceTable, targetDBName, targetTableName string,
	columnNames []string,
	row []interface{},
) {
	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}
	query := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
		targetDBName, targetTableName,
		strings.Join(columnNames, ", "),
		strings.Join(placeholders, ", "))
	_, err := h.targetDB.Exec(query, row...)
	if err != nil {
		h.logger.Errorf("[MySQL] [INSERT] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} Insert row error: %v", sourceDB, sourceTable, targetDBName, targetTableName, err)
		atomic.StoreInt32(&h.lastExecError, 1)
	} else {
		h.logger.Debugf("[MySQL] [INSERT] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} Values: %+v", sourceDB, sourceTable, targetDBName, targetTableName, row)
		atomic.StoreInt32(&h.lastExecError, 0)
	}
}

func (h *MyEventHandler) handleUpdate(
	sourceDB, sourceTable, targetDBName, targetTableName string,
	columnNames []string,
	table *schema.Table,
	oldRow, newRow []interface{},
) {
	setClauses := make([]string, len(columnNames))
	for i, col := range columnNames {
		setClauses[i] = fmt.Sprintf("%s = ?", col)
	}
	whereClauses := []string{}
	whereValues := []interface{}{}

	for _, pkIndex := range table.PKColumns {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", columnNames[pkIndex]))
		whereValues = append(whereValues, oldRow[pkIndex])
	}

	if len(whereClauses) == 0 {
		h.logger.Warnf("[MySQL] [UPDATE] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} No primary key", sourceDB, sourceTable, targetDBName, targetTableName)
		return
	}
	query := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
		targetDBName, targetTableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	args := append(newRow, whereValues...)
	_, err := h.targetDB.Exec(query, args...)
	if err != nil {
		h.logger.Errorf("[MySQL] [UPDATE] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} Update row error: %v", sourceDB, sourceTable, targetDBName, targetTableName, err)
		atomic.StoreInt32(&h.lastExecError, 1)
	} else {
		h.logger.Debugf("[MySQL] [UPDATE] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} Old Values: %+v, New Values: %+v", sourceDB, sourceTable, targetDBName, targetTableName, oldRow, newRow)
		atomic.StoreInt32(&h.lastExecError, 0)
	}
}

func (h *MyEventHandler) handleDelete(
	sourceDB, sourceTable, targetDBName, targetTableName string,
	columnNames []string,
	table *schema.Table,
	row []interface{},
) {
	whereClauses := []string{}
	whereValues := []interface{}{}

	for _, pkIndex := range table.PKColumns {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", columnNames[pkIndex]))
		whereValues = append(whereValues, row[pkIndex])
	}
	if len(whereClauses) == 0 {
		h.logger.Warnf("[MySQL] [DELETE] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} No primary key", sourceDB, sourceTable, targetDBName, targetTableName)
		return
	}
	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		targetDBName, targetTableName, strings.Join(whereClauses, " AND "))
	_, err := h.targetDB.Exec(query, whereValues...)
	if err != nil {
		h.logger.Errorf("[MySQL] [DELETE] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} Delete error: %v", sourceDB, sourceTable, targetDBName, targetTableName, err)
		atomic.StoreInt32(&h.lastExecError, 1)
	} else {
		h.logger.Debugf("[MySQL] [DELETE] {src_db: %s, src_table: %s} => {dst_db: %s, dst_table: %s} Deleted row PK: %+v", sourceDB, sourceTable, targetDBName, targetTableName, whereValues)
		atomic.StoreInt32(&h.lastExecError, 0)
	}
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func (h *MyEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, gs mysql.GTIDSet, force bool) error {
	return nil
}
