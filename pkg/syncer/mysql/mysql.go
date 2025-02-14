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
	"github.com/retail-ai-inc/sync/pkg/syncer/common"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLSyncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger
}

func NewMySQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *MySQLSyncer {
	return &MySQLSyncer{
		cfg:    cfg,
		logger: logger.WithField("sync_task_id", cfg.ID),
	}
}

func (s *MySQLSyncer) Start(ctx context.Context) {
	s.logger.Info("[MySQL] Starting synchronization...")

	cfg := canal.NewDefaultConfig()
	if strings.ToLower(s.cfg.Type) == "mariadb" {
		cfg.Flavor = "mariadb"
	} else {
		cfg.Flavor = "mysql"
	}
	cfg.Addr = s.parseAddr(s.cfg.SourceConnection)
	cfg.User, cfg.Password = s.parseUserPassword(s.cfg.SourceConnection)
	cfg.Dump.ExecutionPath = s.cfg.DumpExecutionPath

	var includeTables []string
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			includeTables = append(includeTables, fmt.Sprintf("%s\\.%s", common.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection), table.SourceTable))
		}
	}
	cfg.IncludeTableRegex = includeTables

	var c *canal.Canal
	err := utils.Retry(5, 2*time.Second, 2.0, func() error {
		var e error
		c, e = canal.NewCanal(cfg)
		return e
	})
	if err != nil {
		s.logger.Errorf("[MySQL] Failed to create canal after retries: %v", err)
		return
	}

	var targetDB *sql.DB
	err = utils.Retry(5, 2*time.Second, 2.0, func() error {
		var connErr error
		targetDB, connErr = sql.Open("mysql", s.cfg.TargetConnection)
		if connErr != nil {
			return connErr
		}
		return targetDB.PingContext(ctx)
	})
	if err != nil {
		s.logger.Errorf("[MySQL] Failed to connect to target DB after retries: %v", err)
		return
	}

	// Perform initial sync if target is empty
	s.doInitialSync(ctx, c, targetDB)

	h := &MyEventHandler{
		targetDB:          targetDB,
		mappings:          s.cfg.Mappings,
		logger:            s.logger,
		positionSaverPath: s.cfg.MySQLPositionPath,
		canal:             c,
		lastExecError:     0,
		TargetConnection:  s.cfg.TargetConnection,
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
					data, errPos := json.Marshal(pos)
					if errPos != nil {
						s.logger.Errorf("[MySQL] Failed to marshal binlog position: %v", errPos)
						continue
					}
					if h.positionSaverPath != "" {
						positionDir := filepath.Dir(h.positionSaverPath)
						if errMk := os.MkdirAll(positionDir, os.ModePerm); errMk != nil {
							s.logger.Errorf("[MySQL] Failed to create dir for binlog position => %s: %v", s.cfg.MySQLPositionPath, errMk)
							continue
						}
						if errWrite := os.WriteFile(h.positionSaverPath, data, 0644); errWrite != nil {
							s.logger.Errorf("[MySQL] Failed to write binlog position to %s: %v", h.positionSaverPath, errWrite)
						} else {
							s.logger.Debugf("[MySQL] Timer => saved position %v", pos)
						}
					}
				} else {
					s.logger.Warn("[MySQL] Timer => lastExecError != 0, skip saving binlog position")
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
				s.logger.Warnf("[MySQL] canal run context canceled => %v", runErr)
			} else {
				s.logger.Errorf("[MySQL] Failed to run canal => %v", runErr)
			}
		}
	}()

	<-ctx.Done()
	s.logger.Info("[MySQL] Synchronization stopped.")
}

func (s *MySQLSyncer) doInitialSync(ctx context.Context, c *canal.Canal, targetDB *sql.DB) {
	s.logger.Info("[MySQL] Checking if initial full sync is needed...")

	sourceDB, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Errorf("[MySQL] Failed to open source DB: %v", err)
		return
	}
	defer sourceDB.Close()

	const batchSize = 100
	sourceDBName := common.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	targetDBName := common.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection)

	for _, mapping := range s.cfg.Mappings {
		for _, tableMap := range mapping.Tables {
			exists, errExist := s.targetTableExists(ctx, targetDB, targetDBName, tableMap.TargetTable)
			if errExist != nil {
				s.logger.Errorf("[MySQL] Could not check if target table %s.%s exists: %v", targetDBName, tableMap.TargetTable, errExist)
				continue
			}
			if !exists {
				if errCreate := s.createTargetTableAndIndexes(ctx, sourceDB, targetDB, sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable); errCreate != nil {
					s.logger.Errorf("[MySQL] Failed to create target table %s.%s: %v", targetDBName, tableMap.TargetTable, errCreate)
					continue
				}
				s.logger.Infof("[MySQL] Created table %s.%s from source %s.%s", targetDBName, tableMap.TargetTable, sourceDBName, tableMap.SourceTable)
			}

			targetCountQuery := fmt.Sprintf("SELECT COUNT(1) FROM %s.%s", targetDBName, tableMap.TargetTable)
			var count int
			if errC := targetDB.QueryRow(targetCountQuery).Scan(&count); errC != nil {
				s.logger.Errorf("[MySQL] Could not check if table %s.%s is empty: %v", targetDBName, tableMap.TargetTable, errC)
				continue
			}
			if count > 0 {
				s.logger.Infof("[MySQL] table %s.%s has %d rows => skip initial sync", targetDBName, tableMap.TargetTable, count)
				continue
			}

			s.logger.Infof("[MySQL] Doing initial full sync from %s.%s => %s.%s", sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable)

			cols, errCols := s.getTableColumns(ctx, sourceDB, sourceDBName, tableMap.SourceTable)
			if errCols != nil {
				s.logger.Errorf("[MySQL] get columns fail => %s.%s => %v", sourceDBName, tableMap.SourceTable, errCols)
				continue
			}

			selectSQL := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(cols, ","), sourceDBName, tableMap.SourceTable)
			srcRows, errQ := sourceDB.QueryContext(ctx, selectSQL)
			if errQ != nil {
				s.logger.Errorf("[MySQL] query fail => %s.%s => %v", sourceDBName, tableMap.SourceTable, errQ)
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
					s.logger.Errorf("[MySQL] scan row fail => %s.%s => %v", sourceDBName, tableMap.SourceTable, errScan)
					continue
				}
				batchRows = append(batchRows, rowValues)
				if len(batchRows) == batchSize {
					if errB := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows); errB != nil {
						s.logger.Errorf("[MySQL] batchInsert fail => %v", errB)
					} else {
						insertedCount += len(batchRows)
					}
					batchRows = batchRows[:0]
				}
			}
			srcRows.Close()

			if len(batchRows) > 0 {
				if errB2 := s.batchInsert(ctx, targetDB, targetDBName, tableMap.TargetTable, cols, batchRows); errB2 != nil {
					s.logger.Errorf("[MySQL] batchInsert fail => %v", errB2)
				} else {
					insertedCount += len(batchRows)
				}
			}
			s.logger.Infof("[MySQL] initial sync => %s.%s => %s.%s inserted=%d rows",
				sourceDBName, tableMap.SourceTable, targetDBName, tableMap.TargetTable, insertedCount)
		}
	}
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
	sourceDB, targetDB *sql.DB,
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
	s.logger.Infof("[MySQL] Creating table => %s", createStmt)
	if _, errExec := targetDB.ExecContext(ctx, createStmt); errExec != nil {
		return fmt.Errorf("create table fail: %w", errExec)
	}
	return nil
}

func (s *MySQLSyncer) generateCreateTableSQL(
	ctx context.Context,
	sourceDB *sql.DB,
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
	insertSQL := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES", dbName, tableName, strings.Join(cols, ", "))
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
		return fmt.Errorf("batchInsert Exec => %w", err)
	}
	ra, _ := res.RowsAffected()
	s.logger.Infof("[MySQL][BULK-INSERT] table=%s.%s insertedRows=%d", dbName, tableName, ra)
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
		s.logger.Warnf("[MySQL] create dir for position file => %s => %v", path, err)
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		s.logger.Infof("[MySQL] No binlog position file => %s => %v", path, err)
		return nil
	}
	if len(data) <= 1 {
		s.logger.Infof("[MySQL] binlog position file => %s => empty", path)
		return nil
	}
	var pos mysql.Position
	if errU := json.Unmarshal(data, &pos); errU != nil {
		s.logger.Errorf("[MySQL] unmarshal binlog position => %s => %v", path, errU)
		return nil
	}
	return &pos
}

func (s *MySQLSyncer) parseAddr(dsn string) string {
	parts := strings.Split(dsn, "@tcp(")
	if len(parts) < 2 {
		s.logger.Errorf("[MySQL] Invalid DSN => %s", dsn)
		return ""
	}
	addr := strings.Split(parts[1], ")")[0]
	return addr
}

func (s *MySQLSyncer) parseUserPassword(dsn string) (string, string) {
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

func (s *MySQLSyncer) getTableColumns(ctx context.Context, db *sql.DB, database, table string) ([]string, error) {
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
	targetDB          *sql.DB
	mappings          []config.DatabaseMapping
	logger            logrus.FieldLogger
	positionSaverPath string
	canal             *canal.Canal
	lastExecError     int32
	TargetConnection  string
}

func (h *MyEventHandler) OnRow(e *canal.RowsEvent) error {
	table := e.Table
	sourceDB := table.Schema
	tableName := table.Name

	var targetTableName string
	targetDBName := common.GetDatabaseName("mysql", h.TargetConnection)
	found := false
	for _, mapping := range h.mappings {
		for _, tableMap := range mapping.Tables {
			if tableMap.SourceTable == tableName {
				targetTableName = tableMap.TargetTable
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		h.logger.Debugf("[MySQL] No mapping found for source table %s.%s => skip event", sourceDB, tableName)
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

func (h *MyEventHandler) handleDML(
	opType, srcDB, srcTable, tgtDB, tgtTable string,
	cols []string,
	table *schema.Table,
	newRow []interface{},
	oldRow []interface{},
) {
	var query string

	switch opType {
	case "INSERT":
		placeholders := make([]string, len(cols))
		for i := range placeholders {
			placeholders[i] = "?"
		}
		query = fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES (%s)",
			tgtDB, tgtTable,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)
		h.logger.Debugf("[MySQL][INSERT] table=%s.%s query=%s", tgtDB, tgtTable, query)

		res, err := h.targetDB.Exec(query, newRow...)
		if err != nil {
			h.logger.Errorf("[MySQL][INSERT] table=%s.%s error=%v", tgtDB, tgtTable, err)
			atomic.StoreInt32(&h.lastExecError, 1)
		} else {
			ra, _ := res.RowsAffected()
			h.logger.Infof("[MySQL][INSERT] table=%s.%s rowsAffected=%d", tgtDB, tgtTable, ra)
			atomic.StoreInt32(&h.lastExecError, 0)
		}
	case "UPDATE":
		setClauses := make([]string, len(cols))
		for i, colName := range cols {
			setClauses[i] = fmt.Sprintf("%s = ?", colName)
		}
		var whereClauses []string
		var whereVals []interface{}
		for _, pkIndex := range table.PKColumns {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", cols[pkIndex]))
			whereVals = append(whereVals, oldRow[pkIndex])
		}
		if len(whereClauses) == 0 {
			h.logger.Warnf("[MySQL][UPDATE] table=%s.%s no PK => skip", tgtDB, tgtTable)
			return
		}
		query = fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s",
			tgtDB, tgtTable,
			strings.Join(setClauses, ", "),
			strings.Join(whereClauses, " AND "),
		)
		h.logger.Debugf("[MySQL][UPDATE] table=%s.%s query=%s", tgtDB, tgtTable, query)

		args := append(newRow, whereVals...)
		res, err := h.targetDB.Exec(query, args...)
		if err != nil {
			h.logger.Errorf("[MySQL][UPDATE] table=%s.%s error=%v", tgtDB, tgtTable, err)
			atomic.StoreInt32(&h.lastExecError, 1)
		} else {
			ra, _ := res.RowsAffected()
			h.logger.Infof("[MySQL][UPDATE] table=%s.%s rowsAffected=%d", tgtDB, tgtTable, ra)
			atomic.StoreInt32(&h.lastExecError, 0)
		}
	case "DELETE":
		var whereClauses []string
		var whereVals []interface{}
		for _, pkIndex := range table.PKColumns {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", cols[pkIndex]))
			whereVals = append(whereVals, newRow[pkIndex])
		}
		if len(whereClauses) == 0 {
			h.logger.Warnf("[MySQL][DELETE] table=%s.%s no PK => skip", tgtDB, tgtTable)
			return
		}
		query = fmt.Sprintf("DELETE FROM %s.%s WHERE %s", tgtDB, tgtTable, strings.Join(whereClauses, " AND "))
		h.logger.Debugf("[MySQL][DELETE] table=%s.%s query=%s", tgtDB, tgtTable, query)

		res, err := h.targetDB.Exec(query, whereVals...)
		if err != nil {
			h.logger.Errorf("[MySQL][DELETE] table=%s.%s error=%v", tgtDB, tgtTable, err)
			atomic.StoreInt32(&h.lastExecError, 1)
		} else {
			ra, _ := res.RowsAffected()
			h.logger.Infof("[MySQL][DELETE] table=%s.%s rowsAffected=%d", tgtDB, tgtTable, ra)
			atomic.StoreInt32(&h.lastExecError, 0)
		}
	}
}

func (h *MyEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, gs mysql.GTIDSet, force bool) error {
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}
