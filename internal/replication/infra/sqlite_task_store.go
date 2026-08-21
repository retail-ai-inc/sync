// Package infra holds the replication context's access to the outside world:
// the sync_tasks table, the monitoring log it reads progress from, and the four
// engine adapters in its subdirectories.
package infra

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

// Stages a store call can fail at, named by the message the endpoint answers
// with.
const (
	StageOpen    = "open db fail"
	StageQuery   = "query sync_tasks fail"
	StageScan    = "scan sync_tasks fail"
	StageIterate = "sync_tasks iteration error"
	StageInsert  = "insert fail"
	StageUpdate  = "update fail"
	StageDelete  = "delete fail"
	StageDBFail  = "db fail"
	StageMonitor = "query monitoring_log fail"
)

// Fault is a store failure tagged with the message the endpoint answers with.
type Fault struct {
	Stage string
	Err   error
}

func (f *Fault) Error() string { return f.Stage + ": " + f.Err.Error() }
func (f *Fault) Unwrap() error { return f.Err }

func faultAt(stage string, err error) *Fault { return &Fault{Stage: stage, Err: err} }

// ErrNoSuchTask means the id names no row.
var ErrNoSuchTask = errors.New("no such sync task")

// ListTasks returns every task, oldest id first.
func ListTasks() ([]domain.SyncTask, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, faultAt(StageOpen, err)
	}
	defer db.Close()

	rows, err := db.Query(`
SELECT
  id,
  enable,
  COALESCE(last_update_time,''),
  COALESCE(last_run_time,''),
  config_json
FROM sync_tasks
ORDER BY id ASC
`)
	if err != nil {
		return nil, faultAt(StageQuery, err)
	}
	defer rows.Close()

	var tasks []domain.SyncTask
	for rows.Next() {
		var (
			id         int
			enableInt  int
			lastUpdate string
			lastRun    string
			cfgJSON    string
		)
		if err := rows.Scan(&id, &enableInt, &lastUpdate, &lastRun, &cfgJSON); err != nil {
			return nil, faultAt(StageScan, err)
		}
		logrus.Debugf("Configuration JSON from database: %s", cfgJSON)
		tasks = append(tasks, domain.NewSyncTask(id, enableInt, lastUpdate, lastRun, cfgJSON))
	}
	if err := rows.Err(); err != nil {
		return nil, faultAt(StageIterate, err)
	}
	return tasks, nil
}

// InsertTask stores a new task and returns its id.
func InsertTask(enable int, now string, config domain.Config) (int64, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return 0, faultAt(StageOpen, err)
	}
	defer db.Close()

	cfgBytes, _ := json.Marshal(config)
	res, err := db.Exec(`
INSERT INTO sync_tasks(enable, last_update_time, last_run_time, config_json)
VALUES(?, ?, ?, ?)
`, enable, now, "", string(cfgBytes))
	if err != nil {
		return 0, faultAt(StageInsert, err)
	}
	newID, _ := res.LastInsertId()
	return newID, nil
}

// UpdateTask replaces a task's configuration and enable column.
func UpdateTask(id string, enable int, now string, config domain.Config) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return faultAt(StageDBFail, err)
	}
	defer db.Close()

	cfgBytes, _ := json.Marshal(config)
	res, err := db.Exec(`
UPDATE sync_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, enable, now, string(cfgBytes), id)
	if err != nil {
		return faultAt(StageUpdate, err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		return ErrNoSuchTask
	}
	return nil
}

// DeleteTask removes a task. It reports ErrNoSuchTask when the id matched no
// row.
func DeleteTask(id string) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	res, err := db.Exec(`DELETE FROM sync_tasks WHERE id=?`, id)
	if err != nil {
		return faultAt(StageDelete, err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		return ErrNoSuchTask
	}
	return nil
}

// SetEnable flips a task's enable column and writes the matching status into
// its stored configuration. A configuration that will not parse is replaced
// with a document holding only the status.
func SetEnable(id string, toStart bool) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return err
	}
	defer db.Close()

	var oldCfgJSON string
	if err = db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=?`, id).Scan(&oldCfgJSON); err != nil {
		return err
	}

	statusStr, newEnable := domain.StatusStopped, 0
	if toStart {
		statusStr, newEnable = domain.StatusRunning, 1
	}

	var data map[string]interface{}
	if err2 := json.Unmarshal([]byte(oldCfgJSON), &data); err2 != nil {
		data = make(map[string]interface{})
	}
	data["status"] = statusStr
	newBytes, _ := json.Marshal(data)

	nowStr := httpx.TimeNowStr()
	_, err = db.Exec(`
UPDATE sync_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, newEnable, nowStr, string(newBytes), id)
	return err
}

// ReadTaskEngine returns the engine named in a task's stored configuration,
// empty when the row is missing or the document will not parse.
func ReadTaskEngine(id string) string {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return ""
	}
	defer db.Close()

	var configJSON string
	if err := db.QueryRow("SELECT config_json FROM sync_tasks WHERE id = ?", id).Scan(&configJSON); err != nil || configJSON == "" {
		return ""
	}
	var cfg struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
		return ""
	}
	return cfg.Type
}

// TodayTableStats reports each table's replication progress for the UTC day
// that contains now.
//
// The window is a UTC calendar day while the endpoint labels the answer with a
// JST date, so the figures belong to a different day than the label says
// (T-055).
func TodayTableStats(id string, now time.Time) ([]domain.TableStat, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, faultAt(StageOpen, err)
	}
	defer db.Close()

	todayStart := now.Format("2006-01-02") + " 00:00:00"
	todayEnd := now.Format("2006-01-02") + " 23:59:59"

	rows, err := db.Query(`
		SELECT
			tgt_table,
			MAX(tgt_row_count) - MIN(tgt_row_count) AS synced_today,
			MAX(tgt_row_count) AS total_rows,
			MAX(logged_at) AS last_sync_time
		FROM monitoring_log
		WHERE sync_task_id = ?
		AND logged_at BETWEEN ? AND ?
		GROUP BY tgt_table
	`, id, todayStart, todayEnd)
	if err != nil {
		return nil, faultAt(StageMonitor, err)
	}
	defer rows.Close()

	stats := make([]domain.TableStat, 0)
	for rows.Next() {
		var s domain.TableStat
		if err := rows.Scan(&s.TableName, &s.SyncedToday, &s.TotalRows, &s.LastSyncTime); err != nil {
			logrus.Warnf("[SyncTables] Error scanning row: %v", err)
			continue
		}
		s.SyncedToday = domain.ClampSyncedToday(s.SyncedToday)
		stats = append(stats, s)
	}
	return stats, nil
}
