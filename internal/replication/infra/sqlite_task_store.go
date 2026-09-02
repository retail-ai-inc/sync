// Package infra holds the replication context's access to the outside world:
// the sync_tasks table, the monitoring log it reads progress from, and the four
// engine adapters in its subdirectories.
package infra

import (
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/platform/secret"
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
	StageLookup  = "query sync_tasks fail"
	// StageDBFail is kept for callers outside this package that still name it;
	// nothing here uses it any more, because "db fail" said less than
	// "open db fail" for the same failure.
	StageDBFail  = "db fail"
	StageMonitor = "query monitoring_log fail"
)

type Fault struct {
	Stage string
	Err   error
}

func (f *Fault) Error() string { return f.Stage + ": " + f.Err.Error() }
func (f *Fault) Unwrap() error { return f.Err }

func faultAt(stage string, err error) *Fault { return &Fault{Stage: stage, Err: err} }

var ErrNoSuchTask = errors.New("no such sync task")

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
		// The credentials are opened here so the rest of the read path sees a
		// task the way it was written. They are masked again on the way out of
		// the API, so a value that cannot be opened is not worth failing the
		// whole listing for — the task simply shows the sealed form.
		opened, err := secret.OpenTaskConfig(cfgJSON)
		if err != nil {
			logrus.Errorf("Task %d: %v", id, err)
			opened = cfgJSON
		}
		tasks = append(tasks, domain.NewSyncTask(id, enableInt, lastUpdate, lastRun, opened))
	}
	if err := rows.Err(); err != nil {
		return nil, faultAt(StageIterate, err)
	}
	return tasks, nil
}

func InsertTask(enable int, now string, config domain.Config) (int64, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return 0, faultAt(StageOpen, err)
	}
	defer db.Close()

	cfgBytes, _ := json.Marshal(config)
	stored, err := secret.SealTaskConfig(string(cfgBytes))
	if err != nil {
		return 0, faultAt(StageInsert, err)
	}
	res, err := db.Exec(`
INSERT INTO sync_tasks(enable, last_update_time, last_run_time, config_json)
VALUES(?, ?, ?, ?)
`, enable, now, "", stored)
	if err != nil {
		return 0, faultAt(StageInsert, err)
	}
	newID, _ := res.LastInsertId()
	return newID, nil
}

func UpdateTask(id string, enable int, now string, config domain.Config) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		// The same failure as everywhere else, so the same wording: this used to
		// say "db fail" while every other call said "open db fail", and the
		// handler renders the stage.
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	cfgBytes, _ := json.Marshal(config)
	stored, err := secret.SealTaskConfig(string(cfgBytes))
	if err != nil {
		return faultAt(StageUpdate, err)
	}
	res, err := db.Exec(`
UPDATE sync_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, enable, now, stored, id)
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

	// The task's monitoring history goes with it. There is no foreign key and no
	// cascade, so those rows used to stay for good, pointing at an id that names
	// nothing — and monitoring_log is the table that grows without bound.
	if _, err := db.Exec(`DELETE FROM monitoring_log WHERE sync_task_id=?`, id); err != nil {
		logrus.Warnf("[Store] Task %s was deleted but its monitoring history could "+
			"not be: %v", id, err)
	}
	return nil
}

// SetEnable flips a task's enable column and writes the matching status into
// its stored configuration. A configuration that will not parse is replaced
// with a document holding only the status.
func SetEnable(id string, toStart bool) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		// Every other call in this file returns a Fault carrying the stage, so
		// the handler can say which step failed. These two returned the driver's
		// error bare, and "the table is missing", "the row is missing" and "the
		// database is locked" all came out as one word.
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	var oldCfgJSON string
	if err = db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=?`, id).Scan(&oldCfgJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoSuchTask
		}
		return faultAt(StageLookup, err)
	}

	statusStr, newEnable := domain.StatusStopped, 0
	if toStart {
		statusStr, newEnable = domain.StatusRunning, 1
	}

	data := storedConfig(oldCfgJSON)
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
	if err != nil {
		return faultAt(StageUpdate, err)
	}
	return nil
}

// storedConfig decodes a stored configuration document into a map that can be
// written to.
//
// It never returns nil. A document of literal "null" decodes *without an error*
// and leaves the map nil, which an err != nil guard does not catch — and the
// caller then assigns into it, which panics on a nil map. Nothing writes "null"
// today; one hand-edited row or one migration that went wrong would.
func storedConfig(configJSON string) map[string]interface{} {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(configJSON), &data); err != nil || data == nil {
		return make(map[string]interface{})
	}
	return data
}

// ReadTaskConfig returns a task's stored configuration.
//
// It reports why it could not, which ReadTaskEngine below cannot: that answers
// the empty string for a database it could not open, a row that is not there, a
// document that is empty, a document that will not parse and a table that does
// not exist, and the caller reads all five as "not a MongoDB task" and quietly
// falls back to stale figures.
func ReadTaskConfig(id string) (domain.Config, error) {
	var cfg domain.Config

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return cfg, faultAt(StageOpen, err)
	}
	defer db.Close()

	var configJSON string
	if err := db.QueryRow("SELECT config_json FROM sync_tasks WHERE id = ?", id).Scan(&configJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return cfg, ErrNoSuchTask
		}
		return cfg, faultAt(StageLookup, err)
	}
	opened, err := secret.OpenTaskConfig(configJSON)
	if err != nil {
		return cfg, faultAt(StageScan, err)
	}
	if opened == "" {
		return cfg, faultAt(StageScan, errors.New("the stored configuration is empty"))
	}
	if err := json.Unmarshal([]byte(opened), &cfg); err != nil {
		return cfg, faultAt(StageScan, err)
	}
	return cfg, nil
}

// ReadTaskEngine returns the engine named in a task's stored configuration,
// empty when the row is missing or the document will not parse.
func ReadTaskEngine(id string) string {
	cfg, err := ReadTaskConfig(id)
	if err != nil {
		logrus.Warnf("[Store] Could not read the engine of task %s, so it will be "+
			"treated as one this does not measure live: %v", id, err)
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
			// A row that will not scan used to be dropped with a warning, so the
			// table it described simply did not appear in the answer — and "this
			// table had no traffic today" and "this table's row could not be
			// read" look the same from the outside.
			return nil, faultAt(StageScan, err)
		}
		s.SyncedToday = domain.ClampSyncedToday(s.SyncedToday)
		stats = append(stats, s)
	}
	if err := rows.Err(); err != nil {
		return nil, faultAt(StageIterate, err)
	}
	return stats, nil
}
