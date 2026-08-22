// Package infra holds the backup context's access to the outside world: the
// backup_tasks table, the export commands, the transfer commands and the system
// crontab.
package infra

import (
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/retail-ai-inc/sync/internal/backup/domain"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// Stages a store call can fail at. The endpoints answer with a different
// message per stage, so the store says which one it reached.
const (
	StageOpen        = "open db fail"
	StageQuery       = "query backup_tasks fail"
	StageScan        = "scan backup_tasks fail"
	StageIterate     = "backup_tasks iteration error"
	StageInsert      = "insert fail"
	StageUpdate      = "update fail"
	StageDelete      = "delete fail"
	StageLookup      = "query fail"
	StageSelect      = "select fail"
	StageFetchConfig = "fetch existing config fail"
	StageDBFail      = "db fail"
)

// Fault is a store failure tagged with the message the endpoint answers with.
type Fault struct {
	Stage string
	Err   error
}

func (f *Fault) Error() string { return f.Stage + ": " + f.Err.Error() }
func (f *Fault) Unwrap() error { return f.Err }

func faultAt(stage string, err error) *Fault { return &Fault{Stage: stage, Err: err} }

// ErrNoSuchJob means the id names no row. The endpoints answer with a 200 and
// success:false rather than a 404.
var ErrNoSuchJob = errors.New("no such backup job")

// ListJobs returns every job, oldest id first.
func ListJobs() ([]domain.BackupJob, error) {
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
  COALESCE(last_backup_time,''),
  COALESCE(next_backup_time,''),
  config_json,
  COALESCE(last_run_time,''),
  COALESCE(last_run_status,''),
  COALESCE(last_run_message,'')
FROM backup_tasks
ORDER BY id ASC
`)
	if err != nil {
		return nil, faultAt(StageQuery, err)
	}
	defer rows.Close()

	var jobs []domain.BackupJob
	for rows.Next() {
		var (
			id         int
			enableInt  int
			lastUpdate string
			lastBackup string
			nextBackup string
			cfgJSON    string
			outcome    domain.RunOutcome
		)
		if err := rows.Scan(&id, &enableInt, &lastUpdate, &lastBackup, &nextBackup, &cfgJSON,
			&outcome.At, &outcome.Status, &outcome.Message); err != nil {
			return nil, faultAt(StageScan, err)
		}
		job := domain.NewBackupJob(id, enableInt, lastUpdate, lastBackup, nextBackup, cfgJSON)
		job.SetLastRun(outcome)
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, faultAt(StageIterate, err)
	}
	return jobs, nil
}

// InsertJob stores a new job and returns its id.
func InsertJob(enable int, now, nextBackup string, config domain.Config) (int64, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return 0, faultAt(StageOpen, err)
	}
	defer db.Close()

	cfgBytes, _ := json.Marshal(config)
	res, err := db.Exec(`
INSERT INTO backup_tasks(enable, last_update_time, last_backup_time, next_backup_time, config_json)
VALUES(?, ?, ?, ?, ?)
`, enable, now, "", nextBackup, string(cfgBytes))
	if err != nil {
		return 0, faultAt(StageInsert, err)
	}
	newID, _ := res.LastInsertId()
	return newID, nil
}

// ReadJobRow returns the stored configuration document and the enable column
// of one job, in that order, as the update endpoint reads them.
func ReadJobRow(id string) (configJSON string, enable int, err error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return "", 0, faultAt(StageOpen, err)
	}
	defer db.Close()

	if err = db.QueryRow(`SELECT config_json, enable FROM backup_tasks WHERE id=?`, id).
		Scan(&configJSON, &enable); err != nil {
		return "", 0, faultAt(StageFetchConfig, err)
	}
	return configJSON, enable, nil
}

// UpdateJob replaces a job's configuration. It reports ErrNoSuchJob when the id
// matched no row.
func UpdateJob(id, now, nextBackup string, config domain.Config) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	cfgBytes, _ := json.Marshal(config)
	res, err := db.Exec(`
UPDATE backup_tasks
SET last_update_time=?,
    next_backup_time=?,
    config_json=?
WHERE id=?
`, now, nextBackup, string(cfgBytes), id)
	if err != nil {
		return faultAt(StageUpdate, err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		return ErrNoSuchJob
	}
	return nil
}

// DeleteJob removes a job. It reports ErrNoSuchJob when the id matched no row.
func DeleteJob(id string) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	res, err := db.Exec(`DELETE FROM backup_tasks WHERE id=?`, id)
	if err != nil {
		return faultAt(StageDelete, err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		return ErrNoSuchJob
	}
	return nil
}

// SetEnable flips a job's enable column and writes the matching status into its
// stored configuration. A configuration that will not parse is replaced with a
// document holding only the status.
func SetEnable(id string, enable bool, now string) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		// Every other call here returns a Fault carrying the stage, so the
		// handler can say which step failed. This one returned the driver's
		// error bare, and "the table is missing", "the row is missing" and "the
		// database is locked" all came out as "pause fail".
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	var oldCfgJSON string
	if err = db.QueryRow(`SELECT config_json FROM backup_tasks WHERE id=?`, id).Scan(&oldCfgJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNoSuchJob
		}
		return faultAt(StageLookup, err)
	}

	statusStr, newEnable := domain.StatusDisabled, 0
	if enable {
		statusStr, newEnable = domain.StatusEnabled, 1
	}

	// See ParseStoredConfig: a stored "null" decodes without an error into a nil
	// map, and assigning into one panics.
	data := domain.ParseStoredConfig(oldCfgJSON)
	data["status"] = statusStr
	newBytes, _ := json.Marshal(data)

	_, err = db.Exec(`
UPDATE backup_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, newEnable, now, string(newBytes), id)
	if err != nil {
		return faultAt(StageUpdate, err)
	}
	return nil
}

// JobExists reports whether an id names a row.
func JobExists(id string) (bool, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return false, faultAt(StageOpen, err)
	}
	defer db.Close()

	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM backup_tasks WHERE id=?", id).Scan(&count); err != nil {
		return false, faultAt(StageLookup, err)
	}
	return count > 0, nil
}

// StampLastBackup records that a job ran, without running it.
func StampLastBackup(id, now string) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	res, err := db.Exec("UPDATE backup_tasks SET last_backup_time=? WHERE id=?", now, id)
	if err != nil {
		return faultAt(StageUpdate, err)
	}
	// An id that matches no row used to be a success, so "run this backup now"
	// answered "started successfully" for a job that does not exist.
	if ra, _ := res.RowsAffected(); ra == 0 {
		return ErrNoSuchJob
	}
	return nil
}

// RecordRunOutcome stores how a job's last run went, so the answer survives a
// restart.
//
// Whether a backup worked was held only in an in-process map. A job that failed
// at three in the morning left the dashboard showing the timestamp of the last
// run that succeeded — days earlier — with nothing anywhere saying the newer one
// had failed, because the process that knew had been restarted since. Before a
// regional switchover that reads as "the backup is a few days old", not as "the
// backup has been failing since Tuesday".
//
// last_backup_time keeps its meaning of "when a backup last succeeded"; these
// columns say what happened the last time one was attempted, whatever that was.
func RecordRunOutcome(id int, at, status, message string) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return faultAt(StageOpen, err)
	}
	defer db.Close()

	res, err := db.Exec(
		`UPDATE backup_tasks SET last_run_time=?, last_run_status=?, last_run_message=? WHERE id=?`,
		at, status, message, id)
	if err != nil {
		return faultAt(StageUpdate, err)
	}
	if ra, _ := res.RowsAffected(); ra == 0 {
		return ErrNoSuchJob
	}
	return nil
}
