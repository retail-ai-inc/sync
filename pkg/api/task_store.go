package api

import (
	"database/sql"
	"encoding/json"

	"github.com/retail-ai-inc/sync/internal/platform/db"
)

func openLocalDB() (*sql.DB, error) {
	return db.OpenSQLiteDB()
}

func updateTaskStatus(id string, toStart bool) error {
	db, err := openLocalDB()
	if err != nil {
		return err
	}
	defer db.Close()

	var oldCfgJSON string
	err = db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=?`, id).Scan(&oldCfgJSON)
	if err != nil {
		return err
	}
	var statusStr string
	var newEnable int
	if toStart {
		statusStr = "Running"
		newEnable = 1
	} else {
		statusStr = "Stopped"
		newEnable = 0
	}

	var data map[string]interface{}
	if err2 := json.Unmarshal([]byte(oldCfgJSON), &data); err2 != nil {
		data = make(map[string]interface{})
	}
	data["status"] = statusStr
	newBytes, _ := json.Marshal(data)

	nowStr := timeNowStr()
	_, err = db.Exec(`
UPDATE sync_tasks
SET enable=?,
    last_update_time=?,
    config_json=?
WHERE id=?
`, newEnable, nowStr, string(newBytes), id)
	return err
}
