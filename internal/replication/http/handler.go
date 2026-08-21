// Package replicationhttp adapts the replication use cases to HTTP.
package replicationhttp

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/internal/platform/httpx"
	"github.com/retail-ai-inc/sync/internal/replication/app"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra"
	"github.com/sirupsen/logrus"
)

// fail answers with the plain JSON error envelope, using the message the store
// tagged its failure with when there is one.
func fail(w http.ResponseWriter, fallback string, err error) {
	var fault *infra.Fault
	if errors.As(err, &fault) {
		httpx.ErrorJSON(w, fault.Stage, fault.Err)
		return
	}
	httpx.ErrorJSON(w, fallback, err)
}

// taskPayload is the shape both the list and the create endpoints answer with
// for one task.
func taskPayload(id interface{}, enable bool, status, lastUpdate, lastRun, name string, c domain.Config) map[string]interface{} {
	return map[string]interface{}{
		"id":             id,
		"enable":         enable,
		"status":         status,
		"lastUpdateTime": lastUpdate,
		"lastRunTime":    lastRun,
		"taskName":       name,
		"sourceType":     c.Type,
		"sourceConn":     c.SourceConn,
		"targetConn":     c.TargetConn,
		"mappings":       c.Mappings,

		"pg_replication_slot":       c.PgReplicationSlot,
		"pg_plugin":                 c.PgPlugin,
		"pg_position_path":          c.PgPositionPath,
		"pg_publication_names":      c.PgPublicationNames,
		"mysql_position_path":       c.MysqlPositionPath,
		"mongodb_resume_token_path": c.MongodbResumeTokenPath,
		"redis_position_path":       c.RedisPositionPath,
		"securityEnabled":           c.SecurityEnabled,
	}
}

// SyncListHandler GET /api/sync
func SyncListHandler(w http.ResponseWriter, r *http.Request) {
	views, err := app.ListTasks()
	if err != nil {
		fail(w, "query sync_tasks fail", err)
		return
	}

	var result []map[string]interface{}
	for _, v := range views {
		item := taskPayload(v.Task.ID(), v.Task.IsEnabled(), v.Status,
			httpx.ConvertTimeToJST(v.Task.LastUpdateTime()),
			httpx.ConvertTimeToJST(v.Task.LastRunTime()),
			v.Name, v.Config)

		itemJSON, _ := json.Marshal(item)
		logrus.Debugf("Returned item: %s", string(itemJSON))

		result = append(result, item)
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    result,
	})
}

// SyncCreateHandler POST /api/sync
func SyncCreateHandler(w http.ResponseWriter, r *http.Request) {
	logrus.Infof("SyncCreateHandler => method=%s, URL=%s", r.Method, r.URL.String())

	var req domain.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.ErrorJSON(w, "decode fail", err)
		return
	}

	newID, stored, nowStr, enable, err := app.CreateTask(req)
	if err != nil {
		fail(w, "insert fail", err)
		return
	}

	respData := taskPayload(newID, enable != 0, stored.Status, nowStr, "",
		stored.TaskName, domain.ConfigFrom(stored))

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg":      "Added successfully",
			"formData": respData,
		},
	})
}

// SyncStartHandler PUT /api/sync/{id}/start
func SyncStartHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := app.StartTask(id); err != nil {
		httpx.ErrorJSON(w, "start fail", err)
		return
	}
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Started the sync task: " + id},
	})
}

// SyncStopHandler PUT /api/sync/{id}/stop
func SyncStopHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	if err := app.StopTask(id); err != nil {
		httpx.ErrorJSON(w, "stop fail", err)
		return
	}
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Stopped the sync task: " + id},
	})
}

// SyncUpdateHandler PUT /api/sync/{id}
func SyncUpdateHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	var req domain.Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpx.ErrorJSON(w, "decode fail", err)
		return
	}

	stored, err := app.UpdateTask(id, req)
	switch {
	case err == nil:
	case errors.Is(err, infra.ErrNoSuchTask):
		httpx.ErrorJSON(w, "no record found", errors.New("no rows affected"))
		return
	default:
		fail(w, "update fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"msg": "Update success",
			"formData": map[string]interface{}{
				"id":                        id,
				"taskName":                  stored.TaskName,
				"sourceType":                stored.SourceType,
				"status":                    stored.Status,
				"pg_replication_slot":       stored.PgReplicationSlot,
				"pg_plugin":                 stored.PgPlugin,
				"pg_position_path":          stored.PgPositionPath,
				"pg_publication_names":      stored.PgPublicationNames,
				"mysql_position_path":       stored.MysqlPositionPath,
				"mongodb_resume_token_path": stored.MongodbResumeTokenPath,
				"redis_position_path":       stored.RedisPositionPath,
				"securityEnabled":           stored.SecurityEnabled,
			},
		},
	})
}

// SyncDeleteHandler DELETE /api/sync/{id}
func SyncDeleteHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	switch err := app.DeleteTask(id); {
	case err == nil:
	case errors.Is(err, infra.ErrNoSuchTask):
		httpx.WriteJSON(w, map[string]interface{}{
			"success": false,
			"data":    map[string]interface{}{"msg": "Deletion failed: no record"},
		})
		return
	default:
		fail(w, "delete fail", err)
		return
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Deleted successfully"},
	})
}

// SyncTablesHandler GET /api/sync/{id}/tables
func SyncTablesHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	logrus.Infof("[SyncTables] Fetching tables data for task: %s", id)

	now := time.Now().UTC()

	stats, err := app.TableProgress(r.Context(), id, now)
	if err != nil {
		fail(w, "query monitoring_log fail", err)
		return
	}

	tableStats := make([]map[string]interface{}, 0, len(stats))
	for _, s := range stats {
		tableStats = append(tableStats, map[string]interface{}{
			"tableName":    s.TableName,
			"syncedToday":  s.SyncedToday,
			"totalRows":    s.TotalRows,
			"lastSyncTime": httpx.ConvertTimeToJST(s.LastSyncTime),
		})
	}

	// Get JST date for display purposes only
	jst := time.FixedZone("JST", 9*60*60)
	jstDate := now.In(jst).Format("2006-01-02")

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"taskId":     id,
			"tableCount": len(tableStats),
			"syncDate":   jstDate,
			"tables":     tableStats,
		},
	})
}
