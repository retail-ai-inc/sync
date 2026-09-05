// Package replicationhttp adapts the replication use cases to HTTP.
package replicationhttp

import (
	"context"
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

// redactedPassword is what a stored password is replaced with on its way out.
// It is a fixed string rather than the real length, so it says nothing about
// the value it hides.
const redactedPassword = "********"

// withoutCredentials copies a connection map with its password masked. The
// list endpoint answers with the connection settings of every task, and those
// carry the passwords the syncer authenticates with — in the clear, to anybody
// who could reach the port.
func withoutCredentials(conn map[string]string) map[string]string {
	if conn == nil {
		return nil
	}
	safe := make(map[string]string, len(conn))
	for k, v := range conn {
		safe[k] = v
	}
	if safe["password"] != "" {
		safe["password"] = redactedPassword
	}
	return safe
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
		"sourceConn":     withoutCredentials(c.SourceConn),
		"targetConn":     withoutCredentials(c.TargetConn),
		"mappings":       c.Mappings,

		"pg_replication_slot":       c.PgReplicationSlot,
		"pg_plugin":                 c.PgPlugin,
		"pg_position_path":          c.PgPositionPath,
		"pg_publication_names":      c.PgPublicationNames,
		"mysql_position_path":       c.MysqlPositionPath,
		"mongodb_resume_token_path": c.MongodbResumeTokenPath,
		"redis_position_path":       c.RedisPositionPath,
		"redis_buffer_dir":          c.RedisBufferDir,
		"redis_buffer_bytes":        c.RedisBufferBytes,
		"redis_batch_window":        c.RedisBatchWindow,
		"redis_reconcile_interval":  c.RedisReconcileInterval,
		"redis_source_read_rate":    c.RedisSourceReadRate,
		"retention_window":          c.RetentionWindow,
		"dump_execution_path":       c.DumpExecutionPath,
		"resync":                    c.Resync,
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

	// The day is the one an operator is looking at, which is JST — everything
	// else this API reports is converted to it. The window used to be built from
	// the UTC calendar day while the answer was labelled with the JST date, so
	// for the nine hours of each JST morning the figures belonged to the day
	// before the label said.
	now := time.Now().In(time.FixedZone("JST", 9*60*60))

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

// GET /api/sync/{id}/position
//
// Has the target applied what the source had? The endpoint a switch-over asks,
// and the one that was missing: replication lag answers it only while the task
// is running, and a task that has stopped is when it is asked.
func SyncPositionHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	progress, err := app.TaskProgress(r.Context(), id)
	if err != nil {
		fail(w, "read the task's position", err)
		return
	}

	shards := make([]map[string]interface{}, 0, len(progress.Shards))
	for _, shard := range progress.Shards {
		entry := map[string]interface{}{
			"shard":      shard.Shard,
			"source":     shard.Source,
			"applied":    shard.Applied,
			"caughtUp":   shard.CaughtUp,
			"comparable": shard.Comparable,
		}
		// Only where the engine's position is a byte offset. Elsewhere the
		// distance is not a number, and reporting 0 would read as "caught up".
		if shard.BehindBytes >= 0 {
			entry["behindBytes"] = shard.BehindBytes
		}
		if shard.Note != "" {
			entry["note"] = shard.Note
		}
		shards = append(shards, entry)
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"engine":   progress.Engine,
			"caughtUp": progress.CaughtUp(),
			"shards":   shards,
		},
	})
}

// countDeadline bounds the row count, and is what the write deadline is
// extended to. Long enough for a sharded MongoDB source counted on both sides,
// short enough that a wedged count releases the connection rather than holding
// it for ever.
const countDeadline = 15 * time.Minute

// GET /api/sync/{id}/rowcounts
//
// What the "objects captured" figure is made of: every replicated table or
// collection with the number of rows on each side. Counted when asked, because
// an exact count of both sides of a sharded MongoDB source takes minutes and
// cannot sit on a monitoring interval.
func SyncRowCountsHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// The server writes with a sixty-second deadline, which every other
	// endpoint here answers well inside. This one does not: an exact count of
	// both sides of the sharded MongoDB source took about five minutes, so the
	// connection was closed with nothing written and the caller saw an empty
	// reply. The deadline is extended for this handler alone rather than raised
	// on the server, which would take the guard off every other endpoint.
	if controller := http.NewResponseController(w); controller != nil {
		if err := controller.SetWriteDeadline(time.Now().Add(countDeadline)); err != nil {
			// Not fatal: a connection that will not take a deadline still
			// answers, and a short count still fits in the server's own.
			logrus.Debugf("[SyncRowCounts] could not extend the write deadline: %v", err)
		}
	}

	ctx, giveUp := context.WithTimeout(r.Context(), countDeadline)
	defer giveUp()

	counts, err := app.TaskRowCounts(ctx, id)
	if err != nil {
		fail(w, "count the task's objects", err)
		return
	}

	objects := make([]map[string]interface{}, 0, len(counts.Objects))
	for _, object := range counts.Objects {
		entry := map[string]interface{}{
			"source": object.Source,
			"target": object.Target,
			"agrees": object.Agrees(),
		}
		// A side that could not be counted is absent rather than zero: a table
		// that is missing and one that is empty are different problems, and a
		// zero here would read as the second.
		if object.SourceRows >= 0 {
			entry["sourceRows"] = object.SourceRows
		}
		if object.TargetRows >= 0 {
			entry["targetRows"] = object.TargetRows
		}
		if object.Agrees() || (object.SourceRows >= 0 && object.TargetRows >= 0) {
			entry["difference"] = object.Difference()
		}
		if object.Note != "" {
			entry["note"] = object.Note
		}
		objects = append(objects, entry)
	}

	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"engine":     counts.Engine,
			"discovered": counts.Discovered,
			"objects":    objects,
			"differing":  counts.Difference(),
		},
	})
}
