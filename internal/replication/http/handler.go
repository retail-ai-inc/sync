// Package replicationhttp adapts the replication use cases to HTTP.
package replicationhttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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

// SyncPromoteHandler POST and DELETE /api/sync/{id}/promotion
//
// Marks the task's target as promoted, or clears the mark. A failover writes
// nothing by itself -- somebody repoints the application at Osaka -- so this
// is how the databases are told the direction has changed. Until it is
// cleared, no task will replicate into that database, which is what stops a
// returning Tokyo overwriting everything Osaka has taken since.
func SyncPromoteHandler(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	switch r.Method {
	case http.MethodPost:
		stopped, err := app.PromoteTarget(r.Context(), id, "")
		if stopped == nil {
			stopped = []int{}
		}
		var halfDone *app.StopAfterPromotion
		switch {
		case errors.As(err, &halfDone):
			// Not "promote the target": the marker is written, and saying the
			// promotion failed would have somebody try again while a task they
			// were not told about keeps writing.
			httpx.WriteJSON(w, map[string]interface{}{
				"success": false, "errorMessage": err.Error(),
				"data": map[string]interface{}{"promoted": true, "stoppedTasks": stopped},
			})
			return
		case err != nil:
			fail(w, "promote the target", err)
			return
		}
		msg := "The target is marked as promoted. No task will replicate into it " +
			"until the mark is cleared."
		if len(stopped) > 0 {
			msg += fmt.Sprintf(" Task(s) %s replicated into it and have been stopped; "+
				"they exit within a few seconds.", joinIDs(stopped))
		}
		httpx.WriteJSON(w, map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"promoted":     true,
				"stoppedTasks": stopped,
				"msg":          msg,
			},
		})
	case http.MethodDelete:
		if err := app.DemoteTarget(r.Context(), id); err != nil {
			fail(w, "clear the target's promotion", err)
			return
		}
		httpx.WriteJSON(w, map[string]interface{}{
			"success": true,
			"data":    map[string]interface{}{"promoted": false, "msg": "Promotion cleared."},
		})
	default:
		httpx.WriteJSON(w, map[string]interface{}{"success": false,
			"errorMessage": "use POST to promote and DELETE to clear"})
	}
}

func joinIDs(ids []int) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = strconv.Itoa(id)
	}
	return strings.Join(parts, ", ")
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

// GET /api/sync/{id}/ddl-acknowledgements
//
// What this task is allowed to pass over, and what it already has. A standing
// permission to skip a destructive schema change is the kind of thing that has
// to be readable without grepping a log.
func SyncDDLAcknowledgementsHandler(w http.ResponseWriter, r *http.Request) {
	all, err := app.DDLAcknowledgements(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		fail(w, "read the acknowledgements", err)
		return
	}

	items := make([]map[string]interface{}, 0, len(all))
	for _, a := range all {
		item := map[string]interface{}{
			"id":        a.ID,
			"statement": a.Statement,
			"digest":    a.Digest,
			"createdBy": a.CreatedBy,
			"createdAt": a.CreatedAt,
			"used":      a.Used(),
		}
		if a.Used() {
			item["usedAt"] = a.UsedAt
		}
		items = append(items, item)
	}
	httpx.WriteJSON(w, map[string]interface{}{"success": true, "data": items})
}

// SyncDDLAcknowledgeHandler POST /api/sync/{id}/ddl-acknowledgements
//
// The way back from a task halted on a schema change it refuses to carry: make
// the change on the target, then acknowledge the statement here. The task
// passes over that one statement, once, and goes on replicating the rows around
// it -- which moving the stored position by hand would have thrown away.
//
// who is passed in rather than read here, so this package does not need the
// identity context to record a name.
func SyncDDLAcknowledgeHandler(who func(*http.Request) string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Statement string `json:"statement"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			fail(w, "read the request", err)
			return
		}

		ack, err := app.AcknowledgeDDL(r.Context(), chi.URLParam(r, "id"), body.Statement, who(r))
		if err != nil {
			fail(w, "record the acknowledgement", err)
			return
		}
		httpx.WriteJSON(w, map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"id":     ack.ID,
				"digest": ack.Digest,
				"msg": "Acknowledged. Start the task again; it will pass over this " +
					"statement once and carry on. The target is not changed by this.",
			},
		})
	}
}

// SyncDDLAcknowledgementDeleteHandler DELETE /api/sync/{id}/ddl-acknowledgements/{ack}
//
// Withdraws one that has not been used yet.
func SyncDDLAcknowledgementDeleteHandler(w http.ResponseWriter, r *http.Request) {
	ackID, err := strconv.ParseInt(chi.URLParam(r, "ack"), 10, 64)
	if err != nil {
		fail(w, "read the acknowledgement id", err)
		return
	}
	if err := app.RevokeDDLAcknowledgement(r.Context(), chi.URLParam(r, "id"), ackID); err != nil {
		fail(w, "withdraw the acknowledgement", err)
		return
	}
	httpx.WriteJSON(w, map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{"msg": "Withdrawn."},
	})
}
