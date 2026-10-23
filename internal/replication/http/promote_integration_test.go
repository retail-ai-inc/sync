//go:build integration

package replicationhttp

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/app"
	"github.com/retail-ai-inc/sync/test/harness"
)

// twoTasksIntoTheRedisTarget inserts the promoted task and a sibling that
// replicates into the same Redis target, and clears the marker afterwards.
func twoTasksIntoTheRedisTarget(t *testing.T, db *sql.DB) (id, sibling int) {
	t.Helper()

	sourceHost, sourcePort := harness.SplitHostPort(t, harness.RedisSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.RedisTarget)
	cfg := fmt.Sprintf(`{"type":"redis",
		"sourceConn":{"host":%q,"port":%q,"database":"0"},
		"targetConn":{"host":%q,"port":%q,"database":"0"}}`,
		sourceHost, sourcePort, targetHost, targetPort)
	insertSyncTask(t, db, 1, cfg)
	insertSyncTask(t, db, 1, cfg)

	rows, err := db.Query(`SELECT id FROM sync_tasks ORDER BY id`)
	if err != nil {
		t.Fatalf("read task ids: %v", err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var n int
		if err := rows.Scan(&n); err != nil {
			t.Fatalf("scan task id: %v", err)
		}
		ids = append(ids, n)
	}
	if len(ids) != 2 {
		t.Fatalf("task ids = %v, want two", ids)
	}
	t.Cleanup(func() { _ = app.DemoteTarget(context.Background(), strconv.Itoa(ids[0])) })
	return ids[0], ids[1]
}

func promotion(t *testing.T, method string, id int) map[string]interface{} {
	t.Helper()

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(method, "/sync/"+strconv.Itoa(id)+"/promotion", nil),
		SyncPromoteHandler, map[string]string{"id": strconv.Itoa(id)})
	return decodeEnvelope(t, rec)
}

func stoppedTasks(t *testing.T, resp map[string]interface{}) []int {
	t.Helper()

	data, _ := resp["data"].(map[string]interface{})
	raw, ok := data["stoppedTasks"].([]interface{})
	if !ok {
		t.Fatalf("data.stoppedTasks = %#v, want a list", data["stoppedTasks"])
	}
	ids := make([]int, len(raw))
	for i, v := range raw {
		ids[i] = int(v.(float64))
	}
	return ids
}

func enableOf(t *testing.T, db *sql.DB, id int) int {
	t.Helper()

	var enable int
	if err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, id).Scan(&enable); err != nil {
		t.Fatalf("read enable: %v", err)
	}
	return enable
}

func targetPromoted(t *testing.T, id int) bool {
	t.Helper()

	_, promoted, err := app.TargetPromotion(context.Background(), strconv.Itoa(id))
	if err != nil {
		t.Fatalf("TargetPromotion: %v", err)
	}
	return promoted
}

// A failure means the operator is not told which tasks were stopped, or the clear does not reach the target.
func TestAPromotionOverHTTPStopsEveryTaskIntoTheTargetAndCanBeCleared(t *testing.T) {
	db := useTempTaskDB(t)
	id, sibling := twoTasksIntoTheRedisTarget(t, db)

	resp := promotion(t, http.MethodPost, id)
	if resp["success"] != true {
		t.Fatalf("POST success = %v: %v", resp["success"], resp)
	}
	data, _ := resp["data"].(map[string]interface{})
	if data["promoted"] != true {
		t.Errorf("data.promoted = %v, want true", data["promoted"])
	}
	if got, want := stoppedTasks(t, resp), []int{id, sibling}; !reflect.DeepEqual(got, want) {
		t.Errorf("data.stoppedTasks = %v, want %v", got, want)
	}
	if msg, _ := data["msg"].(string); !strings.Contains(msg, fmt.Sprintf("Task(s) %d, %d", id, sibling)) {
		t.Errorf("data.msg = %q, want it to name both stopped tasks", msg)
	}
	for _, task := range []int{id, sibling} {
		if enable := enableOf(t, db, task); enable != 0 {
			t.Errorf("task %d enable = %d after the promotion, want 0", task, enable)
		}
	}
	if !targetPromoted(t, id) {
		t.Fatal("the target carries no marker after a successful POST")
	}

	resp = promotion(t, http.MethodDelete, id)
	if resp["success"] != true {
		t.Fatalf("DELETE success = %v: %v", resp["success"], resp)
	}
	if data, _ := resp["data"].(map[string]interface{}); data["promoted"] != false {
		t.Errorf("DELETE data.promoted = %v, want false", data["promoted"])
	}
	if targetPromoted(t, id) {
		t.Error("the marker survived a successful DELETE")
	}
}

// A failure means a half-done promotion reads as a plain failure, hiding the marker and the task still writing.
func TestAPromotionThatCouldNotStopASiblingSaysTheMarkerIsWrittenAndNamesIt(t *testing.T) {
	db := useTempTaskDB(t)
	id, sibling := twoTasksIntoTheRedisTarget(t, db)
	if _, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER refuse BEFORE UPDATE ON sync_tasks
		WHEN NEW.id=%d BEGIN SELECT RAISE(ABORT,'locked'); END`, sibling)); err != nil {
		t.Fatalf("create trigger: %v", err)
	}

	resp := promotion(t, http.MethodPost, id)
	if resp["success"] != false {
		t.Fatalf("success = %v with a sibling left running: %v", resp["success"], resp)
	}
	data, _ := resp["data"].(map[string]interface{})
	if data["promoted"] != true {
		t.Errorf("data.promoted = %v, want true: the marker is written", data["promoted"])
	}
	if got, want := stoppedTasks(t, resp), []int{id}; !reflect.DeepEqual(got, want) {
		t.Errorf("data.stoppedTasks = %v, want %v", got, want)
	}
	if message, _ := resp["errorMessage"].(string); !strings.Contains(message, fmt.Sprintf("task %d", sibling)) {
		t.Errorf("errorMessage = %q, want it to name task %d", message, sibling)
	}
	if enable := enableOf(t, db, sibling); enable != 1 {
		t.Errorf("sibling enable = %d, want 1: the trigger refused the stop", enable)
	}
	if !targetPromoted(t, id) {
		t.Error("the target carries no marker although the response says it was promoted")
	}
}
