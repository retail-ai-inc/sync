package replicationhttp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The three handlers that change a task, and the one a switch-over reads.

func TestSyncCreateHandlerStoresTheTask(t *testing.T) {
	db := useTempTaskDB(t)

	body := `{"taskName":"trial","sourceType":"mysql",
		"sourceConn":{"host":"10.118.192.8","port":"3306","user":"root",
			"password":"a-password","database":"tenant_trial_naviee"},
		"targetConn":{"host":"10.118.192.8","port":"3306","user":"root",
			"password":"a-password","database":"tenant_trial_naviee_bk"}}`
	rec := httptest.NewRecorder()
	SyncCreateHandler(rec, httptest.NewRequest(http.MethodPost, "/sync",
		strings.NewReader(body)))

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if resp := decodeEnvelope(t, rec); resp["success"] != true {
		t.Fatalf("create answered %v", resp)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sync_tasks`).Scan(&count); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if count != 1 {
		t.Errorf("%d tasks stored, want 1", count)
	}
}

// TestANewTaskIsCreatedStoppedUnlessAskedToRun. A task carries payment data
// between regions; creating one and having it start copying immediately, before
// anybody has looked at the mapping it was given, is not the safe default. The
// status has to be asked for.
func TestANewTaskIsCreatedStoppedUnlessAskedToRun(t *testing.T) {
	for name, c := range map[string]struct {
		body string
		want int
	}{
		"no status":       {`{"taskName":"t","sourceType":"mysql"}`, 0},
		"stopped":         {`{"taskName":"t","sourceType":"mysql","status":"Stopped"}`, 0},
		"asked to run":    {`{"taskName":"t","sourceType":"mysql","status":"Running"}`, 1},
		"asked, any case": {`{"taskName":"t","sourceType":"mysql","status":"running"}`, 1},
	} {
		t.Run(name, func(t *testing.T) {
			db := useTempTaskDB(t)

			rec := httptest.NewRecorder()
			SyncCreateHandler(rec, httptest.NewRequest(http.MethodPost, "/sync",
				strings.NewReader(c.body)))
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}

			var enable int
			if err := db.QueryRow(`SELECT enable FROM sync_tasks`).Scan(&enable); err != nil {
				t.Fatalf("read enable: %v", err)
			}
			if enable != c.want {
				t.Errorf("enable = %d, want %d", enable, c.want)
			}
		})
	}
}

// TestATaskWithNoNameGetsOne: the list is the only way to tell tasks apart, and
// a blank row in it is worse than a generic name.
func TestATaskWithNoNameGetsOne(t *testing.T) {
	db := useTempTaskDB(t)

	rec := httptest.NewRecorder()
	SyncCreateHandler(rec, httptest.NewRequest(http.MethodPost, "/sync",
		strings.NewReader(`{"sourceType":"mysql"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var stored string
	if err := db.QueryRow(`SELECT config_json FROM sync_tasks`).Scan(&stored); err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(stored, `"taskName":"Sync Task"`) {
		t.Errorf("a task with no name was stored without one: %s", stored)
	}
}

func TestSyncCreateHandlerRejectsABodyThatIsNotJSON(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	SyncCreateHandler(rec, httptest.NewRequest(http.MethodPost, "/sync",
		strings.NewReader("not json")))

	if rec.Code == http.StatusOK {
		t.Errorf("a body that is not JSON was accepted: %s", rec.Body.String())
	}
}

func TestSyncUpdateHandlerReplacesTheConfiguration(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, `{"taskName":"before","type":"mysql","status":"Running"}`)

	body := `{"taskName":"after","sourceType":"mysql"}`
	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/sync/1",
		strings.NewReader(body)), SyncUpdateHandler, map[string]string{"id": "1"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var stored string
	if err := db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=1`).
		Scan(&stored); err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(stored, "after") {
		t.Errorf("the task was not replaced: %s", stored)
	}
}

// TestAnUpdateKeepsTheStoredStatus covers the rule that an update takes the
// status from the stored task and not from the defaults a new task gets --
// otherwise editing a stopped task's name starts it.
func TestAnUpdateKeepsTheStoredStatus(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 0, `{"taskName":"before","type":"mysql","status":"Stopped"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/sync/1",
		strings.NewReader(`{"taskName":"after","sourceType":"mysql"}`)),
		SyncUpdateHandler, map[string]string{"id": "1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var enable int
	var stored string
	if err := db.QueryRow(`SELECT enable, config_json FROM sync_tasks WHERE id=1`).
		Scan(&enable, &stored); err != nil {
		t.Fatalf("read task: %v", err)
	}
	if enable != 0 {
		t.Errorf("enable = %d; editing a stopped task started it", enable)
	}
	if !strings.Contains(stored, "Stopped") {
		t.Errorf("the stored status was not carried over: %s", stored)
	}
}

// TestAnUpdateThatOmitsTheNameKeepsTheStoredOne: the edit form sends what it
// was given, and a name it did not touch must not become empty.
func TestAnUpdateThatOmitsTheNameKeepsTheStoredOne(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, `{"taskName":"kept","type":"mysql","status":"Running"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/sync/1",
		strings.NewReader(`{"sourceType":"mysql"}`)),
		SyncUpdateHandler, map[string]string{"id": "1"})
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}

	var stored string
	if err := db.QueryRow(`SELECT config_json FROM sync_tasks WHERE id=1`).
		Scan(&stored); err != nil {
		t.Fatalf("read config: %v", err)
	}
	if !strings.Contains(stored, "kept") {
		t.Errorf("the stored name was lost: %s", stored)
	}
}

func TestSyncUpdateHandlerRejectsABodyThatIsNotJSON(t *testing.T) {
	db := useTempTaskDB(t)
	insertSyncTask(t, db, 1, `{"taskName":"t","type":"mysql"}`)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodPut, "/sync/1",
		strings.NewReader("not json")), SyncUpdateHandler, map[string]string{"id": "1"})

	if rec.Code == http.StatusOK {
		t.Errorf("a body that is not JSON was accepted: %s", rec.Body.String())
	}
}

// TestSyncPositionHandlerRejectsANonNumericID covers the near edge of the
// switch-over endpoint. Reaching a database needs one, so this is what can be
// checked without one.
func TestSyncPositionHandlerRejectsANonNumericID(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/abc/position", nil),
		SyncPositionHandler, map[string]string{"id": "abc"})

	if rec.Code == http.StatusOK {
		t.Errorf("a non-numeric task id was accepted: %s", rec.Body.String())
	}
}

// TestSyncPositionHandlerOnATaskThatDoesNotExist: the answer has to be a
// failure and never "caught up", which is the reading that would let somebody
// promote a region on the strength of a task id they mistyped.
func TestSyncPositionHandlerOnATaskThatDoesNotExist(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/9999/position", nil),
		SyncPositionHandler, map[string]string{"id": "9999"})

	if rec.Code == http.StatusOK {
		resp := decodeEnvelope(t, rec)
		if resp["success"] == true {
			t.Fatalf("a task that does not exist answered successfully: %v", resp)
		}
	}
	if strings.Contains(rec.Body.String(), `"caughtUp":true`) {
		t.Error("a task that does not exist reported itself caught up")
	}
}
