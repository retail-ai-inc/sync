package backuphttp

import (
	"database/sql"
	"encoding/json"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"net/http/httptest"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// isolateCrontab empties PATH so the `crontab` command cannot be found. The
// backup handlers call CronManager.SyncCrontab unconditionally, which shells
// out to crontab and would otherwise rewrite the crontab of whoever runs the
// suite. With crontab unreachable the handlers log a warning and carry on,
// which is the behaviour under test.
func isolateCrontab(t *testing.T) {
	t.Helper()
	t.Setenv("PATH", t.TempDir())
}

// useTempTaskDB points the package at a throwaway SQLite file carrying the
// sync_tasks and backup_tasks schema, so the list and mutate handlers can be
// exercised without touching the database tracked in this repository.
func useTempTaskDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	// Through the real opener, which carries the whole schema and creates it
	// only when it is missing — rather than a copy kept here that can drift from
	// it, and that a background goroutine racing to the same path turns into
	// "table already exists".
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func decodeEnvelope(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()

	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	return resp
}
