package mongodb

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

// quietLogger returns a logger that discards output, so the connection failures
// below do not flood the test log.
func quietLogger() *logrus.Logger {
	l := logrus.New()
	l.SetOutput(io.Discard)
	return l
}

// useTempTaskDB points SYNC_DB_PATH at a throwaway SQLite file carrying the
// sync_tasks table.
func useTempTaskDB(t *testing.T) *sql.DB {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`
CREATE TABLE sync_tasks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    enable      INTEGER NOT NULL DEFAULT 1,
    config_json TEXT NOT NULL
);`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func insertTask(t *testing.T, db *sql.DB, cfg string) string {
	t.Helper()

	res, err := db.Exec(`INSERT INTO sync_tasks (enable, config_json) VALUES (1, ?)`, cfg)
	if err != nil {
		t.Fatalf("insert task: %v", err)
	}
	id, _ := res.LastInsertId()
	return strconv.FormatInt(id, 10)
}

func TestGetMongoClientRejectsAMalformedURI(t *testing.T) {
	for _, uri := range []string{"", "not-a-uri", "http://localhost:27017", "mongodb://"} {
		t.Run(uri, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			client, err := GetMongoClient(ctx, uri)
			if err == nil {
				_ = client.Disconnect(ctx)
				t.Fatalf("GetMongoClient(%q) succeeded", uri)
			}
			if client != nil {
				t.Error("a client was returned alongside the error")
			}
		})
	}
}

// TestGetMongoClientPingsBeforeReturning records that this helper verifies the
// connection: a well-formed URI pointing nowhere is rejected on the ping rather
// than on the connect. The server-selection timeout is ten seconds, so a caller
// blocks for that long whenever MongoDB is down.
func TestGetMongoClientPingsBeforeReturning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := GetMongoClient(ctx, "mongodb://127.0.0.1:1/db")
	if err == nil {
		_ = client.Disconnect(ctx)
		t.Fatal("something answered on port 1")
	}
	if !strings.Contains(err.Error(), "failed to ping MongoDB") {
		t.Errorf("error = %q, want the ping failure", err)
	}
}

// TestConnectMongoDBAlwaysReportsTheDatabaseName records that the database name
// comes back even when the connection failed, so a caller that ignores the error
// is handed a usable-looking name next to a nil client.
func TestConnectMongoDBAlwaysReportsTheDatabaseName(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, database, err := ConnectMongoDB(ctx, "127.0.0.1", "1", "u", "p", "orders", quietLogger())
	if err == nil {
		_ = client.Disconnect(ctx)
		t.Fatal("something answered on port 1")
	}
	if database != "orders" {
		t.Errorf("database = %q on failure, want %q", database, "orders")
	}
	if client != nil {
		t.Error("a client was returned alongside the error")
	}
}

// TestConnectMongoDBAcceptsANilLogger records that a nil logger is replaced with
// the standard one rather than panicking, which the sync-tables path relies on.
func TestConnectMongoDBAcceptsANilLogger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	prev := logrus.StandardLogger().Out
	logrus.StandardLogger().SetOutput(io.Discard)
	t.Cleanup(func() { logrus.StandardLogger().SetOutput(prev) })

	if _, _, err := ConnectMongoDB(ctx, "127.0.0.1", "1", "", "", "d", nil); err == nil {
		t.Fatal("something answered on port 1")
	}
}

func TestConnectMongoDBFromTaskIDRejectsAnUnknownTask(t *testing.T) {
	useTempTaskDB(t)

	_, _, err := ConnectMongoDBFromTaskID(context.Background(), "999", quietLogger())
	if err == nil {
		t.Fatal("ConnectMongoDBFromTaskID succeeded for an unknown task")
	}
	if !strings.Contains(err.Error(), "failed to get task configuration") {
		t.Errorf("error = %q", err)
	}
}

func TestConnectMongoDBFromTaskIDRejectsACorruptConfiguration(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, `{"type":`)

	_, _, err := ConnectMongoDBFromTaskID(context.Background(), id, quietLogger())
	if err == nil {
		t.Fatal("ConnectMongoDBFromTaskID succeeded for a corrupt configuration")
	}
	if !strings.Contains(err.Error(), "failed to parse config JSON") {
		t.Errorf("error = %q", err)
	}
}

func TestConnectMongoDBFromTaskIDRejectsAnotherEngine(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, `{"type":"mysql"}`)

	_, _, err := ConnectMongoDBFromTaskID(context.Background(), id, quietLogger())
	if err == nil {
		t.Fatal("ConnectMongoDBFromTaskID accepted a MySQL task")
	}
	if !strings.Contains(err.Error(), "task is not MongoDB type") {
		t.Errorf("error = %q", err)
	}
}

// TestTheEngineCheckFoldsCase records that this call accepts "MongoDB" where the
// syncer dispatch in cmd/sync does not, which is the split T-053 records.
func TestTheEngineCheckFoldsCase(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db,
		`{"type":"MongoDB","targetConn":{"host":"127.0.0.1","port":"1","database":"d"}}`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _, err := ConnectMongoDBFromTaskID(ctx, id, quietLogger())
	if err == nil {
		t.Fatal("something answered on port 1")
	}
	if strings.Contains(err.Error(), "not MongoDB type") {
		t.Fatalf("error = %q; the check appears to be case-sensitive now, so assert "+
			"that instead", err)
	}
}

// TestItConnectsToTheTargetNotTheSource records that this helper reads the
// target connection, because the row counts it exists to fetch come from the
// destination. A task whose target is unreachable cannot report progress even
// when its source is fine.
func TestItConnectsToTheTargetNotTheSource(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, `{"type":"mongodb",
		"sourceConn":{"host":"127.0.0.1","port":"27017","database":"src"},
		"targetConn":{"host":"127.0.0.1","port":"1","database":"tgt"}}`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, database, err := ConnectMongoDBFromTaskID(ctx, id, quietLogger())
	if err == nil {
		t.Fatal("something answered on port 1")
	}
	if database != "tgt" {
		t.Errorf("database = %q, want the target's %q", database, "tgt")
	}
}

// TestAMissingTargetConnectionBecomesAnEmptyURI records that the connection
// parameters are read out of a map with no checks, so a configuration with no
// targetConn builds "mongodb://:@:/?authSource=admin" and fails on the URI
// rather than reporting the missing configuration.
func TestAMissingTargetConnectionBecomesAnEmptyURI(t *testing.T) {
	db := useTempTaskDB(t)
	id := insertTask(t, db, `{"type":"mongodb"}`)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, database, err := ConnectMongoDBFromTaskID(ctx, id, quietLogger())
	if err == nil {
		t.Fatal("ConnectMongoDBFromTaskID succeeded with no target connection")
	}
	if database != "" {
		t.Errorf("database = %q, want empty", database)
	}
	if strings.Contains(err.Error(), "targetConn") {
		t.Fatalf("error = %q; the missing configuration appears to be reported now, "+
			"so assert that instead", err)
	}
}

func TestConnectMongoDBFromTaskIDReportsAnUnopenableDatabase(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("SYNC_DB_PATH", filepath.Join(blocker, "sub", "sync.db"))

	_, _, err := ConnectMongoDBFromTaskID(context.Background(), "1", quietLogger())
	if err == nil {
		t.Fatal("ConnectMongoDBFromTaskID succeeded with no database")
	}
	if !strings.Contains(err.Error(), "failed to open local DB") {
		t.Errorf("error = %q", err)
	}
}

func TestConnectMongoDBFromTaskIDAcceptsANilLogger(t *testing.T) {
	useTempTaskDB(t)

	prev := logrus.StandardLogger().Out
	logrus.StandardLogger().SetOutput(io.Discard)
	t.Cleanup(func() { logrus.StandardLogger().SetOutput(prev) })

	if _, _, err := ConnectMongoDBFromTaskID(context.Background(), "999", nil); err == nil {
		t.Fatal("ConnectMongoDBFromTaskID succeeded for an unknown task")
	}
}
