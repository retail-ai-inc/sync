package infra

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// captureLog returns a logger writing into a buffer, which is the only place
// the counters report anything: every one of them logs and returns rather than
// propagating a failure.
func captureLog() (*logrus.Logger, *bytes.Buffer) {
	var out bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&out)
	logger.SetLevel(logrus.DebugLevel)
	return logger, &out
}

// briefCtx bounds the dial attempts so an unreachable host fails in
// milliseconds rather than on the driver's own timeout.
func briefCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

func oneMapping() []config.DatabaseMapping {
	return []config.DatabaseMapping{{
		SourceDatabase: "shop",
		TargetDatabase: "shop",
		Tables:         []config.TableMapping{{SourceTable: "orders", TargetTable: "orders"}},
	}}
}

// TestTheMySQLCounterReportsAnUnreachableSource records that a monitoring pass
// against a source that is down logs and returns. Nothing is written to
// monitoring_log, so the dashboard keeps showing the last figures it had with
// no indication that they are stale.
func TestTheMySQLCounterReportsAnUnreachableSource(t *testing.T) {
	logger, out := captureLog()

	CountAndLogMySQLOrMariaDB(briefCtx(t), config.SyncConfig{
		Type:             "mysql",
		SourceConnection: "u:p@tcp(127.0.0.1:1)/shop",
		TargetConnection: "u:p@tcp(127.0.0.1:1)/shop",
		Mappings:         oneMapping(),
	}, logger)

	if !strings.Contains(out.String(), "Fail to ping source database") {
		t.Errorf("output = %q, want a source ping failure", out.String())
	}
}

func TestTheMySQLCounterReportsAnUnparseableDSN(t *testing.T) {
	logger, out := captureLog()

	CountAndLogMySQLOrMariaDB(briefCtx(t), config.SyncConfig{
		Type:             "mysql",
		SourceConnection: "not a dsn at all",
	}, logger)

	if !strings.Contains(out.String(), "Fail to connect to source") {
		t.Errorf("output = %q, want a connect failure", out.String())
	}
}

// TestTheMySQLCounterStopsAtTheTarget records the second half of the check: a
// reachable source and a dead target still produces no row, because both sides
// are pinged before any counting starts.
func TestTheMySQLCounterStopsAtTheTarget(t *testing.T) {
	logger, out := captureLog()

	CountAndLogMySQLOrMariaDB(briefCtx(t), config.SyncConfig{
		Type:             "mysql",
		SourceConnection: "u:p@tcp(127.0.0.1:1)/shop",
		TargetConnection: "not a dsn at all",
		Mappings:         oneMapping(),
	}, logger)

	// The source ping fails first, so the target is never reached — which is
	// what makes the source the only failure an operator sees.
	if !strings.Contains(out.String(), "source") {
		t.Errorf("output = %q", out.String())
	}
}

// TestThePostgreSQLCounterHasNoDriverOfItsOwn records that this package never
// imports a PostgreSQL driver: the counter calls sql.Open("postgres", ...) and
// relies on some other package in the same binary having registered it with a
// blank import. In the production binary the replication syncer does, so the
// counter works — but nothing in this package guarantees it, and dropping that
// import elsewhere would break monitoring at runtime with "unknown driver".
// The MySQL counter has the same dependency; it only reaches its ping in this
// suite because a sibling test file imports the driver.
func TestThePostgreSQLCounterHasNoDriverOfItsOwn(t *testing.T) {
	logger, out := captureLog()

	CountAndLogPostgreSQL(briefCtx(t), config.SyncConfig{
		Type:             "postgresql",
		SourceConnection: "postgres://u:p@127.0.0.1:1/shop?sslmode=disable",
		TargetConnection: "postgres://u:p@127.0.0.1:1/shop?sslmode=disable",
		Mappings:         oneMapping(),
	}, logger)

	if !strings.Contains(out.String(), "unknown driver") {
		t.Fatalf("output = %q; the driver appears to be imported here now, so "+
			"assert the ping failure instead", out.String())
	}
}

func TestTheRedisCounterReportsAnUnparseableDSN(t *testing.T) {
	logger, out := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		Type:             "redis",
		SourceConnection: "not a redis url",
	}, logger)

	if !strings.Contains(out.String(), "Fail to parse source Redis DSN") {
		t.Errorf("output = %q, want a parse failure", out.String())
	}
}

func TestTheRedisCounterReportsAnUnreachableSource(t *testing.T) {
	logger, out := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		Type:             "redis",
		SourceConnection: "redis://127.0.0.1:1/0",
		TargetConnection: "redis://127.0.0.1:1/0",
	}, logger)

	if !strings.Contains(out.String(), "Fail to connect to source Redis") {
		t.Errorf("output = %q, want a connect failure", out.String())
	}
}

// TestTheRedisCounterReportsAnUnparseableTarget covers the target half, which
// needs a reachable source in production — here the source parse succeeds and
// the ping fails first, so this documents the ordering rather than reaching the
// target branch.
func TestTheRedisCounterChecksTheSourceFirst(t *testing.T) {
	logger, out := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		Type:             "redis",
		SourceConnection: "redis://127.0.0.1:1/0",
		TargetConnection: "not a redis url",
	}, logger)

	if strings.Contains(out.String(), "target") {
		t.Errorf("output = %q, want the source checked first", out.String())
	}
}

// TestTheMongoDBCounterReportsAnInvalidURI records the one MongoDB failure the
// connect call itself catches — the driver dials lazily, so everything else
// surfaces later, per collection.
func TestTheMongoDBCounterReportsAnInvalidURI(t *testing.T) {
	logger, out := captureLog()

	CountAndLogMongoDB(briefCtx(t), config.SyncConfig{
		Type:             "mongodb",
		SourceConnection: "not-a-uri",
	}, logger)

	if !strings.Contains(out.String(), "Fail to connect to source") {
		t.Errorf("output = %q, want a connect failure", out.String())
	}
}

// TestTheMongoDBCounterRecordsMinusOneForAFailedCount records what the MongoDB
// counter does that the other three do not: it writes a monitoring_log row even
// when the count failed, carrying -1 as the row count. So the dashboard shows
// -1 documents rather than the last good figure — visible, unlike the silent
// staleness of the SQL counters (T-194), but not labelled as a failure either.
//
// The temporary database matters here: without it the counter's writer opens the
// sync.db tracked in this repository and inserts into it.
func TestTheMongoDBCounterRecordsMinusOneForAFailedCount(t *testing.T) {
	conn := useMonitoringDB(t)
	logger, out := captureLog()

	CountAndLogMongoDB(briefCtx(t), config.SyncConfig{
		ID:               42,
		Type:             "mongodb",
		SourceConnection: "mongodb://127.0.0.1:1/shop",
		TargetConnection: "mongodb://127.0.0.1:1/shop",
		Mappings:         oneMapping(),
	}, logger)

	if strings.Contains(out.String(), "Fail to connect to source") {
		t.Errorf("output = %q; the connection appears to be checked upfront now, "+
			"so assert that instead", out.String())
	}

	var src, tgt int64
	err := conn.QueryRow(`SELECT src_row_count, tgt_row_count FROM monitoring_log
		WHERE sync_task_id = 42`).Scan(&src, &tgt)
	if err != nil {
		t.Fatalf("no row was written for a failed count: %v", err)
	}
	if src != -1 || tgt != -1 {
		t.Errorf("counts = %d/%d, want -1/-1", src, tgt)
	}
}

// ------------------------------------------------------------- slack

// TestANegativeCountSkipsTheNotification records the guard that keeps a failed
// count from being reported as a discrepancy: -1 is the counters' failure value,
// and a notification built from it would claim a difference of billions.
func TestANegativeCountSkipsTheNotification(t *testing.T) {
	logger, out := captureLog()

	// No database is configured, so reaching the configuration lookup would be
	// fatal — the fact that this returns proves the guard runs first.
	SendTableComparisonSlackNotification(context.Background(), config.SyncConfig{ID: 1},
		"shop", "orders", "shop", "orders", -1, 5, time.Now(), logger)
	SendTableComparisonSlackNotification(context.Background(), config.SyncConfig{ID: 1},
		"shop", "orders", "shop", "orders", 5, -1, time.Now(), logger)

	if out.Len() != 0 {
		t.Errorf("output = %q, want nothing", out.String())
	}
}

// TestAnUnconfiguredSlackIsSilent records that a task with no webhook produces
// no notification and no complaint, so an operator who forgot to configure Slack
// sees exactly what one whose counts always agree sees.
func TestAnUnconfiguredSlackIsSilent(t *testing.T) {
	logger, out := captureLog()
	seedGlobalConfig(t, "", "")

	SendTableComparisonSlackNotification(context.Background(), config.SyncConfig{ID: 1},
		"shop", "orders", "shop", "orders", 100, 90, time.Now(), logger)

	if strings.Contains(out.String(), "notification sent") {
		t.Errorf("output = %q, want no notification", out.String())
	}
}

// seedGlobalConfig points the process at a throwaway database carrying the one
// row config.NewConfig insists on. Without it the lookup calls log.Fatalf and
// takes the test binary with it.
func seedGlobalConfig(t *testing.T, webhook, channel string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE config_global (
    id                                INTEGER PRIMARY KEY,
    enable_table_row_count_monitoring INTEGER NOT NULL DEFAULT 0,
    log_level                         TEXT    NOT NULL DEFAULT 'info',
    monitor_interval                  INTEGER DEFAULT 60,
    slackWebhookURL                   TEXT,
    slackChannel                      TEXT
);
CREATE TABLE sync_tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    enable           INTEGER NOT NULL DEFAULT 1,
    last_update_time DATETIME,
    last_run_time    DATETIME,
    config_json      TEXT NOT NULL
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO config_global
		(id, enable_table_row_count_monitoring, log_level, monitor_interval,
		 slackWebhookURL, slackChannel)
		VALUES (1, 1, 'info', 60, ?, ?)`, webhook, channel); err != nil {
		t.Fatalf("insert settings: %v", err)
	}
}
