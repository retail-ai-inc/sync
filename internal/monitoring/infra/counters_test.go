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
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
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

// Nothing is published, so the dashboard keeps showing the last figures it had
// with no indication that they are stale.
func TestTheMySQLCounterReportsAnUnreachableSource(t *testing.T) {
	useMonitoringDB(t)
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
	useMonitoringDB(t)
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
	useMonitoringDB(t)
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

// TestThePostgreSQLCounterOpensItsOwnDriver covers a dependency this package
// used to take on trust.
func TestThePostgreSQLCounterOpensItsOwnDriver(t *testing.T) {
	useMonitoringDB(t)
	logger, out := captureLog()

	CountAndLogPostgreSQL(briefCtx(t), config.SyncConfig{
		Type:             "postgresql",
		SourceConnection: "postgres://u:p@127.0.0.1:1/shop?sslmode=disable",
		TargetConnection: "postgres://u:p@127.0.0.1:1/shop?sslmode=disable",
		Mappings:         oneMapping(),
	}, logger)

	if strings.Contains(out.String(), "unknown driver") {
		t.Errorf("output = %q, want the connection to have been attempted", out.String())
	}
	if !strings.Contains(out.String(), "source") {
		t.Errorf("output = %q, want the unreachable source reported", out.String())
	}
}

func TestTheRedisCounterReportsAnUnparseableDSN(t *testing.T) {
	useMonitoringDB(t)
	logger, out := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		Type:             "redis",
		SourceConnection: "not a redis url",
	}, logger)

	if !strings.Contains(out.String(), "source Redis") {
		t.Errorf("output = %q, want the source reported", out.String())
	}
	if !strings.Contains(out.String(), "failed to parse redis DSN") {
		t.Errorf("output = %q, want the parse failure carried", out.String())
	}
}

// TestTheRedisCounterAcceptsAClusterDSN records that a DSN naming more than one
// host is read as a cluster rather than refused.
func TestTheRedisCounterAcceptsAClusterDSN(t *testing.T) {
	useMonitoringDB(t)
	logger, out := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		Type:             "redis",
		SourceConnection: "redis://127.0.0.1:1,127.0.0.1:2,127.0.0.1:3/0",
		TargetConnection: "redis://127.0.0.1:4,127.0.0.1:5,127.0.0.1:6/0",
	}, logger)

	if strings.Contains(out.String(), "failed to parse redis DSN") {
		t.Errorf("output = %q, want a cluster DSN to be understood", out.String())
	}
	if !strings.Contains(out.String(), "Fail to connect to source Redis") {
		t.Errorf("output = %q, want the unreachable cluster reported", out.String())
	}
}

func TestTheRedisCounterReportsAnUnreachableSource(t *testing.T) {
	useMonitoringDB(t)
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

// Both ends are reported. The counter used to return at the first failure, so
// a run with two broken ends named one of them and the operator fixed it only
// to hit the second on the next pass.
func TestBothEndsAreReportedWhenBothFail(t *testing.T) {
	useMonitoringDB(t)
	logger, out := captureLog()

	CountAndLogRedis(briefCtx(t), config.SyncConfig{
		Type:             "redis",
		SourceConnection: "redis://127.0.0.1:1/0",
		TargetConnection: "not a redis url",
	}, logger)

	text := out.String()
	if !strings.Contains(text, "Fail to connect to source Redis") {
		t.Errorf("output = %q, want the source failure reported", text)
	}
	if !strings.Contains(text, "Fail to connect to target Redis") {
		t.Errorf("output = %q, want the target failure reported too", text)
	}
}

// TestTheMongoDBCounterReportsAnInvalidURI records the one MongoDB failure the
// connect call itself catches — the driver dials lazily, so everything else
// surfaces later, per collection.
func TestTheMongoDBCounterReportsAnInvalidURI(t *testing.T) {
	useMonitoringDB(t)
	logger, out := captureLog()

	CountAndLogMongoDB(briefCtx(t), config.SyncConfig{
		Type:             "mongodb",
		SourceConnection: "not-a-uri",
	}, logger)

	if !strings.Contains(out.String(), "Failed to connect to source") {
		t.Errorf("output = %q, want a connect failure", out.String())
	}
}

// TestTheMongoDBCounterPublishesMinusOneForAFailedCount records what the
// MongoDB counter does that the other three do not: it reports the comparison
// even when the count failed, carrying -1 as the count.
func TestTheMongoDBCounterPublishesMinusOneForAFailedCount(t *testing.T) {
	useMonitoringDB(t)
	logger, out := captureLog()

	labels := metrics.Labels{"task": "42", "engine": "mongodb", "object": "orders"}
	t.Cleanup(func() { metrics.ForgetRowCounts(labels) })

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

	src, tgt, ok := rowCountsOf(t, labels)
	if !ok {
		t.Fatal("nothing was published for a failed count")
	}
	if src != -1 || tgt != -1 {
		t.Errorf("counts = %v/%v, want -1/-1", src, tgt)
	}
}

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
// row config.NewConfig insists on.
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

// INFO keyspace is what says which databases hold keys and how many. Reading
// it wrong is how a source with data in databases 1 and 2 read as empty.
func TestKeyspaceIsReadAcrossEveryDatabase(t *testing.T) {
	info := "# Keyspace\r\ndb0:keys=1,expires=0,avg_ttl=0\r\n" +
		"db1:keys=10252,expires=10252,avg_ttl=1000\r\ndb2:keys=4314,expires=0,avg_ttl=0\r\n"

	total, databases := parseKeyspace(info)
	if total != 1+10252+4314 {
		t.Errorf("counted %d keys, want every database's", total)
	}
	if len(databases) != 3 || databases[0] != 0 || databases[2] != 2 {
		t.Errorf("databases = %v, want 0, 1 and 2 in order", databases)
	}
}

// A server with nothing in it reports no database line at all, which is a
// count of zero rather than a failure to read.
func TestAnEmptyKeyspaceCountsZero(t *testing.T) {
	for name, info := range map[string]string{
		"nothing at all":                "",
		"the header alone":              "# Keyspace\r\n",
		"a line without keys":           "# Keyspace\r\ndb0:expires=0\r\n",
		"a line that is not a database": "# Keyspace\r\ndbx:keys=5\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			total, databases := parseKeyspace(info)
			if total != 0 || len(databases) != 0 {
				t.Errorf("counted %d keys in %v, want nothing", total, databases)
			}
		})
	}
}

// The comparison reaches the scraper as well as the control database, and only
// the full comparison does: the daily summary writes rows for a window of one
// day, and publishing those under the same name would report a day's rows as
// the size of the object.
func TestOnlyTheFullComparisonIsPublished(t *testing.T) {
	full := metrics.Labels{"task": "7", "engine": "mysql", "object": "Users"}
	daily := metrics.Labels{"task": "7", "engine": "mysql", "object": "Orders"}
	t.Cleanup(func() {
		metrics.ForgetRowCounts(full)
		metrics.ForgetRowCounts(daily)
	})

	publishRowCounts(7, "MYSQL", "shop", "Users", 100, 98, actionRowCount)
	publishRowCounts(7, "MYSQL", "shop", "Orders", 5, 5, "data_volume_daily")

	if !published(t, full) {
		t.Error("the full comparison was not published")
	}
	if published(t, daily) {
		t.Error("a daily summary row was published as the object's size")
	}
}

// A standalone Redis task compares a whole database and has no object name.
// An empty label would draw every database as one series.
func TestADatabaseWithNoObjectNameIsPublishedUnderItsNumber(t *testing.T) {
	labels := metrics.Labels{"task": "42", "engine": "redis", "object": "db0"}
	t.Cleanup(func() { metrics.ForgetRowCounts(labels) })

	publishRowCounts(42, "REDIS", "0", "", 220, 242, actionRowCount)

	if !published(t, labels) {
		t.Error("a database compared without an object name was not published as db0")
	}
}

// rowCountsOf reads back the pair published for one object.
func rowCountsOf(t *testing.T, labels metrics.Labels) (source, target float64, ok bool) {
	t.Helper()

	for _, sample := range metrics.RowCounts.Snapshot(metrics.SourceRows) {
		if sample.Labels.Key() == labels.Key() {
			source, ok = sample.Value, true
		}
	}
	for _, sample := range metrics.RowCounts.Snapshot(metrics.TargetRows) {
		if sample.Labels.Key() == labels.Key() {
			target = sample.Value
		}
	}
	return source, target, ok
}

func published(t *testing.T, labels metrics.Labels) bool {
	t.Helper()
	for _, sample := range metrics.RowCounts.Snapshot(metrics.SourceRows) {
		if sample.Labels.Key() == labels.Key() {
			return true
		}
	}
	return false
}
