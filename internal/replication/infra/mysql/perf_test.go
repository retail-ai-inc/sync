//go:build perf

// Performance of the MySQL path against a real pair of servers.
package mysql

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/test/harness"
)

const (
	perfSourceDB = "source_db"
	perfTargetDB = "target_db"
)

func perfInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

func perfOpen(t *testing.T, endpoint, database string) *sql.DB {
	t.Helper()

	host, port := harness.SplitHostPort(t, endpoint)
	db, err := sql.Open("mysql", dsn.BuildDSNByType("mysql", map[string]string{
		"user": "root", "password": "root", "host": host, "port": port, "database": database,
	}))
	if err != nil {
		t.Fatalf("open %s: %v", endpoint, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", endpoint, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// perfTable makes a table shaped like something a payment service would write
// to: a numeric key, money, a status and a couple of strings, so a row is not a
// single integer.
func perfTable(t *testing.T, src, tgt *sql.DB) string {
	t.Helper()

	table := harness.UniqueName("perf")
	if _, err := src.Exec(fmt.Sprintf(`CREATE TABLE %s (
		id BIGINT PRIMARY KEY,
		order_id VARCHAR(32) NOT NULL,
		amount BIGINT NOT NULL,
		currency CHAR(3) NOT NULL,
		status VARCHAR(16) NOT NULL,
		customer VARCHAR(32) NOT NULL,
		written_at DATETIME(3) NOT NULL
	)`, table)); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	t.Cleanup(func() {
		_, _ = src.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = tgt.Exec("DROP TABLE IF EXISTS " + table)
	})
	return table
}

func perfSyncer(t *testing.T, table string) config.SyncConfig {
	t.Helper()

	srcHost, srcPort := harness.SplitHostPort(t, harness.MySQLSource)
	tgtHost, tgtPort := harness.SplitHostPort(t, harness.MySQLTarget)

	cfg := config.SyncConfig{
		ID:     harness.UniqueTaskID(),
		Enable: true,
		Type:   "mysql",
		SourceConnection: dsn.BuildDSNByType("mysql", map[string]string{
			"user": "root", "password": "root", "host": srcHost, "port": srcPort,
			"database": perfSourceDB,
		}),
		TargetConnection: dsn.BuildDSNByType("mysql", map[string]string{
			"user": "root", "password": "root", "host": tgtHost, "port": tgtPort,
			"database": perfTargetDB,
		}),
		MySQLPositionPath: t.TempDir() + "/binlog.pos",
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{
			{SourceTable: table, TargetTable: table},
		}}},
	}

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	// The pipeline's syncer, so a measurement describes the code that runs.
	syncer := NewSyncer(cfg, logger)
	if syncer == nil {
		t.Fatal("NewSyncer returned nil")
	}
	t.Cleanup(harness.RunSyncer(t, syncer.Start))
	return cfg
}

func perfInsert(t *testing.T, db *sql.DB, table string, id int) error {
	t.Helper()

	_, err := db.Exec(fmt.Sprintf(
		"INSERT INTO %s (id, order_id, amount, currency, status, customer, written_at) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?)", table),
		id, fmt.Sprintf("ORD-%08d", id), id*13, "JPY", "captured",
		fmt.Sprintf("CUS-%06d", id%1000), time.Now().UTC())
	return err
}

func perfCount(db *sql.DB, table string) int64 {
	var n int64
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		return -1
	}
	return n
}

func percentiles(t *testing.T, latencies []time.Duration) {
	t.Helper()

	if len(latencies) == 0 {
		t.Fatal("no rows were traced")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	at := func(q float64) time.Duration {
		return latencies[int(float64(len(latencies)-1)*q)].Round(time.Millisecond)
	}
	t.Logf("traced %d rows: p50=%v p90=%v p99=%v max=%v", len(latencies),
		at(0.50), at(0.90), at(0.99),
		latencies[len(latencies)-1].Round(time.Millisecond))
}

// TestTheLatencyUnderSteadyWrites is the recovery-point measurement: with the
// source being written to continuously, how far behind is the replica?
func TestTheLatencyUnderSteadyWrites(t *testing.T) {
	src := perfOpen(t, harness.MySQLSource, perfSourceDB)
	tgt := perfOpen(t, harness.MySQLTarget, perfTargetDB)
	table := perfTable(t, src, tgt)

	if err := perfInsert(t, src, table, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	perfSyncer(t, table)
	harness.Eventually(t, 60*time.Second, func() error {
		if perfCount(tgt, table) != 1 {
			return fmt.Errorf("the syncer has not caught up with the seed row")
		}
		return nil
	})

	rate := perfInt("SYNC_PERF_RATE", 100)
	seconds := perfInt("SYNC_PERF_SECONDS", 30)
	sampleEvery := perfInt("SYNC_PERF_SAMPLE_EVERY", 50)
	t.Logf("writing at %d rows/s for %ds, tracing one row in %d", rate, seconds, sampleEvery)

	var (
		mu        sync.Mutex
		latencies []time.Duration
		traced    sync.WaitGroup
		written   int64
		worst     int64
	)

	var overran int
	trace := func(id int, sentAt time.Time) {
		defer traced.Done()
		// A row that takes longer than the window is a measurement, not a test
		// failure: falling behind at a given rate on given hardware is exactly
		// what this is here to find out.
		for time.Since(sentAt) < 120*time.Second {
			var one int
			err := tgt.QueryRow(fmt.Sprintf(
				"SELECT 1 FROM %s WHERE id = ?", table), id).Scan(&one)
			if err == nil {
				mu.Lock()
				latencies = append(latencies, time.Since(sentAt))
				mu.Unlock()
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		mu.Lock()
		overran++
		mu.Unlock()
	}

	// The backlog sampler, one count a second.
	stopSampling := make(chan struct{})
	sampling := make(chan struct{})
	go func() {
		defer close(sampling)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopSampling:
				return
			case <-ticker.C:
				behind := atomic.LoadInt64(&written) - (perfCount(tgt, table) - 1)
				if behind > atomic.LoadInt64(&worst) {
					atomic.StoreInt64(&worst, behind)
				}
			}
		}
	}()

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	deadline := start.Add(time.Duration(seconds) * time.Second)
	id := 1
	var slow int
	for now := range ticker.C {
		if now.After(deadline) {
			break
		}
		before := time.Now()
		if err := perfInsert(t, src, table, id); err != nil {
			t.Fatalf("insert %d: %v", id, err)
		}
		atomic.AddInt64(&written, 1)
		if time.Since(before) > interval {
			slow++
		}
		if id%sampleEvery == 0 {
			traced.Add(1)
			go trace(id, before)
		}
		id++
	}
	total := id - 1
	elapsed := time.Since(start)
	t.Logf("wrote %d rows in %v (%.0f/s achieved, %d writes slower than the interval)",
		total, elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds(), slow)

	traced.Wait()

	drainStart := time.Now()
	harness.Eventually(t, 5*time.Minute, func() error {
		if got := perfCount(tgt, table); got < int64(total+1) {
			return fmt.Errorf("the target holds %d of %d rows", got, total+1)
		}
		return nil
	})
	close(stopSampling)
	<-sampling

	percentiles(t, latencies)
	if overran > 0 {
		t.Logf("%d traced rows took longer than the two-minute window", overran)
	}
	t.Logf("the target caught up %v after the last write",
		time.Since(drainStart).Round(time.Millisecond))
	t.Logf("largest backlog observed: %d rows (%.1fs of writing at %d/s)",
		atomic.LoadInt64(&worst), float64(atomic.LoadInt64(&worst))/float64(rate), rate)
}

// TestTheApplyThroughput hands the syncer more than it can absorb at once and
// times the catch-up. It is the number that says whether a burst — a batch job,
// a backfill, a retry storm — is absorbed in seconds or in hours.
func TestTheApplyThroughput(t *testing.T) {
	src := perfOpen(t, harness.MySQLSource, perfSourceDB)
	tgt := perfOpen(t, harness.MySQLTarget, perfTargetDB)
	table := perfTable(t, src, tgt)

	if err := perfInsert(t, src, table, 0); err != nil {
		t.Fatalf("seed: %v", err)
	}
	perfSyncer(t, table)
	harness.Eventually(t, 60*time.Second, func() error {
		if perfCount(tgt, table) != 1 {
			return fmt.Errorf("the syncer has not started")
		}
		return nil
	})

	rows := perfInt("SYNC_PERF_BURST", 20000)
	const perStatement = 500

	// Write the burst as multi-row inserts, which is how a batch job would.
	writeStart := time.Now()
	id := 1
	for id <= rows {
		values := ""
		args := make([]interface{}, 0, perStatement*7)
		for i := 0; i < perStatement && id <= rows; i++ {
			if values != "" {
				values += ","
			}
			values += "(?,?,?,?,?,?,?)"
			args = append(args, id, fmt.Sprintf("ORD-%08d", id), id*13, "JPY",
				"captured", fmt.Sprintf("CUS-%06d", id%1000), time.Now().UTC())
			id++
		}
		if _, err := src.Exec(fmt.Sprintf(
			"INSERT INTO %s (id, order_id, amount, currency, status, customer, written_at) "+
				"VALUES %s", table, values), args...); err != nil {
			t.Fatalf("insert burst: %v", err)
		}
	}
	writeElapsed := time.Since(writeStart)
	t.Logf("the source took %v to accept %d rows (%.0f/s)",
		writeElapsed.Round(time.Millisecond), rows, float64(rows)/writeElapsed.Seconds())

	catchUp := time.Now()
	harness.Eventually(t, 10*time.Minute, func() error {
		if got := perfCount(tgt, table); got < int64(rows+1) {
			return fmt.Errorf("the target holds %d of %d rows", got, rows+1)
		}
		return nil
	})
	applied := time.Since(catchUp)
	t.Logf("the target absorbed %d rows in %v after the writes stopped (%.0f rows/s applied)",
		rows, applied.Round(time.Millisecond), float64(rows)/applied.Seconds())
}
