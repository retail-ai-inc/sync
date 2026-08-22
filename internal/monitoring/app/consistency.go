package app

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/slack"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
	"github.com/retail-ai-inc/sync/internal/replication/infra/verify"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

// The metrics a comparison records.
const (
	differencesMetric = "sync_verify_differences"
	verifiedAtMetric  = "sync_verify_last_run_timestamp_seconds"

	helpDifferences = "Rows or documents on which the source and the target disagree, at the last comparison"
	helpVerifiedAt  = "When the last comparison finished, as a Unix timestamp"
)

// verifyInterval reports how often to compare, and whether to compare at all.
//
// It is off unless asked for. A full comparison reads every row of every
// replicated table twice, which on a payment ledger is a real cost and a real
// load on the source; when to pay it is an operational decision rather than
// something to decide on somebody's behalf.
func verifyInterval() time.Duration {
	raw := os.Getenv("SYNC_VERIFY_INTERVAL")
	if raw == "" {
		return 0
	}
	interval, err := time.ParseDuration(raw)
	if err != nil || interval <= 0 {
		return 0
	}
	return interval
}

// repairEnabled reports whether a difference should be corrected as well as
// reported.
//
// Correcting means writing to the target from the source, which is what
// replication does anyway — but doing it automatically after a divergence
// nobody has looked at yet is a decision an operator has to make deliberately.
func repairEnabled() bool {
	return strings.EqualFold(os.Getenv("SYNC_VERIFY_REPAIR"), "true")
}

// StartConsistencyChecks compares each replicated table against its source on a
// schedule, and reports what disagrees.
//
// Replication can only report what it applied. It cannot report what it never
// read: an event dropped before the offset was written, a row written while a
// subscription was reconnecting, a change somebody made on the replica by hand.
// None of those surface as an error, and for a disaster-recovery copy of a
// payment ledger "probably identical" is not something anyone can act on.
func StartConsistencyChecks(ctx context.Context, cfg *config.Config, log *logrus.Logger) {
	interval := verifyInterval()
	if interval == 0 {
		log.Info("[Verify] Consistency checking is off; set SYNC_VERIFY_INTERVAL to turn it on")
		return
	}

	go func() {
		var notifier notifier
		if cfg != nil {
			notifier = slack.NewSlackNotifierFromConfig(cfg, log)
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runConsistencyChecks(ctx, cfg, notifier, log)
			}
		}
	}()
}

// runConsistencyChecks compares every task's tables once.
func runConsistencyChecks(ctx context.Context, cfg *config.Config, n notifier, log *logrus.Logger) {
	if cfg == nil {
		return
	}
	for _, task := range cfg.SyncConfigs {
		if !task.Enable {
			continue
		}
		switch strings.ToLower(task.Type) {
		case "mysql", "mariadb":
			checkSQLTask(ctx, task, n, log)
		case "mongodb":
			checkMongoTask(ctx, task, n, log)
		}
	}
}

// report records and announces one comparison.
func report(ctx context.Context, n notifier, log *logrus.Logger, task config.SyncConfig, name string, result verify.Result) {
	labels := metrics.Labels{
		"task":   strconv.Itoa(task.ID),
		"engine": task.Type,
		"table":  name,
		"source": dsn.Endpoint(task.Type, task.SourceConnection),
		"target": dsn.Endpoint(task.Type, task.TargetConnection),
	}
	metrics.Default.SetGauge(differencesMetric, helpDifferences, labels, float64(result.Total()))
	metrics.Default.SetGauge(verifiedAtMetric, helpVerifiedAt, labels, float64(time.Now().Unix()))

	if result.Identical() {
		log.Infof("[Verify] %s: %s", name, result.Summary())
		return
	}
	if result.Repaired > 0 || result.RepairFailed > 0 {
		log.Warnf("[Verify] %s: repaired %d of %d differences, %d could not be fixed",
			name, result.Repaired, result.Total(), result.RepairFailed)
	}

	message := fmt.Sprintf("⚠️ The replica does not match the source\n\nTask: %d\nTable: %s\n"+
		"Source: %s\nTarget: %s\n\n%s\n\nFirst differences: %s",
		task.ID, name, labels["source"], labels["target"], result.Summary(),
		describe(result.Sample))
	log.Warn(message)

	if n == nil || !n.IsConfigured() {
		return
	}
	if err := n.SendNotification(ctx, message, &slack.SlackNotificationOptions{
		AlertType: slack.SlackAlertWarning,
		Trigger:   "consistency-check",
	}); err != nil {
		log.Warnf("[Verify] Could not send the consistency alert: %v", err)
	}
}

// describe renders a handful of differences for a human to read.
func describe(sample []verify.Difference) string {
	const shown = 5
	if len(sample) > shown {
		sample = sample[:shown]
	}
	parts := make([]string, 0, len(sample))
	for _, d := range sample {
		parts = append(parts, fmt.Sprintf("%s %s", d.Kind, verify.DescribeKey(d.Key)))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}

// -------------------------------------------------------------------- SQL

func checkSQLTask(ctx context.Context, task config.SyncConfig, n notifier, log *logrus.Logger) {
	source, err := sql.Open("mysql", task.SourceConnection)
	if err != nil {
		log.Errorf("[Verify] Task %d: open the source: %v", task.ID, err)
		return
	}
	defer source.Close()

	target, err := sql.Open("mysql", task.TargetConnection)
	if err != nil {
		log.Errorf("[Verify] Task %d: open the target: %v", task.ID, err)
		return
	}
	defer target.Close()

	sourceDB := dsn.GetDatabaseName(task.Type, task.SourceConnection)
	targetDB := dsn.GetDatabaseName(task.Type, task.TargetConnection)

	for _, pair := range sqlTablePairs(ctx, task, source, sourceDB, log) {
		columns, err := verify.SQLColumns(ctx, source, sourceDB, pair.source)
		if err != nil {
			log.Errorf("[Verify] Task %d: %v", task.ID, err)
			continue
		}
		keys, err := primaryKey(ctx, source, sourceDB, pair.source)
		if err != nil {
			log.Warnf("[Verify] Task %d: %s cannot be compared: %v", task.ID, pair.source, err)
			continue
		}

		sourceSide := &verify.SQLEnd{DB: source, Schema: sourceDB, Table: pair.source, Keys: keys, Columns: columns}
		targetSide := &verify.SQLEnd{DB: target, Schema: targetDB, Table: pair.target, Keys: keys, Columns: columns}

		// Repairing during the walk rather than from the reported sample
		// afterwards: the sample is capped, so a table a thousand rows apart
		// used to need ten passes to converge with nothing saying how far along
		// it was.
		var fix func(verify.Difference) error
		if repairEnabled() {
			repairer := &verify.SQLRepairer{Source: sourceSide, Target: targetSide, Upsert: mysqlUpsert}
			fix = func(d verify.Difference) error {
				_, err := repairer.Repair(ctx, []verify.Difference{d})
				if err != nil {
					log.Errorf("[Verify] Task %d: could not repair %s of %s: %v",
						task.ID, d.Key, pair.source, err)
				}
				return err
			}
		}

		result, err := verify.CompareAndRepair(ctx, sourceSide, targetSide, 0, fix)
		if err != nil {
			log.Errorf("[Verify] Task %d: comparing %s: %v", task.ID, pair.source, err)
			continue
		}
		report(ctx, n, log, task, pair.source, result)
	}
}

// tablePair names one table on each side.
type tablePair struct{ source, target string }

// sqlTablePairs reports the tables to compare, discovering them when the task
// lists none — which is the same rule replication itself follows.
func sqlTablePairs(ctx context.Context, task config.SyncConfig, source *sql.DB, sourceDB string, log *logrus.Logger) []tablePair {
	var pairs []tablePair
	for _, mapping := range task.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			target := table.TargetTable
			if target == "" {
				target = table.SourceTable
			}
			pairs = append(pairs, tablePair{source: table.SourceTable, target: target})
		}
	}
	if len(pairs) > 0 {
		return pairs
	}

	tables, err := discovery.MySQLTables(ctx, source, sourceDB)
	if err != nil {
		log.Errorf("[Verify] Task %d: %v", task.ID, err)
		return nil
	}
	for _, table := range tables {
		pairs = append(pairs, tablePair{source: table, target: table})
	}
	return pairs
}

// primaryKey reports the columns a table's rows are identified by, in order.
//
// A composite key is returned whole. Comparing on part of one would report
// differences that are not there — every row sharing the first column would look
// like a duplicate — and a payment ledger's tables are commonly keyed by a pair,
// so refusing them would have left the tables that matter most unverifiable.
func primaryKey(ctx context.Context, db *sql.DB, schema, table string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.key_column_usage
		 WHERE table_schema = ? AND table_name = ? AND constraint_name = 'PRIMARY'
		 ORDER BY ordinal_position`, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("it has no primary key, so a row cannot be addressed")
	}
	return columns, nil
}

// mysqlUpsert renders the statement a repair writes with.
func mysqlUpsert(schema, table string, columns []string) string {
	name := table
	if schema != "" {
		name = schema + "." + table
	}
	placeholders := make([]string, len(columns))
	assignments := make([]string, len(columns))
	for i, c := range columns {
		placeholders[i] = "?"
		assignments[i] = fmt.Sprintf("%s = VALUES(%s)", c, c)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		name, strings.Join(columns, ", "), strings.Join(placeholders, ", "),
		strings.Join(assignments, ", "))
}

// ---------------------------------------------------------------- MongoDB

func checkMongoTask(ctx context.Context, task config.SyncConfig, n notifier, log *logrus.Logger) {
	source, err := mongo.Connect(ctx, mongooptions.Client().ApplyURI(task.SourceConnection))
	if err != nil {
		log.Errorf("[Verify] Task %d: connect to the source: %v", task.ID, err)
		return
	}
	defer func() { _ = source.Disconnect(ctx) }()

	target, err := mongo.Connect(ctx, mongooptions.Client().ApplyURI(task.TargetConnection))
	if err != nil {
		log.Errorf("[Verify] Task %d: connect to the target: %v", task.ID, err)
		return
	}
	defer func() { _ = target.Disconnect(ctx) }()

	sourceDB := source.Database(dsn.GetDatabaseName(task.Type, task.SourceConnection))
	targetDB := target.Database(dsn.GetDatabaseName(task.Type, task.TargetConnection))

	for _, pair := range mongoCollectionPairs(ctx, task, sourceDB, log) {
		sourceColl := sourceDB.Collection(pair.source)
		targetColl := targetDB.Collection(pair.target)

		var fix func(verify.Difference) error
		if repairEnabled() {
			repairer := &verify.MongoRepairer{Source: sourceColl, Target: targetColl}
			fix = func(d verify.Difference) error {
				_, err := repairer.Repair(ctx, []verify.Difference{d})
				if err != nil {
					log.Errorf("[Verify] Task %d: could not repair %s of %s: %v",
						task.ID, d.Key, pair.source, err)
				}
				return err
			}
		}

		result, err := verify.CompareAndRepair(ctx,
			&verify.MongoEnd{Coll: sourceColl}, &verify.MongoEnd{Coll: targetColl}, 0, fix)
		if err != nil {
			log.Errorf("[Verify] Task %d: comparing %s: %v", task.ID, pair.source, err)
			continue
		}
		report(ctx, n, log, task, pair.source, result)
	}
}

// mongoCollectionPairs reports the collections to compare, discovering them when
// the task lists none.
func mongoCollectionPairs(ctx context.Context, task config.SyncConfig, sourceDB *mongo.Database, log *logrus.Logger) []tablePair {
	var pairs []tablePair
	for _, mapping := range task.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			target := table.TargetTable
			if target == "" {
				target = table.SourceTable
			}
			pairs = append(pairs, tablePair{source: table.SourceTable, target: target})
		}
	}
	if len(pairs) > 0 {
		return pairs
	}

	names, err := discovery.MongoCollections(ctx, sourceDB)
	if err != nil {
		log.Errorf("[Verify] Task %d: %v", task.ID, err)
		return nil
	}
	for _, name := range names {
		pairs = append(pairs, tablePair{source: name, target: name})
	}
	return pairs
}
