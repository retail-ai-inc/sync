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
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"github.com/retail-ai-inc/sync/internal/replication/infra/verify"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongooptions "go.mongodb.org/mongo-driver/v2/mongo/options"
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
// The stored setting is the value. SYNC_VERIFY_INTERVAL still overrules it,
// because that is how this is deployed today and an upgrade that ignored it
// would change behaviour without anyone asking.
func verifyInterval() time.Duration {
	if raw := os.Getenv("SYNC_VERIFY_INTERVAL"); raw != "" {
		interval, err := time.ParseDuration(raw)
		if err != nil || interval <= 0 {
			return 0
		}
		return interval
	}

	stored, err := config.LoadSettings()
	if err != nil {
		// Reported by the caller: off because the settings could not be read is
		// a different thing from off because nobody turned it on.
		return 0
	}
	return stored.VerifyInterval
}

// repairEnabled reports whether a difference should be corrected as well as
// reported.
//
// Correcting means writing to the target from the source, which is what
// replication does anyway — but doing it automatically after a divergence
// nobody has looked at yet is a decision an operator has to make deliberately.
func repairEnabled() bool {
	if raw := os.Getenv("SYNC_VERIFY_REPAIR"); raw != "" {
		return strings.EqualFold(raw, "true")
	}
	stored, err := config.LoadSettings()
	if err != nil {
		return false
	}
	return stored.VerifyRepair
}

// StartConsistencyChecks compares each replicated table against its source on a
// schedule, and reports what disagrees. Replication can only report what it
// applied — never what it failed to read: an event dropped before the offset
// was written, a row written during a reconnect, a change somebody made on the
// replica by hand.
//
// tasks reports the current task list rather than being read from cfg once.
// The sweep walks it on every tick, and a list captured at start-up went stale
// the moment a task was added, edited, disabled or deleted.
func StartConsistencyChecks(ctx context.Context, cfg *config.Config, log *logrus.Logger,
	tasks func() []config.SyncConfig) {
	interval := verifyInterval()
	if interval == 0 {
		log.Info("[Verify] Consistency checking is off; set SYNC_VERIFY_INTERVAL to turn it on")
		return
	}

	watch(func() {
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
				runConsistencyChecks(ctx, tasks(), cfg, notifier, log)
			}
		}
	})
}

func runConsistencyChecks(ctx context.Context, tasks []config.SyncConfig,
	cfg *config.Config, n notifier, log *logrus.Logger) {
	if cfg == nil {
		return
	}
	// Once for the sweep. It reads the control database, and asking per table
	// meant one open per table -- a whole-database task has a hundred of them.
	repair := repairEnabled()

	for _, task := range tasks {
		if !task.Enable {
			continue
		}
		switch strings.ToLower(task.Type) {
		case "mysql", "mariadb":
			checkSQLTask(ctx, task, n, log, repair)
		case "mongodb":
			checkMongoTask(ctx, task, n, log, repair)
		}
	}
}

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

func checkSQLTask(ctx context.Context, task config.SyncConfig, n notifier, log *logrus.Logger,
	repair bool) {
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
		columns, err := verify.SQLColumns(ctx, source, sourceDB, pair.Source)
		if err != nil {
			log.Errorf("[Verify] Task %d: %v", task.ID, err)
			continue
		}
		keys, err := primaryKey(ctx, source, sourceDB, pair.Source)
		if err != nil {
			log.Warnf("[Verify] Task %d: %s cannot be compared: %v", task.ID, pair.Source, err)
			continue
		}

		sourceSide, targetSide, repairer, err := sqlComparison(task, pair, source, target,
			sourceDB, targetDB, keys, columns)
		if err != nil {
			log.Warnf("[Verify] Task %d: %s cannot be compared: %v", task.ID, pair.Source, err)
			continue
		}

		// Repairing during the walk rather than from the reported sample
		// afterwards: the sample is capped, so a table a thousand rows apart
		// used to need ten passes to converge with nothing saying how far along
		// it was.
		var fix func(verify.Difference) error
		if repair {
			fix = func(d verify.Difference) error {
				_, err := repairer.Repair(ctx, []verify.Difference{d})
				if err != nil {
					log.Errorf("[Verify] Task %d: could not repair %s of %s: %v",
						task.ID, d.Key, pair.Source, err)
				}
				return err
			}
		}

		result, err := verify.CompareAndRepair(ctx, sourceSide, targetSide, 0, fix)
		if err != nil {
			log.Errorf("[Verify] Task %d: comparing %s: %v", task.ID, pair.Source, err)
			continue
		}
		report(ctx, n, log, task, pair.Source, result)
	}
}

// sqlComparison builds one table's comparison against what replication writes:
// the table's field security is applied to the source as replication applies it.
func sqlComparison(task config.SyncConfig, pair discovery.Pair, source, target *sql.DB,
	sourceDB, targetDB string, keys, columns []string) (*verify.SQLEnd, *verify.SQLEnd, *verify.SQLRepairer, error) {
	// By the source table and its target, as the stream and the first copy look it up.
	protection := verify.ProtectionOf(security.FindTableSecurityFromMappings(security.TableRef{
		Database: sourceDB, Table: pair.Source, Target: pair.Target,
	}, task.Mappings))
	if protected := protection.Protected(keys); len(protected) > 0 {
		return nil, nil, nil, fmt.Errorf("its key column %s has field security, so its rows "+
			"cannot be matched with the target's", strings.Join(protected, ", "))
	}

	compared := protection.Comparable(columns)
	sourceSide := &verify.SQLEnd{DB: source, Schema: sourceDB, Table: pair.Source, Keys: keys,
		Columns: compared, Protect: protection}
	targetSide := &verify.SQLEnd{DB: target, Schema: targetDB, Table: pair.Target, Keys: keys,
		Columns: compared}
	repairer := &verify.SQLRepairer{Source: sourceSide, Target: targetSide, Upsert: mysqlUpsert,
		Columns: columns, Protect: protection}
	return sourceSide, targetSide, repairer, nil
}

// sqlTablePairs reports the tables to compare, discovering them when the task
// lists none — which is the same rule replication itself follows.
func sqlTablePairs(ctx context.Context, task config.SyncConfig, source *sql.DB, sourceDB string, log *logrus.Logger) []discovery.Pair {
	if pairs := discovery.ConfiguredPairs(task.Mappings); len(pairs) > 0 {
		return pairs
	}

	tables, err := discovery.MySQLTables(ctx, source, sourceDB)
	if err != nil {
		log.Errorf("[Verify] Task %d: %v", task.ID, err)
		return nil
	}
	return discovery.SamePairs(tables)
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

func checkMongoTask(ctx context.Context, task config.SyncConfig, n notifier, log *logrus.Logger,
	repair bool) {
	source, err := mongo.Connect(mongooptions.Client().ApplyURI(task.SourceConnection))
	if err != nil {
		log.Errorf("[Verify] Task %d: connect to the source: %v", task.ID, err)
		return
	}
	defer func() { _ = source.Disconnect(ctx) }()

	target, err := mongo.Connect(mongooptions.Client().ApplyURI(task.TargetConnection))
	if err != nil {
		log.Errorf("[Verify] Task %d: connect to the target: %v", task.ID, err)
		return
	}
	defer func() { _ = target.Disconnect(ctx) }()

	sourceDB := source.Database(dsn.GetDatabaseName(task.Type, task.SourceConnection))
	targetDB := target.Database(dsn.GetDatabaseName(task.Type, task.TargetConnection))

	for _, pair := range mongoCollectionPairs(ctx, task, sourceDB, log) {
		sourceSide, targetSide, repairer, err := mongoComparison(task, pair,
			sourceDB.Collection(pair.Source), targetDB.Collection(pair.Target))
		if err != nil {
			log.Warnf("[Verify] Task %d: %s cannot be compared: %v", task.ID, pair.Source, err)
			continue
		}

		var fix func(verify.Difference) error
		if repair {
			fix = func(d verify.Difference) error {
				_, err := repairer.Repair(ctx, []verify.Difference{d})
				if err != nil {
					log.Errorf("[Verify] Task %d: could not repair %s of %s: %v",
						task.ID, d.Key, pair.Source, err)
				}
				return err
			}
		}

		result, err := verify.CompareAndRepair(ctx, sourceSide, targetSide, 0, fix)
		if err != nil {
			log.Errorf("[Verify] Task %d: comparing %s: %v", task.ID, pair.Source, err)
			continue
		}
		report(ctx, n, log, task, pair.Source, result)
	}
}

// mongoComparison builds one collection's comparison against what replication
// writes: the collection's field security is applied to the source as
// replication applies it.
func mongoComparison(task config.SyncConfig, pair discovery.Pair, source, target *mongo.Collection) (
	*verify.MongoEnd, *verify.MongoEnd, *verify.MongoRepairer, error) {
	// By the source's database and name, as the MongoDB syncer looks it up.
	protection := verify.ProtectionOf(security.FindTableSecurityFromMappings(security.TableRef{
		Database: dsn.GetDatabaseName(task.Type, task.SourceConnection), Table: pair.Source,
	}, task.Mappings))
	if protection.ProtectsID() {
		return nil, nil, nil, fmt.Errorf("its _id has field security, so its documents " +
			"cannot be matched with the target's")
	}

	skip := protection.Encrypted()
	return &verify.MongoEnd{Coll: source, Protect: protection, Skip: skip},
		&verify.MongoEnd{Coll: target, Skip: skip},
		&verify.MongoRepairer{Source: source, Target: target, Protect: protection},
		nil
}

// mongoCollectionPairs reports the collections to compare, discovering them when
// the task lists none.
func mongoCollectionPairs(ctx context.Context, task config.SyncConfig, sourceDB *mongo.Database, log *logrus.Logger) []discovery.Pair {
	if pairs := discovery.ConfiguredPairs(task.Mappings); len(pairs) > 0 {
		return pairs
	}

	names, err := discovery.MongoCollections(ctx, sourceDB)
	if err != nil {
		log.Errorf("[Verify] Task %d: %v", task.ID, err)
		return nil
	}
	return discovery.SamePairs(names)
}
