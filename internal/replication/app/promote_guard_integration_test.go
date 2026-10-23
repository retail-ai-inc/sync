//go:build integration

package app

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	intRedis "github.com/retail-ai-inc/sync/internal/platform/dbconn/redis"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/mongodb"
	"github.com/retail-ai-inc/sync/internal/replication/infra/mysql"
	"github.com/retail-ai-inc/sync/internal/replication/infra/postgresql"
	"github.com/retail-ai-inc/sync/internal/replication/infra/redis"
	"github.com/retail-ai-inc/sync/test/harness"
)

// The marker PromoteTarget writes is read back by a guard each engine builds
// for itself. These start the engine the supervisor would start, against the
// target just promoted, so the two cannot drift apart unnoticed.

// promoted stores a task, promotes its target and returns the task as the
// supervisor would load it.
func promoted(t *testing.T, cfg string) config.SyncConfig {
	t.Helper()

	db := useTempTaskDB(t)
	id := insertTask(t, db, 1, cfg)
	ctx := context.Background()
	t.Cleanup(func() { _ = DemoteTarget(ctx, itoa(id)) })

	if _, err := PromoteTarget(ctx, itoa(id), "operator"); err != nil {
		t.Fatalf("PromoteTarget: %v", err)
	}
	task, err := config.LoadSyncTask(int(id))
	if err != nil {
		t.Fatalf("load the promoted task: %v", err)
	}
	return task
}

// mustRefuse starts an engine and requires it to stop for good on its own.
func mustRefuse(t *testing.T, start func(context.Context) error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err := start(ctx)
	if ctx.Err() != nil {
		t.Fatalf("the task ran into a promoted target until it was cancelled (%v)", err)
	}
	if !domain.IsUnrecoverable(err) || !strings.Contains(err.Error(), "has been promoted") {
		t.Fatalf("the task started into a promoted target returned %v, want the promotion refusal", err)
	}
}

func TestAPromotedMySQLTargetStopsTheTask(t *testing.T) {
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.MySQLSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.MySQLTarget)
	table := harness.UniqueName("promoted")
	cfg := promoted(t, fmt.Sprintf(`{"type":"mysql",
		"sourceConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"source_db"},
		"targetConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"target_db"},
		"mysql_position_path":%q,
		"mappings":[{"tables":[{"sourceTable":%q,"targetTable":%q}]}]}`,
		sourceHost, sourcePort, targetHost, targetPort, t.TempDir()+"/binlog.pos", table, table))

	source := openSQL(t, "mysql", cfg.SourceConnection)
	target := openSQL(t, "mysql", cfg.TargetConnection)
	execSQL(t, source, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table))
	t.Cleanup(func() {
		_, _ = source.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = target.Exec("DROP TABLE IF EXISTS " + table)
	})
	execSQL(t, source, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table))

	// A failure means the MySQL guard looks for the marker somewhere PromoteTarget does not write it.
	mustRefuse(t, mysql.NewSyncer(cfg, quietLogger()).Start)

	var n int
	if err := target.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'target_db' AND table_name = ?`, table).Scan(&n); err != nil {
		t.Fatalf("look for the table on the target: %v", err)
	}
	if n != 0 {
		t.Errorf("the refused task created %s on the promoted target", table)
	}
}

func TestAPromotedPostgresTargetStopsTheTask(t *testing.T) {
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.PostgresSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.PostgresTarget)
	table := harness.UniqueName("promoted")
	slot := "slot_" + table
	cfg := promoted(t, fmt.Sprintf(`{"type":"postgresql",
		"sourceConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"source_db","sslmode":"disable"},
		"targetConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"target_db","sslmode":"disable"},
		"pg_replication_slot":%q,"pg_plugin":"pgoutput","pg_publication_names":%q,
		"pg_position_path":%q,
		"mappings":[{"tables":[{"sourceTable":%q,"targetTable":%q}]}]}`,
		sourceHost, sourcePort, targetHost, targetPort, slot, "pub_"+table,
		t.TempDir()+"/pg.pos", table, table))

	source := openSQL(t, "postgres", cfg.SourceConnection)
	target := openSQL(t, "postgres", cfg.TargetConnection)
	execSQL(t, source, fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY)", table))
	execSQL(t, source, fmt.Sprintf("CREATE PUBLICATION pub_%s FOR TABLE %s", table, table))
	t.Cleanup(func() {
		_, _ = source.Exec(`SELECT pg_drop_replication_slot($1)
			WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, slot)
		_, _ = source.Exec("DROP PUBLICATION IF EXISTS pub_" + table)
		_, _ = source.Exec("DROP TABLE IF EXISTS " + table)
		_, _ = target.Exec("DROP TABLE IF EXISTS " + table)
	})
	execSQL(t, source, fmt.Sprintf("INSERT INTO %s (id) VALUES (1)", table))

	// A failure means the PostgreSQL guard looks for the marker somewhere PromoteTarget does not write it.
	mustRefuse(t, postgresql.NewSyncer(cfg, quietLogger()).Start)

	var n int
	if err := source.QueryRow(`SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = $1`,
		slot).Scan(&n); err != nil {
		t.Fatalf("look for the slot: %v", err)
	}
	if n != 0 {
		t.Errorf("the refused task created slot %s on the source, which holds WAL for nothing", slot)
	}
}

func TestAPromotedMongoDBTargetStopsTheTask(t *testing.T) {
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.MongoSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.MongoTarget)
	collection := harness.UniqueName("promoted")
	cfg := promoted(t, fmt.Sprintf(`{"type":"mongodb",
		"sourceConn":{"host":%q,"port":%q,"database":"source_db","directConnection":"true"},
		"targetConn":{"host":%q,"port":%q,"database":"target_db","directConnection":"true"},
		"mongodb_resume_token_path":%q,
		"mappings":[{"tables":[{"sourceTable":%q,"targetTable":%q}]}]}`,
		sourceHost, sourcePort, targetHost, targetPort, t.TempDir(), collection, collection))

	ctx := context.Background()
	source := connectMongo(t, cfg.SourceConnection)
	target := connectMongo(t, cfg.TargetConnection)
	t.Cleanup(func() {
		_ = source.Database("source_db").Collection(collection).Drop(ctx)
		_ = target.Database("target_db").Collection(collection).Drop(ctx)
	})
	if _, err := source.Database("source_db").Collection(collection).
		InsertOne(ctx, bson.M{"seq": 1}); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	// A failure means the MongoDB guard looks for the marker somewhere PromoteTarget does not write it.
	mustRefuse(t, mongodb.NewSyncer(cfg, &config.Config{}, quietLogger()).Start)

	n, err := target.Database("target_db").Collection(collection).CountDocuments(ctx, bson.M{})
	if err != nil {
		t.Fatalf("count the target: %v", err)
	}
	if n != 0 {
		t.Errorf("the refused task copied %d documents into the promoted target", n)
	}
}

func TestAPromotedRedisTargetStopsTheTask(t *testing.T) {
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.RedisSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.RedisTarget)
	cfg := promoted(t, fmt.Sprintf(`{"type":"redis",
		"sourceConn":{"host":%q,"port":%q,"database":"0"},
		"targetConn":{"host":%q,"port":%q,"database":"0"},
		"redis_position_path":%q,"redis_buffer_dir":%q}`,
		sourceHost, sourcePort, targetHost, targetPort, t.TempDir()+"/redis.pos", t.TempDir()))

	ctx := context.Background()
	key := harness.UniqueName("promoted")
	source, err := intRedis.GetRedisClient(cfg.SourceConnection)
	if err != nil {
		t.Fatalf("connect to the source: %v", err)
	}
	defer source.Close()
	target, err := intRedis.GetRedisClient(cfg.TargetConnection)
	if err != nil {
		t.Fatalf("connect to the target: %v", err)
	}
	defer target.Close()
	t.Cleanup(func() {
		_ = source.Del(ctx, key).Err()
		_ = target.Del(ctx, key).Err()
	})
	if err := source.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatalf("seed the source: %v", err)
	}

	// A failure means the Redis guard looks for the marker somewhere PromoteTarget does not write it.
	mustRefuse(t, redis.NewSyncer(cfg, quietLogger()).Start)

	if n, err := target.Exists(ctx, key).Result(); err != nil || n != 0 {
		t.Errorf("the refused task copied %s into the promoted target (exists=%d, %v)", key, n, err)
	}
}

func openSQL(t *testing.T, driver, connection string) *sql.DB {
	t.Helper()

	db, err := sql.Open(driver, connection)
	if err != nil {
		t.Fatalf("open %s: %v", driver, err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping %s: %v", driver, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func execSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func connectMongo(t *testing.T, connection string) *mongo.Client {
	t.Helper()

	client, err := mongo.Connect(options.Client().ApplyURI(connection))
	if err != nil {
		t.Fatalf("connect to MongoDB: %v", err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client
}
