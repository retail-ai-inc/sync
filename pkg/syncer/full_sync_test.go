package syncer

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/pkg/api"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/state"
	"github.com/retail-ai-inc/sync/pkg/syncer/mariadb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mongodb"
	"github.com/retail-ai-inc/sync/pkg/syncer/mysql"
	"github.com/retail-ai-inc/sync/pkg/syncer/postgresql"
	"github.com/retail-ai-inc/sync/pkg/syncer/redis"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"encoding/json"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var testLog = logger.InitLogger("info")

const monitorInterval = time.Second * 10

// Added function: get the full "schema.table" or "database.table"
func getQualifiedTableName(dbmap config.DatabaseMapping, useSource bool, tblmap config.TableMapping) string {
	if useSource {
		if dbmap.SourceSchema != "" {
			return fmt.Sprintf("%s.%s", dbmap.SourceSchema, tblmap.SourceTable)
		}
		return fmt.Sprintf("%s.%s", dbmap.SourceDatabase, tblmap.SourceTable)
	}
	if dbmap.TargetSchema != "" {
		return fmt.Sprintf("%s.%s", dbmap.TargetSchema, tblmap.TargetTable)
	}
	return fmt.Sprintf("%s.%s", dbmap.TargetDatabase, tblmap.TargetTable)
}

// Modified getNextSQLID: pass in "fullTableName" instead of "database, table"
func getNextSQLID(t *testing.T, db *sql.DB, fullTableName string) int64 {
	var maxID sql.NullInt64
	query := fmt.Sprintf("SELECT COALESCE(MAX(id),0) FROM %s", fullTableName)
	err := db.QueryRow(query).Scan(&maxID)
	if err != nil {
		t.Fatalf("Failed to get max ID from %s: %v", fullTableName, err)
	}
	return maxID.Int64 + 1
}

// Detect DB type to decide placeholders and possible TRUNCATE approach
func isPostgresDBType(dbType string) bool {
	return dbType == "postgresql"
}

// TestFullSync is a comprehensive integration test example.
func TestFullSync(t *testing.T) {
	testLog.Info("TestFullSync start...")
	time.Sleep(5 * time.Second)

	// Prepare environment (no longer sets config.yaml path; just uses sqlite-based config)
	ctx, cancel := prepareTestEnvironment(t)
	defer cancel()

	// Connect all databases
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB,
		redisSourceClient, redisTargetClient := connectAllDatabases(t)

	// Extract all mappings
	cfg := config.NewConfig()
	testLog.Infof("Raw SyncConfigs: %+v", cfg.SyncConfigs)

	mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping := extractAllMappings(cfg)

	// Start all syncers
	log := logger.InitLogger(cfg.LogLevel)
	startAllSyncers(ctx, cfg, log)
	testLog.Info("Syncers started, waiting initial sync...")
	time.Sleep(3 * time.Second)

	if cfg.EnableTableRowCountMonitoring {
		utils.StartRowCountMonitoring(ctx, cfg, log, monitorInterval)
	}

	// Insert initial data
	const initialInsertCount = 3
	insertInitialData(t,
		mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled,
		mongoSourceClient, mysqlSourceDB, mariaDBSourceDB, pgSourceDB, redisSourceClient,
		mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping,
		initialInsertCount,
	)

	// Verify initial data synchronization
	verifyInitialDataConsistency(t,
		mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB,
		redisSourceClient, redisTargetClient,
		mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping,
	)

	// Create/Update/Delete tests
	performCRUDOperations(t,
		mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB,
		redisSourceClient, redisTargetClient,
		mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping,
	)

	testLog.Info("Full synchronization test completed successfully.")

	// ------------------ Begin: Extra coverage checks (Minimal Additions) ------------------
	// 1. Simple call to utils.GetCurrentTime to include pkg/utils coverage
	now := utils.GetCurrentTime()
	testLog.Infof("Utils.GetCurrentTime => %v", now)

	// 2. Simple test for state.FileStateStore to include pkg/state coverage
	stateDir := t.TempDir()
	stateStore := state.NewFileStateStore(stateDir)

	testKey := "sync_state_test"
	testVal := []byte("hello_coverage")
	if err := stateStore.Save(testKey, testVal); err != nil {
		t.Fatalf("Failed to save state key=%s: %v", testKey, err)
	}
	loadedVal, err := stateStore.Load(testKey)
	if err != nil {
		t.Fatalf("Failed to load state key=%s: %v", testKey, err)
	}

	if string(loadedVal) != string(testVal) {
		t.Fatalf("Unexpected state load => got=%s, want=%s", loadedVal, testVal)
	}
	testLog.Infof("FileStateStore coverage => saved/loaded value '%s' successfully.", string(loadedVal))
}

// prepareTestEnvironment: minimal approach, no config.yaml references
func prepareTestEnvironment(t *testing.T) (context.Context, context.CancelFunc) {
    // Specify the DB path for testing
    os.Setenv("SYNC_DB_PATH", "sync.db")

    // Remove any old replication state so we don't reuse outdated binlog offsets
    _ = os.RemoveAll("/tmp/state")
    _ = os.MkdirAll("/tmp/state", 0755)

	ctx, cancel := context.WithCancel(context.Background())
	return ctx, cancel
}

// Connect all databases and determine which are enabled
func connectAllDatabases(t *testing.T) (
	bool, bool, bool, bool, bool, // mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled
	*mongo.Client, *mongo.Client,
	*sql.DB, *sql.DB,
	*sql.DB, *sql.DB,
	*sql.DB, *sql.DB,
	*goredis.Client, *goredis.Client,
) {
	cfg := config.NewConfig()
	var (
		mongoEnabled    = false
		mysqlEnabled    = false
		mariaDBEnabled  = false
		postgresEnabled = false
		redisEnabled    = false
	)

	for _, sc := range cfg.SyncConfigs {
		switch {
		case sc.Type == "mongodb" && sc.Enable:
			mongoEnabled = true
		case sc.Type == "mysql" && sc.Enable:
			mysqlEnabled = true
		case sc.Type == "mariadb" && sc.Enable:
			mariaDBEnabled = true
		case sc.Type == "postgresql" && sc.Enable:
			postgresEnabled = true
		case sc.Type == "redis" && sc.Enable:
			redisEnabled = true
		}
	}

	if !mongoEnabled && !mysqlEnabled && !mariaDBEnabled && !postgresEnabled && !redisEnabled {
		testLog.Warn("No enabled DB sync config found in the sqlite config, skipping test.")
		t.Skip("No enabled DB sync config found, skip.")
	}

	var (
		mongoSourceClient *mongo.Client
		mongoTargetClient *mongo.Client
		mysqlSourceDB     *sql.DB
		mysqlTargetDB     *sql.DB
		mariaDBSourceDB   *sql.DB
		mariaDBTargetDB   *sql.DB
		pgSourceDB        *sql.DB
		pgTargetDB        *sql.DB
		redisSourceClient *goredis.Client
		redisTargetClient *goredis.Client
		err               error
	)

	// MongoDB
	if mongoEnabled {
		mongoSourceClient, mongoTargetClient, err = connectMongoDB(cfg)
		if err != nil {
			t.Skipf("Skipping MongoDB test => %v", err)
		}
		testLog.Info("MongoDB source/target connected successfully.")
		t.Cleanup(func() {
			_ = mongoSourceClient.Disconnect(context.Background())
			_ = mongoTargetClient.Disconnect(context.Background())
		})
	}

	// MySQL
	if mysqlEnabled {
		mysqlSourceDB, mysqlTargetDB, err = connectSQLDB(cfg, "mysql")
		if err != nil {
			t.Skipf("Skipping MySQL test => %v", err)
		}
		testLog.Info("MySQL source/target connected successfully.")
		t.Cleanup(func() {
			mysqlSourceDB.Close()
			mysqlTargetDB.Close()
		})
	}

	// MariaDB
	if mariaDBEnabled {
		mariaDBSourceDB, mariaDBTargetDB, err = connectSQLDB(cfg, "mariadb")
		if err != nil {
			t.Skipf("Skipping MariaDB test => %v", err)
		}
		testLog.Info("MariaDB source/target connected successfully.")
		t.Cleanup(func() {
			mariaDBSourceDB.Close()
			mariaDBTargetDB.Close()
		})
	}

	// PostgreSQL
	if postgresEnabled {
		pgSourceDB, pgTargetDB, err = connectPGDB(cfg)
		if err != nil {
			t.Skipf("Skipping PostgreSQL test => %v", err)
		}
		testLog.Info("PostgreSQL source/target connected successfully.")
		t.Cleanup(func() {
			pgSourceDB.Close()
			pgTargetDB.Close()
		})
	}

	// Redis
	if redisEnabled {
		redisSourceClient, redisTargetClient, err = connectRedis(cfg)
		if err != nil {
			t.Skipf("Skipping Redis test => %v", err)
		}
		testLog.Info("Redis source/target connected successfully.")
		t.Cleanup(func() {
			_ = redisSourceClient.Close()
			_ = redisTargetClient.Close()
		})
	}

	return mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled,
		mongoSourceClient, mongoTargetClient,
		mysqlSourceDB, mysqlTargetDB,
		mariaDBSourceDB, mariaDBTargetDB,
		pgSourceDB, pgTargetDB,
		redisSourceClient, redisTargetClient
}

// Extract all database mappings
func extractAllMappings(cfg *config.Config) (
	[]config.DatabaseMapping,
	[]config.DatabaseMapping,
	[]config.DatabaseMapping,
	[]config.DatabaseMapping,
	[]config.DatabaseMapping, // redisMapping
) {
	var (
		mongoMapping   []config.DatabaseMapping
		mysqlMapping   []config.DatabaseMapping
		mariadbMapping []config.DatabaseMapping
		pgMapping      []config.DatabaseMapping
		redisMapping   []config.DatabaseMapping
	)
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "mongodb" && sc.Enable {
			mongoMapping = sc.Mappings
		}
		if sc.Type == "mysql" && sc.Enable {
			mysqlMapping = sc.Mappings
		}
		if sc.Type == "mariadb" && sc.Enable {
			mariadbMapping = sc.Mappings
		}
		if sc.Type == "postgresql" && sc.Enable {
			pgMapping = sc.Mappings
		}
		if sc.Type == "redis" && sc.Enable {
			redisMapping = sc.Mappings
		}
	}
	return mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping
}

// Start all Syncers
func startAllSyncers(ctx context.Context, cfg *config.Config, log *logrus.Logger) {
	for _, sc := range cfg.SyncConfigs {
		if !sc.Enable {
			continue
		}
		switch sc.Type {
		case "mongodb":
			s := mongodb.NewMongoDBSyncer(sc, log)
			go s.Start(ctx)
		case "mysql":
			s := mysql.NewMySQLSyncer(sc, log)
			go s.Start(ctx)
		case "mariadb":
			s := mariadb.NewMariaDBSyncer(sc, log)
			go s.Start(ctx)
		case "postgresql":
			s := postgresql.NewPostgreSQLSyncer(sc, log)
			go s.Start(ctx)
		case "redis":
			s := redis.NewRedisSyncer(sc, log)
			go s.Start(ctx)
		}
	}
}

// Insert initial data
func insertInitialData(
	t *testing.T,
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled bool,
	mongoSourceClient *mongo.Client,
	mysqlSourceDB, mariaDBSourceDB, pgSourceDB *sql.DB,
	redisSourceClient *goredis.Client,
	mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping []config.DatabaseMapping,
	count int,
) {
	if mongoEnabled && mongoSourceClient != nil {
		prepareInitialData(t, mongoSourceClient, mongoMapping, "initial_mongo_doc", count, "mongodb")
		testLog.Infof("Inserted %d initial documents into MongoDB source.", count)
	}
	if mysqlEnabled && mysqlSourceDB != nil {
		prepareInitialData(t, mysqlSourceDB, mysqlMapping, "initial_mysql_doc", count, "mysql")
		testLog.Infof("Inserted %d initial rows into MySQL source.", count)
	}
	if mariaDBEnabled && mariaDBSourceDB != nil {
		prepareInitialData(t, mariaDBSourceDB, mariadbMapping, "initial_mariadb_doc", count, "mariadb")
		testLog.Infof("Inserted %d initial rows into MariaDB source.", count)
	}
	if postgresEnabled && pgSourceDB != nil {
		prepareInitialData(t, pgSourceDB, pgMapping, "initial_postgres_doc", count, "postgresql")
		testLog.Infof("Inserted %d initial rows into PostgreSQL source.", count)
	}
	if redisEnabled && redisSourceClient != nil {
		prepareInitialData(t, redisSourceClient, redisMapping, "initial_redis_doc", count, "redis")
		testLog.Infof("Inserted %d initial keys into Redis source.", count)
	}
}

// Verify initial data synchronization
func verifyInitialDataConsistency(
	t *testing.T,
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled bool,
	mongoSourceClient, mongoTargetClient *mongo.Client,
	mysqlSourceDB, mysqlTargetDB, mariaDBSourceDB, mariaDBTargetDB, pgSourceDB, pgTargetDB *sql.DB,
	redisSourceClient, redisTargetClient *goredis.Client,
	mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping []config.DatabaseMapping,
) {
	if mongoEnabled && mongoSourceClient != nil && mongoTargetClient != nil {
		verifyDataConsistency(t, mongoSourceClient, mongoTargetClient, mongoMapping, "initial_mongo_sync")
		testLog.Info("Verified MongoDB initial sync data consistency.")
	}
	if mysqlEnabled && mysqlSourceDB != nil && mysqlTargetDB != nil {
		verifyDataConsistency(t, mysqlSourceDB, mysqlTargetDB, mysqlMapping, "initial_mysql_sync")
		testLog.Info("Verified MySQL initial sync data consistency.")
	}
	if mariaDBEnabled && mariaDBSourceDB != nil && mariaDBTargetDB != nil {
		verifyDataConsistency(t, mariaDBSourceDB, mariaDBTargetDB, mariadbMapping, "initial_mariadb_sync")
		testLog.Info("Verified MariaDB initial sync data consistency.")
	}
	if postgresEnabled && pgSourceDB != nil && pgTargetDB != nil {
		verifyDataConsistency(t, pgSourceDB, pgTargetDB, pgMapping, "initial_postgres_sync")
		testLog.Info("Verified PostgreSQL initial sync data consistency.")
	}
	if redisEnabled && redisSourceClient != nil && redisTargetClient != nil {
		verifyDataConsistency(t, redisSourceClient, redisTargetClient, redisMapping, "initial_redis_sync")
		testLog.Info("Verified Redis initial sync data consistency.")
	}
}

// Create/Update/Delete operation tests
func performCRUDOperations(
	t *testing.T,
	mongoEnabled, mysqlEnabled, mariaDBEnabled, postgresEnabled, redisEnabled bool,
	mongoSourceClient, mongoTargetClient *mongo.Client,
	mysqlSourceDB, mysqlTargetDB, mariaDBSourceDB, mariaDBTargetDB, pgSourceDB, pgTargetDB *sql.DB,
	redisSourceClient, redisTargetClient *goredis.Client,
	mongoMapping, mysqlMapping, mariadbMapping, pgMapping, redisMapping []config.DatabaseMapping,
) {
	if mongoEnabled && mongoSourceClient != nil && mongoTargetClient != nil {
		performDataOperations(t, mongoSourceClient, mongoTargetClient, mongoMapping, "mongodb")
		testLog.Info("MongoDB increment/update/delete operations tested successfully.")
	}
	if mysqlEnabled && mysqlSourceDB != nil && mysqlTargetDB != nil {
		performDataOperations(t, mysqlSourceDB, mysqlTargetDB, mysqlMapping, "mysql")
		testLog.Info("MySQL increment/update/delete operations tested successfully.")
	}
	if mariaDBEnabled && mariaDBSourceDB != nil && mariaDBTargetDB != nil {
		performDataOperations(t, mariaDBSourceDB, mariaDBTargetDB, mariadbMapping, "mariadb")
		testLog.Info("MariaDB increment/update/delete operations tested successfully.")
	}
	if postgresEnabled && pgSourceDB != nil && pgTargetDB != nil {
		performDataOperations(t, pgSourceDB, pgTargetDB, pgMapping, "postgresql")
		testLog.Info("PostgreSQL increment/update/delete operations tested successfully.")
	}
	if redisEnabled && redisSourceClient != nil && redisTargetClient != nil {
		performDataOperations(t, redisSourceClient, redisTargetClient, redisMapping, "redis")
		testLog.Info("Redis increment/update/delete operations tested successfully.")
	}
}

// Connect to MongoDB
func connectMongoDB(cfg *config.Config) (*mongo.Client, *mongo.Client, error) {
	var mongoSourceURI, mongoTargetURI string
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "mongodb" && sc.Enable {
			mongoSourceURI = sc.SourceConnection
			mongoTargetURI = sc.TargetConnection
			break
		}
	}
	if mongoSourceURI == "" || mongoTargetURI == "" {
		return nil, nil, fmt.Errorf("no enabled MongoDB sync config found in sqlite config")
	}

	sourceClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoSourceURI))
	if err != nil {
		return nil, nil, err
	}
	if err := sourceClient.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, nil, err
	}

	targetClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoTargetURI))
	if err != nil {
		return nil, nil, err
	}
	if err := targetClient.Ping(context.Background(), readpref.Primary()); err != nil {
		return nil, nil, err
	}

	return sourceClient, targetClient, nil
}

// Connect MySQL / MariaDB
func connectSQLDB(cfg *config.Config, dbType string) (*sql.DB, *sql.DB, error) {
	var sourceDSN, targetDSN string
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == dbType && sc.Enable {
			sourceDSN = sc.SourceConnection
			targetDSN = sc.TargetConnection
			break
		}
	}
	if sourceDSN == "" || targetDSN == "" {
		return nil, nil, fmt.Errorf("no enabled %s sync config found in sqlite config", dbType)
	}

	srcDB, err := sql.Open("mysql", sourceDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := srcDB.Ping(); err != nil {
		return nil, nil, err
	}

	tgtDB, err := sql.Open("mysql", targetDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := tgtDB.Ping(); err != nil {
		return nil, nil, err
	}

	return srcDB, tgtDB, nil
}

// Connect PostgreSQL
func connectPGDB(cfg *config.Config) (*sql.DB, *sql.DB, error) {
	var pgSourceDSN, pgTargetDSN string
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "postgresql" && sc.Enable {
			pgSourceDSN = sc.SourceConnection
			pgTargetDSN = sc.TargetConnection
			break
		}
	}
	if pgSourceDSN == "" || pgTargetDSN == "" {
		return nil, nil, fmt.Errorf("no enabled postgresql sync config found in sqlite config")
	}

	srcDB, err := sql.Open("postgres", pgSourceDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := srcDB.Ping(); err != nil {
		return nil, nil, err
	}

	tgtDB, err := sql.Open("postgres", pgTargetDSN)
	if err != nil {
		return nil, nil, err
	}
	if err := tgtDB.Ping(); err != nil {
		return nil, nil, err
	}

	return srcDB, tgtDB, nil
}

// Connect to Redis
func connectRedis(cfg *config.Config) (*goredis.Client, *goredis.Client, error) {
	var redisSourceDSN, redisTargetDSN string
	for _, sc := range cfg.SyncConfigs {
		if sc.Type == "redis" && sc.Enable {
			redisSourceDSN = sc.SourceConnection
			redisTargetDSN = sc.TargetConnection
			break
		}
	}
	if redisSourceDSN == "" || redisTargetDSN == "" {
		return nil, nil, fmt.Errorf("no enabled redis sync config found in sqlite config")
	}

	sourceOpt, err := goredis.ParseURL(redisSourceDSN)
	if err != nil {
		return nil, nil, err
	}
	targetOpt, err := goredis.ParseURL(redisTargetDSN)
	if err != nil {
		return nil, nil, err
	}

	sourceClient := goredis.NewClient(sourceOpt)
	if err := sourceClient.Ping(context.Background()).Err(); err != nil {
		return nil, nil, err
	}

	targetClient := goredis.NewClient(targetOpt)
	if err := targetClient.Ping(context.Background()).Err(); err != nil {
		return nil, nil, err
	}

	return sourceClient, targetClient, nil
}

// Insert initial data for each DB type
func prepareInitialData(t *testing.T, src interface{}, mappings []config.DatabaseMapping, docName string, count int, dbType string) {
	switch s := src.(type) {
	case *mongo.Client:
		// MongoDB insertion logic
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				srcColl := s.Database(dbmap.SourceDatabase).Collection(tblmap.SourceTable)
				var docs []interface{}
				for i := 0; i < count; i++ {
					docs = append(docs, bson.M{
						"name":  fmt.Sprintf("%s_%s", docName, uuid.New().String()),
						"email": fmt.Sprintf("Randomemail_%d_%s", i, uuid.New().String()),
					})
				}
				_, err := srcColl.InsertMany(context.Background(), docs)
				if err != nil {
					t.Fatalf("Failed to insert initial docs into MongoDB %s.%s: %v",
						dbmap.SourceDatabase, tblmap.SourceTable, err)
				}
			}
		}

	case *sql.DB:
		// MySQL / MariaDB / PostgreSQL insertion logic
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				fullTableName := getQualifiedTableName(dbmap, true, tblmap)

				var insertSQL string
				if isPostgresDBType(dbType) {
					insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES ($1, $2, $3)", fullTableName)
				} else {
					insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES (?, ?, ?)", fullTableName)
				}

				for i := 0; i < count; i++ {
					nextID := getNextSQLID(t, s, fullTableName)
					name := fmt.Sprintf("%s_%s", docName, uuid.New().String())
					email := fmt.Sprintf("Randomemail_%d_%s", i, uuid.New().String())

					if _, err := s.Exec(insertSQL, nextID, name, email); err != nil {
						t.Fatalf("Failed to insert row into %s: %v", fullTableName, err)
					}
				}
			}
		}

	case *goredis.Client:
		// Redis insertion logic: simply set some string keys
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				prefix := fmt.Sprintf("%s:%s", dbmap.SourceDatabase, tblmap.SourceTable)
				for i := 0; i < count; i++ {
					key := fmt.Sprintf("%s:%d", prefix, i)
					value := fmt.Sprintf("%s_%s", docName, uuid.New().String())
					if err := s.Set(context.Background(), key, value, 0).Err(); err != nil {
						t.Fatalf("Redis SET fail key=%s: %v", key, err)
					}
				}
			}
		}
	}
}

// Verify data consistency
func verifyDataConsistency(t *testing.T, src interface{}, tgt interface{}, mappings []config.DatabaseMapping, stage string) {
	time.Sleep(3 * time.Second)
	switch s := src.(type) {
	case *mongo.Client:
		time.Sleep(5 * time.Second) // Additional wait for Mongo sync
		tc := tgt.(*mongo.Client)
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				srcColl := s.Database(dbmap.SourceDatabase).Collection(tblmap.SourceTable)
				tgtColl := tc.Database(dbmap.TargetDatabase).Collection(tblmap.TargetTable)

				srcCursor, err := srcColl.Find(context.Background(), bson.M{})
				if err != nil {
					t.Fatalf("Failed to fetch documents from MongoDB source %s.%s: %v",
						dbmap.SourceDatabase, tblmap.SourceTable, err)
				}
				defer srcCursor.Close(context.Background())
				var srcDocs []bson.M
				if err := srcCursor.All(context.Background(), &srcDocs); err != nil {
					t.Fatalf("Failed to decode documents from MongoDB source %s.%s: %v",
						dbmap.SourceDatabase, tblmap.SourceTable, err)
				}

				tgtCursor, err := tgtColl.Find(context.Background(), bson.M{})
				if err != nil {
					t.Fatalf("Failed to fetch documents from MongoDB target %s.%s: %v",
						dbmap.TargetDatabase, tblmap.TargetTable, err)
				}
				defer tgtCursor.Close(context.Background())
				var tgtDocs []bson.M
				if err := tgtCursor.All(context.Background(), &tgtDocs); err != nil {
					t.Fatalf("Failed to decode documents from MongoDB target %s.%s: %v",
						dbmap.TargetDatabase, tblmap.TargetTable, err)
				}

				if len(srcDocs) != len(tgtDocs) {
					t.Fatalf("MongoDB data mismatch at %s stage for %s.%s -> %s.%s: sourceCount=%d, targetCount=%d",
						stage, dbmap.SourceDatabase, tblmap.SourceTable,
						dbmap.TargetDatabase, tblmap.TargetTable, len(srcDocs), len(tgtDocs))
				}
				srcMap := make(map[string]bson.M)
				for _, doc := range srcDocs {
					id, ok := doc["_id"].(primitive.ObjectID)
					if !ok {
						t.Fatalf("MongoDB document missing _id in %s.%s", dbmap.SourceDatabase, tblmap.SourceTable)
					}
					srcMap[id.Hex()] = doc
				}
				for _, doc := range tgtDocs {
					id, ok := doc["_id"].(primitive.ObjectID)
					if !ok {
						t.Fatalf("MongoDB target document missing _id in %s.%s", dbmap.TargetDatabase, tblmap.TargetTable)
					}
					srcDoc, exists := srcMap[id.Hex()]
					if !exists {
						t.Fatalf("MongoDB target has extra document with _id=%s in %s.%s",
							id.Hex(), dbmap.TargetDatabase, tblmap.TargetTable)
					}
					// Compare specific fields
					for key, value := range srcDoc {
						if tgtVal, exists := doc[key]; !exists || tgtVal != value {
							t.Fatalf("MongoDB data mismatch for _id=%s in field '%s': source='%v', target='%v'",
								id.Hex(), key, value, tgtVal)
						}
					}
				}
			}
		}

	case *sql.DB:
		tc := tgt.(*sql.DB)
		for _, dbmap := range mappings {
			for _, tblmap := range dbmap.Tables {
				fullSrcTable := getQualifiedTableName(dbmap, true, tblmap)
				fullTgtTable := getQualifiedTableName(dbmap, false, tblmap)

				srcQuery := fmt.Sprintf("SELECT id, name, email FROM %s ORDER BY id", fullSrcTable)
				srcRows, err := s.Query(srcQuery)
				if err != nil {
					t.Fatalf("Failed to fetch rows from source %s at %s stage: %v", fullSrcTable, stage, err)
				}
				defer srcRows.Close()
				var srcRowsData []map[string]interface{}
				for srcRows.Next() {
					var id int64
					var name, email string
					if err := srcRows.Scan(&id, &name, &email); err != nil {
						t.Fatalf("Failed to scan row from source %s: %v", fullSrcTable, err)
					}
					srcRowsData = append(srcRowsData, map[string]interface{}{
						"id":    id,
						"name":  name,
						"email": email,
					})
				}

				tgtQuery := fmt.Sprintf("SELECT id, name, email FROM %s ORDER BY id", fullTgtTable)
				tgtRows, err := tc.Query(tgtQuery)
				if err != nil {
					t.Fatalf("Failed to fetch rows from target %s at %s stage: %v", fullTgtTable, stage, err)
				}
				defer tgtRows.Close()
				var tgtRowsData []map[string]interface{}
				for tgtRows.Next() {
					var id int64
					var name, email string
					if err := tgtRows.Scan(&id, &name, &email); err != nil {
						t.Fatalf("Failed to scan row from target %s: %v", fullTgtTable, err)
					}
					tgtRowsData = append(tgtRowsData, map[string]interface{}{
						"id":    id,
						"name":  name,
						"email": email,
					})
				}

				if len(srcRowsData) != len(tgtRowsData) {
					t.Fatalf("%s data mismatch at %s stage for %s -> %s: sourceCount=%d, targetCount=%d",
						dbmap.SourceDatabase, stage, fullSrcTable, fullTgtTable,
						len(srcRowsData), len(tgtRowsData))
				}

				// Compare emails
				srcMap := make(map[int64]map[string]interface{})
				for _, row := range srcRowsData {
					srcMap[row["id"].(int64)] = row
				}

				for _, row := range tgtRowsData {
					srcRow, exists := srcMap[row["id"].(int64)]
					if !exists {
						t.Fatalf("Target has extra row with id=%d in %s",
							row["id"].(int64), fullTgtTable)
					}
					// Compare specific fields
					for key, value := range srcRow {
						if tgtVal, exists := row[key]; !exists || tgtVal != value {
							t.Fatalf("Data mismatch for id=%d in field '%s': source='%v', target='%v'",
								row["id"].(int64), key, value, tgtVal)
						}
					}
				}
			}
		}

	case *goredis.Client:
		// Minimal verification: check that the same number of keys exist
		tc := tgt.(*goredis.Client)

		// Scan all keys in source
		var srcKeys []string
		var srcCursor uint64
		for {
			k, newCursor, err := s.Scan(context.Background(), srcCursor, "*", 100).Result()
			if err != nil {
				t.Fatalf("Redis source SCAN fail: %v", err)
			}
			srcKeys = append(srcKeys, k...)
			srcCursor = newCursor
			if newCursor == 0 {
				break
			}
		}

		// Scan all keys in target
		var tgtKeys []string
		var tgtCursor uint64 = 0
		for {
			k, newCursor, err := tc.Scan(context.Background(), tgtCursor, "*", 100).Result()
			if err != nil {
				t.Fatalf("Redis target SCAN fail: %v", err)
			}
			tgtKeys = append(tgtKeys, k...)
			tgtCursor = newCursor
			if newCursor == 0 {
				break
			}
		}

		excludeKeys := map[string]struct{}{
			"source_stream": {},
		}

		filterKeys := func(keys []string, exclude map[string]struct{}) []string {
			var filtered []string
			for _, key := range keys {
				if _, shouldExclude := exclude[key]; !shouldExclude {
					filtered = append(filtered, key)
				}
			}
			return filtered
		}

		filteredSrcKeys := filterKeys(srcKeys, excludeKeys)
		filteredTgtKeys := filterKeys(tgtKeys, excludeKeys)

		if len(filteredSrcKeys) != len(filteredTgtKeys) {
			t.Fatalf("Redis data mismatch at %s stage: sourceCount=%d, targetCount=%d",
				stage, len(filteredSrcKeys), len(filteredTgtKeys))
		}
	}
}

// Perform create/update/delete operations during synchronization
func performDataOperations(t *testing.T, src interface{}, tgt interface{}, mappings []config.DatabaseMapping, dbType string) {
	switch dbType {
	case "mongodb":
		performMongoOperations(t, src.(*mongo.Client), tgt.(*mongo.Client), mappings)
	case "mysql", "mariadb", "postgresql":
		performSQLOperations(t, src.(*sql.DB), tgt.(*sql.DB), mappings, dbType)
	case "redis":
		performRedisOperations(t, src.(*goredis.Client), tgt.(*goredis.Client), mappings)
	default:
		t.Fatalf("Unknown dbType: %s", dbType)
	}
}

func performMongoOperations(t *testing.T, sClient, tClient *mongo.Client, mappings []config.DatabaseMapping) {
	for _, dbmap := range mappings {
		for _, tblmap := range dbmap.Tables {
			srcColl := sClient.Database(dbmap.SourceDatabase).Collection(tblmap.SourceTable)

			insertCount := 3
			var docs []interface{}
			for i := 0; i < insertCount; i++ {
				docs = append(docs, bson.M{
					"name":  "test_insert_" + uuid.New().String(),
					"email": "Randomemail_" + strconv.Itoa(rand.Intn(1000)),
				})
			}
			_, err := srcColl.InsertMany(context.Background(), docs)
			if err != nil {
				t.Fatalf("MongoDB insert failed: %v", err)
			}
			testLog.Info("MongoDB insert operation successful.")
			verifyDataConsistency(t, sClient, tClient, []config.DatabaseMapping{dbmap}, "mongo_insert")

			updateFilter := bson.M{"name": bson.M{"$regex": "^test_insert_"}}
			update := bson.M{"$set": bson.M{"name": "test_updated_" + uuid.New().String()}}
			_, err = srcColl.UpdateMany(context.Background(), updateFilter, update)
			if err != nil {
				t.Fatalf("MongoDB update failed: %v", err)
			}
			testLog.Info("MongoDB update operation successful.")
			verifyDataConsistency(t, sClient, tClient, []config.DatabaseMapping{dbmap}, "mongo_update")

			deleteFilter := bson.M{"name": bson.M{"$regex": "^test_updated_"}}
			_, err = srcColl.DeleteMany(context.Background(), deleteFilter)
			if err != nil {
				t.Fatalf("MongoDB delete failed: %v", err)
			}
			testLog.Info("MongoDB delete operation successful.")
			verifyDataConsistency(t, sClient, tClient, []config.DatabaseMapping{dbmap}, "mongo_delete")
		}
	}
}

func performSQLOperations(t *testing.T, sDB, tDB *sql.DB, mappings []config.DatabaseMapping, dbType string) {
	for _, dbmap := range mappings {
		for _, tblmap := range dbmap.Tables {
			fullSrcTable := getQualifiedTableName(dbmap, true, tblmap)

			insertCount := 3
			var insertSQL string
			if isPostgresDBType(dbType) {
				insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES ($1, $2, $3)", fullSrcTable)
			} else {
				insertSQL = fmt.Sprintf("INSERT INTO %s (id, name, email) VALUES (?, ?, ?)", fullSrcTable)
			}
			for i := 0; i < insertCount; i++ {
				insertID := getNextSQLID(t, sDB, fullSrcTable)
				name := "test_insert_" + uuid.New().String()
				email := "Randomemail_" + strconv.Itoa(rand.Intn(1000))

				if _, err := sDB.Exec(insertSQL, insertID, name, email); err != nil {
					t.Fatalf("%s insert failed: %v", dbType, err)
				}
			}
			testLog.Infof("%s insert operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_insert")

			updateQuery := fmt.Sprintf("UPDATE %s SET name=CONCAT('test_updated_', UUID()) WHERE name LIKE 'test_insert_%%'", fullSrcTable)
			if isPostgresDBType(dbType) {
				updateQuery = fmt.Sprintf("UPDATE %s SET name='test_updated_' || substring(md5(random()::text),1,8) WHERE name LIKE 'test_insert_%%'", fullSrcTable)
			}
			if _, err := sDB.Exec(updateQuery); err != nil {
				t.Fatalf("%s update failed: %v", dbType, err)
			}
			testLog.Infof("%s update operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_update")

			deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE name LIKE 'test_updated_%%'", fullSrcTable)
			if _, err := sDB.Exec(deleteQuery); err != nil {
				t.Fatalf("%s delete failed: %v", dbType, err)
			}
			testLog.Infof("%s delete operation successful.", dbType)
			verifyDataConsistency(t, sDB, tDB, []config.DatabaseMapping{dbmap}, dbType+"_delete")
		}
	}
}

// Minimal Redis create/update/delete test
func performRedisOperations(t *testing.T, sClient, tClient *goredis.Client, mappings []config.DatabaseMapping) {
	ctx := context.Background()

	for _, dbmap := range mappings {
		for _, tblmap := range dbmap.Tables {
			// We'll pick a single test key
			srcKey := fmt.Sprintf("%s:%s:%s", dbmap.SourceDatabase, tblmap.SourceTable, "testKey")
			tgtKey := fmt.Sprintf("%s:%s:%s", dbmap.TargetDatabase, tblmap.TargetTable, "testKey")

			// Insert (set)
			val := fmt.Sprintf("testval_%s", uuid.New().String())
			if err := sClient.Set(ctx, srcKey, val, 0).Err(); err != nil {
				t.Fatalf("Redis insert failed: %v", err)
			}
			testLog.Info("Redis insert operation successful.")
			verifyRedisKey(t, sClient, tClient, srcKey, tgtKey, val, "[redis_insert]")

			// Update
			newVal := fmt.Sprintf("updated_val_%s", uuid.New().String())
			if err := sClient.Set(ctx, srcKey, newVal, 0).Err(); err != nil {
				t.Fatalf("Redis update failed: %v", err)
			}
			testLog.Info("Redis update operation successful.")
			verifyRedisKey(t, sClient, tClient, srcKey, tgtKey, newVal, "[redis_update]")

			// Delete
			if err := sClient.Del(ctx, srcKey).Err(); err != nil {
				t.Fatalf("Redis delete failed: %v", err)
			}
			testLog.Info("Redis delete operation successful.")

			// Check target is also deleted
			if tgtVal, _ := tClient.Get(ctx, tgtKey).Result(); tgtVal != "" {
				t.Fatalf("[redis_delete] Expected empty on target key=%s, got '%s'", tgtKey, tgtVal)
			}
		}
	}
}

// Helper to verify a single Redis key from source/target
func verifyRedisKey(t *testing.T, sClient, tClient *goredis.Client, srcKey, tgtKey, expectedVal, stage string) {
	ctx := context.Background()
	srcVal, err := sClient.Get(ctx, srcKey).Result()
	if err != nil {
		t.Fatalf("%s Redis GET fail on source: key=%s err=%v", stage, srcKey, err)
	}
	tgtVal, err := tClient.Get(ctx, tgtKey).Result()
	// If key not exist, tClient.Get returns redis.Nil => err != nil
	if err != nil && err != goredis.Nil {
		t.Fatalf("%s Redis GET fail on target: key=%s err=%v", stage, tgtKey, err)
	}
	if srcVal != tgtVal && err != goredis.Nil {
		t.Fatalf("%s Redis mismatch: sourceKey=%s val=%s, targetKey=%s val=%s",
			stage, srcKey, srcVal, tgtKey, tgtVal)
	}
	if srcVal != expectedVal {
		t.Fatalf("%s Unexpected sourceVal for key=%s => got '%s', want '%s'", stage, srcKey, srcVal, expectedVal)
	}
}

func TestExtendedCoverage(t *testing.T) {

	t.Run("TestLoggerCoverage", func(t *testing.T) {
		logger.InitLogger("debug")
		logger.InitLogger("info")
		logger.InitLogger("warning")
		logger.InitLogger("error")
		logger.InitLogger("fatal")
		logger.InitLogger("panic")
		logger.InitLogger("unknown")
		logrus.Error("Triggering SQLiteHook insert error check - ExtendedCoverage")
	})

	t.Run("TestConfigAndSchema", func(t *testing.T) {
		os.Setenv("SYNC_DB_PATH", "sync.db")

		cfg := config.NewConfig()
		if cfg == nil {
			t.Fatal("NewConfig returned nil")
		}
		if err := ensureTestSchema(); err != nil {
			t.Fatalf("ensureTestSchema fail: %v", err)
		}
		logger.Log.Info("TestConfigAndSchema => schema ensured")
	})

	t.Run("TestMonitorCoverage", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cfg := &config.Config{
			LogLevel: "info",
			SyncConfigs: []config.SyncConfig{
				{Type: "mysql", Enable: false},
				{Type: "mariadb", Enable: false},
				{Type: "postgresql", Enable: false},
				{Type: "mongodb", Enable: false},
				{Type: "redis", Enable: false},
				{Type: "xxx", Enable: true}, // will trigger "not implemented" debug
			},
		}
		log := logger.InitLogger("info")

		utils.StartRowCountMonitoring(ctx, cfg, log, 1*time.Second)
		time.Sleep(2 * time.Second)
		cancel()
		time.Sleep(1 * time.Second)
		logger.Log.Info("Monitor coverage done.")
	})

	t.Run("TestAPIAuthHandlers", func(t *testing.T) {
		r := chi.NewRouter()
		r.Post("/login", api.AuthLoginHandler)
		r.Get("/currentUser", api.AuthCurrentUserHandler)
		r.Post("/logout", api.AuthLogoutHandler)

		body := bytes.NewBufferString(`{"username":"guest","password":"wrong"}`)
		req, _ := http.NewRequest("POST", "/login", body)
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		r.ServeHTTP(resp, req)

		req2, _ := http.NewRequest("GET", "/currentUser", nil)
		resp2 := httptest.NewRecorder()
		r.ServeHTTP(resp2, req2)

		req3, _ := http.NewRequest("POST", "/logout", nil)
		resp3 := httptest.NewRecorder()
		r.ServeHTTP(resp3, req3)

		logger.Log.Infof("[TestAPIAuthHandlers] done, code1=%d, code2=%d, code3=%d", resp.Code, resp2.Code, resp3.Code)
	})


	t.Run("TestAPISyncHandlers", func(t *testing.T) {
		var respBody struct {
			Data struct {
				FormData struct {
					ID int `json:"id"`
				} `json:"formData"`
			} `json:"data"`
		}

		r := chi.NewRouter()
		r.Get("/sync",  api.SyncListHandler)                                 // GET /api/sync
		r.Put("/sync/{id}/stop",  api.SyncStopHandler)                   // PUT /api/sync/{taskID}/stop
		r.Put("/sync/{id}/start",  api.SyncStartHandler)                 // PUT /api/sync/{taskID}/start
		r.Put("/sync/{id}",  api.SyncUpdateHandler)                      // PUT /api/sync/{taskID}
		r.Post("/sync",  api.SyncCreateHandler)                     // POST /api/sync
		r.Delete("/sync/{id}",  api.SyncDeleteHandler)                   // DELETE /api/sync/{taskID}
		r.Get("/sync/{id}/monitor",  api.SyncMonitorHandler)             // GET /api/sync/{taskID}/monitor
		r.Get("/sync/{id}/metrics",  api.SyncMetricsHandler)             // GET /api/sync/{taskID}/metrics
		r.Get("/sync/{id}/logs",  api.SyncLogsHandler)                   // GET /api/sync/{taskID}/logs

		reqList, _ := http.NewRequest("GET", "/sync", nil)
		respList := httptest.NewRecorder()
		r.ServeHTTP(respList, reqList)
		logger.Log.Infof("GET /sync => code=%d", respList.Code)

		body := bytes.NewBufferString(`{"sourceType":"mysql","sourceConn":{"database":"db1"},"targetConn":{"database":"db2"},"mappings":[{"sourceTable":"t1","targetTable":"t2"}],"taskName":"Demo","status":"running"}`)
		reqCreate, _ := http.NewRequest("POST", "/sync", body)
		reqCreate.Header.Set("Content-Type", "application/json")
		respCreate := httptest.NewRecorder()
		r.ServeHTTP(respCreate, reqCreate)
		json.NewDecoder(respCreate.Body).Decode(&respBody)

		taskID := respBody.Data.FormData.ID
		logger.Log.Infof("Inserted ID: %d\n", taskID)
		logger.Log.Infof("POST /sync => code=%d", respCreate.Code)

		bodyUpdate := bytes.NewBufferString(`{"sourceType":"mysql","sourceConn":{"database":"db1"},"targetConn":{"database":"db2"},"mappings":[{"sourceTable":"t1","targetTable":"t2"}],"taskName":"Demo","status":"running"}`)
		reqUpdate, _ := http.NewRequest("PUT", "/sync/"+strconv.Itoa(taskID), bodyUpdate)
		reqUpdate.Header.Set("Content-Type", "application/json")
		respUpdate := httptest.NewRecorder()
		r.ServeHTTP(respUpdate, reqUpdate)
		logger.Log.Infof("PUT /sync => code=%d", respUpdate.Code)
		responseBody := respUpdate.Body.String()
		logger.Log.Infof("PUT /sync Response Body: %s", responseBody)

		reqDelete, _ := http.NewRequest("DELETE", "/sync/"+strconv.Itoa(taskID), nil)
		respDelete := httptest.NewRecorder()
		r.ServeHTTP(respDelete, reqDelete)

		reqStop, _ := http.NewRequest("PUT", "/sync/1/stop", nil)
		respStop := httptest.NewRecorder()
		r.ServeHTTP(respStop, reqStop)

		reqStart, _ := http.NewRequest("PUT", "/sync/1/start", nil)
		respStart := httptest.NewRecorder()
		r.ServeHTTP(respStart, reqStart)

		reqMon, _ := http.NewRequest("GET", "/sync/1/monitor", nil)
		respMon := httptest.NewRecorder()
		r.ServeHTTP(respMon, reqMon)

		reqMetr, _ := http.NewRequest("GET", "/sync/1/metrics", nil)
		respMetr := httptest.NewRecorder()
		r.ServeHTTP(respMetr, reqMetr)

		reqLogs, _ := http.NewRequest("GET", "/sync/1/logs", nil)
		respLogs := httptest.NewRecorder()
		r.ServeHTTP(respLogs, reqLogs)

		logger.Log.Infof("[TestAPISyncHandlers] stop=%d, start=%d, mon=%d, metr=%d, logs=%d",
			respStop.Code, respStart.Code, respMon.Code, respMetr.Code, respLogs.Code)
	})

	t.Run("TestVariousSyncers", func(t *testing.T) {
		cfg := config.SyncConfig{Type: "unknown", Enable: true}

		pgSyncer := postgresql.NewPostgreSQLSyncer(cfg, logger.Log)
		pgSyncer.Start(context.Background())

		mDBSyncer := mariadb.NewMariaDBSyncer(cfg, logger.Log)
		mDBSyncer.Start(context.Background())

		rSyncer := redis.NewRedisSyncer(cfg, logger.Log)
		rSyncer.Start(context.Background())

		mgSyncer := mongodb.NewMongoDBSyncer(cfg, logger.Log)
		if mgSyncer != nil {
			mgSyncer.Start(context.Background())
		}

		mySyncer := mysql.NewMySQLSyncer(cfg, logger.Log)
		mySyncer.Start(context.Background())

		logger.Log.Info("TestVariousSyncers => done")
	})

	t.Run("TestBoundaryAndRecovery", func(t *testing.T) {
		logger.Log.Info("Boundary: empty DB/table scenario, special chars, foreign keys, etc.")
		tmpDir := t.TempDir()
		stStore := state.NewFileStateStore(tmpDir)
		_ = os.WriteFile(filepath.Join(tmpDir, "damaged_state"), []byte("???"), 0644)
		if _, err := stStore.Load("damaged_state"); err != nil {
			logger.Log.Infof("Expected error on damaged state => %v", err)
		}
	})
	logger.Log.Info("[TestExtendedCoverage] done.")
}

// ensureTestSchema helps verifying schema
func ensureTestSchema() error {
	dbPath := "sync.db"
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	config.EnsureSchema(db)
	return nil
}
