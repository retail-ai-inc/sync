package test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongoopt "go.mongodb.org/mongo-driver/mongo/options"
	"math/rand"
	"testing"
	"time"
	// "github.com/retail-ai-inc/sync/pkg/config"
	// "github.com/sirupsen/logrus"
	// "github.com/retail-ai-inc/sync/pkg/syncer/common"
	goredis "github.com/redis/go-redis/v9"
)

// Helper function to execute queries and handle errors
func mustExec(t *testing.T, db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("[mustExec] query=%s => %v", query, err)
	}
}

// Query row count from database
func queryCount(t *testing.T, db *sql.DB, query string) int {
	var cnt int
	t.Logf("executing count query: %s", query)
	if err := db.QueryRow(query).Scan(&cnt); err != nil {
		t.Fatalf("scan count => %v", err)
	}
	return cnt
}

// Compare the data consistency between source and target databases
func compareDataConsistency(t *testing.T, srcDB, tgtDB *sql.DB, sourceTable, targetTable string) {
	srcRows := queryRows(t, srcDB, fmt.Sprintf("SELECT * FROM %s", sourceTable))
	tgtRows := queryRows(t, tgtDB, fmt.Sprintf("SELECT * FROM %s", targetTable))

	if len(srcRows) != len(tgtRows) {
		t.Errorf("rows length mismatch. Source: %d, Target: %d", len(srcRows), len(tgtRows))
	}

	for i, srcRow := range srcRows {
		tgtRow := tgtRows[i]
		for key, srcValue := range srcRow {
			tgtValue, ok := tgtRow[key]
			if !ok || !compareValues(srcValue, tgtValue) {
				t.Errorf("data mismatch at row %d, column '%s'. Source: %v, Target: %v", i+1, key, srcValue, tgtValue)
			}
		}
	}
}

// compareValues compares two values, handling []uint8 (byte slices) separately
func compareValues(srcValue, tgtValue interface{}) bool {
	switch src := srcValue.(type) {
	case []uint8:
		if tgt, ok := tgtValue.([]uint8); ok {
			return bytes.Equal(src, tgt)
		}
	default:
		return srcValue == tgtValue
	}
	return false
}

// queryRows is a helper function to query rows from a MySQL or PostgreSQL database and return them as a slice of maps.
func queryRows(t *testing.T, db *sql.DB, query string) []map[string]interface{} {
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query: %s failed: %v", query, err)
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	result := make([]map[string]interface{}, 0)

	for rows.Next() {
		row := make(map[string]interface{})
		cols := make([]interface{}, len(columns))
		for i := range cols {
			cols[i] = new(interface{})
		}

		err := rows.Scan(cols...)
		if err != nil {
			t.Fatalf("scan row failed: %v", err)
		}

		for i, col := range columns {
			row[col] = *(cols[i].(*interface{}))
		}

		result = append(result, row)
	}

	return result
}

func getNextID(db *sql.DB, table string) int {
	var nextID int
	query := fmt.Sprintf("SELECT MAX(id) FROM %s", table)
	err := db.QueryRow(query).Scan(&nextID)
	if err != nil {
		return 0
	}
	return nextID + 1
}
func compareMongoDataConsistency(t *testing.T, srcClient, tgtClient *mongo.Client, srcDB, tgtDB, srcCollection, tgtCollection string) {
	srcDocs, err := queryMongoRows(t, srcClient, srcDB, srcCollection)
	if err != nil {
		t.Fatalf("[TC03] failed to query source collection: %v", err)
	}

	tgtDocs, err := queryMongoRows(t, tgtClient, tgtDB, tgtCollection)
	if err != nil {
		t.Fatalf("[TC03] failed to query target collection: %v", err)
	}

	if len(srcDocs) != len(tgtDocs) {
		t.Errorf("Document count mismatch. Source: %d, Target: %d", len(srcDocs), len(tgtDocs))
	}

	for i, srcDoc := range srcDocs {
		tgtDoc := tgtDocs[i]
		if !compareMongoDoc(srcDoc, tgtDoc) {
			t.Errorf("Document mismatch at index %d. Source: %v, Target: %v", i, srcDoc, tgtDoc)
		}
	}
}
func doRandomMySQLInserts(t *testing.T, dsn string, task SyncTaskRow) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Logf("[doRandomMySQLInserts] open fail => %v", err)
		return
	}
	defer db.Close()

	if len(task.Parsed.Mappings) == 0 || len(task.Parsed.Mappings[0].Tables) == 0 {
		t.Logf("[doRandomMySQLInserts] no table mapping => task=%s", task.Parsed.TaskName)
		return
	}
	sourceTable := task.Parsed.Mappings[0].Tables[0].SourceTable

	loopCount := 5
	for i := 0; i < loopCount; i++ {
		id := rand.Intn(99999)
		val := fmt.Sprintf("val_%d", id)
		ins := fmt.Sprintf("INSERT IGNORE INTO %s VALUES(%d, '%s')", sourceTable, id, val)
		_, e2 := db.Exec(ins)
		if e2 != nil {
			t.Logf("[doRandomMySQLInserts] insert fail => %v", e2)
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Logf("[doRandomMySQLInserts] inserted %d random rows => task=%s", loopCount, task.Parsed.TaskName)
}

func doRandomMongoInserts(t *testing.T, dsn string, task SyncTaskRow) {
	client, err := connectMongo(dsn)
	if err != nil {
		t.Logf("[doRandomMongoInserts] connect fail => %v", err)
		return
	}
	defer client.Disconnect(ctx)

	if len(task.Parsed.Mappings) == 0 || len(task.Parsed.Mappings[0].Tables) == 0 {
		t.Logf("[doRandomMongoInserts] no table mapping => task=%s", task.Parsed.TaskName)
		return
	}
	sourceTable := task.Parsed.Mappings[0].Tables[0].SourceTable
	dbName := task.Parsed.SourceConn.Database
	coll := client.Database(dbName).Collection(sourceTable)

	loopCount := 5
	for i := 0; i < loopCount; i++ {
		doc := bson.M{"val": fmt.Sprintf("val_%d", i)}
		_, e2 := coll.InsertOne(ctx, doc)
		if e2 != nil {
			t.Logf("[doRandomMongoInserts] insert fail => %v", e2)
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Logf("[doRandomMongoInserts] inserted %d docs => task=%s", loopCount, task.Parsed.TaskName)
}
func compareMongoDoc(srcDoc, tgtDoc bson.M) bool {
	for key, srcValue := range srcDoc {
		tgtValue, ok := tgtDoc[key]
		if !ok || !compareValues(srcValue, tgtValue) {
			return false
		}
	}
	return true
}

func buildMongoDSN(conn ConnDetail) string {
	return fmt.Sprintf("mongodb://%s:%s/%s", conn.Host, conn.Port, conn.Database)
}
func buildMySQLDSN(conn ConnDetail) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4",
		conn.User, conn.Password, conn.Host, conn.Port, conn.Database)
}

func compareRedisDataConsistency(t *testing.T, srcClient, tgtClient *goredis.Client, sourceTable, targetTable string) {
	cursor := uint64(0)
	for {
		keys, newCursor, err := srcClient.Scan(ctx, cursor, "*", 0).Result()
		if err != nil {
			t.Fatalf("[TC07] get keys from source Redis fail: %v", err)
		}

		for _, key := range keys {
			// Check the type of the source key
			srcType, err := srcClient.Type(ctx, key).Result()
			if err != nil {
				t.Fatalf("[TC07] get type of key=%s from source Redis fail: %v", key, err)
			}

			// Get the value of the key based on its type
			var srcValue, tgtValue string
			switch srcType {
			case "string":
				srcValue, err = srcClient.Get(ctx, key).Result()
				if err != nil && err != goredis.Nil {
					t.Fatalf("[TC07] get key=%s from source Redis fail: %v", key, err)
				}
				if err == goredis.Nil {
					srcValue = "" // Key doesn't exist, set to empty string
				}

				tgtValue, err = tgtClient.Get(ctx, key).Result()
				if err != nil && err != goredis.Nil {
					t.Fatalf("[TC07] get key=%s from target Redis fail: %v", key, err)
				}
				if err == goredis.Nil {
					tgtValue = "" // Key doesn't exist, set to empty string
				}

			case "hash":
				// For hash type, use HGETALL to fetch all fields and values
				srcFields, err := srcClient.HGetAll(ctx, key).Result()
				if err != nil {
					t.Fatalf("[TC07] get hash key=%s from source Redis fail: %v", key, err)
				}
				srcValue = fmt.Sprintf("%v", srcFields)

				tgtFields, err := tgtClient.HGetAll(ctx, key).Result()
				if err != nil {
					t.Fatalf("[TC07] get hash key=%s from target Redis fail: %v", key, err)
				}
				tgtValue = fmt.Sprintf("%v", tgtFields)

			case "list":
				// For list type, use LRANGE to fetch all list items
				srcList, err := srcClient.LRange(ctx, key, 0, -1).Result()
				if err != nil {
					t.Fatalf("[TC07] get list key=%s from source Redis fail: %v", key, err)
				}
				srcValue = fmt.Sprintf("%v", srcList)

				tgtList, err := tgtClient.LRange(ctx, key, 0, -1).Result()
				if err != nil {
					t.Fatalf("[TC07] get list key=%s from target Redis fail: %v", key, err)
				}
				tgtValue = fmt.Sprintf("%v", tgtList)

			case "set":
				// For set type, use SMEMBERS to fetch all members
				srcSet, err := srcClient.SMembers(ctx, key).Result()
				if err != nil {
					t.Fatalf("[TC07] get set key=%s from source Redis fail: %v", key, err)
				}
				srcValue = fmt.Sprintf("%v", srcSet)

				tgtSet, err := tgtClient.SMembers(ctx, key).Result()
				if err != nil {
					t.Fatalf("[TC07] get set key=%s from target Redis fail: %v", key, err)
				}
				tgtValue = fmt.Sprintf("%v", tgtSet)

			case "zset":
				// For sorted set type, use ZRANGE to fetch all members with scores
				srcZSet, err := srcClient.ZRangeWithScores(ctx, key, 0, -1).Result()
				if err != nil {
					t.Fatalf("[TC07] get zset key=%s from source Redis fail: %v", key, err)
				}
				srcValue = fmt.Sprintf("%v", srcZSet)

				tgtZSet, err := tgtClient.ZRangeWithScores(ctx, key, 0, -1).Result()
				if err != nil {
					t.Fatalf("[TC07] get zset key=%s from target Redis fail: %v", key, err)
				}
				tgtValue = fmt.Sprintf("%v", tgtZSet)

			default:
				t.Logf("[TC07] Skipping key=%s due to unsupported type %s", key, srcType)
				continue
			}

			// Compare the source and target values
			if srcValue != tgtValue {
				t.Errorf("[TC07] data mismatch for key=%s. Source: %s, Target: %s", key, srcValue, tgtValue)
			}
		}

		// If we have scanned all keys, exit the loop
		if newCursor == 0 {
			break
		}
		cursor = newCursor
	}

	t.Log("[TC07] Redis data consistency check passed.")
}

func queryMongoRows(t *testing.T, client *mongo.Client, dbName, collectionName string) ([]bson.M, error) {
	collection := client.Database(dbName).Collection(collectionName)
	cursor, err := collection.Find(ctx, bson.M{})
	if err != nil {
		t.Fatalf("find failed: %v", err)
		return nil, err
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		t.Fatalf("cursor to slice failed: %v", err)
		return nil, err
	}

	return docs, nil
}

func connectMongo(uri string) (*mongo.Client, error) {
	cl, err := mongo.NewClient(mongoopt.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	ctx2, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := cl.Connect(ctx2); err != nil {
		return nil, err
	}
	if err := cl.Ping(ctx2, nil); err != nil {
		return nil, err
	}
	return cl, nil
}
