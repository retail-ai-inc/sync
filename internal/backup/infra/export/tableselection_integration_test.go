//go:build integration

package export

import (
	"context"
	"database/sql"
	"reflect"
	"slices"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/test/harness"
)

func createScratchMySQLSchema(t *testing.T, admin *sql.DB, statements ...string) string {
	t.Helper()

	schema := harness.UniqueName("backup_regex")
	if _, err := admin.Exec("CREATE DATABASE `" + schema + "`"); err != nil {
		t.Fatalf("create database %s: %v", schema, err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE IF EXISTS `" + schema + "`") })

	db, err := sql.Open("mysql", "root:root@tcp("+harness.MySQLSource+")/"+schema)
	if err != nil {
		t.Fatalf("open %s: %v", schema, err)
	}
	defer db.Close()
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("%s in %s: %v", stmt, schema, err)
		}
	}
	return schema
}

func TestARegexJobBacksUpOnlyTheMatchingMySQLBaseTablesOfItsOwnSchema(t *testing.T) {
	admin, err := sql.Open("mysql", "root:root@tcp("+harness.MySQLSource+")/")
	if err != nil {
		t.Fatalf("open %s: %v", harness.MySQLSource, err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	if err := admin.Ping(); err != nil {
		t.Fatalf("ping %s: %v", harness.MySQLSource, err)
	}

	schema := createScratchMySQLSchema(t, admin,
		"CREATE TABLE orders_202607 (id INT PRIMARY KEY)",
		"CREATE TABLE orders_202608 (id INT PRIMARY KEY)",
		"CREATE TABLE old_orders_202607 (id INT PRIMARY KEY)",
		"CREATE TABLE customers (id INT PRIMARY KEY)",
		"CREATE VIEW orders_v AS SELECT id FROM orders_202607",
	)
	createScratchMySQLSchema(t, admin, "CREATE TABLE orders_202609 (id INT PRIMARY KEY)")

	cfg := ExecutorBackupConfig{SourceType: "mysql", TableSelectionMode: "regex", RegexPattern: "^orders_"}
	cfg.Database.URL = harness.MySQLSource
	cfg.Database.Username = "root"
	cfg.Database.Password = "root"
	cfg.Database.Database = schema

	groups, err := newExecutor().ExpandAndGroupTables(context.Background(), &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	// Anything else here is a view, an unanchored match or another schema's table being backed up.
	want := map[string][]string{"orders": {"orders_202607", "orders_202608"}}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("groups = %v, want %v", groups, want)
	}
}

func TestARegexJobBacksUpOnlyTheMatchingMongoCollectionsOfItsOwnDatabase(t *testing.T) {
	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI("mongodb://" + harness.MongoDiscoverable + "/"))
	if err != nil {
		t.Fatalf("connect %s: %v", harness.MongoDiscoverable, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("ping %s: %v", harness.MongoDiscoverable, err)
	}

	scratch := func(collections ...string) string {
		name := harness.UniqueName("backup_regex")
		db := client.Database(name)
		t.Cleanup(func() { _ = db.Drop(context.Background()) })
		for _, c := range collections {
			if err := db.CreateCollection(ctx, c); err != nil {
				t.Fatalf("create %s.%s: %v", name, c, err)
			}
		}
		return name
	}
	database := scratch("orders_202607", "orders_202608", "old_orders_202607", "customers")
	scratch("orders_202609")

	cfg := ExecutorBackupConfig{SourceType: "mongodb", TableSelectionMode: "regex", RegexPattern: "^orders_"}
	cfg.Database.URL = harness.MongoDiscoverable
	cfg.Database.Database = database

	groups, err := newExecutor().ExpandAndGroupTables(ctx, &cfg)
	if err != nil {
		t.Fatalf("ExpandAndGroupTables: %v", err)
	}
	for _, tables := range groups {
		slices.Sort(tables)
	}
	// Anything else here is an unanchored match or another database's collection being backed up.
	want := map[string][]string{"orders": {"orders_202607", "orders_202608"}}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("groups = %v, want %v", groups, want)
	}
}
