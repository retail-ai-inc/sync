//go:build integration

package dbinspect

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/retail-ai-inc/sync/test/harness"
)

// probeConnection posts a connection test and returns the recorder and the decoded body.
// A failure is reported as plain text, so the body is only decoded on a 200.
func probeConnection(t *testing.T, body map[string]string) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()

	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/test-connection", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	TestConnectionHandler(rec, req)

	if rec.Code != http.StatusOK {
		return rec, nil
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not JSON: %v (%q)", err, rec.Body.String())
	}
	return rec, resp
}

func tableList(t *testing.T, resp map[string]interface{}) []string {
	t.Helper()

	data, ok := resp["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("data is missing: %#v", resp)
	}
	raw, ok := data["tables"].([]interface{})
	if !ok {
		// A server with no tables answers null rather than an empty list.
		return nil
	}
	names := make([]string, 0, len(raw))
	for _, n := range raw {
		names = append(names, n.(string))
	}
	return names
}

// TestTheProbeListsMySQLTables covers what an operator sees when they press
// "test connection" while entering a task: reaching the server is only half of
// it, since a name they then have to pick a table from comes back with it.
func TestTheProbeListsMySQLTables(t *testing.T) {
	host, port := harness.SplitHostPort(t, harness.MySQLSource)
	db := openSchemaMySQL(t, harness.MySQLSource, schemaSourceDB)

	table := harness.UniqueName("probe")
	if _, err := db.Exec("CREATE TABLE " + table + " (id INT PRIMARY KEY)"); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })

	rec, resp := probeConnection(t, map[string]string{
		"dbType": "mysql", "host": host, "port": port,
		"user": "root", "password": "root", "database": schemaSourceDB,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%q)", rec.Code, rec.Body.String())
	}

	var found bool
	for _, name := range tableList(t, resp) {
		if name == table {
			found = true
		}
	}
	if !found {
		t.Errorf("the table just created is not in %v", tableList(t, resp))
	}
}

// TestTheProbeReportsBadMySQLCredentials records that a password that does not
// work is a failure rather than an empty table list, which would read as "the
// database is there and has nothing in it".
func TestTheProbeReportsBadMySQLCredentials(t *testing.T) {
	host, port := harness.SplitHostPort(t, harness.MySQLSource)

	rec, _ := probeConnection(t, map[string]string{
		"dbType": "mysql", "host": host, "port": port,
		"user": "root", "password": "not-the-password", "database": schemaSourceDB,
	})
	if rec.Code == http.StatusOK {
		t.Errorf("status = 200 with the wrong password: %q", rec.Body.String())
	}
}

// TestTheProbeListsPostgreSQLTables covers the PostgreSQL branch, which lists
// the public schema.
func TestTheProbeListsPostgreSQLTables(t *testing.T) {
	host, port := harness.SplitHostPort(t, harness.PostgresSource)

	rec, resp := probeConnection(t, map[string]string{
		"dbType": "postgresql", "host": host, "port": port,
		"user": "root", "password": "root", "database": "source_db",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%q)", rec.Code, rec.Body.String())
	}
	if resp["success"] != true {
		t.Errorf("success = %v, want true", resp["success"])
	}
}

// TestTheProbeListsMongoCollections covers the MongoDB branch. It builds its URI
// through the shared builder, which is what stops the probe from reporting a
// connection the task will not be able to make — and which is why it discovers
// the replica set rather than pinning one node, so this addresses the set whose
// member advertises an address the host can reach.
func TestTheProbeListsMongoCollections(t *testing.T) {
	host, port := harness.SplitHostPort(t, harness.MongoDiscoverable)

	rec, resp := probeConnection(t, map[string]string{
		"dbType": "mongodb", "host": host, "port": port, "database": schemaSourceDB,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%q)", rec.Code, rec.Body.String())
	}
	if resp["success"] != true {
		t.Errorf("success = %v, want true", resp["success"])
	}
}

// TestTheProbeReportsAnUnreachableMongo records that a host nobody is listening
// on fails rather than reporting a working connection. mongo.Connect does not
// dial, so without the ping this branch answered 200 for any address at all.
func TestTheProbeReportsAnUnreachableMongo(t *testing.T) {
	rec, _ := probeConnection(t, map[string]string{
		"dbType": "mongodb", "host": "127.0.0.1", "port": "1", "database": "shop",
	})
	if rec.Code == http.StatusOK {
		t.Errorf("status = 200 against a port nobody is listening on: %q", rec.Body.String())
	}
}

// TestTheProbeReachesRedis covers the Redis branch, which reports no tables —
// Redis has none — but does have to reach the server to say so.
func TestTheProbeReachesRedis(t *testing.T) {
	host, port := harness.SplitHostPort(t, harness.RedisSource)

	rec, resp := probeConnection(t, map[string]string{
		"dbType": "redis", "host": host, "port": port, "database": "0",
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d (%q)", rec.Code, rec.Body.String())
	}
	if resp["success"] != true {
		t.Errorf("success = %v, want true", resp["success"])
	}
}

// TestTheProbeReportsAnUnreachableRedis is the other half: an address nobody
// answers on must not come back as a working connection.
func TestTheProbeReportsAnUnreachableRedis(t *testing.T) {
	rec, _ := probeConnection(t, map[string]string{
		"dbType": "redis", "host": "127.0.0.1", "port": "1", "database": "0",
	})
	if rec.Code == http.StatusOK {
		t.Errorf("status = 200 against a port nobody is listening on: %q", rec.Body.String())
	}
}
