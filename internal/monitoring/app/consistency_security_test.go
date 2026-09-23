package app

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
	"github.com/retail-ai-inc/sync/internal/replication/infra/verify"
)

// securedTask replicates users to users_dr under field, securityType pairs.
func securedTask(rules ...string) config.SyncConfig {
	table := config.TableMapping{SourceTable: "users", TargetTable: "users_dr", SecurityEnabled: len(rules) > 0}
	for i := 0; i+1 < len(rules); i += 2 {
		table.FieldSecurity = append(table.FieldSecurity,
			map[string]interface{}{"field": rules[i], "securityType": rules[i+1]})
	}
	return config.SyncConfig{ID: 7, Type: "mysql",
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{table}}}}
}

// usersTable creates a SQLite table named table; each row is id, email, card, name.
func usersTable(t *testing.T, table string, rows ...[4]interface{}) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), table+".db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(fmt.Sprintf(
		`CREATE TABLE %s (id INTEGER PRIMARY KEY, email TEXT, card TEXT, name TEXT)`, table)); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (?, ?, ?, ?)`, table), r[:]...); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return db
}

func sqliteUpsert(schema, table string, columns []string) string {
	return fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)", table,
		strings.Join(columns, ", "), strings.TrimSuffix(strings.Repeat("?, ", len(columns)), ", "))
}

var usersPair = discovery.Pair{Source: "users", Target: "users_dr"}

func compareUsers(t *testing.T, task config.SyncConfig, source, target *sql.DB, repair bool) verify.Result {
	t.Helper()

	sourceSide, targetSide, repairer, err := sqlComparison(task, usersPair, source, target, "", "",
		[]string{"id"}, []string{"id", "email", "card", "name"})
	if err != nil {
		t.Fatalf("sqlComparison: %v", err)
	}
	repairer.Upsert = sqliteUpsert

	var fix func(verify.Difference) error
	if repair {
		fix = func(d verify.Difference) error {
			_, err := repairer.Repair(context.Background(), []verify.Difference{d})
			return err
		}
	}
	result, err := verify.CompareAndRepair(context.Background(), sourceSide, targetSide, 0, fix)
	if err != nil {
		t.Fatalf("CompareAndRepair: %v", err)
	}
	return result
}

func TestAProtectedTableIsComparedWithWhatReplicationWrites(t *testing.T) {
	task := securedTask("email", "masked", "card", "encrypted")
	source := usersTable(t, "users", [4]interface{}{1, "ann@example.com", "4111", "Ann"})
	target := usersTable(t, "users_dr", [4]interface{}{1, strings.Repeat("*", 15), "c2VhbGVk", "Ann"})

	if got := compareUsers(t, task, source, target, false); !got.Identical() {
		t.Errorf("a target holding what replication writes was reported: %s", got.Summary())
	}
}

func TestAProtectedTableIsRepairedWithWhatReplicationWrites(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", "abcdefghijklmnopqrstuvwxyz012345")
	task := securedTask("email", "masked", "card", "encrypted")
	source := usersTable(t, "users", [4]interface{}{1, "ann@example.com", "4111", "Ann"})
	target := usersTable(t, "users_dr")

	if got := compareUsers(t, task, source, target, true); got.Repaired != 1 {
		t.Fatalf("repaired %d, failed %d", got.Repaired, got.RepairFailed)
	}

	var email, card string
	if err := target.QueryRow(`SELECT email, card FROM users_dr WHERE id = 1`).Scan(&email, &card); err != nil {
		t.Fatalf("read: %v", err)
	}
	if email != strings.Repeat("*", 15) {
		t.Errorf("email = %q, want the mask", email)
	}
	if card == "" || card == "4111" {
		t.Errorf("card = %q, want ciphertext", card)
	}
	if got := compareUsers(t, task, source, target, false); !got.Identical() {
		t.Errorf("the repaired target does not match: %s", got.Summary())
	}
}

func TestATableWithoutFieldSecurityIsComparedAsBefore(t *testing.T) {
	task := securedTask()
	source := usersTable(t, "users", [4]interface{}{1, "ann@example.com", "4111", "Ann"})
	target := usersTable(t, "users_dr", [4]interface{}{1, strings.Repeat("*", 15), "4111", "Ann"})

	if got := compareUsers(t, task, source, target, true); got.Differing != 1 || got.Repaired != 1 {
		t.Fatalf("differing %d, repaired %d", got.Differing, got.Repaired)
	}
	var email string
	if err := target.QueryRow(`SELECT email FROM users_dr WHERE id = 1`).Scan(&email); err != nil {
		t.Fatalf("read: %v", err)
	}
	if email != "ann@example.com" {
		t.Errorf("email = %q, want the source's value", email)
	}
}

func TestATableWhoseKeyIsProtectedIsNotCompared(t *testing.T) {
	_, _, _, err := sqlComparison(securedTask("ID", "masked"), usersPair, nil, nil, "", "",
		[]string{"id"}, []string{"id", "email"})
	if err == nil {
		t.Error("a table whose key replication masks was compared")
	}
}

func TestThePolicyIsFoundByTheNameReplicationFindsItBy(t *testing.T) {
	task := securedTask("email", "masked")
	task.Mappings[0].Tables = append([]config.TableMapping{{SourceTable: "accounts", TargetTable: "users"}},
		task.Mappings[0].Tables...)

	table, _, _, err := sqlComparison(task, usersPair, nil, nil, "", "", []string{"id"}, []string{"id", "email"})
	if err != nil {
		t.Fatalf("sqlComparison: %v", err)
	}
	if table.Protect == nil {
		t.Error("a table's policy was not found by its target name, as the stream finds it")
	}

	collection, _, _, err := mongoComparison(task, usersPair, nil, nil)
	if err != nil {
		t.Fatalf("mongoComparison: %v", err)
	}
	if collection.Protect != nil {
		t.Error("a collection's policy was not found by its source name, as the MongoDB syncer finds it")
	}
}

func TestACollectionPolicyIsWiredToTheSourceAndTheRepair(t *testing.T) {
	task := securedTask("contact.email", "masked", "card", "encrypted")

	source, target, repairer, err := mongoComparison(task, usersPair, nil, nil)
	if err != nil {
		t.Fatalf("mongoComparison: %v", err)
	}
	if source.Protect == nil || repairer.Protect == nil || target.Protect != nil {
		t.Error("the policy is not applied to the source and the repair alone")
	}
	if strings.Join(source.Skip, ",") != "card" || strings.Join(target.Skip, ",") != "card" {
		t.Errorf("skipped %v and %v, want the encrypted field on both sides", source.Skip, target.Skip)
	}

	if _, _, _, err := mongoComparison(securedTask("_id", "masked"), usersPair, nil, nil); err == nil {
		t.Error("a collection whose _id replication masks was compared")
	}
}
