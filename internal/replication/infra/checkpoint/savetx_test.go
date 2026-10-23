package checkpoint

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
)

// SaveTx is how a MySQL task keeps its position honest: the row is written
// through the caller's transaction, so it commits with the data it describes. A
// separate connection lets a crash land between the two, and the position is
// then ahead of the rows -- a restart resumes past changes the target never
// received.

// recordingExecer stands in for a *sql.Tx.
type recordingExecer struct {
	queries []string
	args    [][]interface{}
	err     error
}

func (r *recordingExecer) ExecContext(_ context.Context, query string,
	args ...interface{}) (sql.Result, error) {
	r.queries = append(r.queries, query)
	r.args = append(r.args, args)
	return nil, r.err
}

func TestSaveTxWritesThroughTheCallersTransaction(t *testing.T) {
	store := &SQLStore{TaskID: 41}
	tx := &recordingExecer{}

	if err := store.SaveTx(context.Background(), tx, "", "the payload"); err != nil {
		t.Fatalf("SaveTx: %v", err)
	}

	if len(tx.queries) != 1 {
		t.Fatalf("SaveTx ran %d statements, want 1", len(tx.queries))
	}
	if !strings.Contains(tx.queries[0], tableName) {
		t.Errorf("the statement does not name the table: %q", tx.queries[0])
	}
	if len(tx.args[0]) != 3 || tx.args[0][0] != 41 || tx.args[0][2] != "the payload" {
		t.Errorf("the statement was given %v", tx.args[0])
	}
}

// TestSaveTxRunsNoDDL is the rule the doc comment states: creating the table
// here would be DDL inside the caller's transaction, which MySQL commits
// implicitly -- so the data would be committed early, before the position was
// written, which is the very thing SaveTx exists to prevent.
func TestSaveTxRunsNoDDL(t *testing.T) {
	store := &SQLStore{TaskID: 41}
	tx := &recordingExecer{}

	if err := store.SaveTx(context.Background(), tx, "", "payload"); err != nil {
		t.Fatalf("SaveTx: %v", err)
	}

	for _, query := range tx.queries {
		upper := strings.ToUpper(query)
		for _, ddl := range []string{"CREATE ", "ALTER ", "DROP ", "TRUNCATE "} {
			if strings.Contains(upper, ddl) {
				t.Errorf("SaveTx ran DDL inside the caller's transaction: %q", query)
			}
		}
	}
}

func TestSaveTxReportsAFailure(t *testing.T) {
	store := &SQLStore{TaskID: 41, Schema: "tenant_trial_naviee_bk"}
	tx := &recordingExecer{err: errors.New("the transaction is aborted")}

	err := store.SaveTx(context.Background(), tx, "", "payload")
	if err == nil {
		t.Fatal("a failed write was reported as a success, so the task would go on " +
			"believing its position was stored")
	}
	if !strings.Contains(err.Error(), "tenant_trial_naviee_bk") {
		t.Errorf("the error does not say which table: %v", err)
	}
}

// TestSaveTxSpellsPlaceholdersForTheEngine: PostgreSQL takes $1, and MySQL and
// SQLite take question marks. The wrong spelling is a syntax error on every
// save, which the task reports as a lost position.
func TestSaveTxSpellsPlaceholdersForTheEngine(t *testing.T) {
	for name, numbered := range map[string]bool{"question marks": false, "numbered": true} {
		t.Run(name, func(t *testing.T) {
			store := &SQLStore{TaskID: 1, NumberedPlaceholders: numbered}
			tx := &recordingExecer{}
			if err := store.SaveTx(context.Background(), tx, "", "payload"); err != nil {
				t.Fatalf("SaveTx: %v", err)
			}
			query := tx.queries[0]
			if numbered && !strings.Contains(query, "$1") {
				t.Errorf("numbered placeholders were not used: %q", query)
			}
			if !numbered && !strings.Contains(query, "?") {
				t.Errorf("question marks were not used: %q", query)
			}
		})
	}
}

func TestEnsureCreatesTheTable(t *testing.T) {
	store := sqlStore(t, 7)

	if err := store.Ensure(context.Background()); err != nil {
		t.Fatalf("Ensure: %v", err)
	}

	// The table is there: a save through a transaction now works, which it
	// could not before, since SaveTx runs no DDL of its own.
	tx, err := store.DB.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := store.SaveTx(context.Background(), tx, "", "payload"); err != nil {
		t.Fatalf("SaveTx after Ensure: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	payload, err := store.Load(context.Background(), "")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if payload != "payload" {
		t.Errorf("Load returned %q, want the payload written in the transaction", payload)
	}
}

// TestSaveTxWithoutEnsureFails pins why Ensure exists as a separate call.
func TestSaveTxWithoutEnsureFails(t *testing.T) {
	store := sqlStore(t, 7)

	tx, err := store.DB.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := store.SaveTx(context.Background(), tx, "", "payload"); err == nil {
		t.Error("SaveTx succeeded against a table that does not exist")
	}
}

// TestTheKeyNamingKeepsTasksApart covers the Mongo and Redis stores' key
// naming, which is the whole of what keeps two tasks sharing one target from
// overwriting each other's position. Their reads and writes need a server; the
// naming does not, and the naming is where a collision would come from.
func TestTheKeyNamingKeepsTasksApart(t *testing.T) {
	mongoOne := (&MongoStore{TaskID: 39}).id("")
	mongoTwo := (&MongoStore{TaskID: 41}).id("")
	if mongoOne == mongoTwo {
		t.Errorf("two tasks share the document id %q", mongoOne)
	}

	redisOne := (&RedisStore{TaskID: 39}).field("0-5460")
	redisTwo := (&RedisStore{TaskID: 41}).field("0-5460")
	if redisOne == redisTwo {
		t.Errorf("two tasks share the hash field %q", redisOne)
	}

	// And one task's shards stay apart from each other.
	first := (&RedisStore{TaskID: 44}).field("0-5460")
	second := (&RedisStore{TaskID: 44}).field("5461-10922")
	if first == second {
		t.Errorf("two shards of one task share the hash field %q", first)
	}
}

// TestTheRedisCheckpointKeyIsNotReplicated: the hash lives in the target's own
// keyspace, so a task replicating a whole Redis instance would copy it across
// and overwrite the other side's position. The name is what stops that, and
// IsOffsetKey-style filtering keys off it.
func TestTheRedisCheckpointKeyIsNotReplicated(t *testing.T) {
	if !strings.HasPrefix(redisKey, "_sync") {
		t.Errorf("the checkpoint key %q is not marked as internal, so a whole-instance "+
			"replication task would carry it to the target", redisKey)
	}
}
