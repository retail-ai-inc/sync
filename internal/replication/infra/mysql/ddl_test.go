package mysql

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// plan runs one statement through the planner and returns the single decision
// it produced.
func plan(t *testing.T, h *MyEventHandler, query string) ddlDecision {
	t.Helper()

	decisions, err := h.planDDL(sourceSchema, query)
	if err != nil {
		t.Fatalf("planDDL(%q): %v", query, err)
	}
	if len(decisions) != 1 {
		t.Fatalf("planDDL(%q) produced %d decisions, want 1", query, len(decisions))
	}
	return decisions[0]
}

func query(sql string) *replication.QueryEvent {
	return &replication.QueryEvent{Schema: []byte("shop"), Query: []byte(sql)}
}

func TestAnAddedColumnIsRewrittenForTheTarget(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := plan(t, h, "ALTER TABLE orders ADD COLUMN email VARCHAR(100)")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply (%s)", got.action, got.reason)
	}
	if !strings.Contains(got.query, "`main`.`orders`") {
		t.Errorf("statement = %q, want the target database and table named", got.query)
	}
	if !strings.Contains(strings.ToUpper(got.query), "ADD COLUMN") {
		t.Errorf("statement = %q, want the column addition preserved", got.query)
	}
}

// TestTheTargetNameFromTheMappingIsUsed covers a task that replicates a table
// under a different name: the DDL has to follow the same mapping the rows do.
func TestTheTargetNameFromTheMappingIsUsed(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders_archive"))

	got := plan(t, h, "ALTER TABLE orders ADD COLUMN email VARCHAR(100)")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply", got.action)
	}
	if !strings.Contains(got.query, "`orders_archive`") {
		t.Errorf("statement = %q, want the mapped target name", got.query)
	}
	if strings.Contains(got.query, "`orders`") {
		t.Errorf("statement = %q, still names the source table", got.query)
	}
}

// The parser needs a driver registered before it can build a literal value,
// and without one every literal restored as nothing at all: a column declared
// DEFAULT 'new' was rewritten as "DEFAULT" with no value.
func TestALiteralDefaultSurvivesTheRewrite(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := plan(t, h, "ALTER TABLE orders ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'new'")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply (%s)", got.action, got.reason)
	}
	if !strings.Contains(got.query, "DEFAULT 'new'") {
		t.Errorf("statement = %q, want the default value kept", got.query)
	}
}

// The driver renders a string as _UTF8MB4'new'.
func TestALiteralDefaultKeepsNoCharsetIntroducer(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := plan(t, h, "ALTER TABLE orders ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'new'")

	if strings.Contains(strings.ToUpper(got.query), "_UTF8MB4") {
		t.Errorf("statement = %q, want no charset introducer on the literal", got.query)
	}
}

// TestANumericDefaultSurvivesTheRewrite covers the other literal kind, which the
// missing driver dropped just as silently.
func TestANumericDefaultSurvivesTheRewrite(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := plan(t, h, "ALTER TABLE orders ADD COLUMN retries INT NOT NULL DEFAULT 3")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply (%s)", got.action, got.reason)
	}
	if !strings.Contains(got.query, "DEFAULT 3") {
		t.Errorf("statement = %q, want the default value kept", got.query)
	}
}

func TestASchemaQualifiedStatementIsRewritten(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := plan(t, h, "ALTER TABLE shop.orders ADD COLUMN email VARCHAR(100)")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply", got.action)
	}
	if strings.Contains(got.query, "`shop`") {
		t.Errorf("statement = %q, still names the source database", got.query)
	}
}

// TestADDLOnAnUnmappedTableIsSkipped is what keeps a shared source server from
// dragging its other schema changes into the replica.
// planFrom runs one statement through the planner as though the source had
// issued it against defaultSchema, which is what the binlog event carries.
func planFrom(t *testing.T, h *MyEventHandler, defaultSchema, query string) ddlDecision {
	t.Helper()

	decisions, err := h.planDDL(defaultSchema, query)
	if err != nil {
		t.Fatalf("planDDL(%q): %v", query, err)
	}
	if len(decisions) != 1 {
		t.Fatalf("planDDL(%q) produced %d decisions, want 1", query, len(decisions))
	}
	return decisions[0]
}

// The table reference used to be matched on its name alone and its database
// thrown away, so this statement — which has nothing to do with the task — was
// rewritten as one against the target and applied.
func TestADDLInAnotherDatabaseIsSkipped(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := plan(t, h, "ALTER TABLE warehouse.orders ADD COLUMN bin VARCHAR(20)")

	if got.action != ddlSkip {
		t.Fatalf("action = %v, want skip; statement = %q", got.action, got.query)
	}
	if !strings.Contains(got.reason, "warehouse") {
		t.Errorf("reason = %q, want the database named", got.reason)
	}
}

// TestAnUnqualifiedDDLFromAnotherDatabaseIsSkipped covers the same statement
// without the qualifier: the source resolves it against the database the
// session was using, and so must this.
func TestAnUnqualifiedDDLFromAnotherDatabaseIsSkipped(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := planFrom(t, h, "warehouse", "ALTER TABLE orders ADD COLUMN bin VARCHAR(20)")

	if got.action != ddlSkip {
		t.Fatalf("action = %v, want skip; statement = %q", got.action, got.query)
	}
}

// TestAnUnqualifiedDDLFromTheReplicatedDatabaseIsApplied is the other side of
// it.
func TestAnUnqualifiedDDLFromTheReplicatedDatabaseIsApplied(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := planFrom(t, h, sourceSchema, "ALTER TABLE orders ADD COLUMN email VARCHAR(100)")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply (%s)", got.action, got.reason)
	}
	if !strings.Contains(got.query, "`main`.`orders`") {
		t.Errorf("statement = %q, want the target named", got.query)
	}
}

// TestDiscoveryDoesNotReachIntoAnotherDatabase covers the task that lists no
// tables.
func TestDiscoveryDoesNotReachIntoAnotherDatabase(t *testing.T) {
	h := newHandler(t, nil, nil)
	h.discovering = true

	got := plan(t, h, "ALTER TABLE warehouse.pallets ADD COLUMN bin VARCHAR(20)")

	if got.action != ddlSkip {
		t.Fatalf("action = %v, want skip; statement = %q", got.action, got.query)
	}
}

// TestWithNoSourceDatabaseTheNameStillMatches pins the fallback down.
func TestWithNoSourceDatabaseTheNameStillMatches(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))
	h.sourceDatabase = ""

	got := plan(t, h, "ALTER TABLE warehouse.orders ADD COLUMN bin VARCHAR(20)")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply (%s)", got.action, got.reason)
	}
}

func TestADDLOnAnUnmappedTableIsSkipped(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	for _, sql := range []string{
		"ALTER TABLE audit_log ADD COLUMN note TEXT",
		"CREATE TABLE sessions (id INT PRIMARY KEY)",
		"DROP TABLE sessions",
		"CREATE INDEX idx ON sessions (id)",
	} {
		t.Run(sql, func(t *testing.T) {
			if got := plan(t, h, sql); got.action != ddlSkip {
				t.Errorf("action = %v, want skip (%s)", got.action, got.reason)
			}
		})
	}
}

func TestAStatementThatNamesNoTableIsSkipped(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	if got := plan(t, h, "CREATE DATABASE analytics"); got.action != ddlSkip {
		t.Errorf("action = %v, want skip", got.action)
	}
}

// TestDestructiveStatementsAreBlocked pins the rule that matters most for a
// disaster-recovery target: the copy must not be destroyed by a mistake at the
// source, because the copy is what the mistake would be recovered from.
func TestDestructiveStatementsAreBlocked(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	tests := []struct {
		sql  string
		want string
	}{
		{"DROP TABLE orders", "drops a replicated table"},
		{"TRUNCATE TABLE orders", "truncates a replicated table"},
		{"ALTER TABLE orders DROP COLUMN email", "drops a column"},
		{"ALTER TABLE orders DROP PRIMARY KEY", "primary key"},
		{"RENAME TABLE orders TO orders_old", "renames a replicated table"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			got := plan(t, h, tt.sql)
			if got.action != ddlBlock {
				t.Fatalf("action = %v, want block", got.action)
			}
			if !strings.Contains(got.reason, tt.want) {
				t.Errorf("reason = %q, want it to mention %q", got.reason, tt.want)
			}
		})
	}
}

// TestAnAdditiveIndexIsApplied records the other side of the same rule: an
// index added at the source is not destructive and is worth having on the
// target, where it serves the same reads after a failover.
func TestAnAdditiveIndexIsApplied(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	got := plan(t, h, "CREATE INDEX idx_customer ON orders (customer)")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply (%s)", got.action, got.reason)
	}
	if !strings.Contains(got.query, "`main`.`orders`") {
		t.Errorf("statement = %q, want the target named", got.query)
	}
}

func TestAnUnparseableStatementIsReported(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	if _, err := h.planDDL(sourceSchema, "this is not sql"); err == nil {
		t.Error("planDDL accepted a statement it cannot have parsed")
	}
}

func TestOnDDLAppliesTheChangeToTheTarget(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := h.OnDDL(nil, mysql.Position{}, query("ALTER TABLE orders ADD COLUMN note TEXT")); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}

	var count int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info('orders') WHERE name = 'note'`).Scan(&count); err != nil {
		t.Fatalf("inspect target: %v", err)
	}
	if count != 1 {
		t.Error("the column was not added to the target")
	}
}

// TestARowUsingANewColumnLandsAfterTheDDL is the sequence the source produces
// and the reason the gap mattered: without the schema change the row that uses
// the new column could not be applied at all.
func TestARowUsingANewColumnLandsAfterTheDDL(t *testing.T) {
	db := sqliteTarget(t, `CREATE TABLE orders (id TEXT, customer TEXT)`)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := h.OnDDL(nil, mysql.Position{}, query("ALTER TABLE orders ADD COLUMN email TEXT")); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}
	if err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("orders", "id", "customer", "email"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "Ada", "ada@example.com"}},
	}); err != nil {
		t.Fatalf("the row after the schema change: %v", err)
	}

	if got := rows(t, db); len(got) != 1 || got[0] != "1|Ada|ada@example.com" {
		t.Errorf("target holds %v", got)
	}
}

func TestOnDDLStopsOnABlockedStatement(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	err := h.OnDDL(nil, mysql.Position{}, query("DROP TABLE orders"))
	if err == nil {
		t.Fatal("OnDDL accepted a statement that would drop the replicated table")
	}
	if !strings.Contains(err.Error(), "refusing to replicate") {
		t.Errorf("error = %v", err)
	}
	if atomic.LoadInt32(&h.lastExecError) != 1 {
		t.Error("the error flag was not raised, so the offset could still advance")
	}
	if _, qerr := db.Query("SELECT 1 FROM orders"); qerr != nil {
		t.Errorf("the target table was dropped anyway: %v", qerr)
	}
}

// TestOnDDLIgnoresATransactionMarker covers the query events that are not DDL
// at all. BEGIN arrives on the same channel and must not stop replication.
func TestOnDDLIgnoresATransactionMarker(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	for _, marker := range []string{"BEGIN", "# a comment", ""} {
		if err := h.OnDDL(nil, mysql.Position{}, query(marker)); err != nil {
			t.Errorf("OnDDL(%q): %v", marker, err)
		}
	}
}

func TestOnDDLWithNoEventIsANoOp(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	if err := h.OnDDL(nil, mysql.Position{}, nil); err != nil {
		t.Errorf("OnDDL(nil): %v", err)
	}
}

// TestOnDDLAppliesTheOpenTransactionFirst pins the ordering.
func TestOnDDLAppliesTheOpenTransactionFirst(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := h.OnRow(insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if err := h.OnDDL(nil, mysql.Position{}, query("ALTER TABLE orders ADD COLUMN note TEXT")); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("target holds %v, want the buffered row applied before the DDL", got)
	}
}

func TestOnTableChangedAppliesTheOpenTransaction(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("orders", "orders"))

	if err := h.OnRow(insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}
	if err := h.OnTableChanged(nil, "shop", "orders"); err != nil {
		t.Fatalf("OnTableChanged: %v", err)
	}

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("target holds %v, want the buffered row applied", got)
	}
}

func TestADDLWithNoTargetConnectionIsReported(t *testing.T) {
	h := newHandler(t, nil, mapTable("orders", "orders"))

	err := h.OnDDL(nil, mysql.Position{}, query("ALTER TABLE orders ADD COLUMN note TEXT"))
	if err == nil {
		t.Fatal("OnDDL with no connection reported nothing")
	}
	if !strings.Contains(err.Error(), "no target connection") {
		t.Errorf("error = %v", err)
	}
}

// Every table it sees is replicated, including one created after the task
// started — which used simply not to be replicated, with no warning anywhere.
func TestADiscoveredTableIsReplicatedUnderItsOwnName(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, nil)
	h.discovering = true

	if err := apply(h, insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	if got := rows(t, db); len(got) != 1 {
		t.Errorf("target holds %v, want the row from the discovered table", got)
	}
}

// TestADiscoveredTablesSchemaChangeIsPropagated is the other half.
func TestADiscoveredTablesSchemaChangeIsPropagated(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, nil)
	h.discovering = true

	got := plan(t, h, "ALTER TABLE orders ADD COLUMN note TEXT")

	if got.action != ddlApply {
		t.Fatalf("action = %v, want apply (%s)", got.action, got.reason)
	}
	if !strings.Contains(got.query, "`main`.`orders`") {
		t.Errorf("statement = %q, want the target named", got.query)
	}
}

// TestTheDirectionLockIsNeverReplicated is why discovery filters names at all:
// copying the lock table would tell the target it is a source, which is exactly
// the state the lock exists to detect.
func TestTheDirectionLockIsNeverReplicated(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, nil)
	h.discovering = true

	err := apply(h, &canal.RowsEvent{
		Table:  sourceTable("_sync_direction_lock", "task_id", "role"),
		Action: canal.InsertAction,
		Rows:   [][]interface{}{{"1", "source"}},
	})
	if err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	if got := plan(t, h, "DROP TABLE _sync_direction_lock"); got.action != ddlSkip {
		t.Errorf("action = %v, want skip: the lock table is not replicated data", got.action)
	}
}

// TestWithoutDiscoveryAnUnlistedTableIsStillSkipped keeps the configured case
// unchanged: a task that names its tables replicates only those.
func TestWithoutDiscoveryAnUnlistedTableIsStillSkipped(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, mapTable("customers", "customers"))

	if err := apply(h, insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	if got := rows(t, db); len(got) != 0 {
		t.Errorf("target holds %v, want nothing from an unlisted table", got)
	}
}
