package mysql

import (
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
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

// The sequence the source produces, and the reason the gap mattered: without
// the schema change the row that uses the new column cannot be applied at all.
// The reader emits the statement and the applier writes it, so the write is
// done here in the applier's place.
func TestARowUsingANewColumnLandsAfterTheDDL(t *testing.T) {
	db := sqliteTarget(t, `CREATE TABLE orders (id TEXT, customer TEXT)`)
	h := newHandler(t, db, mapTable("orders", "orders"))

	r := readerWithMappings(t, mapTable("orders", "orders"))
	if err := r.OnDDL(nil, mysql.Position{}, query("ALTER TABLE orders ADD COLUMN email TEXT")); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}
	events := handedOver(r)
	if len(events) != 1 {
		t.Fatalf("the reader emitted %d events for the schema change", len(events))
	}
	if _, err := db.Exec(events[0].Payload.(statement).query); err != nil {
		t.Fatalf("apply the schema change to the target: %v", err)
	}

	if err := apply(db, h, &canal.RowsEvent{
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

// Every table it sees is replicated, including one created after the task
// started — which used simply not to be replicated, with no warning anywhere.
func TestADiscoveredTableIsReplicatedUnderItsOwnName(t *testing.T) {
	db := sqliteTarget(t, ordersSchema)
	h := newHandler(t, db, nil)
	h.discovering = true

	if err := apply(db, h, insertEvent("1", "Ada", "a@x")); err != nil {
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

	err := apply(db, h, &canal.RowsEvent{
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

	if err := apply(db, h, insertEvent("1", "Ada", "a@x")); err != nil {
		t.Fatalf("OnRow: %v", err)
	}

	if got := rows(t, db); len(got) != 0 {
		t.Errorf("target holds %v, want nothing from an unlisted table", got)
	}
}

// readerWithMappings builds a Reader whose converter renders statements, which
// is what the DDL path needs and all it needs.
func readerWithMappings(t *testing.T, mappings []config.DatabaseMapping) *Reader {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)

	r := readerFor(mappings, "u:p@tcp(h:3306)/shop")
	r.Logger = logger
	r.conv = r.converter()
	r.appliedSince = map[string]bool{}
	// OnDDL hands the event straight over, a schema change being its own batch,
	// so there has to be somewhere for it to go.
	r.out = make(chan *domain.Event, 8)
	r.done = make(chan struct{})
	return r
}

// handedOver drains what the reader pushed onto its channel.
func handedOver(r *Reader) []*domain.Event {
	var events []*domain.Event
	for {
		select {
		case e := <-r.out:
			events = append(events, e)
		default:
			return events
		}
	}
}

// A schema change reaches the target as an event of its own, carrying the
// statement rewritten for the target's names. These tests used to drive
// MyEventHandler.OnDDL, which applied the statement itself; Reader.OnDDL is the
// path the supervisor takes, and the applier does the writing.
func TestASchemaChangeBecomesAnEvent(t *testing.T) {
	r := readerWithMappings(t, mapTable("orders", "orders"))

	if err := r.OnDDL(nil, mysql.Position{}, query("ALTER TABLE orders ADD COLUMN note TEXT")); err != nil {
		t.Fatalf("OnDDL: %v", err)
	}

	events := handedOver(r)
	if len(events) != 1 {
		t.Fatalf("the reader handed over %d events, want the schema change", len(events))
	}
	event := events[0]
	if event.Op != domain.OpSchema {
		t.Errorf("op = %v, want a schema change", event.Op)
	}
	stmt, ok := event.Payload.(statement)
	if !ok {
		t.Fatalf("payload = %T, want a statement", event.Payload)
	}
	if !strings.Contains(stmt.query, "note") {
		t.Errorf("statement = %q, want it to add the column", stmt.query)
	}
}

// A statement that would destroy replicated data stops the task rather than
// being carried, and stopping has to be permanent: retrying would offer the
// same DROP for as long as anybody let it.
func TestADestructiveStatementStopsTheTask(t *testing.T) {
	r := readerWithMappings(t, mapTable("orders", "orders"))

	err := r.OnDDL(nil, mysql.Position{}, query("DROP TABLE orders"))
	if err == nil {
		t.Fatal("OnDDL accepted a statement that would drop the replicated table")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("err = %v, want it unrecoverable", err)
	}
	if events := handedOver(r); len(events) != 0 {
		t.Errorf("the reader handed over %d events for a refused statement", len(events))
	}
}

// BEGIN and COMMIT arrive as query events and do not parse. Carrying on is
// right for those, and a nil event is not an event at all.
func TestATransactionMarkerIsNotASchemaChange(t *testing.T) {
	r := readerWithMappings(t, mapTable("orders", "orders"))

	for _, marker := range []string{"BEGIN", "COMMIT", "# Dumm"} {
		if err := r.OnDDL(nil, mysql.Position{}, query(marker)); err != nil {
			t.Errorf("OnDDL(%q): %v", marker, err)
		}
	}
	if err := r.OnDDL(nil, mysql.Position{}, nil); err != nil {
		t.Errorf("OnDDL(nil): %v", err)
	}
	if events := handedOver(r); len(events) != 0 {
		t.Errorf("the reader handed over %d events for statements that are not DDL",
			len(events))
	}
}
