package postgresql

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/jackc/pglogrepl"
	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// newSyncer builds a syncer with no connections. Only the pure helpers and the
// statement builders can be exercised on it; anything that touches the source
// connection needs a live PostgreSQL.
func newSyncer(t *testing.T, cfg config.SyncConfig) *PostgreSQLSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetOutput(discard{})
	return NewPostgreSQLSyncer(cfg, logger)
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// targetDB stands in for the replication target. The handlers build plain SQL
// text and hand it to a *sql.DB, so SQLite can accept it as long as the
// statements avoid PostgreSQL-only syntax — which is itself worth knowing.
// SQLite calls its own schema "main", so relations are declared in that
// namespace and the generated "main.orders" resolves.
func targetDB(t *testing.T, schema string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

// relation describes a replicated table to the handlers.
func relation(id uint32, namespace, name string, columns ...string) *pglogrepl.RelationMessageV2 {
	cols := make([]*pglogrepl.RelationMessageColumn, len(columns))
	for i, c := range columns {
		cols[i] = &pglogrepl.RelationMessageColumn{Name: c}
	}

	rel := &pglogrepl.RelationMessageV2{}
	rel.RelationID = id
	rel.Namespace = namespace
	rel.RelationName = name
	rel.Columns = cols
	return rel
}

// tuple builds a tuple where every value is a text column. A nil entry becomes
// a NULL column instead.
func tuple(values ...*string) *pglogrepl.TupleData {
	cols := make([]*pglogrepl.TupleDataColumn, len(values))
	for i, v := range values {
		if v == nil {
			cols[i] = &pglogrepl.TupleDataColumn{DataType: 'n'}
			continue
		}
		cols[i] = &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte(*v)}
	}
	return &pglogrepl.TupleData{ColumnNum: uint16(len(cols)), Columns: cols}
}

func text(s string) *string { return &s }

// stateWith returns a replication state that knows the given relations and
// writes to db.
func stateWith(db *sql.DB, rels ...*pglogrepl.RelationMessageV2) *replicationState {
	st := &replicationState{
		relations:   map[uint32]*pglogrepl.RelationMessageV2{},
		replicaConn: db,
	}
	for _, r := range rels {
		st.relations[r.RelationID] = r
	}
	return st
}

// mappingWithSecurity builds the mapping list the handlers consult for field
// masking.
func mappingWithSecurity(table string, fields ...string) []config.DatabaseMapping {
	secured := make([]interface{}, len(fields))
	for i, f := range fields {
		secured[i] = map[string]interface{}{"field": f, "securityType": "masked"}
	}
	return []config.DatabaseMapping{{
		Tables: []config.TableMapping{{
			SourceTable:     table,
			TargetTable:     table,
			SecurityEnabled: true,
			FieldSecurity:   secured,
		}},
	}}
}
