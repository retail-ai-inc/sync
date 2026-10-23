package postgresql

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/sirupsen/logrus"
)

// quiet is a logger that discards, so the tests do not flood the log.
func quiet() logrus.FieldLogger {
	logger := logrus.New()
	logger.SetOutput(discard{})
	return logger
}

// readerFor builds a reader over a stream of canned messages.
func readerFor(t *testing.T, cfg config.SyncConfig, messages ...[]byte) *Reader {
	t.Helper()

	return &Reader{
		Source: &cannedStream{messages: messages},
		Config: cfg,
		Logger: quiet(),
		Labels: metrics.Labels{"task": t.Name(), "engine": "postgresql"},
	}
}

// cannedStream hands out prepared messages and then blocks the way a quiet
// source does, so the reader sees exactly what a real one would.
type cannedStream struct {
	messages [][]byte
	at       int
	// keepalives, when set, are returned once the messages run out rather than
	// timing out, which is what a real source sends on an idle connection.
	keepalives [][]byte
}

func (c *cannedStream) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	if c.at < len(c.messages) {
		payload := c.messages[c.at]
		c.at++
		return &pgproto3.CopyData{Data: payload}, nil
	}
	if len(c.keepalives) > 0 {
		payload := c.keepalives[0]
		c.keepalives = c.keepalives[1:]
		return &pgproto3.CopyData{Data: payload}, nil
	}
	// Nothing more to say, which the reader must treat as a quiet source rather
	// than a failure.
	<-ctx.Done()
	return nil, ctx.Err()
}

// wal wraps a logical message as the XLogData the stream carries it in.
func wal(walEnd uint64, body []byte) []byte {
	out := []byte{pglogrepl.XLogDataByteID}
	out = append(out, u64(walEnd)...) // start
	out = append(out, u64(walEnd)...) // server WAL end
	out = append(out, u64(0)...)      // server time
	return append(out, body...)
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }

// targetDB stands in for the replication target.
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

// tuple builds a tuple where every value is a text column.
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

// rig is a reader that knows some relations and a target to write what it
// decodes, which is what the reader and the applier do between them.
//
// The two halves used to be one method, so a test could only see the row that
// landed. Keeping the same end-to-end assertion here means these tests still
// say what they said, while each half is now separately reachable.
type rig struct {
	reader *Reader
	db     *sql.DB
}

func stateWith(t *testing.T, db *sql.DB, rels ...*pglogrepl.RelationMessageV2) *rig {
	t.Helper()
	return withConfig(t, config.SyncConfig{}, db, rels...)
}

func withConfig(t *testing.T, cfg config.SyncConfig, db *sql.DB,
	rels ...*pglogrepl.RelationMessageV2) *rig {
	t.Helper()

	r := readerFor(t, cfg)
	r.relations = map[uint32]*pglogrepl.RelationMessageV2{}
	for _, rel := range rels {
		r.relations[rel.RelationID] = rel
	}
	r.inTransaction = true
	return &rig{reader: r, db: db}
}

// keys tells the rig which columns address a row, as the source's catalogue
// would.
func (g *rig) keys(columns ...string) *rig {
	g.reader.Keys = func(string, string) ([]string, error) { return columns, nil }
	return g
}

func (g *rig) handleInsert(msg *pglogrepl.InsertMessageV2) (bool, error) {
	return g.carry(g.reader.decode(msg))
}

func (g *rig) handleUpdate(msg *pglogrepl.UpdateMessageV2) (bool, error) {
	return g.carry(g.reader.decode(msg))
}

func (g *rig) handleDelete(msg *pglogrepl.DeleteMessageV2) (bool, error) {
	return g.carry(g.reader.decode(msg))
}

// carry writes whatever the decode produced, so a test can assert on the target
// rather than on a statement.
func (g *rig) carry(err error) (bool, error) {
	if err != nil {
		return false, err
	}
	events := g.reader.open
	g.reader.open = []*domain.Event{}
	if len(events) == 0 {
		return false, nil
	}

	applier := &Applier{DB: g.db, Logger: quiet()}
	return applier.Apply(context.Background(), [][]*domain.Event{events}, domain.Position{})
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
