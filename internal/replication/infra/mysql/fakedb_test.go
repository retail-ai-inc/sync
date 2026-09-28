package mysql

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// A database/sql driver that answers canned results.
//
// Most of what this package does with a *sql.DB is ask a server what it is set
// up to do and decide from the answer -- the preflight checks, the binary log
// position, the column and key discovery. The deciding is pure and tested as
// such, but the asking is not: which statement is sent, whether a value is
// bound or interpolated, and what is made of a server that answers with
// something unexpected. Those are only reachable through a driver, and a fake
// one costs less than a dependency and runs everywhere.
//
// It matches statements by substring, in the order they are registered, so a
// test says what it expects to be asked and nothing else has to be modelled.

type reply struct {
	// match is a substring of the statement this answers.
	match   string
	columns []string
	rows    [][]driver.Value
	err     error
	// rowsErr ends the rows with this error instead of io.EOF, as a connection
	// dropped partway through a read does.
	rowsErr error
	// sequence answers successive calls with successive row sets, the last one
	// repeating. A server whose binlog moves between two reads cannot be
	// modelled with a single fixed answer.
	sequence [][][]driver.Value
	calls    int
}

type fakeDB struct {
	replies []reply

	mu    sync.Mutex
	asked []string
}

var fakeDriverOnce sync.Once
var fakeRegistry sync.Map // name -> *fakeDB
var fakeSequence atomic.Int64

// open registers this fake under a fresh name and returns a *sql.DB that talks
// to it. Fresh per call, so tests do not share recorded statements.
func (f *fakeDB) open(t *testing.T) *sql.DB {
	t.Helper()

	fakeDriverOnce.Do(func() { sql.Register("mysqlfake", fakeDriver{}) })

	name := fmt.Sprintf("fake-%d", fakeSequence.Add(1))
	fakeRegistry.Store(name, f)
	t.Cleanup(func() { fakeRegistry.Delete(name) })

	db, err := sql.Open("mysqlfake", name)
	if err != nil {
		t.Fatalf("open the fake driver: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// statements reports what was asked, in order.
func (f *fakeDB) statements() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.asked...)
}

// asked reports whether any statement contained the substring.
func (f *fakeDB) wasAsked(substring string) bool {
	for _, statement := range f.statements() {
		if strings.Contains(statement, substring) {
			return true
		}
	}
	return false
}

func (f *fakeDB) answer(query string) (reply, bool) {
	f.mu.Lock()
	f.asked = append(f.asked, query)
	f.mu.Unlock()

	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.replies {
		r := &f.replies[i]
		if !strings.Contains(query, r.match) {
			continue
		}
		answer := *r
		if len(r.sequence) > 0 {
			at := r.calls
			if at >= len(r.sequence) {
				at = len(r.sequence) - 1
			}
			answer.rows = r.sequence[at]
		}
		r.calls++
		return answer, true
	}
	return reply{}, false
}

type fakeDriver struct{}

func (fakeDriver) Open(name string) (driver.Conn, error) {
	held, ok := fakeRegistry.Load(name)
	if !ok {
		return nil, fmt.Errorf("no fake registered as %q", name)
	}
	return &fakeConn{db: held.(*fakeDB)}, nil
}

type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(query string) (driver.Stmt, error) {
	return &fakeStmt{db: c.db, query: query}, nil
}
func (c *fakeConn) Close() error              { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) { return fakeTx{}, nil }

type fakeTx struct{}

func (fakeTx) Commit() error   { return nil }
func (fakeTx) Rollback() error { return nil }

type fakeStmt struct {
	db    *fakeDB
	query string
}

func (s *fakeStmt) Close() error  { return nil }
func (s *fakeStmt) NumInput() int { return -1 } // any number of parameters

func (s *fakeStmt) Exec(args []driver.Value) (driver.Result, error) {
	answer, ok := s.db.answer(s.query)
	if !ok {
		return nil, fmt.Errorf("the fake was not told how to answer %q", s.query)
	}
	if answer.err != nil {
		return nil, answer.err
	}
	return driver.RowsAffected(0), nil
}

func (s *fakeStmt) Query(args []driver.Value) (driver.Rows, error) {
	answer, ok := s.db.answer(s.query)
	if !ok {
		return nil, fmt.Errorf("the fake was not told how to answer %q", s.query)
	}
	if answer.err != nil {
		return nil, answer.err
	}
	return &fakeRows{columns: answer.columns, rows: answer.rows, err: answer.rowsErr}, nil
}

type fakeRows struct {
	columns []string
	rows    [][]driver.Value
	at      int
	err     error
}

func (r *fakeRows) Columns() []string { return r.columns }
func (r *fakeRows) Close() error      { return nil }

func (r *fakeRows) Next(dest []driver.Value) error {
	if r.at >= len(r.rows) {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	copy(dest, r.rows[r.at])
	r.at++
	return nil
}

// variable is a SHOW GLOBAL VARIABLES answer.
func variable(name, value string) reply {
	return reply{
		match:   "'" + name + "'",
		columns: []string{"Variable_name", "Value"},
		rows:    [][]driver.Value{{name, value}},
	}
}

// noSuchVariable is how a server that has never heard of a setting answers:
// the statement works and returns nothing.
func noSuchVariable(name string) reply {
	return reply{
		match:   "'" + name + "'",
		columns: []string{"Variable_name", "Value"},
	}
}
