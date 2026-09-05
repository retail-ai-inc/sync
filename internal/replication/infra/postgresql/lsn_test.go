package postgresql

import (
	"context"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

func TestHexStrToUint32(t *testing.T) {
	tests := []struct {
		in   string
		want uint32
		ok   bool
	}{
		{"0", 0, true},
		{"1", 1, true},
		{"FF", 255, true},
		{"ff", 255, true},
		{"FFFFFFFF", 4294967295, true},
		{"  1A  ", 26, true},
		{"", 0, false},
		{"   ", 0, false},
		{"100000000", 0, false}, // one bit past uint32
		{"zz", 0, false},
		{"-1", 0, false},
		{"0x10", 0, false}, // the 0x prefix is not accepted
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := hexStrToUint32(tc.in)
			if tc.ok && err != nil {
				t.Fatalf("hexStrToUint32(%q) = %v", tc.in, err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatalf("hexStrToUint32(%q) = %d, want an error", tc.in, got)
				}
				return
			}
			if got != tc.want {
				t.Errorf("hexStrToUint32(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestParseLSNFromString(t *testing.T) {
	tests := []struct {
		in   string
		want pglogrepl.LSN
		ok   bool
	}{
		{"0/0", 0, true},
		{"0/1", 1, true},
		{"1/0", 1 << 32, true},
		{"2/16B3748", pglogrepl.LSN(uint64(2)<<32 + 0x16B3748), true},
		{" 1/2 ", 1<<32 + 2, true},
		{"", 0, false},
		{"1", 0, false},
		{"1/2/3", 0, false},
		{"/2", 0, false},
		{"1/", 0, false},
		{"x/2", 0, false},
		{"1/x", 0, false},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseLSNFromString(tc.in)
			if tc.ok && err != nil {
				t.Fatalf("parseLSNFromString(%q) = %v", tc.in, err)
			}
			if !tc.ok {
				if err == nil {
					t.Fatalf("parseLSNFromString(%q) = %s, want an error", tc.in, got)
				}
				return
			}
			if got != tc.want {
				t.Errorf("parseLSNFromString(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

// TestAnLSNSurvivesTheRoundTrip checks the parser against the formatter the
// writer uses, since the position file is written by one and read by the other.
func TestAnLSNSurvivesTheRoundTrip(t *testing.T) {
	for _, lsn := range []pglogrepl.LSN{1, 1 << 32, 0xABCDEF, pglogrepl.LSN(uint64(3)<<32 + 0x7FFFFFF)} {
		got, err := parseLSNFromString(lsn.String())
		if err != nil {
			t.Fatalf("parseLSNFromString(%q): %v", lsn.String(), err)
		}
		if got != lsn {
			t.Errorf("round trip of %s gave %s", lsn, got)
		}
	}
}

// fileBacked returns a syncer whose position is recorded in one file, which is
// what the layered store falls back to when no target is configured.
// The stored position, which is a document rather than a bare number so it can
// say which server it belongs to.

const thisSource = "10.118.192.8:5432/shop"

func TestTheRecordedPositionIsReadBack(t *testing.T) {
	want := pglogrepl.LSN(uint64(3)<<32 | 0x1A2B)

	payload, err := encodeLSN(want, thisSource)
	if err != nil {
		t.Fatalf("encodeLSN: %v", err)
	}
	got, elsewhere, err := decodeLSN(payload, thisSource)
	if err != nil {
		t.Fatalf("decodeLSN: %v", err)
	}
	if elsewhere != "" {
		t.Errorf("a position from this source was attributed to %q", elsewhere)
	}
	if got != want {
		t.Errorf("read back %s, want %s", got, want)
	}
}

func TestNoRecordedPositionIsZero(t *testing.T) {
	for _, payload := range []string{"", "   ", "\n"} {
		got, _, err := decodeLSN(payload, thisSource)
		if err != nil {
			t.Fatalf("decodeLSN(%q): %v", payload, err)
		}
		if got != 0 {
			t.Errorf("decodeLSN(%q) = %s, want zero", payload, got)
		}
	}
}

// TestAPlainTextPositionFileIsStillRead keeps an existing deployment resuming.
// An older build wrote the LSN as bare text; refusing that form would make
// every task that had already run copy its source again.
func TestAPlainTextPositionFileIsStillRead(t *testing.T) {
	got, _, err := decodeLSN("3/1A2B", thisSource)
	if err != nil {
		t.Fatalf("decodeLSN: %v", err)
	}
	if want := pglogrepl.LSN(uint64(3)<<32 | 0x1A2B); got != want {
		t.Errorf("read %s, want %s", got, want)
	}
}

func TestAMalformedPositionIsReported(t *testing.T) {
	if _, _, err := decodeLSN("not a position", thisSource); err == nil {
		t.Error("a position that cannot be read was accepted, so the task would " +
			"resume from zero and copy the source again without saying why")
	}
}

// TestAPositionFromAnotherSourceIsIgnored: an LSN addresses one server's WAL
// and nowhere else. Read against a different server it names unrelated bytes,
// and the read succeeds -- so the task would resume from somewhere arbitrary
// with nothing to show for it.
func TestAPositionFromAnotherSourceIsIgnored(t *testing.T) {
	payload, err := encodeLSN(pglogrepl.LSN(1<<32), "10.118.192.9:5432/shop")
	if err != nil {
		t.Fatalf("encodeLSN: %v", err)
	}

	got, elsewhere, err := decodeLSN(payload, thisSource)
	if err != nil {
		t.Fatalf("decodeLSN: %v", err)
	}
	if got != 0 {
		t.Errorf("a position from another server was used: %s", got)
	}
	if elsewhere != "10.118.192.9:5432/shop" {
		t.Errorf("the other server was reported as %q", elsewhere)
	}
}

// TestTheRecordedPositionCarriesNoCredentials pins what is written down: the
// position is stored on the target and read by whoever can read the target.
func TestTheRecordedPositionCarriesNoCredentials(t *testing.T) {
	payload, err := encodeLSN(pglogrepl.LSN(1), endpointOf(
		"postgres://someone:hunter2@10.118.192.8:5432/shop"))
	if err != nil {
		t.Fatalf("encodeLSN: %v", err)
	}
	if strings.Contains(payload, "hunter2") {
		t.Errorf("the stored position carries the password: %s", payload)
	}
}

func TestBuildReplicationDSNAddsTheReplicationParameter(t *testing.T) {
	got, err := replicationConnection("postgres://u:p@host:5432/shop?sslmode=disable")
	if err != nil {
		t.Fatalf("replicationConnection: %v", err)
	}
	if !strings.Contains(got, "replication=database") {
		t.Errorf("dsn = %q, want the replication parameter", got)
	}
	if !strings.Contains(got, "sslmode=disable") {
		t.Errorf("dsn = %q, want the existing parameters kept", got)
	}
	if !strings.HasPrefix(got, "postgres://u:p@host:5432/shop?") {
		t.Errorf("dsn = %q, want the rest of the URL untouched", got)
	}
}

// TestBuildReplicationDSNOverwritesAnExistingValue records that a caller cannot
// ask for a different replication mode: whatever the configured DSN says is
// replaced with "database".
func TestBuildReplicationDSNOverwritesAnExistingValue(t *testing.T) {
	got, err := replicationConnection("postgres://host/shop?replication=true")
	if err != nil {
		t.Fatalf("replicationConnection: %v", err)
	}
	if strings.Contains(got, "replication=true") {
		t.Errorf("dsn = %q, want the value replaced", got)
	}
}

func TestBuildReplicationDSNReportsAnUnparseableURL(t *testing.T) {
	if _, err := replicationConnection("postgres://host:port/shop"); err == nil {
		t.Fatal("replicationConnection on an invalid URL returned no error")
	}
}

// "host=x dbname=y" parses as a relative URL path rather than failing, so the
// replication parameter used to be appended as a query string onto something
// that has no query string, and the connection was refused with an error
// naming neither.
func TestBuildReplicationDSNAcceptsAKeywordDSN(t *testing.T) {
	got, err := replicationConnection("host=127.0.0.1 dbname=shop")
	if err != nil {
		t.Fatalf("replicationConnection: %v", err)
	}
	if want := "host=127.0.0.1 dbname=shop replication=database"; got != want {
		t.Errorf("dsn = %q, want %q", got, want)
	}

	// A value already there is replaced rather than repeated.
	got, err = replicationConnection("host=127.0.0.1 replication=false dbname=shop")
	if err != nil {
		t.Fatalf("replicationConnection: %v", err)
	}
	if strings.Contains(got, "replication=false") {
		t.Errorf("dsn = %q, still carries the old value", got)
	}
}

// TestBuildReplicationDSNRefusesWhatIsNeitherForm covers a connection string
// that is neither: it used to be accepted as a relative URL and fail much later.
func TestBuildReplicationDSNRefusesWhatIsNeitherForm(t *testing.T) {
	for _, given := range []string{"", "   ", "127.0.0.1:5432/shop"} {
		if got, err := replicationConnection(given); err == nil {
			t.Errorf("buildReplicationDSN(%q) = %q, want a refusal", given, got)
		}
	}
}

func TestExtractSequenceName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"nextval('orders_id_seq'::regclass)", "orders_id_seq"},
		{"nextval('public.orders_id_seq'::regclass)", "public.orders_id_seq"},
		{"now()", ""},
		{"", ""},
		{"nextval('orders_id_seq')", ""}, // the ::regclass cast is required
		{"NEXTVAL('orders_id_seq'::regclass)", ""},
	}

	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			if got := extractSequenceName(tc.in); got != tc.want {
				t.Errorf("extractSequenceName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// The position is written by the applier, in the same transaction as the rows,
// so the two cannot disagree. A task that also names a path gets a copy on
// disk, written after the commit: only the target can take part in the
// transaction, and the file is there to be looked at.

func TestThePositionIsRecordedOnTheTargetWithTheRows(t *testing.T) {
	target := targetDB(t, `CREATE TABLE orders (id TEXT)`)
	store := &checkpoint.SQLStore{DB: target, TaskID: 3}
	if err := store.Ensure(context.Background()); err != nil {
		t.Fatalf("prepare the position table: %v", err)
	}

	payload, err := encodeLSN(pglogrepl.LSN(uint64(9)<<32+0x2a), thisSource)
	if err != nil {
		t.Fatalf("encodeLSN: %v", err)
	}

	applier := &Applier{DB: target, Checkpoints: store, Logger: quiet()}
	committed, err := applier.Apply(context.Background(),
		[][]*domain.Event{{{
			NS:      domain.Namespace{DB: "public", Object: "orders"},
			Op:      domain.OpInsert,
			Payload: statement{query: `INSERT INTO orders (id) VALUES (?)`, args: []interface{}{"1"}},
		}}},
		domain.Position{Payload: payload})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !committed {
		t.Fatal("the applier did not record the position, so the runner would " +
			"record it separately and the two could disagree")
	}

	var stored string
	if err := target.QueryRow(
		`SELECT payload FROM _sync_checkpoint WHERE task_id = 3`).Scan(&stored); err != nil {
		t.Fatalf("read the position row: %v", err)
	}
	if !strings.Contains(stored, "9/2A") {
		t.Errorf("the target holds %q", stored)
	}

	// And the row landed in the same transaction.
	var rows int
	if err := target.QueryRow(`SELECT COUNT(*) FROM orders`).Scan(&rows); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if rows != 1 {
		t.Errorf("%d rows were written alongside the position", rows)
	}
}

// TestAFailedBatchRecordsNoPosition is the property the shared transaction is
// for: a position ahead of the rows it describes means the next start resumes
// past changes the target never received.
func TestAFailedBatchRecordsNoPosition(t *testing.T) {
	target := targetDB(t, `CREATE TABLE orders (id TEXT)`)
	store := &checkpoint.SQLStore{DB: target, TaskID: 5}
	if err := store.Ensure(context.Background()); err != nil {
		t.Fatalf("prepare the position table: %v", err)
	}

	payload, _ := encodeLSN(pglogrepl.LSN(1<<32), thisSource)
	applier := &Applier{DB: target, Checkpoints: store, Logger: quiet()}

	_, err := applier.Apply(context.Background(),
		[][]*domain.Event{{{
			NS:      domain.Namespace{DB: "public", Object: "orders"},
			Op:      domain.OpInsert,
			Payload: statement{query: `INSERT INTO nonexistent (id) VALUES (?)`, args: []interface{}{"1"}},
		}}},
		domain.Position{Payload: payload})
	if err == nil {
		t.Fatal("a batch against a table that is not there was reported as applied")
	}

	var rows int
	if err := target.QueryRow(
		`SELECT COUNT(*) FROM _sync_checkpoint WHERE task_id = 5`).Scan(&rows); err != nil {
		t.Fatalf("count positions: %v", err)
	}
	if rows != 0 {
		t.Error("a position was recorded for a batch that did not land")
	}
}

// TestThePositionIsAlsoWrittenToTheConfiguredFile keeps the setting doing
// something: an operator who configured a path still has a file to look at.
func TestThePositionIsAlsoWrittenToTheConfiguredFile(t *testing.T) {
	target := targetDB(t, `CREATE TABLE orders (id TEXT)`)
	store := &checkpoint.SQLStore{DB: target, TaskID: 4}
	if err := store.Ensure(context.Background()); err != nil {
		t.Fatalf("prepare the position table: %v", err)
	}

	path := filepath.Join(t.TempDir(), "nested", "pos")
	payload, _ := encodeLSN(pglogrepl.LSN(uint64(7)<<32), thisSource)

	applier := &Applier{
		DB:          target,
		Checkpoints: store,
		Mirror:      &checkpoint.FileStore{Path: path},
		Logger:      quiet(),
	}
	if _, err := applier.Apply(context.Background(),
		[][]*domain.Event{{{
			NS:      domain.Namespace{DB: "public", Object: "orders"},
			Op:      domain.OpInsert,
			Payload: statement{query: `INSERT INTO orders (id) VALUES (?)`, args: []interface{}{"1"}},
		}}},
		domain.Position{Payload: payload}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("the configured position file was not written: %v", err)
	}
	if !strings.Contains(string(written), "7/0") {
		t.Errorf("the file holds %q", written)
	}
}

// TestAnUnwritableFileDoesNotFailABatchThatLanded: the position on the target
// is the one that is resumed from, and the batch is already committed by the
// time the file is touched.
func TestAnUnwritableFileDoesNotFailABatchThatLanded(t *testing.T) {
	target := targetDB(t, `CREATE TABLE orders (id TEXT)`)
	store := &checkpoint.SQLStore{DB: target, TaskID: 6}
	if err := store.Ensure(context.Background()); err != nil {
		t.Fatalf("prepare the position table: %v", err)
	}

	payload, _ := encodeLSN(pglogrepl.LSN(1<<32), thisSource)
	applier := &Applier{
		DB:          target,
		Checkpoints: store,
		// A directory, which cannot be written to as a file.
		Mirror: &checkpoint.FileStore{Path: t.TempDir()},
		Logger: quiet(),
	}

	committed, err := applier.Apply(context.Background(),
		[][]*domain.Event{{{
			NS:      domain.Namespace{DB: "public", Object: "orders"},
			Op:      domain.OpInsert,
			Payload: statement{query: `INSERT INTO orders (id) VALUES (?)`, args: []interface{}{"1"}},
		}}},
		domain.Position{Payload: payload})
	if err != nil {
		t.Fatalf("a batch that landed was failed by the copy on disk: %v", err)
	}
	if !committed {
		t.Error("the position was not recorded on the target")
	}
}
