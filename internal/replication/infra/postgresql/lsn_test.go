package postgresql

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/retail-ai-inc/sync/internal/platform/config"
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
func fileBacked(t *testing.T, source string) (*PostgreSQLSyncer, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "nested", "pos")
	s := newSyncer(t, config.SyncConfig{
		PGPositionPath:   path,
		SourceConnection: source,
	})
	s.checkpoints = &checkpoint.FileStore{Path: path}
	return s, path
}

func TestTheRecordedPositionIsReadBack(t *testing.T) {
	s, _ := fileBacked(t, "postgres://u:p@tokyo:5432/shop")
	want := pglogrepl.LSN(uint64(7)<<32 + 0x1234)

	if err := s.recordLSN(context.Background(), want); err != nil {
		t.Fatalf("recordLSN: %v", err)
	}

	got, err := s.loadStoredLSN(context.Background())
	if err != nil {
		t.Fatalf("loadStoredLSN: %v", err)
	}
	if got != want {
		t.Errorf("read back %s, recorded %s", got, want)
	}
}

func TestNoRecordedPositionIsZero(t *testing.T) {
	s, _ := fileBacked(t, "postgres://u:p@tokyo:5432/shop")

	got, err := s.loadStoredLSN(context.Background())
	if err != nil {
		t.Fatalf("loadStoredLSN: %v", err)
	}
	if got != 0 {
		t.Errorf("lsn = %s with nothing recorded", got)
	}
}

// TestAPlainTextPositionFileIsStillRead keeps an existing deployment resuming.
func TestAPlainTextPositionFileIsStillRead(t *testing.T) {
	s, path := fileBacked(t, "postgres://u:p@tokyo:5432/shop")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("2/16B3748\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	got, err := s.loadStoredLSN(context.Background())
	if err != nil {
		t.Fatalf("loadStoredLSN: %v", err)
	}
	if want := pglogrepl.LSN(uint64(2)<<32 + 0x16B3748); got != want {
		t.Errorf("lsn = %s, want %s", got, want)
	}
}

func TestAMalformedPositionIsReported(t *testing.T) {
	s, path := fileBacked(t, "postgres://u:p@tokyo:5432/shop")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte("not-an-lsn"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := s.loadStoredLSN(context.Background()); err == nil {
		t.Error("a malformed position was accepted")
	}
}

// An LSN means nothing on another server: read there it addresses unrelated
// WAL and the read succeeds, so the task resumes from somewhere arbitrary with
// nothing to show for it.
func TestAPositionFromAnotherSourceIsIgnored(t *testing.T) {
	s, _ := fileBacked(t, "postgres://u:p@tokyo:5432/shop")
	if err := s.recordLSN(context.Background(), pglogrepl.LSN(uint64(7)<<32)); err != nil {
		t.Fatalf("recordLSN: %v", err)
	}

	// The same task, repointed at a different server.
	s.cfg.SourceConnection = "postgres://u:p@osaka:5432/shop"
	got, err := s.loadStoredLSN(context.Background())
	if err != nil {
		t.Fatalf("loadStoredLSN: %v", err)
	}
	if got != 0 {
		t.Errorf("lsn = %s; a position from another server was accepted", got)
	}
}

// TestTheRecordedPositionCarriesNoCredentials pins what is written into a
// database somebody will read.
func TestTheRecordedPositionCarriesNoCredentials(t *testing.T) {
	s, path := fileBacked(t, "postgres://u:hunter2@tokyo:5432/shop")
	if err := s.recordLSN(context.Background(), pglogrepl.LSN(1)); err != nil {
		t.Fatalf("recordLSN: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if strings.Contains(string(data), "hunter2") {
		t.Errorf("the recorded position carries the password: %s", data)
	}
	if !strings.Contains(string(data), "tokyo:5432/shop") {
		t.Errorf("the recorded position does not name the source: %s", data)
	}
}

func TestBuildReplicationDSNAddsTheReplicationParameter(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{})

	got, err := s.buildReplicationDSN("postgres://u:p@host:5432/shop?sslmode=disable")
	if err != nil {
		t.Fatalf("buildReplicationDSN: %v", err)
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
	got, err := newSyncer(t, config.SyncConfig{}).
		buildReplicationDSN("postgres://host/shop?replication=true")
	if err != nil {
		t.Fatalf("buildReplicationDSN: %v", err)
	}
	if strings.Contains(got, "replication=true") {
		t.Errorf("dsn = %q, want the value replaced", got)
	}
}

func TestBuildReplicationDSNReportsAnUnparseableURL(t *testing.T) {
	if _, err := newSyncer(t, config.SyncConfig{}).
		buildReplicationDSN("postgres://host:port/shop"); err == nil {
		t.Fatal("buildReplicationDSN on an invalid URL returned no error")
	}
}

// "host=x dbname=y" parses as a relative URL path rather than failing, so the
// replication parameter used to be appended as a query string onto something
// that has no query string, and the connection was refused with an error
// naming neither.
func TestBuildReplicationDSNAcceptsAKeywordDSN(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{})

	got, err := s.buildReplicationDSN("host=127.0.0.1 dbname=shop")
	if err != nil {
		t.Fatalf("buildReplicationDSN: %v", err)
	}
	if want := "host=127.0.0.1 dbname=shop replication=database"; got != want {
		t.Errorf("dsn = %q, want %q", got, want)
	}

	// A value already there is replaced rather than repeated.
	got, err = s.buildReplicationDSN("host=127.0.0.1 replication=false dbname=shop")
	if err != nil {
		t.Fatalf("buildReplicationDSN: %v", err)
	}
	if strings.Contains(got, "replication=false") {
		t.Errorf("dsn = %q, still carries the old value", got)
	}
}

// TestBuildReplicationDSNRefusesWhatIsNeitherForm covers a connection string
// that is neither: it used to be accepted as a relative URL and fail much later.
func TestBuildReplicationDSNRefusesWhatIsNeitherForm(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{})

	for _, given := range []string{"", "   ", "127.0.0.1:5432/shop"} {
		if got, err := s.buildReplicationDSN(given); err == nil {
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

// TestThePositionIsRecordedOnTheTarget is the reason the store is layered.
func TestThePositionIsRecordedOnTheTarget(t *testing.T) {
	target := targetDB(t, "")
	s := newSyncer(t, config.SyncConfig{
		ID:               3,
		SourceConnection: "postgres://u:p@tokyo:5432/shop",
	})
	s.targetDB = target
	s.checkpoints = s.checkpointStore()

	want := pglogrepl.LSN(uint64(9)<<32 + 0x2a)
	if err := s.recordLSN(context.Background(), want); err != nil {
		t.Fatalf("recordLSN: %v", err)
	}

	// Read it through a fresh store, so nothing in-process is answering.
	s.checkpoints = s.checkpointStore()
	got, err := s.loadStoredLSN(context.Background())
	if err != nil {
		t.Fatalf("loadStoredLSN: %v", err)
	}
	if got != want {
		t.Errorf("read back %s, recorded %s", got, want)
	}

	var stored string
	if err := target.QueryRow(
		`SELECT payload FROM _sync_checkpoint WHERE task_id = 3`).Scan(&stored); err != nil {
		t.Fatalf("read the checkpoint row: %v", err)
	}
	if !strings.Contains(stored, "9/2A") {
		t.Errorf("the target holds %q", stored)
	}
}

// TestThePositionIsAlsoWrittenToTheConfiguredFile keeps the setting doing
// something: an operator who configured a path still has a file to look at.
func TestThePositionIsAlsoWrittenToTheConfiguredFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "pos")
	s := newSyncer(t, config.SyncConfig{
		ID:               4,
		PGPositionPath:   path,
		SourceConnection: "postgres://u:p@tokyo:5432/shop",
	})
	s.targetDB = targetDB(t, "")
	s.checkpoints = s.checkpointStore()

	if err := s.recordLSN(context.Background(), pglogrepl.LSN(1)); err != nil {
		t.Fatalf("recordLSN: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Errorf("the configured position file was not written: %v", err)
	}
}

// TestTheMetricsNameTheTaskWithoutItsCredentials pins what goes into an
// exposition that is scraped and stored for months.
func TestTheMetricsNameTheTaskWithoutItsCredentials(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{
		ID:               11,
		SourceConnection: "postgres://u:hunter2@tokyo:5432/shop",
		TargetConnection: "postgres://u:hunter2@osaka:5432/shop",
	})

	labels := s.metricLabels()

	if labels["task"] != "11" || labels["engine"] != "postgresql" {
		t.Errorf("labels = %v", labels)
	}
	// The endpoints are on sync_task_info, not on every series a task publishes.
	if _, ok := labels["source"]; ok {
		t.Errorf("labels carry the endpoints: %v", labels)
	}
	if _, ok := labels["target"]; ok {
		t.Errorf("labels carry the endpoints: %v", labels)
	}
	for name, value := range labels {
		if strings.Contains(value, "hunter2") {
			t.Errorf("label %s carries the password: %q", name, value)
		}
	}
}

// TestAnUnreachableSourceStopsTheDirectionClaim covers the refusal path: a task
// that cannot record which way it replicates must not start replicating, or two
// syncers can overwrite each other after a failover.
func TestAnUnreachableSourceStopsTheDirectionClaim(t *testing.T) {
	s := newSyncer(t, config.SyncConfig{
		ID:               5,
		SourceConnection: "postgres://u:p@127.0.0.1:1/shop?sslmode=disable&connect_timeout=1",
		TargetConnection: "postgres://u:p@osaka:5432/shop",
	})
	s.targetDB = targetDB(t, "")

	release, err := s.claimDirection(context.Background())
	if err == nil {
		release()
		t.Fatal("the direction was claimed against an unreachable source")
	}
}
