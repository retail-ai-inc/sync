package postgresql

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/retail-ai-inc/sync/internal/platform/config"
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

func TestLoadPositionReadsAStoredLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos")
	if err := os.WriteFile(path, []byte("2/16B3748\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	lsn, err := newSyncer(t, config.SyncConfig{}).loadPosition(path)
	if err != nil {
		t.Fatalf("loadPosition: %v", err)
	}
	if want := pglogrepl.LSN(uint64(2)<<32 + 0x16B3748); lsn != want {
		t.Errorf("lsn = %s, want %s", lsn, want)
	}
}

func TestLoadPositionReportsAMissingFile(t *testing.T) {
	_, err := newSyncer(t, config.SyncConfig{}).loadPosition(
		filepath.Join(t.TempDir(), "absent"))
	if err == nil {
		t.Fatal("loadPosition on a missing file returned no error")
	}
}

// TestAShortPositionFileIsRejectedByLength records the length guard: anything
// under three characters is refused before parsing, so the shortest legal LSN
// text — "0/0" — is exactly at the limit and "1/2" is accepted while a
// two-character file is not, whatever it contains.
func TestAShortPositionFileIsRejectedByLength(t *testing.T) {
	dir := t.TempDir()
	s := newSyncer(t, config.SyncConfig{})

	for _, content := range []string{"", "0", "/0", "  "} {
		path := filepath.Join(dir, "pos")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := s.loadPosition(path); err == nil {
			t.Errorf("loadPosition(%q) returned no error", content)
		}
	}

	path := filepath.Join(dir, "pos")
	if err := os.WriteFile(path, []byte("0/0"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := s.loadPosition(path); err != nil {
		t.Errorf("loadPosition(\"0/0\"): %v", err)
	}
}

func TestLoadPositionRejectsAMalformedLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos")
	if err := os.WriteFile(path, []byte("not-an-lsn"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	if _, err := newSyncer(t, config.SyncConfig{}).loadPosition(path); err == nil {
		t.Fatal("loadPosition on a malformed file returned no error")
	}
}

func TestWriteWALPositionCreatesTheDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "deeper", "pos")
	s := newSyncer(t, config.SyncConfig{PGPositionPath: path})

	if err := s.writeWALPosition(pglogrepl.LSN(uint64(2)<<32 + 0x16B3748)); err != nil {
		t.Fatalf("writeWALPosition: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if got := strings.TrimSpace(string(data)); got != "2/16B3748" {
		t.Errorf("file = %q", got)
	}
}

// TestWriteWALPositionWithNoConfiguredPathIsANoOp records that an unset
// pg_position_path turns the writer off silently: the call reports success and
// nothing is stored, so a PostgreSQL task with no configured path restarts from
// whatever the slot offers rather than from its last committed LSN.
func TestWriteWALPositionWithNoConfiguredPathIsANoOp(t *testing.T) {
	if err := newSyncer(t, config.SyncConfig{}).writeWALPosition(42); err != nil {
		t.Fatalf("writeWALPosition with no path: %v — an unset path appears to be "+
			"reported now, so assert that instead", err)
	}
}

func TestWriteWALPositionRejectsAZeroLSN(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos")
	s := newSyncer(t, config.SyncConfig{PGPositionPath: path})

	if err := s.writeWALPosition(0); err == nil {
		t.Fatal("writeWALPosition(0) returned no error")
	}
	if _, err := os.Stat(path); err == nil {
		t.Error("a file was written for a zero LSN")
	}
}

func TestWriteWALPositionReportsAnUnwritablePath(t *testing.T) {
	// A path whose parent is a regular file cannot be created.
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, nil, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	s := newSyncer(t, config.SyncConfig{PGPositionPath: filepath.Join(blocker, "pos")})

	if err := s.writeWALPosition(42); err == nil {
		t.Fatal("writeWALPosition to an unwritable path returned no error")
	}
}

// TestTheWrittenPositionIsReadBack closes the loop between the two halves, which
// is the only thing that makes a restart resume where the stream stopped.
func TestTheWrittenPositionIsReadBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pos")
	s := newSyncer(t, config.SyncConfig{PGPositionPath: path})
	want := pglogrepl.LSN(uint64(7)<<32 + 0x1234)

	if err := s.writeWALPosition(want); err != nil {
		t.Fatalf("writeWALPosition: %v", err)
	}
	got, err := s.loadPosition(path)
	if err != nil {
		t.Fatalf("loadPosition: %v", err)
	}
	if got != want {
		t.Errorf("read back %s, wrote %s", got, want)
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

// TestBuildReplicationDSNAcceptsAKeywordDSN records that the builder only
// understands URL form. libpq's keyword form ("host=x dbname=y") parses as a
// relative URL path, so the replication parameter is appended as a query string
// and the result is not a valid keyword DSN any more.
func TestBuildReplicationDSNAcceptsAKeywordDSN(t *testing.T) {
	got, err := newSyncer(t, config.SyncConfig{}).
		buildReplicationDSN("host=127.0.0.1 dbname=shop")
	if err != nil {
		t.Fatalf("buildReplicationDSN: %v", err)
	}
	if !strings.Contains(got, "?replication=database") {
		t.Fatalf("dsn = %q; the keyword form appears to be handled now, so assert "+
			"that instead", got)
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
