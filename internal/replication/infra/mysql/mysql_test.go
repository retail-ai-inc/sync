package mysql

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/sirupsen/logrus"
)

// newSyncer builds a syncer that writes SQLite, which is what the hermetic
// suite has to drive it against. Production leaves the dialect unset and gets
// MySQL; upsertStatement is covered in both flavours by its own table test.
func newSyncer(t *testing.T) *MySQLSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	// Type is set because the endpoint and database-name helpers dispatch on
	// it; a syncer in this package is always a MySQL one.
	return &MySQLSyncer{
		cfg:     config.SyncConfig{Type: "mysql"},
		logger:  logger,
		dialect: dialectSQLite,
	}
}

func TestParseAddr(t *testing.T) {
	s := newSyncer(t)

	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{"standard", "root:root@tcp(localhost:3306)/source_db", "localhost:3306"},
		{"with parameters", "root:root@tcp(db:3307)/source_db?charset=utf8", "db:3307"},
		{"no database", "root:root@tcp(db:3306)/", "db:3306"},
		{"not a tcp dsn", "root:root@unix(/tmp/mysql.sock)/db", ""},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := s.parseAddr(tt.dsn); got != tt.want {
				t.Errorf("parseAddr(%q) = %q, want %q", tt.dsn, got, tt.want)
			}
		})
	}
}

func TestParseUserPassword(t *testing.T) {
	s := newSyncer(t)

	tests := []struct {
		name           string
		dsn            string
		wantUser, want string
	}{
		{"standard", "root:secret@tcp(localhost:3306)/db", "root", "secret"},
		{"no credentials", "tcp(localhost:3306)/db", "", ""},
		{"user without password", "root@tcp(localhost:3306)/db", "root", ""},
		{"empty", "", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, pass := s.parseUserPassword(tt.dsn)
			if user != tt.wantUser || pass != tt.want {
				t.Errorf("parseUserPassword(%q) = %q/%q, want %q/%q",
					tt.dsn, user, pass, tt.wantUser, tt.want)
			}
		})
	}
}

// TestParseUserPasswordSurvivesSpecialCharacters covers the characters that
// used to truncate the credentials. Both are legal inside a password and both
// appear in passwords Cloud SQL generates; the DSN is assembled from whatever
// an operator typed into the UI, so nothing rejects them earlier either.
func TestParseUserPasswordSurvivesSpecialCharacters(t *testing.T) {
	s := newSyncer(t)

	tests := []struct {
		name               string
		dsn                string
		wantUser, wantPass string
	}{
		{"at sign", "root:p@ss@tcp(localhost:3306)/db", "root", "p@ss"},
		{"colon", "root:pa:ss@tcp(localhost:3306)/db", "root", "pa:ss"},
		{"both", "root:p@s:s@tcp(localhost:3306)/db", "root", "p@s:s"},
		{"slash", "root:p/ss@tcp(localhost:3306)/db", "root", "p/ss"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, pass := s.parseUserPassword(tt.dsn)
			if user != tt.wantUser || pass != tt.wantPass {
				t.Errorf("parseUserPassword(%q) = %q/%q, want %q/%q",
					tt.dsn, user, pass, tt.wantUser, tt.wantPass)
			}
		})
	}
}

// TestTheAddressSurvivesSpecialCharacters is the same guarantee for the host
// and port, which were recovered from the same split.
func TestTheAddressSurvivesSpecialCharacters(t *testing.T) {
	s := newSyncer(t)

	if got := s.parseAddr("root:p@ss@tcp(db.internal:3306)/shop"); got != "db.internal:3306" {
		t.Errorf("parseAddr = %q, want db.internal:3306", got)
	}
}

// TestAnEmptyDSNIsNotTheDriverDefault records the guard in front of the parser.
// The driver reads an empty DSN as its own defaults, which would have canal
// dial 127.0.0.1:3306 rather than report that nothing was configured.
func TestAnEmptyDSNIsNotTheDriverDefault(t *testing.T) {
	if got := newSyncer(t).parseAddr(""); got != "" {
		t.Errorf("parseAddr(\"\") = %q, want the empty string", got)
	}
}

func TestMakeQuestionMarks(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, ""},
		{1, "?"},
		{3, "?,?,?"},
	}

	for _, tt := range tests {
		got := makeQuestionMarks(tt.n)
		if len(got) != tt.n {
			t.Errorf("makeQuestionMarks(%d) has %d elements, want %d", tt.n, len(got), tt.n)
		}
		if joined := strings.Join(got, ","); joined != tt.want {
			t.Errorf("makeQuestionMarks(%d) = %q, want %q", tt.n, joined, tt.want)
		}
	}
}

func TestLoadBinlogPosition(t *testing.T) {
	s := newSyncer(t)
	dir := t.TempDir()

	t.Run("round trips a saved position", func(t *testing.T) {
		path := filepath.Join(dir, "binlog.pos")
		want := mysql.Position{Name: "mysql-bin.000042", Pos: 1234}
		data, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}

		got := s.loadBinlogPosition(path)
		if got == nil {
			t.Fatal("loadBinlogPosition returned nil for a valid file")
		}
		if got.Name != want.Name || got.Pos != want.Pos {
			t.Errorf("position = %v, want %v", *got, want)
		}
	})

	t.Run("missing file yields nil", func(t *testing.T) {
		if got := s.loadBinlogPosition(filepath.Join(dir, "absent.pos")); got != nil {
			t.Errorf("loadBinlogPosition = %v, want nil", *got)
		}
	})

	t.Run("empty file yields nil", func(t *testing.T) {
		path := filepath.Join(dir, "empty.pos")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if got := s.loadBinlogPosition(path); got != nil {
			t.Errorf("loadBinlogPosition = %v, want nil", *got)
		}
	})

	t.Run("malformed file yields nil", func(t *testing.T) {
		path := filepath.Join(dir, "bad.pos")
		if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		// A corrupt position file is indistinguishable from a missing one, so
		// the syncer silently restarts from the master's current coordinates
		// and everything written in between is skipped.
		if got := s.loadBinlogPosition(path); got != nil {
			t.Errorf("loadBinlogPosition = %v, want nil", *got)
		}
	})

	t.Run("a missing directory is not an error", func(t *testing.T) {
		// Reading no longer creates the directory it was going to read from.
		// The saver creates it when it writes, which is the only moment it
		// needs to exist.
		path := filepath.Join(dir, "nested", "deep", "binlog.pos")
		if got := s.loadBinlogPosition(path); got != nil {
			t.Errorf("loadBinlogPosition = %v for a path that does not exist", *got)
		}
	})
}
