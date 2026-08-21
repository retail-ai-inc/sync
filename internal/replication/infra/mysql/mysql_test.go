package mysql

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

func newSyncer(t *testing.T) *MySQLSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	return &MySQLSyncer{logger: logger}
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
		{"user without password", "root@tcp(localhost:3306)/db", "", ""},
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

// TestParseUserPasswordTruncatesOnSpecialCharacters records that credentials
// are recovered by splitting the DSN on "@" and then on ":", with no awareness
// that either character is legal inside a password. A password containing one
// is silently truncated, and the syncer then fails to authenticate with an
// error that says nothing about the mangled credential.
//
// The DSN is assembled by config.buildDSNByType from the values an operator
// typed into the UI, so nothing rejects such a password earlier either.
func TestParseUserPasswordTruncatesOnSpecialCharacters(t *testing.T) {
	s := newSyncer(t)

	tests := []struct {
		name           string
		dsn            string
		wantUser, want string
	}{
		{
			"at sign in the password",
			"root:p@ss@tcp(localhost:3306)/db",
			"root", "p", // want root/p@ss
		},
		{
			"colon in the password",
			"root:pa:ss@tcp(localhost:3306)/db",
			"root", "pa", // want root/pa:ss
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, pass := s.parseUserPassword(tt.dsn)
			if pass == "p@ss" || pass == "pa:ss" {
				t.Fatalf("the password now survives (%q); parsing may have been "+
					"fixed, so assert the correct value instead", pass)
			}
			if user != tt.wantUser || pass != tt.want {
				t.Errorf("parseUserPassword(%q) = %q/%q, want %q/%q",
					tt.dsn, user, pass, tt.wantUser, tt.want)
			}
		})
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

	t.Run("creates the parent directory", func(t *testing.T) {
		path := filepath.Join(dir, "nested", "deep", "binlog.pos")
		s.loadBinlogPosition(path)
		if _, err := os.Stat(filepath.Dir(path)); err != nil {
			t.Errorf("the parent directory was not created: %v", err)
		}
	})
}
