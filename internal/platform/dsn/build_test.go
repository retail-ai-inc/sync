package dsn

import (
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func conn(user, password, host, port, database string) map[string]string {
	return map[string]string{
		"user":     user,
		"password": password,
		"host":     host,
		"port":     port,
		"database": database,
	}
}

func TestBuildDSNByType(t *testing.T) {
	tests := []struct {
		name   string
		dbType string
		conn   map[string]string
		want   string
	}{
		{
			"mysql",
			"mysql",
			conn("root", "root", "localhost", "3306", "source_db"),
			"root:root@tcp(localhost:3306)/source_db",
		},
		{
			"mariadb uses the mysql form",
			"mariadb",
			conn("root", "root", "localhost", "3307", "source_db"),
			"root:root@tcp(localhost:3307)/source_db",
		},
		{
			"type is case-insensitive",
			"MySQL",
			conn("root", "root", "localhost", "3306", "source_db"),
			"root:root@tcp(localhost:3306)/source_db",
		},
		{
			"postgresql",
			"postgresql",
			conn("root", "root", "localhost", "5432", "source_db"),
			"postgres://root:root@localhost:5432/source_db?sslmode=disable",
		},
		{
			"mongodb with credentials",
			"mongodb",
			conn("root", "root", "localhost", "27017", "source_db"),
			"mongodb://root:root@localhost:27017/source_db?directConnection=true&authSource=admin",
		},
		{
			"mongodb without credentials",
			"mongodb",
			conn("", "", "localhost", "27017", "source_db"),
			"mongodb://localhost:27017/source_db?directConnection=true",
		},
		{
			"redis with password",
			"redis",
			conn("", "secret", "localhost", "6379", "0"),
			"redis://:secret@localhost:6379/0",
		},
		{
			"redis without password",
			"redis",
			conn("", "", "localhost", "6379", "0"),
			"redis://localhost:6379/0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildDSNByType(tt.dbType, tt.conn); got != tt.want {
				t.Errorf("buildDSNByType(%q) =\n  %q\nwant\n  %q", tt.dbType, got, tt.want)
			}
		})
	}
}

func TestBuildDSNByTypeNilAndUnknown(t *testing.T) {
	if got := buildDSNByType("mysql", nil); got != "" {
		t.Errorf("buildDSNByType with nil map = %q, want empty", got)
	}

	// The default branch falls back to the host field on the assumption that
	// the caller supplied a ready-made DSN there.
	c := conn("root", "root", "elasticsearch://localhost:9200", "", "")
	if got, want := buildDSNByType("elasticsearch", c), "elasticsearch://localhost:9200"; got != want {
		t.Errorf("buildDSNByType with unknown type = %q, want %q", got, want)
	}
}

// TestBuildDSNRoundTripsDatabaseName pins the pairing between DSN
// construction here and DSN parsing in pkg/syncer/common: the database name
// put in must come back out. The two directions live in different packages
// with no shared type, so nothing but this test keeps them aligned.
func TestBuildDSNRoundTripsDatabaseName(t *testing.T) {
	tests := []struct {
		dbType   string
		port     string
		database string
	}{
		{"mysql", "3306", "source_db"},
		{"mariadb", "3307", "source_db"},
		{"postgresql", "5432", "source_db"},
		{"mongodb", "27017", "source_db"},
		{"redis", "6379", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			built := buildDSNByType(tt.dbType, conn("root", "root", "localhost", tt.port, tt.database))
			if got := GetDatabaseName(tt.dbType, built); got != tt.database {
				t.Errorf("round trip for %s: built %q, GetDatabaseName returned %q, want %q",
					tt.dbType, built, got, tt.database)
			}
		})
	}
}

// TestBuildDSNByTypeOmitsTLS records that no engine gets transport encryption:
// MySQL has no tls parameter, PostgreSQL hardcodes sslmode=disable, MongoDB
// has no tls option, and Redis uses the plaintext redis:// scheme. Every one
// of these has to change before Tokyo -> Osaka replication carries data across
// regions (F-011, F-049, F-069, F-090).
func TestBuildDSNByTypeOmitsTLS(t *testing.T) {
	if got := buildDSNByType("postgresql", conn("root", "root", "localhost", "5432", "source_db")); !strings.Contains(got, "sslmode=disable") {
		t.Errorf("postgresql DSN = %q; sslmode=disable appears to be gone, "+
			"so update this test to assert the new behaviour", got)
	}
	if got := buildDSNByType("mysql", conn("root", "root", "localhost", "3306", "source_db")); strings.Contains(got, "tls") {
		t.Errorf("mysql DSN = %q; a tls parameter appeared, update this test", got)
	}
	if got := buildDSNByType("redis", conn("", "secret", "localhost", "6379", "0")); !strings.Contains(got, "redis://") || strings.Contains(got, "rediss://") {
		t.Errorf("redis DSN = %q; expected the plaintext redis:// scheme", got)
	}
}

// TestBuildMongoDSNForcesDirectConnection records F-034. directConnection=true
// disables replica-set topology discovery, so the driver stays pinned to one
// node and stops writing after an election — unusable against the Osaka
// cluster. It is appended unconditionally, with no way to opt out.
func TestBuildMongoDSNForcesDirectConnection(t *testing.T) {
	for _, c := range []map[string]string{
		conn("root", "root", "localhost", "27017", "source_db"),
		conn("", "", "localhost", "27017", "source_db"),
	} {
		got := buildDSNByType("mongodb", c)
		if !strings.Contains(got, "directConnection=true") {
			t.Errorf("mongodb DSN = %q; directConnection is no longer forced, "+
				"so F-034 may be fixed — assert the new behaviour instead", got)
		}
	}
}

// TestBuildMongoDSNDropsUserWithoutPassword records a defect: credentials are
// only embedded when both user and password are non-empty, so a task
// configured with a user but no password connects anonymously. The failure
// surfaces later as an authentication error that does not mention the
// discarded user.
func TestBuildMongoDSNDropsUserWithoutPassword(t *testing.T) {
	got := buildDSNByType("mongodb", conn("root", "", "localhost", "27017", "source_db"))

	if strings.Contains(got, "root") {
		t.Fatalf("mongodb DSN = %q; the user is no longer dropped, "+
			"so this defect appears fixed — assert the correct value instead", got)
	}
	if want := "mongodb://localhost:27017/source_db?directConnection=true"; got != want {
		t.Errorf("mongodb DSN = %q, want %q", got, want)
	}
}

// TestBuildDSNByTypeIsExportedUnchanged guards the exported wrapper, which is
// what other packages call.
func TestBuildDSNByTypeIsExportedUnchanged(t *testing.T) {
	c := conn("root", "root", "localhost", "3306", "source_db")
	if got, want := BuildDSNByType("mysql", c), buildDSNByType("mysql", c); got != want {
		t.Errorf("BuildDSNByType = %q, buildDSNByType = %q; they must agree", got, want)
	}
}
