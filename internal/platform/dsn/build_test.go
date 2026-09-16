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

// with returns a copy of c carrying one extra option, so a test can name the
// one thing it is about.
func with(c map[string]string, key, value string) map[string]string {
	out := map[string]string{}
	for k, v := range c {
		out[k] = v
	}
	out[key] = value
	return out
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
			"root:root@tcp(localhost:3306)/source_db?tls=preferred",
		},
		{
			"mariadb uses the mysql form",
			"mariadb",
			conn("root", "root", "localhost", "3307", "source_db"),
			"root:root@tcp(localhost:3307)/source_db?tls=preferred",
		},
		{
			"type is case-insensitive",
			"MySQL",
			conn("root", "root", "localhost", "3306", "source_db"),
			"root:root@tcp(localhost:3306)/source_db?tls=preferred",
		},
		{
			"postgresql",
			"postgresql",
			conn("root", "root", "localhost", "5432", "source_db"),
			"postgres://root:root@localhost:5432/source_db?sslmode=require",
		},
		{
			"mongodb with credentials",
			"mongodb",
			conn("root", "root", "localhost", "27017", "source_db"),
			"mongodb://root:root@localhost:27017/source_db?authSource=admin&journal=true&w=majority",
		},
		{
			"mongodb without credentials",
			"mongodb",
			conn("", "", "localhost", "27017", "source_db"),
			"mongodb://localhost:27017/source_db?journal=true&w=majority",
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

// TestBuildDSNRoundTripsDatabaseName pins the pairing between DSN construction
// here and DSN parsing in the same package: the database name put in must come
// back out.
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

// TestTheDefaultIsEncryptionWhereItCannotBreakAnything records the choice made
// for MySQL, whose driver can negotiate: "preferred" uses a server that offers
// a certificate and still connects to one that does not.
//
// PostgreSQL cannot have that default. libpq spells it "prefer", and the Go
// driver in use here does not implement it -- it refuses the DSN with
// "unsupported sslmode", so a task configured through the interface could not
// connect at all. The default is "require" instead: encrypted or not at all,
// which for payment data is the right way round, and a server without TLS is
// reached by setting sslmode=disable deliberately.
func TestTheDefaultIsEncryptionWhereItCannotBreakAnything(t *testing.T) {
	if got := buildDSNByType("mysql", conn("root", "root", "h", "3306", "db")); !strings.Contains(got, "tls=preferred") {
		t.Errorf("mysql DSN = %q, want tls=preferred", got)
	}
	if got := buildDSNByType("postgresql", conn("root", "root", "h", "5432", "db")); !strings.Contains(got, "sslmode=require") {
		t.Errorf("postgresql DSN = %q, want sslmode=require", got)
	}
}

// TestTheDefaultPostgresModeIsOneTheDriverAccepts is the reason for the mode
// above: lib/pq rejects the DSN outright rather than falling back, so a default
// it does not implement is a task that never connects.
func TestTheDefaultPostgresModeIsOneTheDriverAccepts(t *testing.T) {
	got := buildDSNByType("postgresql", conn("root", "root", "h", "5432", "db"))

	// The four lib/pq accepts, from its own error message.
	accepted := []string{"sslmode=require", "sslmode=verify-full", "sslmode=verify-ca", "sslmode=disable"}
	for _, mode := range accepted {
		if strings.Contains(got, mode) {
			return
		}
	}
	t.Errorf("postgresql DSN = %q, want one of %v", got, accepted)
}

func TestTLSCanBeRequired(t *testing.T) {
	tests := []struct {
		dbType string
		want   string
	}{
		{"mysql", "tls=true"},
		{"postgresql", "sslmode=verify-full"},
		{"mongodb", "tls=true"},
		{"redis", "rediss://"},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			got := buildDSNByType(tt.dbType, with(conn("root", "root", "h", "1", "db"), KeyTLS, "true"))
			if !strings.Contains(got, tt.want) {
				t.Errorf("%s DSN = %q, want it to contain %q", tt.dbType, got, tt.want)
			}
		})
	}
}

// TestTLSCanSkipVerification covers a private certificate authority the syncer
// has no root for, which is the common shape inside a cluster.
func TestTLSCanSkipVerification(t *testing.T) {
	c := with(conn("root", "root", "h", "1", "db"), KeyTLS, "skip-verify")

	if got := buildDSNByType("mysql", c); !strings.Contains(got, "tls=skip-verify") {
		t.Errorf("mysql DSN = %q", got)
	}
	if got := buildDSNByType("mongodb", c); !strings.Contains(got, "tlsInsecure=true") {
		t.Errorf("mongodb DSN = %q", got)
	}
	if got := buildDSNByType("redis", c); !strings.Contains(got, "skip_verify=true") {
		t.Errorf("redis DSN = %q", got)
	}
	if got := buildDSNByType("postgresql", c); !strings.Contains(got, "sslmode=require") {
		t.Errorf("postgresql DSN = %q, want require, which encrypts without verifying", got)
	}
}

func TestTLSCanBeTurnedOff(t *testing.T) {
	c := with(conn("root", "root", "h", "1", "db"), KeyTLS, "false")

	if got := buildDSNByType("mysql", c); strings.Contains(got, "tls=") {
		t.Errorf("mysql DSN = %q, want no tls parameter", got)
	}
	if got := buildDSNByType("postgresql", c); !strings.Contains(got, "sslmode=disable") {
		t.Errorf("postgresql DSN = %q", got)
	}
	if got := buildDSNByType("redis", c); !strings.HasPrefix(got, "redis://") {
		t.Errorf("redis DSN = %q, want the plaintext scheme", got)
	}
}

// TestAnExplicitSSLModeWins records that PostgreSQL's own spelling is honoured
// as written, so an operator can ask for a mode the tls key does not name.
func TestAnExplicitSSLModeWins(t *testing.T) {
	c := with(with(conn("root", "root", "h", "5432", "db"), KeyTLS, "true"), KeySSLMode, "verify-ca")

	if got := buildDSNByType("postgresql", c); !strings.Contains(got, "sslmode=verify-ca") {
		t.Errorf("postgresql DSN = %q", got)
	}
}

// Pinning the driver to one node disables topology discovery, so it neither
// finds the rest of the replica set nor follows an election: against the Osaka
// cluster it would stop writing the moment the primary changed.
func TestDirectConnectionIsNoLongerForced(t *testing.T) {
	for _, c := range []map[string]string{
		conn("root", "root", "localhost", "27017", "source_db"),
		conn("", "", "localhost", "27017", "source_db"),
	} {
		if got := buildDSNByType("mongodb", c); strings.Contains(got, "directConnection") {
			t.Errorf("mongodb DSN = %q; directConnection is still forced", got)
		}
	}
}

// TestDirectConnectionCanStillBeAskedFor keeps the single-node case reachable,
// which is what a local development server needs.
func TestDirectConnectionCanStillBeAskedFor(t *testing.T) {
	c := with(conn("", "", "localhost", "27017", "db"), KeyDirect, "true")

	if got := buildDSNByType("mongodb", c); !strings.Contains(got, "directConnection=true") {
		t.Errorf("mongodb DSN = %q", got)
	}
}

// TestTheWriteConcernIsMajority pins what makes an acknowledged write survive
// the failover the replica exists for.
func TestTheWriteConcernIsMajority(t *testing.T) {
	got := buildDSNByType("mongodb", conn("", "", "h", "27017", "db"))

	for _, want := range []string{"w=majority", "journal=true"} {
		if !strings.Contains(got, want) {
			t.Errorf("mongodb DSN = %q, want it to contain %q", got, want)
		}
	}
}

func TestTheReplicaSetIsNamedWhenConfigured(t *testing.T) {
	c := with(conn("", "", "a:27017,b:27017,c:27017", "", "db"), KeyReplicaSet, "rs0")

	got := buildDSNByType("mongodb", c)
	if !strings.Contains(got, "replicaSet=rs0") {
		t.Errorf("mongodb DSN = %q, want the replica set named", got)
	}
	if !strings.Contains(got, "a:27017,b:27017,c:27017") {
		t.Errorf("mongodb DSN = %q, want the whole seed list", got)
	}
}

// TestASeedListGetsTheSharedPort covers the common configuration where the
// hosts are listed without ports and one port applies to all of them.
func TestASeedListGetsTheSharedPort(t *testing.T) {
	got := buildDSNByType("mongodb", conn("", "", "a,b,c", "27017", "db"))

	if !strings.Contains(got, "a:27017,b:27017,c:27017") {
		t.Errorf("mongodb DSN = %q", got)
	}
}

func TestTheSRVSchemeDropsThePort(t *testing.T) {
	c := with(conn("root", "root", "cluster.example.net", "27017", "db"), KeySRV, "true")

	got := buildDSNByType("mongodb", c)
	if !strings.HasPrefix(got, "mongodb+srv://") {
		t.Errorf("mongodb DSN = %q, want the srv scheme", got)
	}
	if strings.Contains(got, ":27017") {
		t.Errorf("mongodb DSN = %q; an srv URI must carry no port", got)
	}
}

// TestAUserWithNoPasswordIsKept fixes the defect where credentials were only
// embedded when both halves were set, so a task configured with a user and no
// password connected anonymously and failed later with an error that did not
// mention the discarded user.
func TestAUserWithNoPasswordIsKept(t *testing.T) {
	got := buildDSNByType("mongodb", conn("root", "", "localhost", "27017", "source_db"))

	if !strings.Contains(got, "root@") {
		t.Errorf("mongodb DSN = %q, want the user kept", got)
	}
	if !strings.Contains(got, "authSource=admin") {
		t.Errorf("mongodb DSN = %q, want an auth source", got)
	}
}

func TestTheAuthSourceCanBeChosen(t *testing.T) {
	c := with(conn("root", "root", "h", "27017", "db"), KeyAuthSource, "shop")

	if got := buildDSNByType("mongodb", c); !strings.Contains(got, "authSource=shop") {
		t.Errorf("mongodb DSN = %q", got)
	}
}

// TestCredentialsAreEscaped covers the passwords a managed service generates,
// which contain characters that would otherwise end the credential early or
// invent a query parameter.
func TestCredentialsAreEscaped(t *testing.T) {
	c := conn("ro@ot", "p@ss:word/x?y", "localhost", "27017", "db")

	got := buildDSNByType("mongodb", c)
	if strings.Contains(got, "p@ss") {
		t.Errorf("mongodb DSN = %q; the password is not escaped", got)
	}
	if strings.Count(got, "@") != 1 {
		t.Errorf("mongodb DSN = %q; the credential separator is ambiguous", got)
	}
}

// TestTheQueryOrderIsStable matters because config change detection compares
// the built strings: an unstable order would restart every task every ten
// seconds.
func TestTheQueryOrderIsStable(t *testing.T) {
	c := with(with(conn("root", "root", "h", "27017", "db"), KeyReplicaSet, "rs0"), KeyTLS, "true")

	first := buildDSNByType("mongodb", c)
	for i := 0; i < 20; i++ {
		if got := buildDSNByType("mongodb", c); got != first {
			t.Fatalf("the DSN changed between calls:\n  %q\n  %q", first, got)
		}
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
