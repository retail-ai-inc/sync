package dsn

import "testing"

func TestGetDatabaseName(t *testing.T) {
	tests := []struct {
		name   string
		dbType string
		dsn    string
		want   string
	}{
		// MySQL and MariaDB share one extractor.
		{"mysql", "mysql", "root:root@tcp(localhost:3306)/source_db", "source_db"},
		{"mysql with params", "mysql", "root:root@tcp(localhost:3306)/source_db?charset=utf8", "source_db"},
		{"mariadb", "mariadb", "root:root@tcp(localhost:3307)/source_db", "source_db"},
		{"type is case-insensitive", "MySQL", "root:root@tcp(localhost:3306)/source_db", "source_db"},

		{"postgresql", "postgresql", "postgres://root:root@localhost:5432/source_db?sslmode=disable", "source_db"},
		{"postgresql without params", "postgresql", "postgres://root:root@localhost:5432/source_db", "source_db"},

		{"mongodb", "mongodb", "mongodb://localhost:27017/source_db", "source_db"},
		{"mongodb with auth and params", "mongodb", "mongodb://root:root@localhost:27017/source_db?directConnection=true&authSource=admin", "source_db"},
		{"mongodb scheme is case-insensitive", "mongodb", "MongoDB://localhost:27017/source_db", "source_db"},

		{"redis", "redis", "redis://:secret@localhost:6379/0", "0"},
		{"redis without password", "redis", "redis://localhost:6379/1", "1"},

		{"unknown type", "elasticsearch", "http://localhost:9200/idx", ""},
		{"empty type", "", "anything", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetDatabaseName(tt.dbType, tt.dsn); got != tt.want {
				t.Errorf("GetDatabaseName(%q, %q) = %q, want %q", tt.dbType, tt.dsn, got, tt.want)
			}
		})
	}
}

func TestGetDatabaseNameMissingDatabase(t *testing.T) {
	// A DSN carrying no database name must yield an empty string rather than
	// garbage, because callers use the result to address a database directly.
	tests := []struct {
		name   string
		dbType string
		dsn    string
	}{
		{"mysql without slash", "mysql", "root:root@tcp(localhost:3306)"},
		{"mongodb without slash", "mongodb", "mongodb://localhost:27017"},
		{"mongodb trailing slash", "mongodb", "mongodb://localhost:27017/"},
		{"postgresql without path", "postgresql", "postgres://root:root@localhost:5432"},
		{"redis without path", "redis", "redis://localhost:6379"},
		{"empty dsn", "mysql", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetDatabaseName(tt.dbType, tt.dsn); got != "" {
				t.Errorf("GetDatabaseName(%q, %q) = %q, want empty", tt.dbType, tt.dsn, got)
			}
		})
	}
}

// TestExtractMySQLDatabaseSplitsOnFirstSlash records a defect rather than
// desired behaviour: the extractor takes the substring after the *first*
// slash, so any slash appearing earlier in the DSN — most plausibly inside a
// password — yields a wrong database name. A ConnectionEndpoint value object
// that never round-trips through a DSN string would remove the failure mode.
func TestExtractMySQLDatabaseSplitsOnFirstSlash(t *testing.T) {
	const dsn = "root:pa/ss@tcp(localhost:3306)/source_db"

	got := extractMySQLDatabase(dsn)
	if got == "source_db" {
		t.Fatalf("extractMySQLDatabase(%q) = %q; the defect this test documents "+
			"appears to be fixed — assert the correct value instead", dsn, got)
	}
	if want := "ss@tcp(localhost:3306)/source_db"; got != want {
		t.Errorf("extractMySQLDatabase(%q) = %q, want %q", dsn, got, want)
	}
}

// TestExtractMongoDatabaseRejectsSRVScheme documents that `mongodb+srv://`
// URIs yield no database name, since the extractor matches on the literal
// `mongodb://` prefix. Relevant to F-034: cluster targets need either an SRV
// URI or a replicaSet parameter, and neither is understood today.
func TestExtractMongoDatabaseRejectsSRVScheme(t *testing.T) {
	const dsn = "mongodb+srv://root:root@cluster.example.com/source_db"

	if got := extractMongoDatabase(dsn); got != "" {
		t.Errorf("extractMongoDatabase(%q) = %q; SRV support may have landed, "+
			"in which case assert the database name instead", dsn, got)
	}
}
