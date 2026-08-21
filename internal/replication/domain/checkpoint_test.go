package domain

import "testing"

func TestCheckpointsOfReadsEveryPath(t *testing.T) {
	cp := CheckpointsOf(Config{
		PgPositionPath:         "/pg",
		MysqlPositionPath:      "/mysql",
		MongodbResumeTokenPath: "/mongo",
		RedisPositionPath:      "/redis",
	})

	if cp.Postgres != "/pg" || cp.MySQL != "/mysql" || cp.MongoDB != "/mongo" || cp.Redis != "/redis" {
		t.Errorf("CheckpointsOf = %+v", cp)
	}
}

func TestCheckpointsFor(t *testing.T) {
	cp := Checkpoints{Postgres: "/pg", MySQL: "/mysql", MongoDB: "/mongo", Redis: "/redis"}

	for _, tt := range []struct {
		engine string
		want   string
	}{
		{"postgresql", "/pg"},
		{"PostgreSQL", "/pg"},
		{"mysql", "/mysql"},
		{"mariadb", "/mysql"},
		{"MariaDB", "/mysql"},
		{"mongodb", "/mongo"},
		{"MONGODB", "/mongo"},
		{"redis", "/redis"},
		{"", ""},
		{"cassandra", ""},
		{"postgres", ""},
	} {
		t.Run(tt.engine, func(t *testing.T) {
			if got := cp.For(tt.engine); got != tt.want {
				t.Errorf("For(%q) = %q, want %q", tt.engine, got, tt.want)
			}
		})
	}
}

// TestMariaDBSharesMySQLsCheckpoint records that a MariaDB task and a MySQL
// task read the same field. Two tasks of the two engines configured with one
// position path each will therefore be handed the same file, and neither
// notices.
func TestMariaDBSharesMySQLsCheckpoint(t *testing.T) {
	cp := Checkpoints{MySQL: "/shared"}

	if cp.For("mysql") != cp.For("mariadb") {
		t.Fatal("mysql and mariadb now read different checkpoint fields; assert the new mapping")
	}
}

// TestAnUnconfiguredCheckpointIsIndistinguishableFromAnUnknownEngine records
// that For answers with the empty string both when the engine has no path
// configured and when the engine is not one it knows. A caller cannot tell a
// missing configuration from a typo in the engine name.
func TestAnUnconfiguredCheckpointIsIndistinguishableFromAnUnknownEngine(t *testing.T) {
	empty := Checkpoints{}

	if empty.For("mongodb") != "" {
		t.Fatal("an unconfigured path no longer answers empty")
	}
	if (Checkpoints{MongoDB: "/mongo"}).For("cassandra") != "" {
		t.Fatal("an unknown engine no longer answers empty")
	}
}
