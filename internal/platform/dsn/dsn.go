package dsn

import (
	"net/url"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

func GetDatabaseName(dbType, dsn string) string {
	switch strings.ToLower(dbType) {
	case "mysql", "mariadb":
		return extractMySQLDatabase(dsn)
	case "postgresql":
		return extractPostgresDatabase(dsn)
	case "mongodb":
		return extractMongoDatabase(dsn)
	case "redis":
		return extractRedisDatabase(dsn)
	default:
		return ""
	}
}

// extractMySQLDatabase reports the database a MySQL DSN addresses.
//
// The driver's own parser is used rather than splitting on the first slash: a
// slash is legal inside a password, and splitting on it silently returned part
// of the credentials as the database name.
func extractMySQLDatabase(dsn string) string {
	if dsn == "" {
		return ""
	}
	cfg, err := mysqldriver.ParseDSN(dsn)
	if err != nil {
		return ""
	}
	return cfg.DBName
}

func extractPostgresDatabase(dsn string) string {
	//DSN: postgres://user:pass@localhost:5432/mydb?sslmode=disable
	u, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	path := u.Path
	if len(path) > 1 {
		return path[1:]
	}
	return ""
}

// extractMongoDatabase reports the database a MongoDB URI addresses, for both
// the plain scheme and mongodb+srv, which is how a cluster names its seed list.
func extractMongoDatabase(dsn string) string {
	lower := strings.ToLower(dsn)
	var prefix string
	switch {
	case strings.HasPrefix(lower, "mongodb+srv://"):
		prefix = "mongodb+srv://"
	case strings.HasPrefix(lower, "mongodb://"):
		prefix = "mongodb://"
	default:
		return ""
	}

	withoutPrefix := dsn[len(prefix):]
	slashIndex := strings.Index(withoutPrefix, "/")
	if slashIndex == -1 {
		return ""
	}
	remainder := withoutPrefix[slashIndex+1:]
	questionIndex := strings.Index(remainder, "?")
	if questionIndex != -1 {
		return remainder[:questionIndex]
	}
	return remainder
}

func extractRedisDatabase(dsn string) string {
	// DSN: redis://:pass@localhost:6379/0
	u, err := url.Parse(dsn)
	if err != nil {
		return ""
	}
	path := u.Path
	if len(path) > 1 {
		return path[1:]
	}
	return ""
}
