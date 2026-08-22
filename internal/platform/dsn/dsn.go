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

// Endpoint describes a connection without its credentials, as host:port/database.
//
// It is what the direction lock records on each database to name the other
// side, and what error messages name, so neither can leak a password into a
// table an operator will read or a log line that will be shipped somewhere.
func Endpoint(dbType, connection string) string {
	host := extractHost(dbType, connection)
	database := GetDatabaseName(dbType, connection)
	if database == "" {
		return host
	}
	return host + "/" + database
}

// extractHost reports the host and port a DSN addresses.
func extractHost(dbType, connection string) string {
	// An empty DSN must not be described as an endpoint: the MySQL parser reads
	// one as its own defaults, which would have a metric label and an error
	// message name 127.0.0.1:3306 for a connection nobody configured.
	if connection == "" {
		return ""
	}
	switch strings.ToLower(dbType) {
	case "mysql", "mariadb":
		cfg, err := mysqldriver.ParseDSN(connection)
		if err != nil {
			return ""
		}
		return cfg.Addr
	case "mongodb":
		lower := strings.ToLower(connection)
		for _, prefix := range []string{"mongodb+srv://", "mongodb://"} {
			if !strings.HasPrefix(lower, prefix) {
				continue
			}
			rest := connection[len(prefix):]
			if at := strings.LastIndex(rest, "@"); at != -1 {
				rest = rest[at+1:]
			}
			if slash := strings.Index(rest, "/"); slash != -1 {
				rest = rest[:slash]
			}
			return rest
		}
		return ""
	default:
		u, err := url.Parse(connection)
		if err != nil {
			return ""
		}
		return u.Host
	}
}
