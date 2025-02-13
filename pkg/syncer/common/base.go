package common

import (
	"net/url"
	"strings"
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

func extractMySQLDatabase(dsn string) string {
	// dsn: user:pass@tcp(localhost:3306)/mydb?charset=utf8
	slashIndex := strings.Index(dsn, "/")
	if slashIndex == -1 {
		return ""
	}
	remainder := dsn[slashIndex+1:]
	questionIndex := strings.Index(remainder, "?")
	if questionIndex != -1 {
		return remainder[:questionIndex]
	}
	return remainder
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

func extractMongoDatabase(dsn string) string {
	// DSN: mongodb://host:port/mydb?xxx=yyy
	const prefix = "mongodb://"
	if !strings.HasPrefix(strings.ToLower(dsn), prefix) {
		return ""
	}
	withoutPrefix := dsn[len(prefix):]
	slashIndex := strings.Index(withoutPrefix, "/")
	if slashIndex == -1 {
		return ""
	}
	remainder := withoutPrefix[slashIndex+1:]
	questIndex := strings.Index(remainder, "?")
	if questIndex != -1 {
		return remainder[:questIndex]
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
