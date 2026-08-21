package dsn

import (
	"fmt"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

func buildDSNByType(dbType string, c map[string]string) string {
	if c == nil {
		return ""
	}
	switch strings.ToLower(dbType) {
	case "mysql", "mariadb":
		// MySQL => user:password@tcp(host:port)/database
		// e.g. root:root@tcp(localhost:3306)/source_db
		user := c["user"]
		pwd := c["password"]
		host := c["host"]
		port := c["port"]
		dbn := c["database"]
		return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pwd, host, port, dbn)

	case "postgresql":
		// PostgreSQL => postgres://user:pwd@host:port/db?sslmode=disable
		// e.g. "postgres://root:root@localhost:5432/source_db?sslmode=disable"
		user := c["user"]
		pwd := c["password"]
		host := c["host"]
		port := c["port"]
		dbn := c["database"]
		// ?sslmode=disable
		return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, pwd, host, port, dbn)

	case "mongodb":
		// MongoDB => mongodb://username:password@host:port/database
		host := c["host"]
		port := c["port"]
		dbn := c["database"]
		user := c["user"]
		pass := c["password"]

		var uri string
		if user != "" && pass != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/%s", user, pass, host, port, dbn)
		} else {
			uri = fmt.Sprintf("mongodb://%s:%s/%s", host, port, dbn)
		}

		if strings.Contains(uri, "@") && !strings.Contains(uri, "authSource=") {
			uri += "?directConnection=true&authSource=admin"
		} else {
			uri += "?directConnection=true"
		}

		return uri

	case "redis":
		// Redis => redis://:pwd@host:port/db
		// e.g. "redis://:mypwd@localhost:6379/0"
		host := c["host"]
		port := c["port"]
		// user := c["user"]     // redis user rarely used
		pwd := c["password"] // e.g. :mypwd
		dbn := c["database"] // e.g. 0 or 1
		// user part in redis might not always exist, typically we do "redis://:pwd@host:port/db"
		// to keep it simple:
		if pwd != "" {
			return fmt.Sprintf("redis://:%s@%s:%s/%s", pwd, host, port, dbn)
		} else {
			return fmt.Sprintf("redis://%s:%s/%s", host, port, dbn)
		}

	default:
		// fallback => maybe user gave direct DSN
		return c["host"] // or something
	}
}

func BuildDSNByType(dbType string, c map[string]string) string {
	return buildDSNByType(dbType, c)
}
