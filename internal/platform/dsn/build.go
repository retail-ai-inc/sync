package dsn

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Connection map keys the UI and the stored configuration may set beyond the
// five basics. They are optional: a configuration that sets none of them keeps
// working, and gets the safest default the engine allows without a server-side
// change.
const (
	// KeyTLS turns transport encryption on. "true" requires a verified
	// certificate, "skip-verify" encrypts without checking the certificate, and
	// "false" turns it off.
	KeyTLS = "tls"
	// KeyReplicaSet names the MongoDB replica set, which is what makes the
	// driver discover the topology and follow an election.
	KeyReplicaSet = "replicaSet"
	// KeySRV asks for the mongodb+srv:// scheme, where the seed list comes from
	// DNS rather than the configuration.
	KeySRV = "srv"
	// KeyAuthSource names the MongoDB database the credentials live in.
	KeyAuthSource = "authSource"
	// KeyDirect pins the MongoDB driver to a single node. It disables topology
	// discovery, so it is never the default: a driver pinned to one node stops
	// writing after an election instead of following the new primary.
	KeyDirect = "directConnection"
	// KeySSLMode is PostgreSQL's own spelling, honoured when it is set so an
	// operator can ask for verify-full.
	KeySSLMode = "sslmode"
)

// tlsSetting reports how the connection map asks for transport encryption:
// "", "true", "false" or "skip-verify".
func tlsSetting(c map[string]string) string {
	return strings.ToLower(strings.TrimSpace(c[KeyTLS]))
}

func tlsWanted(c map[string]string) bool {
	switch tlsSetting(c) {
	case "true", "1", "yes", "require", "skip-verify":
		return true
	}
	return false
}

func tlsSkipsVerification(c map[string]string) bool {
	return tlsSetting(c) == "skip-verify"
}

// query renders parameters in a stable order, so the same configuration always
// produces the same DSN. Config change detection compares the built strings.
func query(params map[string]string) string {
	if len(params) == 0 {
		return ""
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+params[k])
	}
	return "?" + strings.Join(parts, "&")
}

// hostPort joins a host and port, tolerating a host that already carries one
// and a comma-separated seed list, which is how a replica set is named.
func hostPort(host, port string) string {
	host = strings.TrimSpace(host)
	if port == "" {
		return host
	}
	seeds := strings.Split(host, ",")
	for i, seed := range seeds {
		seed = strings.TrimSpace(seed)
		if seed != "" && !strings.Contains(seed, ":") {
			seed += ":" + port
		}
		seeds[i] = seed
	}
	return strings.Join(seeds, ",")
}

func buildDSNByType(dbType string, c map[string]string) string {
	if c == nil {
		return ""
	}
	switch strings.ToLower(dbType) {
	case "mysql", "mariadb":
		return buildMySQLDSN(c)
	case "postgresql":
		return buildPostgresDSN(c)
	case "mongodb":
		return buildMongoDSN(c)
	case "redis":
		return buildRedisDSN(c)
	default:
		// fallback => maybe user gave direct DSN
		return c["host"]
	}
}

// buildMySQLDSN renders user:password@tcp(host:port)/database.
//
// TLS defaults to "preferred": the driver uses it when the server offers it and
// falls back when it does not. That is strictly better than the plaintext it
// replaces and cannot break a server that has no certificate, while a
// deployment that must not fall back sets tls=true explicitly.
func buildMySQLDSN(c map[string]string) string {
	params := map[string]string{}
	switch {
	case tlsSetting(c) == "false":
		// Left off entirely, which is the driver's own default.
	case tlsSkipsVerification(c):
		params["tls"] = "skip-verify"
	case tlsWanted(c):
		params["tls"] = "true"
	default:
		params["tls"] = "preferred"
	}

	return fmt.Sprintf("%s:%s@tcp(%s)/%s%s",
		c["user"], c["password"], hostPort(c["host"], c["port"]), c["database"], query(params))
}

// buildPostgresDSN renders postgres://user:password@host:port/database.
//
// sslmode defaults to "prefer" rather than the "disable" it replaces: the
// server's certificate is used when there is one. An operator who needs the
// connection to fail rather than fall back sets sslmode=require or verify-full.
func buildPostgresDSN(c map[string]string) string {
	mode := c[KeySSLMode]
	if mode == "" {
		switch {
		case tlsSkipsVerification(c):
			mode = "require"
		case tlsWanted(c):
			mode = "verify-full"
		case tlsSetting(c) == "false":
			mode = "disable"
		default:
			mode = "prefer"
		}
	}

	u := url.URL{
		Scheme:   "postgres",
		Host:     hostPort(c["host"], c["port"]),
		Path:     "/" + c["database"],
		RawQuery: "sslmode=" + mode,
	}
	if user := c["user"]; user != "" {
		u.User = url.UserPassword(user, c["password"])
	}
	return u.String()
}

// buildMongoDSN renders a MongoDB URI.
//
// directConnection is no longer forced. It pins the driver to one node, which
// means it neither discovers the rest of the replica set nor follows an
// election — against a cluster it stops writing the moment the primary changes.
// It is now set only when the configuration asks for it by name.
//
// The write concern is majority with journalling, so a write this syncer has
// acknowledged survives the failover the replica exists for. Without it a
// failover can roll back writes the target has already reported as applied,
// and the checkpoint has already moved past them.
func buildMongoDSN(c map[string]string) string {
	scheme := "mongodb"
	host := hostPort(c["host"], c["port"])
	if strings.EqualFold(c[KeySRV], "true") {
		// The seed list and the port come from DNS.
		scheme = "mongodb+srv"
		host = strings.TrimSpace(c["host"])
	}

	params := map[string]string{
		"w":       "majority",
		"journal": "true",
	}
	if rs := c[KeyReplicaSet]; rs != "" {
		params["replicaSet"] = url.QueryEscape(rs)
	}
	if strings.EqualFold(c[KeyDirect], "true") {
		params[KeyDirect] = "true"
	}
	if tlsWanted(c) {
		params["tls"] = "true"
		if tlsSkipsVerification(c) {
			params["tlsInsecure"] = "true"
		}
	}

	var credentials string
	if user := c["user"]; user != "" {
		credentials = url.QueryEscape(user)
		if password := c["password"]; password != "" {
			credentials += ":" + url.QueryEscape(password)
		}
		credentials += "@"

		source := c[KeyAuthSource]
		if source == "" {
			source = "admin"
		}
		params[KeyAuthSource] = url.QueryEscape(source)
	}

	return fmt.Sprintf("%s://%s%s/%s%s", scheme, credentials, host, c["database"], query(params))
}

// buildRedisDSN renders redis://:password@host:port/db, or rediss:// when the
// connection is encrypted.
func buildRedisDSN(c map[string]string) string {
	scheme := "redis"
	if tlsWanted(c) {
		scheme = "rediss"
	}

	u := url.URL{
		Scheme: scheme,
		Host:   hostPort(c["host"], c["port"]),
		Path:   "/" + c["database"],
	}
	// Redis 6 usernames are rare; the password alone is the common form.
	if password := c["password"]; password != "" {
		u.User = url.UserPassword(c["user"], password)
	} else if user := c["user"]; user != "" {
		u.User = url.User(user)
	}
	if tlsSkipsVerification(c) {
		u.RawQuery = "skip_verify=true"
	}
	return u.String()
}

func BuildDSNByType(dbType string, c map[string]string) string {
	return buildDSNByType(dbType, c)
}
