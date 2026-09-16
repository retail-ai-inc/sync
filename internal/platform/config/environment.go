package config

import (
	"os"
	"sort"
	"strings"
)

// The variables this process reads, and what to do about one it does not.
//
// A knob that does nothing is worse than a knob that is missing: an operator
// sets it, sees no error, and believes the deployment is configured the way the
// documentation says. Four of them were documented and read by no code at all,
// including one for how often MySQL records its position -- somebody could have
// set it during an incident and watched nothing change.

// knownVariables is every SYNC_-prefixed variable this process reads. Test-only
// ones are here too: a developer who mistypes one should be told as well.
var knownVariables = map[string]bool{
	"SYNC_ADMIN_PASSWORD":       true,
	"SYNC_CONFIG_KEY":           true,
	"SYNC_DB_ALLOW_CREATE":      true,
	"SYNC_DB_PATH":              true,
	"SYNC_FIELD_KEY":            true,
	"SYNC_HTTP_ADDR":            true,
	"SYNC_INSTANCE":             true,
	"SYNC_LAG_ALERT_SECONDS":    true,
	"SYNC_MONGO_NO_TRANSACTION": true,
	"SYNC_PASSWORD_ITERATIONS":  true,
	"SYNC_TOKEN_SECRET":         true,
	"SYNC_VERIFY_INTERVAL":      true,
	"SYNC_VERIFY_REPAIR":        true,

	// The test harness and the tagged suites.
	"SYNC_PERF_BURST":           true,
	"SYNC_PERF_RATE":            true,
	"SYNC_PERF_SAMPLE_EVERY":    true,
	"SYNC_PERF_SECONDS":         true,
	"SYNC_REDIS_ALLOW_FLUSH":    true,
	"SYNC_REDIS_SOURCE":         true,
	"SYNC_REDIS_SOURCE_CLUSTER": true,
	"SYNC_REDIS_TARGET":         true,
	"SYNC_REDIS_TARGET_CLUSTER": true,
	"SYNC_STG_ALLOW_FAILOVER":   true,
	"SYNC_STG_LOG":              true,
	"SYNC_STG_MONGO":            true,
	"SYNC_STG_PASS":             true,
	"SYNC_STG_RATE":             true,
	"SYNC_STG_SAMPLE_EVERY":     true,
	"SYNC_STG_SECONDS":          true,
	"SYNC_STG_SHARD_MEMBERS":    true,
	"SYNC_STG_SOURCE_DB":        true,
	"SYNC_STG_TARGET_DB":        true,
	"SYNC_STG_USER":             true,
	"SYNC_TEST_MONGO_SOURCE":    true,
	"SYNC_TEST_MONGO_TARGET":    true,
	"SYNC_TEST_MYSQL_SOURCE":    true,
	"SYNC_TEST_MYSQL_TARGET":    true,
	"SYNC_TEST_MARIADB_SOURCE":  true,
	"SYNC_TEST_MARIADB_TARGET":  true,
	"SYNC_TEST_POSTGRES_SOURCE": true,
	"SYNC_TEST_POSTGRES_TARGET": true,
	"SYNC_TEST_REDIS_SOURCE":    true,
	"SYNC_TEST_REDIS_TARGET":    true,
}

// UnknownVariables lists the SYNC_-prefixed variables in the environment that
// nothing reads, so a deployment can be told rather than left believing a
// setting took effect.
func UnknownVariables(environ []string) []string {
	var unknown []string
	for _, entry := range environ {
		name, _, found := strings.Cut(entry, "=")
		if !found || !strings.HasPrefix(name, "SYNC_") {
			continue
		}
		if !knownVariables[name] {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	return unknown
}

// EnvironmentFromOS is UnknownVariables over this process's environment.
func EnvironmentFromOS() []string { return UnknownVariables(os.Environ()) }
