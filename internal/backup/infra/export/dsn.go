package export

import (
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
)

// buildMongoDBConnectionString builds the URI mongodump reads through.
//
// It goes through the shared builder so a backup addresses a cluster the same
// way replication does. It used to force directConnection, which pins the
// driver to one node: against a replica set the backup would then read from
// whichever node it happened to reach, and fail outright once that node was no
// longer serving.
func buildMongoDBConnectionString(url, username, password string) string {
	return dsn.BuildDSNByType("mongodb", map[string]string{
		"user":     username,
		"password": password,
		"host":     url,
	})
}

// parseMySQLConnectionURL parses MySQL connection URL into components
// Format: host:port or just host
func parseMySQLConnectionURL(url string) (host, port, username, password string) {
	parts := strings.Split(url, ":")
	host = "localhost"
	port = "3306"

	if len(parts) >= 1 && parts[0] != "" {
		host = parts[0]
	}
	if len(parts) >= 2 && parts[1] != "" {
		port = parts[1]
	}

	return host, port, "", ""
}

// buildMySQLConnectionString builds MySQL connection parameters
func buildMySQLConnectionString(url, username, password string) (host, port, user, pass string) {
	host, port, _, _ = parseMySQLConnectionURL(url)
	user = username
	pass = password
	return
}

// maskMySQLPassword masks MySQL password in command arguments
func (e *BackupExecutor) maskMySQLPassword(args []string) string {
	maskedArgs := make([]string, len(args))
	copy(maskedArgs, args)

	for i, arg := range maskedArgs {
		// Mask password argument (-pPASSWORD)
		if strings.HasPrefix(arg, "-p") && len(arg) > 2 {
			maskedArgs[i] = "-p***"
		}
	}

	return strings.Join(maskedArgs, " ")
}

// maskSensitiveArgs masks sensitive information like passwords in command arguments
func (e *BackupExecutor) maskSensitiveArgs(args []string) string {
	maskedArgs := make([]string, len(args))
	copy(maskedArgs, args)

	for i, arg := range maskedArgs {
		if arg == "--uri" && i+1 < len(maskedArgs) {
			// Mask credentials in URI
			uri := maskedArgs[i+1]
			if strings.Contains(uri, "://") && strings.Contains(uri, "@") {
				// Format: mongodb://username:password@host:port/...
				parts := strings.Split(uri, "://")
				if len(parts) == 2 {
					protocolPart := parts[0] + "://"
					remaining := parts[1]

					if atIndex := strings.Index(remaining, "@"); atIndex != -1 {
						hostPart := remaining[atIndex:]
						credPart := remaining[:atIndex]

						// Check if there are credentials
						if strings.Contains(credPart, ":") {
							maskedArgs[i+1] = protocolPart + "***:***" + hostPart
						}
					}
				}
			}
		}
	}

	return strings.Join(maskedArgs, " ")
}
