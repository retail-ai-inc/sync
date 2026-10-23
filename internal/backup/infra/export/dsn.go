package export

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"
)

// buildMongoDBConnectionString builds the URI mongodump reads through. It goes
// through the shared builder so a backup addresses a cluster the same way
// replication does.
func buildMongoDBConnectionString(url, username, password string) string {
	return dsn.BuildDSNByType("mongodb", map[string]string{
		"user":     username,
		"password": password,
		"host":     url,
	})
}

// parseMySQLConnectionURL reads the host and port a backup connects to. The
// field is labelled a connection URL in the interface, so an operator fills in
// an ordinary DSN — and this used to be Split(url, ":") taking the first two
// parts, so "mysql://u:p@db:3306" gave the host "mysql" and the port "//u",
// and mysqldump went looking for a machine called mysql.
func parseMySQLConnectionURL(url string) (host, port string) {
	trimmed := strings.TrimSpace(url)
	host, port = "localhost", "3306"
	if trimmed == "" {
		return host, port
	}

	// A scheme, if there is one, and then anything before an @ is credentials
	// this does not use.
	if _, rest, found := strings.Cut(trimmed, "://"); found {
		trimmed = rest
	}
	if _, rest, found := strings.Cut(trimmed, "@"); found {
		trimmed = rest
	}
	// A trailing path is the database name, which the job names separately.
	trimmed, _, _ = strings.Cut(trimmed, "/")
	// A DSN in go-sql-driver's own form wraps the address in tcp(...).
	if inside, _, found := strings.Cut(strings.TrimPrefix(trimmed, "tcp("), ")"); found {
		trimmed = inside
	}

	if givenHost, givenPort, found := strings.Cut(trimmed, ":"); found {
		if givenHost != "" {
			host = givenHost
		}
		if givenPort != "" {
			port = givenPort
		}
		return host, port
	}
	if trimmed != "" {
		host = trimmed
	}
	return host, port
}

func buildMySQLConnectionString(url, username, password string) (host, port, user, pass string) {
	host, port = parseMySQLConnectionURL(url)
	user = username
	pass = password
	return
}

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

// splitMongoPassword takes the password out of a MongoDB URI and returns the
// two separately, so the password never reaches a command line.
//
// A URI without one comes back unchanged with an empty password, which is what
// a source with no authentication looks like.
func splitMongoPassword(uri string) (string, string) {
	parsed, err := url.Parse(uri)
	if err != nil || parsed.User == nil {
		return uri, ""
	}
	password, set := parsed.User.Password()
	if !set || password == "" {
		return uri, ""
	}
	parsed.User = url.User(parsed.User.Username())
	return parsed.String(), password
}

// mongoConfigFile writes the password where only this process can read it.
// mongoexport takes --config with a YAML password field, which is how it is
// meant to be given one.
func mongoConfigFile(password string) (string, func(), error) {
	if password == "" {
		return "", func() {}, nil
	}

	file, err := os.CreateTemp("", "mongo-credentials-*.yaml")
	if err != nil {
		return "", func() {}, fmt.Errorf("write the credentials file: %w", err)
	}
	remove := func() {
		if err := os.Remove(file.Name()); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("[BackupExecutor] Failed to remove %s: %v", file.Name(), err)
		}
	}

	// CreateTemp already makes it 0600, which is the point of the file. The
	// value is quoted because a password may hold anything YAML would otherwise
	// read as syntax.
	quoted := strings.NewReplacer("\\", `\\`, `"`, `\"`).Replace(password)
	if _, err := fmt.Fprintf(file, "password: \"%s\"\n", quoted); err != nil {
		_ = file.Close()
		remove()
		return "", func() {}, fmt.Errorf("write the credentials file: %w", err)
	}
	if err := file.Close(); err != nil {
		remove()
		return "", func() {}, fmt.Errorf("write the credentials file: %w", err)
	}
	return file.Name(), remove, nil
}
