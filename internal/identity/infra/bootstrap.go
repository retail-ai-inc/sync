package infra

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// ErrNoBootstrapPassword means the directory is empty and no password was
// supplied to create the first administrator with.
var ErrNoBootstrapPassword = errors.New(
	"the users table is empty and SYNC_ADMIN_PASSWORD is not set, so there is no " +
		"way to sign in; set it and restart to create the first administrator")

const BootstrapUsername = "admin"

// EnsureAdmin creates the first administrator on a database that has none. The
// credentials used to arrive with the repository: sync.db was committed with
// an admin row in it, so every deployment of this tool shared one password
// that was published on a Git remote.
func EnsureAdmin() (created bool, err error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return false, err
	}
	defer db.Close()

	return ensureAdmin(db, os.Getenv("SYNC_ADMIN_PASSWORD"))
}

func ensureAdmin(db *sql.DB, password string) (bool, error) {
	var users int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&users); err != nil {
		return false, fmt.Errorf("count the users: %w", err)
	}
	if users > 0 {
		return false, nil
	}
	if password == "" {
		return false, ErrNoBootstrapPassword
	}

	hashed, err := domain.HashPassword(password)
	if err != nil {
		return false, fmt.Errorf("hash the bootstrap password: %w", err)
	}

	// INSERT ... SELECT ... WHERE NOT EXISTS rather than a plain insert: two
	// replicas starting against the same file would otherwise both see an empty
	// table and both try to create the row.
	result, err := db.Exec(`
INSERT INTO users (username, password, name, avatar, userId, email, access, status)
SELECT ?, ?, 'Administrator', '', ?, '', ?, 'active'
WHERE NOT EXISTS (SELECT 1 FROM users)`,
		BootstrapUsername, hashed, BootstrapUsername, domain.AccessAdmin)
	if err != nil {
		return false, fmt.Errorf("create the first administrator: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, nil
	}
	return rows > 0, nil
}
