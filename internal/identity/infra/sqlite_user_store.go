package infra

import (
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"
)

func GetUserByUsername(username string) (map[string]interface{}, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	row := db.QueryRow(`
SELECT id, username, password, name, avatar, userId, email, access, status
FROM users
WHERE username = ?`, username)

	var id int
	var password, name, access string
	var avatar, userId, email sql.NullString // Use sql.NullString for fields that can be NULL
	var status sql.NullString                // Status can be NULL

	err = row.Scan(&id, &username, &password, &name, &avatar, &userId, &email, &access, &status)
	if err != nil {
		return nil, err
	}

	user := map[string]interface{}{
		"id":       id,
		"username": username,
		"password": password,
		"name":     name,
		"access":   access,
	}

	if avatar.Valid {
		user["avatar"] = avatar.String
	} else {
		user["avatar"] = ""
	}

	if userId.Valid {
		user["userId"] = userId.String
	} else {
		user["userId"] = ""
	}

	if email.Valid {
		user["email"] = email.String
	} else {
		user["email"] = ""
	}

	if status.Valid {
		user["status"] = status.String
	} else {
		user["status"] = "active" // Default status is active
	}

	return user, nil
}

func ValidateUser(username, password string) (bool, string, error) {
	user, err := GetUserByUsername(username)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, "", nil
		}
		return false, "", err
	}

	stored, _ := user["password"].(string)
	matches, needsRehash := domain.PasswordMatches(stored, password)
	if !matches {
		return false, "", nil
	}

	// The row still holds the password as it was typed. Rewriting it here means
	// no migration step and no password reset: the cleartext leaves the file the
	// first time each account is used. A failure to rewrite is not a failure to
	// log in.
	if needsRehash {
		if err := UpdateUserPassword(username, password); err != nil {
			logrus.Warnf("[Identity] The password for %q is stored in the clear and "+
				"could not be replaced with a hash: %v", username, err)
		}
	}

	access, _ := user["access"].(string)
	return true, access, nil
}

func GetUserData(username string) (map[string]interface{}, error) {
	user, err := GetUserByUsername(username)
	if err != nil {
		return nil, err
	}

	delete(user, "password")

	return user, nil
}

// UpdateUserPassword updates user password.
//
// The value written is a hash. It used to be the password as given, so the users
// table held working credentials for anyone who could read the file — and a
// username that names no row was reported as a successful change, because
// RowsAffected was never read.
func UpdateUserPassword(username, newPassword string) error {
	hashed, err := domain.HashPassword(newPassword)
	if err != nil {
		return err
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return err
	}
	defer db.Close()

	res, err := db.Exec("UPDATE users SET password = ? WHERE username = ?", hashed, username)
	if err != nil {
		return err
	}
	if affected, err := res.RowsAffected(); err == nil && affected == 0 {
		return ErrNoSuchUser
	}
	return nil
}

func SaveGoogleUser(email, name string) (string, string, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return "", "", err
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE email = ?", email).Scan(&count)
	if err != nil {
		return "", "", err
	}

	username := email
	access := "guest" // Default access level for Google users (changed from "user" to "guest")

	if count > 0 {
		// User exists, update information
		_, err = db.Exec(`
			UPDATE users 
			SET name = ?, 
				avatar = ? 
			WHERE email = ?`,
			name,
			"https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png", // Default avatar
			email)
		if err != nil {
			return "", "", err
		}

		err = db.QueryRow("SELECT username, access FROM users WHERE email = ?", email).Scan(&username, &access)
		if err != nil {
			return "", "", err
		}
	} else {
		// Create new user with a random password. It is hashed like any other:
		// nobody ever uses it, but a readable one in the table is a readable
		// credential.
		randomPassword, err := domain.HashPassword(domain.GenerateRandomPassword())
		if err != nil {
			return "", "", err
		}

		// Random, not the current second: the access and delete writes key on
		// userId, and the column has no UNIQUE constraint.
		defaultUserId := "g_" + rand.Text()

		// Use email as username
		_, err = db.Exec(`
			INSERT INTO users (username, password, name, avatar, email, userId, access)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			username,
			randomPassword,
			name,
			"https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png", // Default avatar
			email,
			defaultUserId,
			access)
		if err != nil {
			return "", "", err
		}
	}

	return username, access, nil
}

// PageOfUsers reads one page of the directory, and how many users there are.
//
// The paging is done by the database. It used to read every row and slice the
// result in memory, with no upper bound on the page size — so ?pageSize=1000000
// loaded the whole table through a connection pool that holds exactly one
// connection, which every other part of the process is also waiting on.
func PageOfUsers(offset, limit int) (users []map[string]interface{}, total int, err error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()

	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := db.Query(`
SELECT id, username, password, name, avatar, userId, email, access, status
FROM users
ORDER BY id
LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	users, err = scanUsers(rows)
	if err != nil {
		return nil, 0, err
	}
	return users, total, nil
}

func GetAllUsers() ([]map[string]interface{}, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
SELECT id, username, password, name, avatar, userId, email, access, status
FROM users
ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanUsers(rows)
}

// scanUsers reads user rows, filling in the columns older records may leave
// NULL.
func scanUsers(rows *sql.Rows) ([]map[string]interface{}, error) {
	var users []map[string]interface{}
	for rows.Next() {
		var id int
		var username, password, name, access string
		var status sql.NullString                // Status can be NULL (for old records)
		var avatar, userId, email sql.NullString // Use sql.NullString for fields that can be NULL

		if err := rows.Scan(&id, &username, &password, &name, &avatar, &userId, &email, &access, &status); err != nil {
			return nil, err
		}

		users = append(users, map[string]interface{}{
			"id":       id,
			"username": username,
			"password": password,
			"name":     name,
			"access":   access,
			"avatar":   nullOrEmpty(avatar),
			"userId":   nullOrEmpty(userId),
			"email":    nullOrEmpty(email),
			"status":   nullOr(status, "active"),
		})
	}
	return users, rows.Err()
}

func nullOrEmpty(s sql.NullString) string { return nullOr(s, "") }

func GetAuthConfig(provider string) (map[string]interface{}, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	var hasEnabledColumn bool
	err = db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('auth_configs') 
                      WHERE name = 'enabled'`).Scan(&hasEnabledColumn)
	if err != nil {
		return nil, err
	}

	var configData []byte
	var enabled bool = false

	if hasEnabledColumn {
		err = db.QueryRow("SELECT config_json, enabled FROM auth_configs WHERE provider = ?", provider).Scan(&configData, &enabled)
	} else {
		err = db.QueryRow("SELECT config_json FROM auth_configs WHERE provider = ?", provider).Scan(&configData)
	}

	if err != nil {
		return nil, err
	}

	var config map[string]interface{}
	if err := json.Unmarshal(configData, &config); err != nil {
		return nil, err
	}

	config["enabled"] = enabled

	return config, nil
}

func UpdateAuthConfig(provider string, config map[string]interface{}) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return err
	}
	defer db.Close()

	var hasEnabledColumn bool
	err = db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('auth_configs') 
                      WHERE name = 'enabled'`).Scan(&hasEnabledColumn)
	if err != nil {
		return err
	}

	// If the table doesn't have an enabled column, add it
	if !hasEnabledColumn {
		_, err = db.Exec("ALTER TABLE auth_configs ADD COLUMN enabled BOOLEAN DEFAULT false")
		if err != nil {
			return fmt.Errorf("failed to add enabled column: %w", err)
		}
		// Column has been added, now hasEnabledColumn is true
		hasEnabledColumn = true
	}

	// Extract the enabled field value, default is false
	enabled, _ := config["enabled"].(bool)

	// The enabled flag is stored in its own column, so it is left out of the
	// document — from a copy. This used to delete it from the caller's own map,
	// so a caller that reused the map afterwards found the field gone.
	stored := make(map[string]interface{}, len(config))
	for key, value := range config {
		if key == "enabled" {
			continue
		}
		stored[key] = value
	}

	configJSON, err := json.Marshal(stored)
	if err != nil {
		return err
	}

	var exists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM auth_configs WHERE provider = ?)", provider).Scan(&exists)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if exists {
		// Update existing configuration, including the enabled field
		_, err = tx.Exec("UPDATE auth_configs SET config_json = ?, enabled = ? WHERE provider = ?",
			configJSON, enabled, provider)
	} else {
		// Insert new configuration, including the enabled field
		_, err = tx.Exec("INSERT INTO auth_configs (provider, config_json, enabled) VALUES (?, ?, ?)",
			provider, configJSON, enabled)
	}

	if err != nil {
		return err
	}

	return tx.Commit()
}
