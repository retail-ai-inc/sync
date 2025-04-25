package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// openLocalDB already defined in sync_handler.go
// No need to redefine here

// Ensure user table exists and has initial admin account

// GetUserByUsername gets user information by username
func GetUserByUsername(username string) (map[string]interface{}, error) {
	db, err := openLocalDB()
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

	// Create user map with proper NULL handling
	user := map[string]interface{}{
		"id":       id,
		"username": username,
		"password": password,
		"name":     name,
		"access":   access,
	}

	// Handle potentially NULL fields
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

	// Process status field
	if status.Valid {
		user["status"] = status.String
	} else {
		user["status"] = "active" // Default status is active
	}

	return user, nil
}

// ValidateUser validates username and password
func ValidateUser(username, password string) (bool, string, error) {
	user, err := GetUserByUsername(username)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, "", nil
		}
		return false, "", err
	}

	// Check if password matches
	if user["password"] == password {
		return true, user["access"].(string), nil
	}

	return false, "", nil
}

// GetUserData gets user data for frontend display
func GetUserData(username string) (map[string]interface{}, error) {
	user, err := GetUserByUsername(username)
	if err != nil {
		return nil, err
	}

	// Remove sensitive information
	delete(user, "password")

	return user, nil
}

// UpdateUserPassword updates user password
func UpdateUserPassword(username, newPassword string) error {
	db, err := openLocalDB()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec("UPDATE users SET password = ? WHERE username = ?", newPassword, username)
	return err
}

// SaveGoogleUser saves or updates Google user information
func SaveGoogleUser(email, name string) (string, string, error) {
	db, err := openLocalDB()
	if err != nil {
		return "", "", err
	}
	defer db.Close()

	// Check if user already exists
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

		// Get access level for existing user
		err = db.QueryRow("SELECT username, access FROM users WHERE email = ?", email).Scan(&username, &access)
		if err != nil {
			return "", "", err
		}
	} else {
		// Create new user with a random password
		randomPassword := generateRandomPassword()

		// Generate a default userId
		defaultUserId := "g_" + fmt.Sprintf("%d", time.Now().Unix())

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

// generateRandomPassword generates a random password for new Google users
func generateRandomPassword() string {
	// Simple implementation that generates a timestamp-based password
	// In production, use a secure random generator
	return "google_" + time.Now().Format("20060102150405")
}

// GetAllUsers gets all user information
func GetAllUsers() ([]map[string]interface{}, error) {
	db, err := openLocalDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(`
SELECT id, username, password, name, avatar, userId, email, access, status
FROM users`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []map[string]interface{}
	for rows.Next() {
		var id int
		var username, password, name, access string
		var status sql.NullString                // Status can be NULL (for old records)
		var avatar, userId, email sql.NullString // Use sql.NullString for fields that can be NULL

		err = rows.Scan(&id, &username, &password, &name, &avatar, &userId, &email, &access, &status)
		if err != nil {
			return nil, err
		}

		// Create user map with proper NULL handling
		user := map[string]interface{}{
			"id":       id,
			"username": username,
			"password": password,
			"name":     name,
			"access":   access,
		}

		// Handle potentially NULL fields
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

		// Process status field
		if status.Valid {
			user["status"] = status.String
		} else {
			user["status"] = "active" // Default status is active
		}

		users = append(users, user)
	}

	return users, nil
}

// Initialize user table when starting service

func GetAuthConfig(provider string) (map[string]interface{}, error) {
	db, err := openLocalDB()
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
	db, err := openLocalDB()
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

	// Remove the enabled field from config, as it will be stored separately
	delete(config, "enabled")

	// Convert config to JSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return err
	}

	// Check if the configuration already exists
	var exists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM auth_configs WHERE provider = ?)", provider).Scan(&exists)
	if err != nil {
		return err
	}

	// Execute operations
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
