package domain

import (
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// GenerateRandomPassword generates a random password for new Google users
func GenerateRandomPassword() string {
	// Simple implementation that generates a timestamp-based password
	// In production, use a secure random generator
	return "google_" + time.Now().Format("20060102150405")
}

// PageOfUsers returns the requested page of a user list with the columns the
// directory endpoint exposes. Sensitive and internal columns (password, the
// numeric id, the login name) are dropped. An out-of-range page is empty
// rather than an error, which is what the endpoint has always answered.
func PageOfUsers(users []map[string]interface{}, current, pageSize int) []map[string]interface{} {
	total := len(users)
	start := (current - 1) * pageSize
	end := start + pageSize

	if start >= total {
		start, end = 0, 0
	}
	if end > total {
		end = total
	}

	var page []map[string]interface{}
	if start < end {
		page = users[start:end]
	} else {
		page = []map[string]interface{}{}
	}

	var out []map[string]interface{}
	for _, user := range page {
		delete(user, "password")
		delete(user, "id")
		delete(user, "username")

		out = append(out, map[string]interface{}{
			"userId": user["userId"],
			"name":   user["name"],
			"email":  user["email"],
			"access": user["access"],
			"avatar": user["avatar"],
			"status": user["status"],
		})
	}
	return out
}
