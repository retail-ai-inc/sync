package domain

import (
	"crypto/rand"
	"encoding/base64"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// GenerateRandomPassword generates a password for a new Google user.
//
// It used to be "google_" followed by the current time to the second, with no
// randomness at all — and password login is accepted for Google accounts too, so
// knowing roughly when an account was created put it within a few hundred
// guesses. The account holder never sees this value and never uses it; it exists
// so the column is not empty.
func GenerateRandomPassword() string {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// A password that cannot be made random must not fall back to one that
		// is guessable.
		panic("identity: no random password could be generated: " + err.Error())
	}
	return "google_" + base64.RawURLEncoding.EncodeToString(buf)
}

// PageOfUsers returns the requested page of a user list with the columns the
// directory endpoint exposes. An out-of-range page is empty rather than an
// error, which is what the endpoint has always answered.
//
// Two things it no longer does. It used to compute a negative lower bound for a
// page number of zero or less and slice with it, which panics — the only guard
// was a check several layers above, in the HTTP handler, and this function is
// exported. And it used to redact by deleting from the caller's own maps, so the
// rows handed in came back stripped — and only the rows on the requested page,
// which means whether a row still carried its password depended on which page it
// fell on. The answer is built from scratch and the caller's rows are untouched.
func PageOfUsers(users []map[string]interface{}, current, pageSize int) []map[string]interface{} {
	if current < 1 {
		current = 1
	}
	if pageSize < 0 {
		pageSize = 0
	}

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
