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

// PublicUsers renders user rows with the columns the directory endpoint
// exposes, leaving out the password, the numeric id and the login name.
//
// It builds fresh rows. Redaction used to be done by deleting from the caller's
// own maps, so the rows handed in came back stripped — and only the rows on the
// requested page, which meant whether a row still carried its password depended
// on which page somebody asked for. Paging itself has moved to the database:
// this used to slice in memory and computed a negative lower bound for a page
// number of zero or less, which panics.
func PublicUsers(users []map[string]interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(users))
	for _, user := range users {
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
