package app

import (
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

func TestValidateUserToken(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	insertUser(t, db, "bob", "secret", "Bob", "guest")

	t.Run("accepts a token it issued", func(t *testing.T) {
		ok, username, access := ValidateUserToken(domain.GenerateUserToken("alice", "admin"))
		if !ok {
			t.Fatal("ValidateUserToken rejected a token it just generated")
		}
		if username != "alice" || access != "admin" {
			t.Errorf("identity = %q/%q, want alice/admin", username, access)
		}
	})

	t.Run("distinguishes users", func(t *testing.T) {
		_, username, access := ValidateUserToken(domain.GenerateUserToken("bob", "guest"))
		if username != "bob" || access != "guest" {
			t.Errorf("identity = %q/%q, want bob/guest", username, access)
		}
	})

	t.Run("rejects unknown tokens", func(t *testing.T) {
		for _, token := range []string{"", "not-a-token", domain.GenerateUserToken("carol", "admin")} {
			if ok, _, _ := ValidateUserToken(token); ok {
				t.Errorf("ValidateUserToken(%q) accepted an unknown token", token)
			}
		}
	})
}

// TestValidateUserTokenIsBoundToTheStoredAccessLevel records that the access
// level is part of the token input, so changing a user's permissions
// invalidates their current token. That is a reasonable property on its own,
// but combined with the daily rotation it means tokens behave like short-lived
// derivations rather than sessions: there is no way to revoke one without
// changing the user's stored access level or waiting for midnight.
func TestValidateUserTokenIsBoundToTheStoredAccessLevel(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "guest")

	issued := domain.GenerateUserToken("alice", "guest")
	if ok, _, _ := ValidateUserToken(issued); !ok {
		t.Fatal("the freshly issued token was rejected")
	}

	if _, err := db.Exec(`UPDATE users SET access = 'admin' WHERE username = 'alice'`); err != nil {
		t.Fatalf("promote user: %v", err)
	}

	if ok, _, _ := ValidateUserToken(issued); ok {
		t.Error("the old token still validates after the access level changed; " +
			"if tokens became independent of it, assert the new behaviour instead")
	}
	// The token for the new level works immediately, with no re-login.
	if ok, _, access := ValidateUserToken(domain.GenerateUserToken("alice", "admin")); !ok || access != "admin" {
		t.Errorf("the promoted token was rejected (ok=%v access=%q)", ok, access)
	}
}

// TestValidateUserTokenScansEveryUser records that validation is a linear scan:
// for each request it loads every user and recomputes that user's token until
// one matches. Cost grows with the size of the user table on every
// authenticated call, and each call also opens its own SQLite connection while
// the pool is limited to one.
func TestValidateUserTokenScansEveryUser(t *testing.T) {
	db := useTempDB(t)
	for i := 0; i < 50; i++ {
		insertUser(t, db, fmt.Sprintf("user%02d", i), "secret", "User", "guest")
	}
	insertUser(t, db, "last", "secret", "Last", "admin")

	ok, username, _ := ValidateUserToken(domain.GenerateUserToken("last", "admin"))
	if !ok || username != "last" {
		t.Errorf("the last user in the table was not matched (ok=%v username=%q)", ok, username)
	}
}
