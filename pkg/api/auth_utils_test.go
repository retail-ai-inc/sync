package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"
)

// hmacToken recomputes a token independently of the implementation, so the
// derivation itself is pinned rather than merely being self-consistent.
func hmacToken(secret, username, accessLevel, date string) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte("user_token:" + username + ":" + accessLevel + ":" + date))
	return hex.EncodeToString(h.Sum(nil))
}

func TestGetTokenSecret(t *testing.T) {
	t.Run("environment variable wins", func(t *testing.T) {
		t.Setenv("SYNC_TOKEN_SECRET", "a-real-secret")
		if got := getTokenSecret(); got != "a-real-secret" {
			t.Errorf("getTokenSecret = %q, want the environment value", got)
		}
	})

	t.Run("falls back to the built-in default", func(t *testing.T) {
		t.Setenv("SYNC_TOKEN_SECRET", "")
		if got, want := getTokenSecret(), "sync_default_secret_key_change_me_in_production"; got != want {
			t.Errorf("getTokenSecret = %q, want %q", got, want)
		}
	})
}

// TestDefaultTokenSecretIsHardcoded records F-252. With SYNC_TOKEN_SECRET
// unset, every deployment shares one secret that is committed to a public
// repository. Since a token is a pure function of secret, username, access
// level and date, anyone can compute a valid admin token offline for any day.
func TestDefaultTokenSecretIsHardcoded(t *testing.T) {
	t.Setenv("SYNC_TOKEN_SECRET", "")

	secret := getTokenSecret()
	if secret != "sync_default_secret_key_change_me_in_production" {
		t.Fatalf("the default secret changed to %q; if key management landed, "+
			"replace this test with one covering the new source", secret)
	}

	// Demonstrate the consequence: the admin token for today is reproducible
	// from public information alone.
	forged := hmacToken(secret, "admin", "admin", time.Now().Format("2006-01-02"))
	if forged != GenerateAdminToken() {
		t.Error("the offline-computed admin token no longer matches; the derivation " +
			"may have gained a non-public input, which would be an improvement")
	}
}

func TestGenerateUserTokenMatchesHMAC(t *testing.T) {
	today := time.Now().Format("2006-01-02")

	tests := []struct{ username, access string }{
		{"admin", "admin"},
		{"alice", "user"},
		{"", ""},
		{"user with spaces", "read-only"},
	}

	for _, tt := range tests {
		t.Run(tt.username+"/"+tt.access, func(t *testing.T) {
			want := hmacToken(tokenSecret, tt.username, tt.access, today)
			if got := GenerateUserToken(tt.username, tt.access); got != want {
				t.Errorf("GenerateUserToken(%q, %q) = %q, want %q",
					tt.username, tt.access, got, want)
			}
		})
	}
}

func TestGenerateUserTokenIsDeterministic(t *testing.T) {
	first := GenerateUserToken("alice", "user")
	for i := 0; i < 5; i++ {
		if got := GenerateUserToken("alice", "user"); got != first {
			t.Fatalf("call %d returned %q, want %q", i, got, first)
		}
	}
	// Hex-encoded SHA-256 is 64 characters.
	if len(first) != 64 {
		t.Errorf("token length = %d, want 64", len(first))
	}
}

func TestGenerateUserTokenVariesByInput(t *testing.T) {
	base := GenerateUserToken("alice", "user")

	for _, tt := range []struct {
		name             string
		username, access string
	}{
		{"different username", "bob", "user"},
		{"different access level", "alice", "admin"},
		{"username case matters", "Alice", "user"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := GenerateUserToken(tt.username, tt.access); got == base {
				t.Errorf("GenerateUserToken(%q, %q) collided with the base token",
					tt.username, tt.access)
			}
		})
	}
}

// TestGenerateAdminTokenEqualsAdminUserToken records that the admin token is
// not a distinct credential: it is exactly the user token for the pair
// ("admin", "admin"). Any account named admin whose access level is admin
// therefore holds the token that ValidateAdminToken accepts.
func TestGenerateAdminTokenEqualsAdminUserToken(t *testing.T) {
	if GenerateAdminToken() != GenerateUserToken("admin", "admin") {
		t.Error("the admin token is now derived separately from user tokens; " +
			"assert the new derivation instead")
	}
}

func TestValidateAdminToken(t *testing.T) {
	valid := GenerateAdminToken()

	if !ValidateAdminToken(valid) {
		t.Error("ValidateAdminToken rejected the token it just generated")
	}

	for _, tt := range []struct{ name, token string }{
		{"empty", ""},
		{"truncated", valid[:63]},
		{"one character changed", "0" + valid[1:]},
		{"uppercase hex", strings.ToUpper(valid)},
		{"a user token", GenerateUserToken("alice", "user")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if ValidateAdminToken(tt.token) {
				t.Errorf("ValidateAdminToken(%q) = true, want false", tt.token)
			}
		})
	}
}

func TestExtractTokenFromHeader(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{"bearer prefix is stripped", "Bearer abc123", "abc123"},
		{"only the first prefix is stripped", "Bearer Bearer abc", "Bearer abc"},
		{"no prefix returns the whole value", "abc123", "abc123"},
		{"empty header", "", ""},
		// The prefix check is case-sensitive and requires the trailing space,
		// so these forms are treated as the token itself and fail validation
		// later with no indication that the header was malformed.
		{"lowercase bearer is not recognised", "bearer abc123", "bearer abc123"},
		{"missing space is not recognised", "Bearerabc123", "Bearerabc123"},
		{"leading whitespace is not trimmed", " Bearer abc123", " Bearer abc123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractTokenFromHeader(tt.header); got != tt.want {
				t.Errorf("ExtractTokenFromHeader(%q) = %q, want %q", tt.header, got, tt.want)
			}
		})
	}
}

// TestTokenSecretIsCapturedAtInit records that the package-level tokenSecret is
// evaluated once during initialisation, so changing SYNC_TOKEN_SECRET at
// runtime has no effect on issued tokens. Rotating the secret requires a
// restart, and the running process gives no sign that the environment and the
// key in use have diverged.
func TestTokenSecretIsCapturedAtInit(t *testing.T) {
	before := GenerateUserToken("alice", "user")

	t.Setenv("SYNC_TOKEN_SECRET", "a-freshly-rotated-secret")

	if after := GenerateUserToken("alice", "user"); after != before {
		t.Error("tokens now follow the environment at call time; the secret may " +
			"have become reloadable, which would be an improvement")
	}
}

// TestGenerateUserTokenSeparatorIsAmbiguous records a design flaw: the HMAC
// input joins username and access level with a colon without escaping either,
// so distinct pairs can produce identical tokens. A user named "a:b" with
// access "c" is indistinguishable from a user named "a" with access "b:c".
// Exploitability depends on the access levels in use, but the derivation should
// not depend on that.
func TestGenerateUserTokenSeparatorIsAmbiguous(t *testing.T) {
	first := GenerateUserToken("a", "b:c")
	second := GenerateUserToken("a:b", "c")

	if first != second {
		t.Errorf("the two pairs now produce different tokens (%q vs %q); the "+
			"separator may have been escaped, which would be an improvement",
			first, second)
	}
}

// ValidateUserToken needs the user table, so it could not be covered until the
// temporary-database fixture existed.

func TestValidateUserToken(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	insertUser(t, db, "bob", "secret", "Bob", "guest")

	t.Run("accepts a token it issued", func(t *testing.T) {
		ok, username, access := ValidateUserToken(GenerateUserToken("alice", "admin"))
		if !ok {
			t.Fatal("ValidateUserToken rejected a token it just generated")
		}
		if username != "alice" || access != "admin" {
			t.Errorf("identity = %q/%q, want alice/admin", username, access)
		}
	})

	t.Run("distinguishes users", func(t *testing.T) {
		_, username, access := ValidateUserToken(GenerateUserToken("bob", "guest"))
		if username != "bob" || access != "guest" {
			t.Errorf("identity = %q/%q, want bob/guest", username, access)
		}
	})

	t.Run("rejects unknown tokens", func(t *testing.T) {
		for _, token := range []string{"", "not-a-token", GenerateUserToken("carol", "admin")} {
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

	issued := GenerateUserToken("alice", "guest")
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
	if ok, _, access := ValidateUserToken(GenerateUserToken("alice", "admin")); !ok || access != "admin" {
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

	ok, username, _ := ValidateUserToken(GenerateUserToken("last", "admin"))
	if !ok || username != "last" {
		t.Errorf("the last user in the table was not matched (ok=%v username=%q)", ok, username)
	}
}
