package identity

import (
	"database/sql"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// useTempDB points the package at a throwaway SQLite file carrying the same
// schema as sync.db, so the user and auth-config helpers can be exercised
// without touching the database tracked in this repository.
func useTempDB(t *testing.T) *sql.DB {
	t.Helper()

	isolateCrontab(t)

	path := filepath.Join(t.TempDir(), "sync.db")
	t.Setenv("SYNC_DB_PATH", path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open temp sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const schema = `
CREATE TABLE users (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    username   TEXT NOT NULL UNIQUE,
    password   TEXT NOT NULL,
    name       TEXT NOT NULL,
    avatar     TEXT,
    userId     TEXT,
    email      TEXT,
    access     TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status     TEXT DEFAULT 'active'
);
CREATE TABLE auth_configs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    provider    TEXT NOT NULL UNIQUE,
    config_json TEXT NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
    enabled     BOOLEAN DEFAULT false
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	return db
}

func insertUser(t *testing.T, db *sql.DB, username, password, name, access string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO users (username, password, name, access) VALUES (?, ?, ?, ?)`,
		username, password, name, access); err != nil {
		t.Fatalf("insert user %q: %v", username, err)
	}
}

func TestGetUserByUsername(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	user, err := GetUserByUsername("alice")
	if err != nil {
		t.Fatalf("GetUserByUsername: %v", err)
	}

	if user["username"] != "alice" || user["name"] != "Alice" || user["access"] != "admin" {
		t.Errorf("user = %+v", user)
	}
	// NULL columns are normalised to empty strings rather than nil, so callers
	// can type-assert without checking.
	for _, key := range []string{"avatar", "userId", "email"} {
		if user[key] != "" {
			t.Errorf("%s = %#v, want an empty string", key, user[key])
		}
	}
	// A NULL status defaults to active.
	if user["status"] != "active" {
		t.Errorf("status = %#v, want active", user["status"])
	}
}

func TestGetUserByUsernameMissing(t *testing.T) {
	useTempDB(t)

	_, err := GetUserByUsername("nobody")
	if err != sql.ErrNoRows {
		t.Errorf("GetUserByUsername for a missing user returned %v, want sql.ErrNoRows", err)
	}
}

func TestValidateUser(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	t.Run("correct password", func(t *testing.T) {
		ok, access, err := ValidateUser("alice", "secret")
		if err != nil {
			t.Fatalf("ValidateUser: %v", err)
		}
		if !ok || access != "admin" {
			t.Errorf("ValidateUser = %v/%q, want true/admin", ok, access)
		}
	})

	t.Run("wrong password", func(t *testing.T) {
		ok, access, err := ValidateUser("alice", "wrong")
		if err != nil {
			t.Fatalf("ValidateUser: %v", err)
		}
		if ok || access != "" {
			t.Errorf("ValidateUser = %v/%q, want false/empty", ok, access)
		}
	})

	t.Run("unknown user is not an error", func(t *testing.T) {
		// sql.ErrNoRows is swallowed so a missing user is indistinguishable
		// from a wrong password, which is the right choice for a login form.
		ok, _, err := ValidateUser("nobody", "secret")
		if err != nil {
			t.Errorf("ValidateUser returned %v, want nil", err)
		}
		if ok {
			t.Error("ValidateUser accepted an unknown user")
		}
	})
}

// TestPasswordsAreStoredInCleartext records that no hashing takes place
// anywhere in the credential path: UpdateUserPassword writes the value it is
// given, and ValidateUser compares it to the submitted password with ==. The
// production database copy in docs/ shows the same, an admin password of
// sixteen printable characters rather than a hash.
//
// The comparison is also not constant time, so it leaks timing information,
// but that is secondary to the passwords being readable by anyone who can open
// the file.
func TestPasswordsAreStoredInCleartext(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "initial", "Alice", "admin")

	if err := UpdateUserPassword("alice", "a-brand-new-password"); err != nil {
		t.Fatalf("UpdateUserPassword: %v", err)
	}

	var stored string
	if err := db.QueryRow("SELECT password FROM users WHERE username = 'alice'").Scan(&stored); err != nil {
		t.Fatalf("read password: %v", err)
	}

	if stored != "a-brand-new-password" {
		t.Fatalf("the stored value is %q; hashing may have been introduced, "+
			"which would be an improvement", stored)
	}
}

func TestUpdateUserPasswordUnknownUserIsSilent(t *testing.T) {
	useTempDB(t)

	// UPDATE affects no rows and reports no error, so a caller that mistypes a
	// username believes the password was changed.
	if err := UpdateUserPassword("nobody", "x"); err != nil {
		t.Errorf("UpdateUserPassword for a missing user returned %v; if it now "+
			"reports the miss, assert that instead", err)
	}
}

func TestGetUserDataStripsPassword(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	data, err := GetUserData("alice")
	if err != nil {
		t.Fatalf("GetUserData: %v", err)
	}

	if _, present := data["password"]; present {
		t.Error("GetUserData returned the password field")
	}
	if data["username"] != "alice" {
		t.Errorf("username = %#v", data["username"])
	}
}

func TestGetAllUsers(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "s1", "Alice", "admin")
	insertUser(t, db, "bob", "s2", "Bob", "guest")

	users, err := GetAllUsers()
	if err != nil {
		t.Fatalf("GetAllUsers: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("GetAllUsers returned %d users, want 2", len(users))
	}

	// Unlike GetUserData, this one keeps the password in the map. Callers are
	// responsible for stripping it; GetUsersHandler builds a whitelist response
	// and ValidateUserToken only reads username and access.
	if _, present := users[0]["password"]; !present {
		t.Error("GetAllUsers no longer returns the password; callers that relied " +
			"on stripping it can be simplified")
	}
}

func TestGetAllUsersEmpty(t *testing.T) {
	useTempDB(t)

	users, err := GetAllUsers()
	if err != nil {
		t.Fatalf("GetAllUsers: %v", err)
	}
	if len(users) != 0 {
		t.Errorf("GetAllUsers returned %d users, want none", len(users))
	}
}

func TestSaveGoogleUserCreatesAndUpdates(t *testing.T) {
	db := useTempDB(t)

	t.Run("creates a guest account", func(t *testing.T) {
		username, access, err := SaveGoogleUser("jack@example.com", "Jack")
		if err != nil {
			t.Fatalf("SaveGoogleUser: %v", err)
		}
		if username != "jack@example.com" {
			t.Errorf("username = %q, want the email address", username)
		}
		if access != "guest" {
			t.Errorf("access = %q, want guest", access)
		}

		var name, userID string
		if err := db.QueryRow(
			`SELECT name, userId FROM users WHERE email = 'jack@example.com'`).Scan(&name, &userID); err != nil {
			t.Fatalf("read user: %v", err)
		}
		if name != "Jack" {
			t.Errorf("name = %q", name)
		}
		if !strings.HasPrefix(userID, "g_") {
			t.Errorf("userId = %q, want a g_ prefix", userID)
		}
	})

	t.Run("second login updates rather than duplicates", func(t *testing.T) {
		if _, _, err := SaveGoogleUser("jack@example.com", "Jack Renamed"); err != nil {
			t.Fatalf("SaveGoogleUser: %v", err)
		}

		var count int
		if err := db.QueryRow(
			`SELECT COUNT(*) FROM users WHERE email = 'jack@example.com'`).Scan(&count); err != nil {
			t.Fatalf("count users: %v", err)
		}
		if count != 1 {
			t.Errorf("the account was duplicated: %d rows", count)
		}

		var name string
		if err := db.QueryRow(
			`SELECT name FROM users WHERE email = 'jack@example.com'`).Scan(&name); err != nil {
			t.Fatalf("read name: %v", err)
		}
		if name != "Jack Renamed" {
			t.Errorf("name = %q, want the updated value", name)
		}
	})

	t.Run("an existing access level is preserved", func(t *testing.T) {
		if _, err := db.Exec(
			`UPDATE users SET access = 'admin' WHERE email = 'jack@example.com'`); err != nil {
			t.Fatalf("promote user: %v", err)
		}

		_, access, err := SaveGoogleUser("jack@example.com", "Jack")
		if err != nil {
			t.Fatalf("SaveGoogleUser: %v", err)
		}
		// A returning user keeps whatever level an administrator granted; the
		// guest default only applies on creation.
		if access != "admin" {
			t.Errorf("access = %q, want admin", access)
		}
	})
}

// TestGeneratedGooglePasswordIsPredictable records that generateRandomPassword
// is not random. It returns "google_" followed by the current time formatted to
// the second, so the credential for an account created by Google sign-in can be
// reconstructed by anyone who knows roughly when it was created — a few hundred
// guesses covers a wide window.
//
// This matters because ValidateUser accepts password logins for every account,
// including ones created through Google, so a predictable password is a way
// into the account without going through Google at all.
func TestGeneratedGooglePasswordIsPredictable(t *testing.T) {
	got := generateRandomPassword()

	if !regexp.MustCompile(`^google_\d{14}$`).MatchString(got) {
		t.Fatalf("generateRandomPassword produced %q; the scheme may have changed, "+
			"so assert the new one instead", got)
	}
	// Reconstructing it needs nothing but a clock.
	if want := "google_" + time.Now().Format("20060102150405"); got != want {
		t.Errorf("generateRandomPassword = %q, and the current second gives %q; "+
			"they normally match", got, want)
	}
	// Two calls inside one second are identical, which is the whole problem.
	if second := generateRandomPassword(); second != got {
		t.Logf("the two calls straddled a second boundary (%q vs %q)", got, second)
	}
}

func TestAuthConfigRoundTrip(t *testing.T) {
	useTempDB(t)

	err := UpdateAuthConfig("google", map[string]interface{}{
		"client_id":     "id-123",
		"client_secret": "secret-456",
		"enabled":       true,
	})
	if err != nil {
		t.Fatalf("UpdateAuthConfig: %v", err)
	}

	got, err := GetAuthConfig("google")
	if err != nil {
		t.Fatalf("GetAuthConfig: %v", err)
	}
	if got["client_id"] != "id-123" || got["client_secret"] != "secret-456" {
		t.Errorf("config = %+v", got)
	}
	// enabled is stored in its own column and merged back in on read.
	if got["enabled"] != true {
		t.Errorf("enabled = %#v, want true", got["enabled"])
	}
}

func TestUpdateAuthConfigOverwrites(t *testing.T) {
	useTempDB(t)

	if err := UpdateAuthConfig("google", map[string]interface{}{"client_id": "first", "enabled": true}); err != nil {
		t.Fatalf("first UpdateAuthConfig: %v", err)
	}
	if err := UpdateAuthConfig("google", map[string]interface{}{"client_id": "second", "enabled": false}); err != nil {
		t.Fatalf("second UpdateAuthConfig: %v", err)
	}

	got, err := GetAuthConfig("google")
	if err != nil {
		t.Fatalf("GetAuthConfig: %v", err)
	}
	if got["client_id"] != "second" {
		t.Errorf("client_id = %#v, want the updated value", got["client_id"])
	}
	if got["enabled"] != false {
		t.Errorf("enabled = %#v, want false", got["enabled"])
	}
}

func TestGetAuthConfigMissingProvider(t *testing.T) {
	useTempDB(t)

	if _, err := GetAuthConfig("github"); err != sql.ErrNoRows {
		t.Errorf("GetAuthConfig for an unknown provider returned %v, want sql.ErrNoRows", err)
	}
}

// TestUpdateAuthConfigMutatesTheCallersMap records a side effect: the function
// deletes "enabled" from the map it was handed, because that value goes to its
// own column. A caller that reuses the map afterwards silently loses the field.
func TestUpdateAuthConfigMutatesTheCallersMap(t *testing.T) {
	useTempDB(t)

	cfg := map[string]interface{}{"client_id": "id", "enabled": true}
	if err := UpdateAuthConfig("google", cfg); err != nil {
		t.Fatalf("UpdateAuthConfig: %v", err)
	}

	if _, present := cfg["enabled"]; present {
		t.Error("the caller's map kept its enabled key; the function may now copy " +
			"before deleting, which would be an improvement")
	}
}
