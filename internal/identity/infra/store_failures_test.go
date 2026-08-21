package infra

import (
	"testing"
)

// TestEveryReadReportsAnUnopenableDatabase covers the branch each store read
// takes when the SQLite file cannot be opened at all. They all answer with the
// driver's error rather than a tagged one, so a caller cannot tell "no database"
// from "no such user".
func TestEveryReadReportsAnUnopenableDatabase(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func() error
	}{
		{"GetUserByUsername", func() error { _, err := GetUserByUsername("alice"); return err }},
		{"ValidateUser", func() error { _, _, err := ValidateUser("alice", "secret"); return err }},
		{"GetUserData", func() error { _, err := GetUserData("alice"); return err }},
		{"UpdateUserPassword", func() error { return UpdateUserPassword("alice", "new") }},
		{"SaveGoogleUser", func() error { _, _, err := SaveGoogleUser("a@b.test", "A"); return err }},
		{"GetAllUsers", func() error { _, err := GetAllUsers(); return err }},
		{"GetAuthConfig", func() error { _, err := GetAuthConfig("google"); return err }},
		{"UpdateAuthConfig", func() error { return UpdateAuthConfig("google", map[string]interface{}{}) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			unopenableIdentityDB(t)

			if err := tt.call(); err == nil {
				t.Errorf("%s returned no error for an unopenable database", tt.name)
			}
		})
	}
}

// TestEveryReadReportsAMissingTable covers the same calls against a database
// that exists but has no tables, which is what a failed migration leaves.
func TestEveryReadReportsAMissingTable(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func() error
	}{
		{"GetUserByUsername", func() error { _, err := GetUserByUsername("alice"); return err }},
		{"ValidateUser", func() error { _, _, err := ValidateUser("alice", "secret"); return err }},
		{"GetUserData", func() error { _, err := GetUserData("alice"); return err }},
		{"UpdateUserPassword", func() error { return UpdateUserPassword("alice", "new") }},
		{"SaveGoogleUser", func() error { _, _, err := SaveGoogleUser("a@b.test", "A"); return err }},
		{"GetAllUsers", func() error { _, err := GetAllUsers(); return err }},
		{"GetAuthConfig", func() error { _, err := GetAuthConfig("google"); return err }},
		{"UpdateAuthConfig", func() error { return UpdateAuthConfig("google", map[string]interface{}{}) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			emptyIdentityDB(t)

			if err := tt.call(); err == nil {
				t.Errorf("%s returned no error with no tables", tt.name)
			}
		})
	}
}

// TestSaveGoogleUserUpdatesAnExistingRow covers the branch a returning Google
// user takes: the name and avatar are refreshed and the stored access level is
// kept, rather than being reset to guest.
func TestSaveGoogleUserUpdatesAnExistingRow(t *testing.T) {
	db := useTempDB(t)
	if _, err := db.Exec(
		`INSERT INTO users (username, password, name, avatar, userId, email, access)
		 VALUES ('a@b.test', 'x', 'Old Name', '', 'uid-1', 'a@b.test', 'admin')`); err != nil {
		t.Fatalf("insert user: %v", err)
	}

	username, access, err := SaveGoogleUser("a@b.test", "New Name")
	if err != nil {
		t.Fatalf("SaveGoogleUser: %v", err)
	}
	if username != "a@b.test" {
		t.Errorf("username = %q", username)
	}
	if access != "admin" {
		t.Errorf("access = %q; a returning user appears to be reset to guest now", access)
	}

	var name string
	if err := db.QueryRow(`SELECT name FROM users WHERE email='a@b.test'`).Scan(&name); err != nil {
		t.Fatalf("read name: %v", err)
	}
	if name != "New Name" {
		t.Errorf("name = %q, want the refreshed one", name)
	}
}

// TestANewGoogleUserStartsAsGuest covers the other branch and records the
// default: a first-time Google sign-in creates a guest.
func TestANewGoogleUserStartsAsGuest(t *testing.T) {
	useTempDB(t)

	username, access, err := SaveGoogleUser("new@b.test", "New")
	if err != nil {
		t.Fatalf("SaveGoogleUser: %v", err)
	}
	if username != "new@b.test" {
		t.Errorf("username = %q, want the email", username)
	}
	if access != "guest" {
		t.Errorf("access = %q, want guest", access)
	}
}

// TestUpdateAuthConfigReplacesAnExistingRow covers the upsert's update branch.
func TestUpdateAuthConfigReplacesAnExistingRow(t *testing.T) {
	db := useTempDB(t)

	if err := UpdateAuthConfig("google", map[string]interface{}{"clientId": "first"}); err != nil {
		t.Fatalf("UpdateAuthConfig: %v", err)
	}
	if err := UpdateAuthConfig("google", map[string]interface{}{"clientId": "second"}); err != nil {
		t.Fatalf("UpdateAuthConfig: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM auth_configs WHERE provider='google'`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 1 {
		t.Errorf("%d rows for one provider, want 1", count)
	}

	got, err := GetAuthConfig("google")
	if err != nil {
		t.Fatalf("GetAuthConfig: %v", err)
	}
	if got["clientId"] != "second" {
		t.Errorf("clientId = %v, want the second write", got["clientId"])
	}
}
