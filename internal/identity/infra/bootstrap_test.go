package infra

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite/sqlitetest"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// freshControlDB points SYNC_DB_PATH at a database the program has just built:
// the schema is there and the users table is empty, which is what a first run
// now looks like since sync.db stopped being shipped with an admin row in it.
func freshControlDB(t *testing.T) {
	t.Helper()
	cheapPasswordHashing(t)
	t.Setenv("SYNC_DB_PATH", filepath.Join(t.TempDir(), "sync.db"))
}

// TestEnsureAdminCreatesTheFirstAdministrator records that a first run with the
// variable set produces an account that can be signed in with — and that the
// password is stored hashed, not as it was typed.
func TestEnsureAdminCreatesTheFirstAdministrator(t *testing.T) {
	freshControlDB(t)
	t.Setenv("SYNC_ADMIN_PASSWORD", "a-first-password")

	created, err := EnsureAdmin()
	if err != nil {
		t.Fatalf("EnsureAdmin: %v", err)
	}
	if !created {
		t.Fatal("EnsureAdmin reported no account was created")
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	var stored, access string
	if err := db.QueryRow(
		`SELECT password, access FROM users WHERE username = ?`, BootstrapUsername).
		Scan(&stored, &access); err != nil {
		t.Fatalf("read the created row: %v", err)
	}
	if stored == "a-first-password" {
		t.Error("the bootstrap password was stored as typed")
	}
	if !domain.IsHashed(stored) {
		t.Errorf("stored password = %q, want a hash", stored)
	}
	if matches, _ := domain.PasswordMatches(stored, "a-first-password"); !matches {
		t.Error("the stored hash does not match the password it was made from")
	}
	if access != domain.AccessAdmin {
		t.Errorf("access = %q, want %q", access, domain.AccessAdmin)
	}
}

// TestEnsureAdminLeavesAnExistingDirectoryAlone records that the variable is
// read only when there is nobody at all. A manifest that keeps SYNC_ADMIN_PASSWORD
// set must not reset a password an operator has since changed, and must not put
// back an administrator that was removed on purpose.
func TestEnsureAdminLeavesAnExistingDirectoryAlone(t *testing.T) {
	freshControlDB(t)
	t.Setenv("SYNC_ADMIN_PASSWORD", "the-bootstrap-password")

	if _, err := EnsureAdmin(); err != nil {
		t.Fatalf("first EnsureAdmin: %v", err)
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		`UPDATE users SET password = 'changed-by-the-operator' WHERE username = ?`,
		BootstrapUsername); err != nil {
		t.Fatalf("change the password: %v", err)
	}

	created, err := EnsureAdmin()
	if err != nil {
		t.Fatalf("second EnsureAdmin: %v", err)
	}
	if created {
		t.Error("EnsureAdmin created a second account")
	}

	var stored string
	if err := db.QueryRow(
		`SELECT password FROM users WHERE username = ?`, BootstrapUsername).Scan(&stored); err != nil {
		t.Fatalf("re-read the row: %v", err)
	}
	if stored != "changed-by-the-operator" {
		t.Errorf("password = %q, want the operator's value kept", stored)
	}
}

// TestEnsureAdminReportsAnEmptyDirectoryWithNoPassword records that a first run
// without the variable is called out. The alternative — starting quietly — gives
// an operator a UI that answers every sign-in with "wrong password" and no clue
// why.
func TestEnsureAdminReportsAnEmptyDirectoryWithNoPassword(t *testing.T) {
	freshControlDB(t)
	t.Setenv("SYNC_ADMIN_PASSWORD", "")

	created, err := EnsureAdmin()
	if created {
		t.Error("an account was created with no password to create it from")
	}
	if !errors.Is(err, ErrNoBootstrapPassword) {
		t.Fatalf("err = %v, want ErrNoBootstrapPassword", err)
	}
}

// TestEnsureAdminReportsAMissingUsersTable records that a database without the
// schema is reported rather than read past.
func TestEnsureAdminReportsAMissingUsersTable(t *testing.T) {
	cheapPasswordHashing(t)
	sqlitetest.Tableless(t)
	t.Setenv("SYNC_ADMIN_PASSWORD", "a-first-password")

	if _, err := EnsureAdmin(); err == nil {
		t.Error("EnsureAdmin succeeded against a database with no users table")
	}
}
