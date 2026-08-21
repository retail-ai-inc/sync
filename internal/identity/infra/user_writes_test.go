package infra

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

// seedUser inserts a user with a userId, which the write calls address rows by.
func seedUser(t *testing.T, db *sql.DB, username, access, status string) {
	t.Helper()

	if _, err := db.Exec(
		`INSERT INTO users (username, password, name, avatar, userId, email, access, status)
		 VALUES (?, 'secret', ?, '', ?, ?, ?, ?)`,
		username, "Name "+username, "uid-"+username, username+"@example.test", access, status); err != nil {
		t.Fatalf("insert user: %v", err)
	}
}

func TestUpdateUserAccessAndStatusAppliesBothFields(t *testing.T) {
	db := useTempDB(t)
	seedUser(t, db, "alice", domain.AccessGuest, domain.StatusActive)

	user, err := UpdateUserAccessAndStatus("uid-alice", domain.AccessAdmin, domain.StatusInactive)
	if err != nil {
		t.Fatalf("UpdateUserAccessAndStatus: %v", err)
	}
	if user["access"] != domain.AccessAdmin || user["status"] != domain.StatusInactive {
		t.Errorf("returned user = %v", user)
	}
	if user["userId"] != "uid-alice" || user["email"] != "alice@example.test" {
		t.Errorf("returned user = %v", user)
	}

	var access, status string
	if err := db.QueryRow(`SELECT access, status FROM users WHERE userId='uid-alice'`).
		Scan(&access, &status); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if access != domain.AccessAdmin || status != domain.StatusInactive {
		t.Errorf("the row holds %q/%q", access, status)
	}
}

func TestUpdateUserAccessAndStatusAppliesOneField(t *testing.T) {
	db := useTempDB(t)
	seedUser(t, db, "alice", domain.AccessGuest, domain.StatusActive)

	if _, err := UpdateUserAccessAndStatus("uid-alice", domain.AccessAdmin, ""); err != nil {
		t.Fatalf("UpdateUserAccessAndStatus: %v", err)
	}

	var access, status string
	if err := db.QueryRow(`SELECT access, status FROM users WHERE userId='uid-alice'`).
		Scan(&access, &status); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if access != domain.AccessAdmin {
		t.Errorf("access = %q", access)
	}
	if status != domain.StatusActive {
		t.Errorf("status = %q; an empty value appears to clear the column now", status)
	}
}

// TestTheReturnedUserIsThePreUpdateRowWithTheChangesPatchedIn records that the
// row is read before the update and the new values are written into the map
// afterwards, rather than being read back. A trigger or a default that changed
// something else would not show up in the response.
func TestTheReturnedUserIsThePreUpdateRowWithTheChangesPatchedIn(t *testing.T) {
	db := useTempDB(t)
	seedUser(t, db, "alice", domain.AccessGuest, domain.StatusActive)

	user, err := UpdateUserAccessAndStatus("uid-alice", domain.AccessAdmin, "")
	if err != nil {
		t.Fatalf("UpdateUserAccessAndStatus: %v", err)
	}
	if user["access"] != domain.AccessAdmin {
		t.Errorf("access = %v, want the patched value", user["access"])
	}
	if user["name"] != "Name alice" {
		t.Errorf("name = %v, want the pre-update value", user["name"])
	}
}

func TestUpdateUserAccessAndStatusRefusesARequestThatChangesNothing(t *testing.T) {
	db := useTempDB(t)
	seedUser(t, db, "alice", domain.AccessGuest, domain.StatusActive)

	_, err := UpdateUserAccessAndStatus("uid-alice", "", "")
	if !errors.Is(err, ErrNothingToUpdate) {
		t.Errorf("UpdateUserAccessAndStatus = %v, want ErrNothingToUpdate", err)
	}
}

func TestUpdateUserAccessAndStatusOnAnUnknownUser(t *testing.T) {
	useTempDB(t)

	_, err := UpdateUserAccessAndStatus("nobody", domain.AccessAdmin, "")
	if !errors.Is(err, ErrNoSuchUser) {
		t.Errorf("UpdateUserAccessAndStatus = %v, want ErrNoSuchUser", err)
	}
}

// TestNullColumnsGetTheirFallbacks records the substitutions the read applies:
// an absent avatar, userId or email becomes the empty string and an absent
// status becomes "active". A user whose status was cleared therefore reads as
// active and can sign in (see IsDeactivated).
func TestNullColumnsGetTheirFallbacks(t *testing.T) {
	db := useTempDB(t)
	if _, err := db.Exec(
		`INSERT INTO users (username, password, name, access, userId) 
		 VALUES ('bob', 'secret', 'Bob', 'guest', 'uid-bob')`); err != nil {
		t.Fatalf("insert user: %v", err)
	}
	if _, err := db.Exec(`UPDATE users SET status = NULL WHERE userId='uid-bob'`); err != nil {
		t.Fatalf("clear status: %v", err)
	}

	user, err := UpdateUserAccessAndStatus("uid-bob", domain.AccessAdmin, "")
	if err != nil {
		t.Fatalf("UpdateUserAccessAndStatus: %v", err)
	}
	if user["status"] != domain.StatusActive {
		t.Errorf("status = %v for a NULL column, want the active fallback", user["status"])
	}
	if user["avatar"] != "" || user["email"] != "" {
		t.Errorf("avatar/email = %v/%v, want the empty fallbacks", user["avatar"], user["email"])
	}
}

func TestUpdateUserAccessAndStatusReportsEachStage(t *testing.T) {
	t.Run("missing table", func(t *testing.T) {
		emptyIdentityDB(t)

		_, err := UpdateUserAccessAndStatus("uid", domain.AccessAdmin, "")
		var fault *Fault
		if !errors.As(err, &fault) {
			t.Fatalf("UpdateUserAccessAndStatus = %v, want a Fault", err)
		}
		if fault.Stage != StageQuery {
			t.Errorf("stage = %q, want %q", fault.Stage, StageQuery)
		}
	})

	t.Run("unopenable database", func(t *testing.T) {
		unopenableIdentityDB(t)

		_, err := UpdateUserAccessAndStatus("uid", domain.AccessAdmin, "")
		var fault *Fault
		if !errors.As(err, &fault) {
			t.Fatalf("UpdateUserAccessAndStatus = %v, want a Fault", err)
		}
		if fault.Stage != StageConnect {
			t.Errorf("stage = %q, want %q", fault.Stage, StageConnect)
		}
	})
}

func TestDeleteUserRemovesTheRow(t *testing.T) {
	db := useTempDB(t)
	seedUser(t, db, "alice", domain.AccessGuest, domain.StatusActive)

	if err := DeleteUser("uid-alice"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("%d users survived", count)
	}
}

func TestDeleteUserOnAnUnknownUser(t *testing.T) {
	useTempDB(t)

	if err := DeleteUser("nobody"); !errors.Is(err, ErrNoSuchUser) {
		t.Errorf("DeleteUser = %v, want ErrNoSuchUser", err)
	}
}

func TestDeleteUserReportsEachStage(t *testing.T) {
	t.Run("missing table", func(t *testing.T) {
		emptyIdentityDB(t)

		var fault *Fault
		if !errors.As(DeleteUser("uid"), &fault) {
			t.Fatal("DeleteUser did not return a Fault")
		}
		if fault.Stage != StageCheck {
			t.Errorf("stage = %q, want %q", fault.Stage, StageCheck)
		}
	})

	t.Run("unopenable database", func(t *testing.T) {
		unopenableIdentityDB(t)

		var fault *Fault
		if !errors.As(DeleteUser("uid"), &fault) {
			t.Fatal("DeleteUser did not return a Fault")
		}
		if fault.Stage != StageConnect {
			t.Errorf("stage = %q, want %q", fault.Stage, StageConnect)
		}
	})
}

func TestFaultCarriesItsStageAndCause(t *testing.T) {
	f := faultAt(StageQuery, sql.ErrNoRows)

	if f.Stage != StageQuery {
		t.Errorf("Stage = %q", f.Stage)
	}
	if !errors.Is(f, sql.ErrNoRows) {
		t.Error("errors.Is does not see through the Fault")
	}
	if got := f.Error(); got != StageQuery+": "+sql.ErrNoRows.Error() {
		t.Errorf("Error = %q", got)
	}
	if f.Unwrap() != sql.ErrNoRows {
		t.Error("Unwrap did not return the cause")
	}
}

func TestNullOr(t *testing.T) {
	if got := nullOr(sql.NullString{String: "x", Valid: true}, "fallback"); got != "x" {
		t.Errorf("nullOr(valid) = %q", got)
	}
	if got := nullOr(sql.NullString{}, "fallback"); got != "fallback" {
		t.Errorf("nullOr(NULL) = %q", got)
	}
	if got := nullOr(sql.NullString{String: "", Valid: true}, "fallback"); got != "" {
		t.Errorf("nullOr(valid empty) = %q, want the empty string not the fallback", got)
	}
}
