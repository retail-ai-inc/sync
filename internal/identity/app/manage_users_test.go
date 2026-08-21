package app

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

func TestListUsersPaginatesAndReportsTheTotal(t *testing.T) {
	db := useTempDB(t)
	for _, name := range []string{"a", "b", "c"} {
		insertUser(t, db, name, "secret", "User "+name, "guest")
	}

	page, total, err := ListUsers(1, 2)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if len(page) != 2 {
		t.Errorf("page holds %d users, want 2", len(page))
	}
	if _, ok := page[0]["password"]; ok {
		t.Error("the page exposes the password")
	}
}

func TestListUsersOnAnEmptyTable(t *testing.T) {
	useTempDB(t)

	page, total, err := ListUsers(1, 10)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if total != 0 || len(page) != 0 {
		t.Errorf("ListUsers = %d users, total %d", len(page), total)
	}
}

func TestListUsersReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	if _, _, err := ListUsers(1, 10); err == nil {
		t.Error("ListUsers on a database with no tables returned no error")
	}
}

// TestTheTotalCountsEveryUserNotThePage records that the total is the size of
// the whole table while the page is a slice of it, so a UI can page through
// correctly — and that the total is computed after the store has already loaded
// every row into memory.
func TestTheTotalCountsEveryUserNotThePage(t *testing.T) {
	db := useTempDB(t)
	for _, name := range []string{"a", "b", "c", "d", "e"} {
		insertUser(t, db, name, "secret", "User", "guest")
	}

	page, total, err := ListUsers(2, 2)
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if total != 5 {
		t.Errorf("total = %d, want 5", total)
	}
	if len(page) != 2 {
		t.Errorf("page holds %d users, want 2", len(page))
	}
}

func TestChangeUserAccessRefusesAnUnknownLevel(t *testing.T) {
	useTempDB(t)

	_, rejection, err := ChangeUserAccess("uid", "superuser", "")
	if err != nil {
		t.Fatalf("ChangeUserAccess: %v", err)
	}
	if rejection != RejectInvalidAccess {
		t.Errorf("rejection = %q, want %q", rejection, RejectInvalidAccess)
	}
}

func TestChangeUserAccessRefusesAnUnknownStatus(t *testing.T) {
	useTempDB(t)

	_, rejection, err := ChangeUserAccess("uid", "admin", "archived")
	if err != nil {
		t.Fatalf("ChangeUserAccess: %v", err)
	}
	if rejection != RejectInvalidStatus {
		t.Errorf("rejection = %q, want %q", rejection, RejectInvalidStatus)
	}
}

// TestTheValidationRunsBeforeTheStoreIsTouched records that an invalid level is
// refused without opening the database, so a bad request costs nothing and
// cannot report a store failure.
func TestTheValidationRunsBeforeTheStoreIsTouched(t *testing.T) {
	unopenableIdentityDB(t)

	_, rejection, err := ChangeUserAccess("uid", "superuser", "")
	if err != nil {
		t.Fatalf("ChangeUserAccess = %v; the store appears to be reached before the "+
			"validation now, so assert that instead", err)
	}
	if rejection != RejectInvalidAccess {
		t.Errorf("rejection = %q", rejection)
	}
}

func TestChangeUserAccessOnAnUnknownUser(t *testing.T) {
	useTempDB(t)

	_, rejection, err := ChangeUserAccess("nobody", domain.AccessAdmin, "")
	if err != nil {
		t.Fatalf("ChangeUserAccess: %v", err)
	}
	if rejection != RejectNoSuchUser {
		t.Errorf("rejection = %q, want %q", rejection, RejectNoSuchUser)
	}
}

func TestChangeUserAccessRefusesARequestThatChangesNothing(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "guest")
	if _, err := db.Exec(`UPDATE users SET userId='uid-alice' WHERE username='alice'`); err != nil {
		t.Fatalf("set userId: %v", err)
	}

	_, rejection, err := ChangeUserAccess("uid-alice", "", "")
	if err != nil {
		t.Fatalf("ChangeUserAccess: %v", err)
	}
	if rejection != RejectNothingToUpdate {
		t.Errorf("rejection = %q, want %q", rejection, RejectNothingToUpdate)
	}
}

func TestChangeUserAccessAppliesBothFields(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "guest")
	if _, err := db.Exec(`UPDATE users SET userId='uid-alice' WHERE username='alice'`); err != nil {
		t.Fatalf("set userId: %v", err)
	}

	user, rejection, err := ChangeUserAccess("uid-alice", domain.AccessAdmin, domain.StatusInactive)
	if err != nil || rejection != "" {
		t.Fatalf("ChangeUserAccess = %v / %q", err, rejection)
	}
	if user["access"] != domain.AccessAdmin {
		t.Errorf("access = %v, want admin", user["access"])
	}
	if user["status"] != domain.StatusInactive {
		t.Errorf("status = %v, want inactive", user["status"])
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

// TestOnlyTheNamedFieldsAreWritten records that an empty value leaves the column
// alone rather than clearing it, so a request that changes only the status keeps
// the access level.
func TestOnlyTheNamedFieldsAreWritten(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	if _, err := db.Exec(`UPDATE users SET userId='uid-alice' WHERE username='alice'`); err != nil {
		t.Fatalf("set userId: %v", err)
	}

	if _, rejection, err := ChangeUserAccess("uid-alice", "", domain.StatusInactive); err != nil || rejection != "" {
		t.Fatalf("ChangeUserAccess = %v / %q", err, rejection)
	}

	var access string
	if err := db.QueryRow(`SELECT access FROM users WHERE userId='uid-alice'`).Scan(&access); err != nil {
		t.Fatalf("read access: %v", err)
	}
	if access != domain.AccessAdmin {
		t.Errorf("access = %q; an empty value appears to clear the column now", access)
	}
}

// TestDeactivatingAUserDoesNotEndTheirSession records that setting a user
// inactive changes only the row: the process-wide session keeps whatever
// identity it held, so a caller already signed in as that user stays signed in.
func TestDeactivatingAUserDoesNotEndTheirSession(t *testing.T) {
	db := useTempDB(t)
	resetSession(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	if _, err := db.Exec(`UPDATE users SET userId='uid-alice' WHERE username='alice'`); err != nil {
		t.Fatalf("set userId: %v", err)
	}
	if ok, _, _, err := Login("alice", "secret"); err != nil || !ok {
		t.Fatalf("Login = %v, %v", ok, err)
	}

	if _, rejection, err := ChangeUserAccess("uid-alice", "", domain.StatusInactive); err != nil || rejection != "" {
		t.Fatalf("ChangeUserAccess = %v / %q", err, rejection)
	}

	if domain.Current().Username() != "alice" {
		t.Fatalf("the session was cleared; deactivation appears to sign the user out " +
			"now, so assert that instead")
	}
}

func TestChangeUserAccessReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	_, _, err := ChangeUserAccess("uid", domain.AccessAdmin, "")
	if err == nil {
		t.Error("ChangeUserAccess on a database with no tables returned no error")
	}
}

func TestRemoveUser(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "guest")
	if _, err := db.Exec(`UPDATE users SET userId='uid-alice' WHERE username='alice'`); err != nil {
		t.Fatalf("set userId: %v", err)
	}

	rejection, err := RemoveUser("uid-alice")
	if err != nil || rejection != "" {
		t.Fatalf("RemoveUser = %v / %q", err, rejection)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 0 {
		t.Errorf("%d users survived", count)
	}
}

func TestRemoveUserOnAnUnknownUser(t *testing.T) {
	useTempDB(t)

	rejection, err := RemoveUser("nobody")
	if err != nil {
		t.Fatalf("RemoveUser: %v", err)
	}
	if rejection != RejectNoSuchUser {
		t.Errorf("rejection = %q, want %q", rejection, RejectNoSuchUser)
	}
}

// TestTheLastAdminCanBeRemoved records that nothing stops the only
// administrator from being deleted. Once the row is gone no caller can mint an
// admin token, and the only way back in is editing the database by hand.
func TestTheLastAdminCanBeRemoved(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "admin", "secret", "Admin", domain.AccessAdmin)
	if _, err := db.Exec(`UPDATE users SET userId='uid-admin' WHERE username='admin'`); err != nil {
		t.Fatalf("set userId: %v", err)
	}

	rejection, err := RemoveUser("uid-admin")
	if err != nil {
		t.Fatalf("RemoveUser: %v", err)
	}
	if rejection != "" {
		t.Fatalf("rejection = %q; removing the last admin appears to be refused now, "+
			"so assert that instead", rejection)
	}
}

func TestRemoveUserReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	if _, err := RemoveUser("uid"); err == nil {
		t.Error("RemoveUser on a database with no tables returned no error")
	}
}
