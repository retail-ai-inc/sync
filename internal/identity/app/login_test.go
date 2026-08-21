package app

import (
	"errors"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

// resetSession clears the process-wide session and restores it afterwards, so
// these tests do not leak an identity into each other.
func resetSession(t *testing.T) {
	t.Helper()

	prevUser, prevAccess := domain.Current().Username(), domain.Current().Access()
	t.Cleanup(func() { domain.Current().Authenticate(prevUser, prevAccess) })
	domain.Current().Clear()
}

func TestLoginAcceptsAStoredPassword(t *testing.T) {
	db := useTempDB(t)
	resetSession(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	ok, access, token, err := Login("alice", "secret")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if !ok {
		t.Fatal("Login rejected a correct password")
	}
	if access != "admin" {
		t.Errorf("access = %q, want admin", access)
	}
	if token == "" {
		t.Error("no token was minted")
	}
	if domain.Current().Username() != "alice" || domain.Current().Access() != "admin" {
		t.Errorf("the session holds %q/%q", domain.Current().Username(), domain.Current().Access())
	}
}

func TestLoginRejectsAWrongPassword(t *testing.T) {
	db := useTempDB(t)
	resetSession(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	ok, access, token, err := Login("alice", "wrong")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if ok {
		t.Fatal("Login accepted a wrong password")
	}
	if access != domain.AccessGuest {
		t.Errorf("access = %q, want %q", access, domain.AccessGuest)
	}
	if token != "" {
		t.Errorf("a token was minted for a rejected login: %q", token)
	}
	if domain.Current().Access() != domain.AccessGuest {
		t.Errorf("the session holds %q, want guest", domain.Current().Access())
	}
}

// TestARejectedLoginDowngradesWhoeverWasSignedIn records T-070 from the login
// side: a failed attempt by one caller replaces the process-wide identity, so
// an administrator working in another tab becomes a guest because somebody
// mistyped a password.
func TestARejectedLoginDowngradesWhoeverWasSignedIn(t *testing.T) {
	db := useTempDB(t)
	resetSession(t)
	insertUser(t, db, "admin", "adminpw", "Admin", "admin")

	if ok, _, _, err := Login("admin", "adminpw"); err != nil || !ok {
		t.Fatalf("Login(admin) = %v, %v", ok, err)
	}
	if !domain.Current().IsAdmin() {
		t.Fatal("the admin did not sign in")
	}

	// A different caller fails to sign in.
	if ok, _, _, _ := Login("admin", "wrong"); ok {
		t.Fatal("the second login succeeded")
	}

	if domain.Current().IsAdmin() {
		t.Fatal("the admin session survived somebody else's failed login; the session " +
			"appears to be per-request now, so assert that instead")
	}
}

func TestLoginOnAnUnknownUser(t *testing.T) {
	useTempDB(t)
	resetSession(t)

	ok, _, _, err := Login("nobody", "secret")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if ok {
		t.Error("Login accepted an unknown user")
	}
}

func TestLoginReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)
	resetSession(t)

	if _, _, _, err := Login("alice", "secret"); err == nil {
		t.Error("Login on a database with no tables returned no error")
	}
}

func TestLogoutClearsTheSession(t *testing.T) {
	resetSession(t)
	domain.Current().Authenticate("alice", "admin")

	Logout()

	if domain.Current().Username() != "" || domain.Current().Access() != "" {
		t.Errorf("the session holds %q/%q after Logout",
			domain.Current().Username(), domain.Current().Access())
	}
}

func TestIdentifyFromHeader(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	token := domain.GenerateUserToken("alice", "admin")

	for _, tt := range []struct {
		name   string
		header string
		want   string
	}{
		{"bearer token", "Bearer " + token, "alice"},
		{"bare token", token, "alice"},
		{"empty header", "", ""},
		{"garbage", "Bearer nonsense", ""},
		{"another user's token", domain.GenerateUserToken("bob", "admin"), ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := IdentifyFromHeader(tt.header); got != tt.want {
				t.Errorf("IdentifyFromHeader = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestATokenIsOnlyValidWhileItsUserRowExists records that a token carries no
// claim: validation regenerates a token for every user in the table and compares.
// Deleting the user therefore invalidates a token that has not expired, and a
// change to the user's access level does too.
func TestATokenIsOnlyValidWhileItsUserRowExists(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	token := domain.GenerateUserToken("alice", "admin")

	if got := IdentifyFromHeader(token); got != "alice" {
		t.Fatalf("IdentifyFromHeader = %q before the change", got)
	}

	if _, err := db.Exec(`UPDATE users SET access='guest' WHERE username='alice'`); err != nil {
		t.Fatalf("update access: %v", err)
	}

	if got := IdentifyFromHeader(token); got != "" {
		t.Fatalf("IdentifyFromHeader = %q after the access level changed; tokens "+
			"appear to carry a claim now, so assert that instead", got)
	}
}

func TestValidateUserTokenReportsTheAccessLevel(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "guest")

	valid, username, access := ValidateUserToken(domain.GenerateUserToken("alice", "guest"))
	if !valid {
		t.Fatal("ValidateUserToken rejected a token it minted")
	}
	if username != "alice" || access != "guest" {
		t.Errorf("ValidateUserToken = %q/%q", username, access)
	}
}

func TestValidateUserTokenOnAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	valid, _, _ := ValidateUserToken("anything")
	if valid {
		t.Error("ValidateUserToken accepted a token with no user table")
	}
}

func TestCurrentUserReturnsTheProfile(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	got, err := CurrentUser("alice")
	if err != nil {
		t.Fatalf("CurrentUser: %v", err)
	}
	if got["name"] != "Alice" || got["access"] != "admin" {
		t.Errorf("CurrentUser = %v", got)
	}
	if _, ok := got["password"]; ok {
		t.Error("CurrentUser exposed the password")
	}
}

func TestCurrentUserOnAnUnknownUser(t *testing.T) {
	useTempDB(t)

	if _, err := CurrentUser("nobody"); err == nil {
		t.Error("CurrentUser on an unknown user returned no error")
	}
}

func TestAdminTokenRequiresTheAdminSession(t *testing.T) {
	resetSession(t)

	if _, ok := AdminToken(); ok {
		t.Error("AdminToken issued a token with no session")
	}

	domain.Current().Authenticate("alice", "admin")
	if _, ok := AdminToken(); ok {
		t.Error("AdminToken issued a token to an admin-level non-admin user")
	}

	domain.Current().Authenticate("admin", "admin")
	token, ok := AdminToken()
	if !ok {
		t.Fatal("AdminToken refused the admin session")
	}
	if !domain.ValidateAdminToken(token) {
		t.Error("the minted token does not validate")
	}
}

// TestAdminTokenNeedsNoCredential records T-070 at its sharpest: once anybody
// has signed in as admin, this use case hands an admin token to any caller,
// because the only thing it checks is the process-wide session.
func TestAdminTokenNeedsNoCredential(t *testing.T) {
	resetSession(t)
	domain.Current().Authenticate("admin", "admin")

	token, ok := AdminToken()
	if !ok {
		t.Fatal("AdminToken refused the admin session")
	}
	if token == "" {
		t.Fatal("an empty token was issued")
	}
}

func TestChangePassword(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	if err := ChangePassword("alice", "secret", "newsecret"); err != nil {
		t.Fatalf("ChangePassword: %v", err)
	}

	ok, _, err := infraValidateUser("alice", "newsecret")
	if err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !ok {
		t.Error("the new password does not work")
	}
}

func TestChangePasswordRejectsAWrongOldPassword(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")

	if err := ChangePassword("alice", "wrong", "newsecret"); !errors.Is(err, ErrPasswordMismatch) {
		t.Errorf("ChangePassword = %v, want ErrPasswordMismatch", err)
	}
}

// TestChangePasswordOnAnUnknownUserIsAMismatchNotAMissingUser records that a
// username that is not in the table is reported the same way as a wrong
// password. A caller cannot tell the two apart, which is the right answer for a
// login form and the wrong one for an administrator's tooling.
func TestChangePasswordOnAnUnknownUserIsAMismatchNotAMissingUser(t *testing.T) {
	useTempDB(t)

	if err := ChangePassword("nobody", "secret", "new"); !errors.Is(err, ErrPasswordMismatch) {
		t.Fatalf("ChangePassword = %v; the two cases appear to be distinguished now, "+
			"so assert that instead", err)
	}
}

func TestChangePasswordReportsALookupFailure(t *testing.T) {
	emptyIdentityDB(t)

	if err := ChangePassword("alice", "secret", "new"); !errors.Is(err, ErrPasswordLookup) {
		t.Errorf("ChangePassword = %v, want ErrPasswordLookup", err)
	}
}

// TestUpdatingAnUnknownUsersPasswordIsSilent records that the write itself does
// not check that it changed a row, so the only thing standing between a caller
// and a silent no-op is the old-password check above.
func TestUpdatingAnUnknownUsersPasswordIsSilent(t *testing.T) {
	useTempDB(t)

	if err := infraUpdateUserPassword("nobody", "new"); err != nil {
		t.Fatalf("UpdateUserPassword on an unknown user = %v; the rows affected count "+
			"appears to be checked now, so assert the error instead", err)
	}
}

func TestResolvePasswordChangeIdentity(t *testing.T) {
	db := useTempDB(t)
	resetSession(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	token := domain.GenerateUserToken("alice", "admin")

	t.Run("a token names the identity", func(t *testing.T) {
		username, ok := ResolvePasswordChangeIdentity("Bearer " + token)
		if !ok || username != "alice" {
			t.Errorf("ResolvePasswordChangeIdentity = %q/%v", username, ok)
		}
	})

	t.Run("no token and no session", func(t *testing.T) {
		domain.Current().Clear()
		if _, ok := ResolvePasswordChangeIdentity(""); ok {
			t.Error("ResolvePasswordChangeIdentity accepted a caller with nothing")
		}
	})

	t.Run("no token but a session", func(t *testing.T) {
		domain.Current().Authenticate("bob", "admin")
		username, ok := ResolvePasswordChangeIdentity("")
		if !ok || username != "bob" {
			t.Errorf("ResolvePasswordChangeIdentity = %q/%v", username, ok)
		}
	})

	t.Run("a guest session is not an identity", func(t *testing.T) {
		domain.Current().Reject()
		if _, ok := ResolvePasswordChangeIdentity(""); ok {
			t.Error("ResolvePasswordChangeIdentity accepted a guest")
		}
	})
}

// TestAPasswordCanBeChangedWithNoTokenAtAll records the consequence of falling
// back to the session: a caller who presents nothing changes the password of
// whoever the process last authenticated.
func TestAPasswordCanBeChangedWithNoTokenAtAll(t *testing.T) {
	db := useTempDB(t)
	resetSession(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	domain.Current().Authenticate("alice", "admin")

	username, ok := ResolvePasswordChangeIdentity("")
	if !ok {
		t.Fatal("ResolvePasswordChangeIdentity refused a caller with no token; the " +
			"fallback appears to be gone, so assert that instead")
	}
	if err := ChangePassword(username, "secret", "hijacked"); err != nil {
		t.Fatalf("ChangePassword: %v", err)
	}
}
