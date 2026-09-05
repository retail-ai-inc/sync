package app

import (
	"errors"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

func TestLoginAcceptsAStoredPassword(t *testing.T) {
	db := useTempDB(t)
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
	if _, _, ok := domain.ParseUserToken(token); !ok {
		t.Error("the minted token does not verify")
	}
}

func TestLoginRejectsAWrongPassword(t *testing.T) {
	db := useTempDB(t)
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
}

// The identity used to live in one package variable shared by every request,
// so a failed attempt by one caller downgraded an administrator working in
// another tab to a guest.
func TestOneCallersFailedLoginDoesNotAffectAnother(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "admin", "adminpw", "Admin", "admin")

	ok, _, token, err := Login("admin", "adminpw")
	if err != nil || !ok {
		t.Fatalf("Login(admin) = %v, %v", ok, err)
	}

	// A different caller fails to sign in.
	if ok, _, _, _ := Login("admin", "wrong"); ok {
		t.Fatal("the second login succeeded")
	}

	if got := IdentifyFromHeader(token); got != "admin" {
		t.Errorf("the first caller's token proves %q after somebody else's failed "+
			"login, want admin", got)
	}
}

func TestLoginOnAnUnknownUser(t *testing.T) {
	useTempDB(t)

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

	if _, _, _, err := Login("alice", "secret"); err == nil {
		t.Error("Login on a database with no tables returned no error")
	}
}

// TestLogoutDoesNotSignAnybodyElseOut records what logging out means now that
// the token is the identity: the client discards it, and nothing on the server
// changes.
func TestLogoutDoesNotSignAnybodyElseOut(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	_, _, token, err := Login("alice", "secret")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	Logout()

	if got := IdentifyFromHeader(token); got != "alice" {
		t.Errorf("another caller's token proves %q after a logout, want alice", got)
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
// claim: validation regenerates a token for every user in the table and
// compares.
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
// password.
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

// TestUpdatingAnUnknownUsersPasswordSaysSo covers a mistyped username.
func TestUpdatingAnUnknownUsersPasswordSaysSo(t *testing.T) {
	useTempDB(t)

	if err := infraUpdateUserPassword("nobody", "new"); err == nil {
		t.Error("UpdateUserPassword on an unknown user reported success")
	}
}

func TestResolvePasswordChangeIdentity(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	token := domain.GenerateUserToken("alice", "admin")

	t.Run("a token names the identity", func(t *testing.T) {
		username, ok := ResolvePasswordChangeIdentity("Bearer " + token)
		if !ok || username != "alice" {
			t.Errorf("ResolvePasswordChangeIdentity = %q/%v", username, ok)
		}
	})

	t.Run("no token", func(t *testing.T) {
		if _, ok := ResolvePasswordChangeIdentity(""); ok {
			t.Error("ResolvePasswordChangeIdentity accepted a caller with nothing")
		}
	})

	t.Run("an unreadable token", func(t *testing.T) {
		if _, ok := ResolvePasswordChangeIdentity("Bearer nonsense"); ok {
			t.Error("ResolvePasswordChangeIdentity accepted an unreadable credential")
		}
	})
}

// TestAPasswordCannotBeChangedWithNoTokenAtAll is the fix for the fallback that
// let a caller presenting nothing change the password of whoever the process
// had last authenticated.
func TestAPasswordCannotBeChangedWithNoTokenAtAll(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	if _, _, _, err := Login("alice", "secret"); err != nil {
		t.Fatalf("Login: %v", err)
	}

	if _, ok := ResolvePasswordChangeIdentity(""); ok {
		t.Fatal("a caller with no token was given an identity to change the " +
			"password of")
	}
}
