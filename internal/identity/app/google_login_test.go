package app

import (
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

func storeGoogleConfig(t *testing.T, cfg string) {
	t.Helper()

	db := currentDB(t)
	if _, err := db.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled) VALUES ('google', ?, 1)`,
		cfg); err != nil {
		t.Fatalf("insert config: %v", err)
	}
}

func TestGoogleLoginRefusesAnEmptyCode(t *testing.T) {
	useTempDB(t)

	authority, token, msg := GoogleLogin("")

	if msg != "Invalid Google authentication data" {
		t.Errorf("message = %q", msg)
	}
	if authority != "" || token != "" {
		t.Errorf("authority/token = %q/%q for a refused code", authority, token)
	}
}

// TestAFailedGoogleLoginAffectsNobodyElse records what a failure means now
// that the token is the whole identity: it produces no token and touches
// nothing.
func TestAFailedGoogleLoginAffectsNobodyElse(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", "admin")
	_, _, existing, err := Login("alice", "secret")
	if err != nil {
		t.Fatalf("Login: %v", err)
	}

	// No configuration is stored, so this fails at the second branch.
	if _, _, msg := GoogleLogin("a-code"); msg != "Please configure Google OAuth information first" {
		t.Fatalf("message = %q, want the missing-configuration one", msg)
	}

	if got := IdentifyFromHeader(existing); got != "alice" {
		t.Errorf("an unrelated caller's token proves %q after a failed Google "+
			"sign-in, want alice", got)
	}
}

func TestGoogleLoginWithoutAStoredConfiguration(t *testing.T) {
	useTempDB(t)

	_, _, msg := GoogleLogin("a-code")
	if msg != "Please configure Google OAuth information first" {
		t.Errorf("message = %q", msg)
	}
}

func TestGoogleLoginReportsEachMissingCredential(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  string
		want string
	}{
		{"no client id", `{}`,
			"Please set Google OAuth Client ID in the admin panel first"},
		{"empty client id", `{"clientId":""}`,
			"Please set Google OAuth Client ID in the admin panel first"},
		{"no secret", `{"clientId":"id"}`,
			"Please set Google OAuth Client Secret in the admin panel first"},
		{"no redirect uri", `{"clientId":"id","clientSecret":"secret"}`,
			"Please set Google OAuth Redirect URI in the admin panel first"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			useTempDB(t)
			storeGoogleConfig(t, tt.cfg)

			_, _, msg := GoogleLogin("a-code")
			if msg != tt.want {
				t.Errorf("message = %q, want %q", msg, tt.want)
			}
		})
	}
}

// TestAnEmptyGoogleIdentityStillReachesTheStore covers what the store does
// with the empty identity Google's error responses used to leave behind — a
// row whose username and email are both empty, found again by the same empty
// name, and a guest token minted for it.
func TestAnEmptyGoogleIdentityStillReachesTheStore(t *testing.T) {
	useTempDB(t)

	username, access, err := infraSaveGoogleUser("", "")
	if err != nil {
		t.Fatalf("SaveGoogleUser(\"\", \"\") = %v; empty input appears to be refused "+
			"now, so assert that instead", err)
	}
	if username != "" || access != domain.AccessGuest {
		t.Fatalf("SaveGoogleUser = %q/%q, want the empty username and guest",
			username, access)
	}

	user, err := infraGetUserByUsername(username)
	if err != nil {
		t.Fatalf("the row SaveGoogleUser inserted cannot be read back: %v", err)
	}
	if status, ok := user["status"].(string); ok && domain.IsDeactivated(status) {
		t.Fatal("the new row is deactivated; new Google users appear to start " +
			"inactive now, so assert that instead")
	}

	token := domain.GenerateUserToken(username, access)
	valid, gotUser, gotAccess := ValidateUserToken(token)
	if !valid {
		t.Fatal("the token minted for the empty identity does not validate; the token " +
			"check appears to reject empty usernames now, so assert that instead")
	}
	if gotUser != "" || gotAccess != domain.AccessGuest {
		t.Errorf("ValidateUserToken = %q/%q", gotUser, gotAccess)
	}
}

// The exchange itself is covered in internal/identity/infra, against a stand-in
// server. The two Google URLs used to be unexported constants, so there was no
// seam at all and testing the exchange meant reaching the real Google.
