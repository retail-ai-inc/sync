package app

import (
	"fmt"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
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

// TestAFailedGoogleLoginAffectsNobodyElse records what a failure means now that
// the token is the whole identity: it produces no token and touches nothing.
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

const completeGoogleConfig = `{"clientId":"id","clientSecret":"secret","redirectUri":"https://sync.test/callback"}`

// googleAnswers stands in for Google; the exchange itself is covered in internal/identity/infra.
func googleAnswers(t *testing.T,
	exchange func(clientID, clientSecret, redirectURI, code string) (string, error),
	fetch func(accessToken string) (email, name string, err error)) {
	t.Helper()

	previousExchange, previousFetch := exchangeGoogleCode, fetchGoogleUser
	exchangeGoogleCode, fetchGoogleUser = exchange, fetch
	t.Cleanup(func() { exchangeGoogleCode, fetchGoogleUser = previousExchange, previousFetch })
}

func googleKnows(t *testing.T, email, name string) {
	t.Helper()

	googleAnswers(t,
		func(clientID, clientSecret, redirectURI, code string) (string, error) {
			if clientID != "id" || clientSecret != "secret" ||
				redirectURI != "https://sync.test/callback" || code != "a-code" {
				t.Errorf("exchanged %q/%q/%q/%q, want the stored credentials and the caller's code",
					clientID, clientSecret, redirectURI, code)
			}
			return "google-token", nil
		},
		func(accessToken string) (string, string, error) {
			if accessToken != "google-token" {
				t.Errorf("asked for the user with %q, want the exchanged token", accessToken)
			}
			return email, name, nil
		})
}

func insertGoogleUser(t *testing.T, username, email, access, status string) {
	t.Helper()

	if _, err := currentDB(t).Exec(
		`INSERT INTO users (username, password, name, email, access, status) VALUES (?, 'x', ?, ?, ?, ?)`,
		username, username, email, access, status); err != nil {
		t.Fatalf("insert user %q: %v", username, err)
	}
}

func countUsers(t *testing.T) int {
	t.Helper()

	var n int
	if err := currentDB(t).QueryRow(`SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		t.Fatalf("count users: %v", err)
	}
	return n
}

// A first Google sign-in that is not a guest, or whose token does not validate, is a wrong or broken account.
func TestAFirstGoogleSignInCreatesAGuestWithAWorkingToken(t *testing.T) {
	useTempDB(t)
	storeGoogleConfig(t, completeGoogleConfig)
	googleKnows(t, "a@x.test", "A")

	authority, token, msg := GoogleLogin("a-code")

	if msg != "" || authority != domain.AccessGuest {
		t.Fatalf("GoogleLogin = %q/%q, want a guest sign-in", authority, msg)
	}
	if valid, username, access := ValidateUserToken(token); !valid || username != "a@x.test" || access != domain.AccessGuest {
		t.Errorf("ValidateUserToken = %v/%q/%q, want the new guest", valid, username, access)
	}
	if n := countUsers(t); n != 1 {
		t.Errorf("%d users after the first sign-in, want 1", n)
	}
}

// Signing in as a new guest instead of the account the email belongs to strands its owner at the wrong access.
func TestGoogleLoginLinksAnExistingAdminByEmail(t *testing.T) {
	useTempDB(t)
	storeGoogleConfig(t, completeGoogleConfig)
	insertGoogleUser(t, "alice", "a@x.test", domain.AccessAdmin, domain.StatusActive)
	googleKnows(t, "a@x.test", "Alice")

	authority, token, msg := GoogleLogin("a-code")

	if msg != "" || authority != domain.AccessAdmin {
		t.Fatalf("GoogleLogin = %q/%q, want the linked administrator", authority, msg)
	}
	if valid, username, access := ValidateUserToken(token); !valid || username != "alice" || access != domain.AccessAdmin {
		t.Errorf("ValidateUserToken = %v/%q/%q, want alice as admin", valid, username, access)
	}
	if n := countUsers(t); n != 1 {
		t.Errorf("%d users, want the existing one and no other", n)
	}
}

// A deactivated account given a token by Google sign-in is let back in.
func TestGoogleLoginRefusesADeactivatedAccount(t *testing.T) {
	useTempDB(t)
	storeGoogleConfig(t, completeGoogleConfig)
	insertGoogleUser(t, "alice", "a@x.test", domain.AccessAdmin, domain.StatusInactive)
	googleKnows(t, "a@x.test", "Alice")

	authority, token, msg := GoogleLogin("a-code")

	if msg != msgAccountInactive {
		t.Errorf("message = %q, want %q", msg, msgAccountInactive)
	}
	if authority != "" || token != "" {
		t.Errorf("authority/token = %q/%q for a deactivated account", authority, token)
	}
}

// A failure answered with another stage's message, or with a token, misleads whoever is signing in.
func TestGoogleLoginAnswersEachExchangeFailureWithItsOwnMessage(t *testing.T) {
	for _, tt := range []struct {
		name        string
		exchangeErr error
		fetchErr    error
		want        string
	}{
		{"token refused", fmt.Errorf("%w: 400 Bad Request", infra.ErrTokenRequest), nil, msgTokenRequest},
		{"token unreadable", fmt.Errorf("%w: no access token", infra.ErrTokenDecode), nil, msgTokenDecode},
		{"user info refused", nil, fmt.Errorf("%w: 401 Unauthorized", infra.ErrUserInfoRequest), msgUserInfoRequest},
		{"user info unreadable", nil, fmt.Errorf("%w: no email", infra.ErrUserInfoDecode), msgUserInfoDecode},
	} {
		t.Run(tt.name, func(t *testing.T) {
			useTempDB(t)
			storeGoogleConfig(t, completeGoogleConfig)
			googleAnswers(t,
				func(string, string, string, string) (string, error) { return "google-token", tt.exchangeErr },
				func(string) (string, string, error) { return "a@x.test", "A", tt.fetchErr })

			authority, token, msg := GoogleLogin("a-code")

			if msg != tt.want {
				t.Errorf("message = %q, want %q", msg, tt.want)
			}
			if authority != "" || token != "" {
				t.Errorf("authority/token = %q/%q for a failed exchange", authority, token)
			}
			if n := countUsers(t); n != 0 {
				t.Errorf("%d users were created by a failed exchange", n)
			}
		})
	}
}

// A store failure while saving the user must refuse the sign-in rather than mint a token for nobody.
func TestGoogleLoginReportsAStoreFailureWhileSavingTheUser(t *testing.T) {
	db := useTempDB(t)
	storeGoogleConfig(t, completeGoogleConfig)
	if _, err := db.Exec(`ALTER TABLE users RENAME TO gone`); err != nil {
		t.Fatalf("rename users: %v", err)
	}
	googleKnows(t, "a@x.test", "A")

	authority, token, msg := GoogleLogin("a-code")

	if !strings.HasPrefix(msg, msgSaveUserPrefix) {
		t.Errorf("message = %q, want it to start with %q", msg, msgSaveUserPrefix)
	}
	if authority != "" || token != "" {
		t.Errorf("authority/token = %q/%q after a store failure", authority, token)
	}
}

// A user saved but not found again must be refused, not handed a token for a row that is not there.
func TestGoogleLoginRefusesAUserItCannotReadBack(t *testing.T) {
	db := useTempDB(t)
	storeGoogleConfig(t, completeGoogleConfig)
	if _, err := db.Exec(`CREATE TRIGGER vanish AFTER INSERT ON users
		BEGIN DELETE FROM users WHERE id = NEW.id; END`); err != nil {
		t.Fatalf("create trigger: %v", err)
	}
	googleKnows(t, "a@x.test", "A")

	authority, token, msg := GoogleLogin("a-code")

	if msg != msgVerifyAccount {
		t.Errorf("message = %q, want %q", msg, msgVerifyAccount)
	}
	if authority != "" || token != "" {
		t.Errorf("authority/token = %q/%q for a user that cannot be read back", authority, token)
	}
}
