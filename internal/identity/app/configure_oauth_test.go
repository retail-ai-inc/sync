package app

import (
	"errors"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

func TestReadOAuthConfig(t *testing.T) {
	db := useTempDB(t)
	if _, err := db.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled)
		 VALUES ('google', '{"clientId":"id","clientSecret":"secret","redirectUri":"uri"}', 1)`); err != nil {
		t.Fatalf("insert config: %v", err)
	}

	got, err := ReadOAuthConfig(domain.ProviderGoogle)
	if err != nil {
		t.Fatalf("ReadOAuthConfig: %v", err)
	}
	if got[domain.FieldClientID] != "id" {
		t.Errorf("clientId = %v", got[domain.FieldClientID])
	}
	if got[domain.FieldEnabled] != true {
		t.Errorf("enabled = %v, want true", got[domain.FieldEnabled])
	}
}

// The endpoint has to stay reachable without a token, because the sign-in page
// needs the client id before anybody has one, so answering with the secret as
// well handed the whole OAuth credential to any caller that could reach the
// port.
func TestTheClientSecretIsMaskedOnTheWayOut(t *testing.T) {
	db := useTempDB(t)
	if _, err := db.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled)
		 VALUES ('google', '{"clientId":"id","clientSecret":"top-secret","redirectUri":"uri"}', 1)`); err != nil {
		t.Fatalf("insert config: %v", err)
	}

	got, err := ReadOAuthConfig(domain.ProviderGoogle)
	if err != nil {
		t.Fatalf("ReadOAuthConfig: %v", err)
	}
	if got[domain.FieldClientSecret] == "top-secret" {
		t.Fatal("the client secret is still served in full")
	}
	if got[domain.FieldClientID] != "id" {
		t.Errorf("clientId = %v, want it kept: the sign-in page needs it",
			got[domain.FieldClientID])
	}
}

// TestAMaskedSecretDoesNotOverwriteTheStoredOne covers the round trip a UI
// makes: it reads the configuration, changes something else, and writes it
// back. Storing the mask would destroy the credential.
func TestAMaskedSecretDoesNotOverwriteTheStoredOne(t *testing.T) {
	db := useTempDB(t)
	if _, err := db.Exec(
		`INSERT INTO auth_configs (provider, config_json, enabled)
		 VALUES ('google', '{"clientId":"id","clientSecret":"top-secret","redirectUri":"uri"}', 1)`); err != nil {
		t.Fatalf("insert config: %v", err)
	}

	roundTripped, err := ReadOAuthConfig(domain.ProviderGoogle)
	if err != nil {
		t.Fatalf("ReadOAuthConfig: %v", err)
	}
	roundTripped[domain.FieldEnabled] = true
	roundTripped[domain.FieldRedirectURI] = "https://new.example.com/callback"

	if err := WriteOAuthConfig(domain.ProviderGoogle, roundTripped); err != nil {
		t.Fatalf("WriteOAuthConfig: %v", err)
	}

	var stored string
	if err := db.QueryRow(
		`SELECT config_json FROM auth_configs WHERE provider='google'`).Scan(&stored); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if !strings.Contains(stored, "top-secret") {
		t.Errorf("the stored secret was replaced by the mask: %s", stored)
	}
	if !strings.Contains(stored, "https://new.example.com/callback") {
		t.Errorf("the change that was actually made did not land: %s", stored)
	}
}

func TestReadOAuthConfigOnAnUnknownProvider(t *testing.T) {
	useTempDB(t)

	_, err := ReadOAuthConfig("facebook")
	if !errors.Is(err, ErrNoOAuthConfig) {
		t.Errorf("ReadOAuthConfig = %v, want ErrNoOAuthConfig", err)
	}
}

func TestReadOAuthConfigReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	_, err := ReadOAuthConfig(domain.ProviderGoogle)
	if err == nil {
		t.Fatal("ReadOAuthConfig on a database with no tables returned no error")
	}
	if errors.Is(err, ErrNoOAuthConfig) {
		t.Error("a missing table was reported as a missing configuration")
	}
}

func TestWriteOAuthConfigStoresTheDocument(t *testing.T) {
	db := useTempDB(t)

	cfg := map[string]interface{}{
		domain.FieldClientID:     "id",
		domain.FieldClientSecret: "secret",
		domain.FieldRedirectURI:  "uri",
		domain.FieldEnabled:      true,
	}
	if err := WriteOAuthConfig(domain.ProviderGoogle, cfg); err != nil {
		t.Fatalf("WriteOAuthConfig: %v", err)
	}

	var stored string
	if err := db.QueryRow(`SELECT config_json FROM auth_configs WHERE provider='google'`).
		Scan(&stored); err != nil {
		t.Fatalf("read config_json: %v", err)
	}
	if stored == "" {
		t.Error("nothing was stored")
	}
}

// TestAnEnabledGoogleProviderGetsGoogleDefaults records that the write path fills
// in Google's fixed endpoint URLs and the default scopes, and that it does so by
// mutating the caller's map.
func TestAnEnabledGoogleProviderGetsGoogleDefaults(t *testing.T) {
	useTempDB(t)

	cfg := map[string]interface{}{
		domain.FieldClientID:     "id",
		domain.FieldClientSecret: "secret",
		domain.FieldRedirectURI:  "uri",
		domain.FieldEnabled:      true,
	}
	if err := WriteOAuthConfig(domain.ProviderGoogle, cfg); err != nil {
		t.Fatalf("WriteOAuthConfig: %v", err)
	}

	if cfg["authUri"] != "https://accounts.google.com/o/oauth2/auth" {
		t.Errorf("authUri = %v", cfg["authUri"])
	}
	if cfg["tokenUri"] != "https://oauth2.googleapis.com/token" {
		t.Errorf("tokenUri = %v", cfg["tokenUri"])
	}
	scopes, ok := cfg["scopes"].([]string)
	if !ok || len(scopes) != 2 {
		t.Errorf("scopes = %v", cfg["scopes"])
	}
}

// TestSuppliedScopesAreKept records that the defaulting only fills in scopes
// that are absent, so a caller can widen them.
func TestSuppliedScopesAreKept(t *testing.T) {
	useTempDB(t)

	cfg := map[string]interface{}{
		domain.FieldClientID:     "id",
		domain.FieldClientSecret: "secret",
		domain.FieldRedirectURI:  "uri",
		domain.FieldEnabled:      true,
		"scopes":                 []string{"email"},
	}
	if err := WriteOAuthConfig(domain.ProviderGoogle, cfg); err != nil {
		t.Fatalf("WriteOAuthConfig: %v", err)
	}

	if scopes, _ := cfg["scopes"].([]string); len(scopes) != 1 {
		t.Errorf("scopes = %v, want the supplied one", cfg["scopes"])
	}
}

func TestWriteOAuthConfigRefusesAnIncompleteEnabledProvider(t *testing.T) {
	useTempDB(t)

	for _, tt := range []struct {
		name  string
		cfg   map[string]interface{}
		field string
	}{
		{"no client id", map[string]interface{}{
			domain.FieldEnabled: true}, domain.FieldClientID},
		{"no secret", map[string]interface{}{
			domain.FieldClientID: "id", domain.FieldEnabled: true}, domain.FieldClientSecret},
		{"no redirect uri", map[string]interface{}{
			domain.FieldClientID: "id", domain.FieldClientSecret: "s",
			domain.FieldEnabled: true}, domain.FieldRedirectURI},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := WriteOAuthConfig(domain.ProviderGoogle, tt.cfg)

			var missing *MissingOAuthFieldError
			if !errors.As(err, &missing) {
				t.Fatalf("WriteOAuthConfig = %v, want a MissingOAuthFieldError", err)
			}
			if missing.Field != tt.field {
				t.Errorf("Field = %q, want %q", missing.Field, tt.field)
			}
			if got := missing.Error(); got != "Missing necessary configuration field: "+tt.field {
				t.Errorf("Error = %q", got)
			}
		})
	}
}

// A disabled one can be stored with nothing in it, and enabling it later goes
// through this same path, so the gap closes on the way in.
func TestADisabledProviderIsStoredWithoutValidation(t *testing.T) {
	useTempDB(t)

	if err := WriteOAuthConfig(domain.ProviderGoogle,
		map[string]interface{}{domain.FieldEnabled: false}); err != nil {
		t.Fatalf("WriteOAuthConfig = %v; a disabled provider appears to be validated "+
			"now, so assert that instead", err)
	}
}

// TestANonGoogleProviderIsNeverValidated records that the check is keyed on the
// provider name, so a provider called anything else is stored unvalidated even
// when enabled — and nothing else in the system knows how to use one.
func TestANonGoogleProviderIsNeverValidated(t *testing.T) {
	useTempDB(t)

	if err := WriteOAuthConfig("facebook",
		map[string]interface{}{domain.FieldEnabled: true}); err != nil {
		t.Fatalf("WriteOAuthConfig(facebook) = %v; other providers appear to be "+
			"validated now, so assert that instead", err)
	}
}

func TestWriteOAuthConfigReportsAStoreFailure(t *testing.T) {
	emptyIdentityDB(t)

	err := WriteOAuthConfig(domain.ProviderGoogle,
		map[string]interface{}{domain.FieldEnabled: false})
	if err == nil {
		t.Error("WriteOAuthConfig on a database with no tables returned no error")
	}
}

func TestAuthoriseAdmin(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	insertUser(t, db, "bob", "secret", "Bob", domain.AccessGuest)

	for _, tt := range []struct {
		name         string
		header       string
		wantSupplied bool
		wantAdmin    bool
	}{
		{"no header", "", false, false},
		{"admin token", "Bearer " + domain.GenerateUserToken("alice", domain.AccessAdmin), true, true},
		{"guest token", domain.GenerateUserToken("bob", domain.AccessGuest), true, false},
		{"garbage", "Bearer nonsense", true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			supplied, isAdmin := AuthoriseAdmin(tt.header)
			if supplied != tt.wantSupplied || isAdmin != tt.wantAdmin {
				t.Errorf("AuthoriseAdmin = %v/%v, want %v/%v",
					supplied, isAdmin, tt.wantSupplied, tt.wantAdmin)
			}
		})
	}
}

// TestAdminAuthorisationReadsTheAccessLevelNotTheUsername records that unlike
// the admin-token endpoint, this check accepts any user whose access level is
// admin.
func TestAdminAuthorisationReadsTheAccessLevelNotTheUsername(t *testing.T) {
	db := useTempDB(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)
	token := domain.GenerateUserToken("alice", domain.AccessAdmin)

	_, isAdmin := AuthoriseAdmin(token)
	if !isAdmin {
		t.Fatal("AuthoriseAdmin refused an admin-level user; the two admin checks " +
			"appear to agree now, so assert the shared rule")
	}

	if _, ok := AdminToken("Bearer " + token); !ok {
		t.Error("AdminToken refused the same credential this check accepted; the " +
			"two appear to disagree in the other direction now")
	}
}

// It used to answer with the whole stored document, client secret included;
// the secret is masked now, and the rest is an allow-list — so a field added
// to the document later does not start being served to the world because
// nobody remembered to add it to a deny-list.
func TestOnlyTheSignInFieldsAreServed(t *testing.T) {
	useTempDB(t)

	if err := WriteOAuthConfig("google", map[string]interface{}{
		"clientId":     "cid",
		"clientSecret": "very-secret",
		"redirectUri":  "https://example/callback",
		"enabled":      true,
	}); err != nil {
		t.Fatalf("WriteOAuthConfig: %v", err)
	}

	got, err := ReadOAuthConfig("google")
	if err != nil {
		t.Fatalf("ReadOAuthConfig: %v", err)
	}

	if got["clientId"] != "cid" {
		t.Errorf("clientId = %v, want it served", got["clientId"])
	}
	if got["clientSecret"] == "very-secret" {
		t.Fatal("the client secret was served to an unauthenticated caller")
	}

	// Anything the document grows later stays in until it is listed.
	if err := WriteOAuthConfig("google", map[string]interface{}{
		"clientId":       "cid",
		"clientSecret":   "very-secret",
		"redirectUri":    "https://example/callback",
		"enabled":        true,
		"internalApiKey": "should-not-leak",
	}); err != nil {
		t.Fatalf("WriteOAuthConfig: %v", err)
	}

	got, err = ReadOAuthConfig("google")
	if err != nil {
		t.Fatalf("ReadOAuthConfig: %v", err)
	}
	if _, leaked := got["internalApiKey"]; leaked {
		t.Errorf("an unlisted field was served: %v", got)
	}
}
