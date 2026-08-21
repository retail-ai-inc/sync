package app

import (
	"errors"
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

// TestTheClientSecretIsReturnedInFull records that the read path hands back the
// stored document as it is, secret included. Combined with the endpoint needing
// no credentials, that publishes the OAuth client secret.
func TestTheClientSecretIsReturnedInFull(t *testing.T) {
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
	if got[domain.FieldClientSecret] != "top-secret" {
		t.Fatalf("clientSecret = %v; the secret appears to be redacted now, so assert "+
			"that instead", got[domain.FieldClientSecret])
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

// TestADisabledProviderIsStoredWithoutValidation records that the completeness
// check only runs for an enabled Google provider. A disabled one can be stored
// with nothing in it, and enabling it later goes through this same path, so the
// gap closes on the way in.
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
// admin. The two admin checks in this context disagree about what an
// administrator is.
func TestAdminAuthorisationReadsTheAccessLevelNotTheUsername(t *testing.T) {
	db := useTempDB(t)
	resetSession(t)
	insertUser(t, db, "alice", "secret", "Alice", domain.AccessAdmin)

	_, isAdmin := AuthoriseAdmin(domain.GenerateUserToken("alice", domain.AccessAdmin))
	if !isAdmin {
		t.Fatal("AuthoriseAdmin refused an admin-level user; the two admin checks " +
			"appear to agree now, so assert the shared rule")
	}

	domain.Current().Authenticate("alice", domain.AccessAdmin)
	if _, ok := AdminToken(); ok {
		t.Error("AdminToken accepted the same user this check accepted")
	}
}
