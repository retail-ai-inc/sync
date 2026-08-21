package domain

import "testing"

func TestReadOAuthCredentials(t *testing.T) {
	full := map[string]interface{}{
		FieldClientID:     "id",
		FieldClientSecret: "secret",
		FieldRedirectURI:  "https://example.test/cb",
	}

	got, missing := ReadOAuthCredentials(full)

	if missing != "" {
		t.Fatalf("missing = %q for a complete configuration", missing)
	}
	if got.ClientID != "id" || got.ClientSecret != "secret" ||
		got.RedirectURI != "https://example.test/cb" {
		t.Errorf("credentials = %+v", got)
	}
}

func TestReadOAuthCredentialsReportsTheFirstMissingField(t *testing.T) {
	base := func() map[string]interface{} {
		return map[string]interface{}{
			FieldClientID:     "id",
			FieldClientSecret: "secret",
			FieldRedirectURI:  "uri",
		}
	}

	for _, tt := range []struct {
		name    string
		mutate  func(map[string]interface{})
		missing string
	}{
		{"no client id", func(m map[string]interface{}) { delete(m, FieldClientID) }, FieldClientID},
		{"empty client id", func(m map[string]interface{}) { m[FieldClientID] = "" }, FieldClientID},
		{"non-string client id", func(m map[string]interface{}) { m[FieldClientID] = 1 }, FieldClientID},
		{"no secret", func(m map[string]interface{}) { delete(m, FieldClientSecret) }, FieldClientSecret},
		{"empty secret", func(m map[string]interface{}) { m[FieldClientSecret] = "" }, FieldClientSecret},
		{"no redirect uri", func(m map[string]interface{}) { delete(m, FieldRedirectURI) }, FieldRedirectURI},
		{"empty redirect uri", func(m map[string]interface{}) { m[FieldRedirectURI] = "" }, FieldRedirectURI},
	} {
		t.Run(tt.name, func(t *testing.T) {
			m := base()
			tt.mutate(m)

			_, missing := ReadOAuthCredentials(m)
			if missing != tt.missing {
				t.Errorf("missing = %q, want %q", missing, tt.missing)
			}
		})
	}
}

// TestOnlyTheFirstMissingFieldIsReported records that a configuration with
// nothing set at all is reported as missing the client id alone. An operator
// filling the form in has to submit three times to learn about three fields.
func TestOnlyTheFirstMissingFieldIsReported(t *testing.T) {
	_, missing := ReadOAuthCredentials(map[string]interface{}{})

	if missing != FieldClientID {
		t.Fatalf("missing = %q for an empty configuration, want %q — all missing "+
			"fields appear to be reported now, so assert that instead", missing, FieldClientID)
	}
}

// TestANonStringFieldIsReportedAsMissingRatherThanInvalid records that a
// configuration whose clientId is a number is described as absent. The operator
// is told to set a value that is already there.
func TestANonStringFieldIsReportedAsMissingRatherThanInvalid(t *testing.T) {
	_, missing := ReadOAuthCredentials(map[string]interface{}{
		FieldClientID:     12345,
		FieldClientSecret: "secret",
		FieldRedirectURI:  "uri",
	})

	if missing != FieldClientID {
		t.Fatalf("missing = %q for a numeric clientId; the type error appears to be "+
			"distinguished now, so assert that", missing)
	}
}

func TestReadOAuthCredentialsOnANilMap(t *testing.T) {
	got, missing := ReadOAuthCredentials(nil)

	if missing != FieldClientID {
		t.Errorf("missing = %q for a nil map, want %q", missing, FieldClientID)
	}
	if got != (OAuthCredentials{}) {
		t.Errorf("credentials = %+v for a nil map, want the zero value", got)
	}
}

func TestTheOAuthFieldNamesAreTheStoredOnes(t *testing.T) {
	// These strings are the keys of the stored document and the request body the
	// UI sends. Renaming one silently breaks both.
	if FieldClientID != "clientId" || FieldClientSecret != "clientSecret" ||
		FieldRedirectURI != "redirectUri" || FieldEnabled != "enabled" {
		t.Errorf("the stored field names changed: %q %q %q %q",
			FieldClientID, FieldClientSecret, FieldRedirectURI, FieldEnabled)
	}
	if ProviderGoogle != "google" {
		t.Errorf("ProviderGoogle = %q, want google", ProviderGoogle)
	}
}
