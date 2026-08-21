package app

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

// ErrNoOAuthConfig means no configuration is stored for the provider. The read
// endpoint answers that with a 200 and success:false, not a 404.
var ErrNoOAuthConfig = errors.New("no oauth configuration stored for the provider")

// ReadOAuthConfig returns the stored configuration of a provider.
//
// The configuration includes the client secret. That the read endpoint needs no
// credentials while the write endpoint demands an admin token is a recorded
// defect, not a design; it is not changed here.
func ReadOAuthConfig(provider string) (map[string]interface{}, error) {
	config, err := infra.GetAuthConfig(provider)
	if err == sql.ErrNoRows {
		return nil, ErrNoOAuthConfig
	}
	return config, err
}

// MissingOAuthFieldError names a required field an enabled provider is missing.
type MissingOAuthFieldError struct{ Field string }

func (e *MissingOAuthFieldError) Error() string {
	return fmt.Sprintf("Missing necessary configuration field: %s", e.Field)
}

// WriteOAuthConfig stores a provider's configuration. An enabled Google
// provider must carry all three credentials, and gets Google's fixed endpoint
// URLs and the default scopes filled in.
func WriteOAuthConfig(provider string, config map[string]interface{}) error {
	enabled, _ := config[domain.FieldEnabled].(bool)

	if provider == domain.ProviderGoogle && enabled {
		for _, field := range []string{domain.FieldClientID, domain.FieldClientSecret, domain.FieldRedirectURI} {
			value, exists := config[field].(string)
			if !exists || value == "" {
				return &MissingOAuthFieldError{Field: field}
			}
		}

		config["authUri"] = "https://accounts.google.com/o/oauth2/auth"
		config["tokenUri"] = "https://oauth2.googleapis.com/token"
		if _, exists := config["scopes"]; !exists {
			config["scopes"] = []string{"email", "profile"}
		}
	}

	return infra.UpdateAuthConfig(provider, config)
}

// AuthoriseAdmin reports whether an Authorization header proves the admin
// identity, and whether a header was supplied at all. The write endpoint
// answers a missing header with 401 and a non-admin one with 403.
func AuthoriseAdmin(authHeader string) (supplied, isAdmin bool) {
	if authHeader == "" {
		return false, false
	}
	valid, _, userAccess := ValidateUserToken(domain.ExtractTokenFromHeader(authHeader))
	return true, valid && userAccess == domain.AccessAdmin
}
