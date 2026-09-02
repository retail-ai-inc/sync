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
	if err != nil {
		return nil, err
	}
	return withoutClientSecret(config), nil
}

// maskedSecret stands in for a stored client secret on the way out. It is a
// fixed string, so it says nothing about the value it hides, and the write path
// recognises it as "leave the stored one alone".
const maskedSecret = "********"

// publicOAuthFields are the fields the read endpoint answers with. A list of
// what may go out, not a list of what may not.
var publicOAuthFields = []string{
	domain.FieldClientID,
	domain.FieldRedirectURI,
	domain.FieldEnabled,
}

// withoutClientSecret copies out the fields a sign-in page needs. The client
// secret is the one that matters: answering with it handed the whole OAuth
// credential to any unauthenticated caller.
func withoutClientSecret(config map[string]interface{}) map[string]interface{} {
	if config == nil {
		return nil
	}

	safe := make(map[string]interface{}, len(publicOAuthFields)+1)
	for _, field := range publicOAuthFields {
		if value, present := config[field]; present {
			safe[field] = value
		}
	}
	if secret, ok := config[domain.FieldClientSecret].(string); ok && secret != "" {
		safe[domain.FieldClientSecret] = maskedSecret
	}
	return safe
}

type MissingOAuthFieldError struct{ Field string }

func (e *MissingOAuthFieldError) Error() string {
	return fmt.Sprintf("Missing necessary configuration field: %s", e.Field)
}

// WriteOAuthConfig stores a provider's configuration. An enabled Google
// provider must carry all three credentials, and gets Google's fixed endpoint
// URLs and the default scopes filled in.
func WriteOAuthConfig(provider string, config map[string]interface{}) error {
	enabled, _ := config[domain.FieldEnabled].(bool)

	// The read endpoint masks the client secret, so a UI that reads the
	// configuration and writes it back sends the mask. Keeping the stored value
	// in that case is what stops the round trip from destroying the credential.
	if secret, _ := config[domain.FieldClientSecret].(string); secret == maskedSecret {
		stored, err := infra.GetAuthConfig(provider)
		if err != nil {
			return err
		}
		config[domain.FieldClientSecret], _ = stored[domain.FieldClientSecret].(string)
	}

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
