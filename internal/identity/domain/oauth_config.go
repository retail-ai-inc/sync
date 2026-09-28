package domain

// OAuthCredentials are the three values an OAuth provider needs before an
// authorization code can be exchanged for a token. They were previously
// pulled out of a map[string]interface{} with three near-identical type
// assertions inside the callback handler.
type OAuthCredentials struct {
	ClientID     string
	ClientSecret string
	RedirectURI  string
}

// Provider names this system knows how to talk to.
const ProviderGoogle = "google"

// Fields the stored configuration uses. Named here so that the reader and the
// writer cannot drift apart.
const (
	FieldClientID     = "clientId"
	FieldClientSecret = "clientSecret"
	FieldRedirectURI  = "redirectUri"
	FieldEnabled      = "enabled"
)

// ReadOAuthCredentials pulls the credentials out of a stored configuration.
// missing names the first field that is absent or not a non-empty string, in
// the order the callback handler checked them, and is empty when all three are
// present.
func ReadOAuthCredentials(config map[string]interface{}) (c OAuthCredentials, missing string) {
	id, ok := config[FieldClientID].(string)
	if !ok || id == "" {
		return c, FieldClientID
	}
	secret, ok := config[FieldClientSecret].(string)
	if !ok || secret == "" {
		return c, FieldClientSecret
	}
	uri, ok := config[FieldRedirectURI].(string)
	if !ok || uri == "" {
		return c, FieldRedirectURI
	}
	return OAuthCredentials{ClientID: id, ClientSecret: secret, RedirectURI: uri}, ""
}
