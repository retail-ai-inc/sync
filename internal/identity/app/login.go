package app

import (
	"errors"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

// Login authenticates a username and password and mints a token on success.
//
// It no longer records anything process-wide. The session used to be a single
// package variable shared by every request, so a login on one connection
// changed who a concurrent request was treated as; the token the caller gets
// back is the whole of the identity now.
func Login(username, password string) (ok bool, access, token string, err error) {
	valid, userAccess, err := infra.ValidateUser(username, password)
	if err != nil {
		return false, "", "", err
	}
	if !valid {
		return false, domain.AccessGuest, "", nil
	}
	return true, userAccess, domain.GenerateUserToken(username, userAccess), nil
}

// Logout is what the client calls when it discards its token. There is no
// server-side session to end: the token is the identity, and it expires by
// itself. It used to clear the one session the whole process shared, which
// signed everybody out.
func Logout() {}

// ValidateUserToken reports whether a token proves an identity the store still
// recognises, and for whom. The token itself names the user now, so the store
// is consulted once rather than being scanned for a user whose derived token
// happens to match.
func ValidateUserToken(token string) (bool, string, string) {
	username, access, ok := domain.ParseUserToken(token)
	if !ok {
		return false, "", ""
	}

	user, err := infra.GetUserByUsername(username)
	if err != nil || user == nil {
		return false, "", ""
	}
	if status, _ := user["status"].(string); domain.IsDeactivated(status) {
		return false, "", ""
	}
	stored, _ := user["access"].(string)
	if stored != access {
		return false, "", ""
	}
	return true, username, access
}

// IdentifyFromHeader resolves the username an Authorization header proves, or
// the empty string when it proves nothing.
func IdentifyFromHeader(authHeader string) string {
	if authHeader == "" {
		return ""
	}
	valid, username, _ := ValidateUserToken(domain.ExtractTokenFromHeader(authHeader))
	if !valid {
		return ""
	}
	return username
}

func CurrentUser(username string) (map[string]interface{}, error) {
	return infra.GetUserData(username)
}

// Errors a password change can end in. The handler maps each to a distinct
// status code and message, so they have to stay distinguishable.
var (
	ErrPasswordLookup   = errors.New("could not verify the old password")
	ErrPasswordMismatch = errors.New("the old password is incorrect")
)

// ChangePassword replaces a user's password after checking the old one.
// A returned ErrPasswordLookup means the store failed; ErrPasswordMismatch
// means the old password did not match; any other error came from the update.
func ChangePassword(username, oldPassword, newPassword string) error {
	valid, _, err := infra.ValidateUser(username, oldPassword)
	if err != nil {
		return ErrPasswordLookup
	}
	if !valid {
		return ErrPasswordMismatch
	}
	return infra.UpdateUserPassword(username, newPassword)
}

// ResolvePasswordChangeIdentity decides whose password a request may change:
// the identity its token proves, and nothing else.
//
// It used to fall back to the process-wide session, so a request carrying no
// token at all could change the password of whoever had last signed in.
func ResolvePasswordChangeIdentity(authHeader string) (string, bool) {
	username := IdentifyFromHeader(authHeader)
	if username == "" {
		return "", false
	}
	return username, true
}

// The messages the Google callback answers with. They are response text rather
// than internal errors, so they live next to the flow that produces them.
const (
	msgInvalidData     = "Invalid Google authentication data"
	msgNoConfig        = "Please configure Google OAuth information first"
	msgNoClientID      = "Please set Google OAuth Client ID in the admin panel first"
	msgNoClientSecret  = "Please set Google OAuth Client Secret in the admin panel first"
	msgNoRedirectURI   = "Please set Google OAuth Redirect URI in the admin panel first"
	msgTokenRequest    = "Failed to get Google Token"
	msgTokenDecode     = "Failed to parse Google Token"
	msgUserInfoRequest = "Failed to get Google user information"
	msgUserInfoDecode  = "Failed to parse Google user information"
	msgVerifyAccount   = "Failed to verify user account status"
	msgAccountInactive = "Your account has been deactivated. Please contact your system administrator for assistance."
	msgSaveUserPrefix  = "Failed to save user information: "
)

// The calls that reach Google, replaced in tests.
var (
	exchangeGoogleCode = infra.ExchangeGoogleCode
	fetchGoogleUser    = infra.FetchGoogleUser
)

// GoogleLogin exchanges a Google authorization code for an identity, returning
// the access level and a freshly minted token, or the message the caller should
// answer with. Like Login it records nothing process-wide.
func GoogleLogin(code string) (authority, token, errorMessage string) {
	if code == "" {
		return "", "", msgInvalidData
	}

	config, err := infra.GetAuthConfig(domain.ProviderGoogle)
	if err != nil {
		return "", "", msgNoConfig
	}

	creds, missing := domain.ReadOAuthCredentials(config)
	switch missing {
	case domain.FieldClientID:
		return "", "", msgNoClientID
	case domain.FieldClientSecret:
		return "", "", msgNoClientSecret
	case domain.FieldRedirectURI:
		return "", "", msgNoRedirectURI
	}

	accessToken, err := exchangeGoogleCode(creds.ClientID, creds.ClientSecret, creds.RedirectURI, code)
	if err != nil {
		if errors.Is(err, infra.ErrTokenDecode) {
			return "", "", msgTokenDecode
		}
		return "", "", msgTokenRequest
	}

	email, name, err := fetchGoogleUser(accessToken)
	if err != nil {
		if errors.Is(err, infra.ErrUserInfoDecode) {
			return "", "", msgUserInfoDecode
		}
		return "", "", msgUserInfoRequest
	}

	username, userAccess, err := infra.SaveGoogleUser(email, name)
	if err != nil {
		return "", "", msgSaveUserPrefix + err.Error()
	}

	user, err := infra.GetUserByUsername(username)
	if err != nil {
		return "", "", msgVerifyAccount
	}
	if status, ok := user["status"].(string); ok && domain.IsDeactivated(status) {
		return "", "", msgAccountInactive
	}

	return userAccess, domain.GenerateUserToken(username, userAccess), ""
}
