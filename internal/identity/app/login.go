package app

import (
	"errors"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
)

// Login authenticates a username and password. On success it records the
// identity on the process session and mints a user token; on failure it marks
// the session as a guest. The boolean says which of the two happened.
func Login(username, password string) (ok bool, access, token string, err error) {
	valid, userAccess, err := infra.ValidateUser(username, password)
	if err != nil {
		return false, "", "", err
	}
	if !valid {
		domain.Current().Reject()
		return false, domain.AccessGuest, "", nil
	}
	domain.Current().Authenticate(username, userAccess)
	return true, userAccess, domain.GenerateUserToken(username, userAccess), nil
}

// Logout discards the identity the process holds.
func Logout() { domain.Current().Clear() }

// ValidateUserToken reports whether a token is one of today's valid user
// tokens, and for whom. It compares against every user in the store rather
// than carrying a claim, so it needs the store.
func ValidateUserToken(token string) (bool, string, string) {
	users, err := infra.GetAllUsers()
	if err != nil {
		fmt.Println("Error getting users:", err)
		return false, "", ""
	}

	for _, user := range users {
		username := user["username"].(string)
		accessLevel := user["access"].(string)

		if domain.GenerateUserToken(username, accessLevel) == token {
			return true, username, accessLevel
		}
	}
	return false, "", ""
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

// CurrentUser returns the stored profile of a username.
func CurrentUser(username string) (map[string]interface{}, error) {
	return infra.GetUserData(username)
}

// AdminToken mints an admin token if the process session is the admin. The
// session is process-wide, so this grants a token to whoever asks once anybody
// has logged in as admin (T-070).
func AdminToken() (string, bool) {
	if !domain.Current().IsAdmin() {
		return "", false
	}
	return domain.GenerateAdminToken(), true
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
// the identity its token proves, or failing that the process session's
// username. It reports false when neither is available.
func ResolvePasswordChangeIdentity(authHeader string) (string, bool) {
	if username := IdentifyFromHeader(authHeader); username != "" {
		return username, true
	}
	if !domain.Current().IsAuthenticated() {
		return "", false
	}
	return domain.Current().Username(), true
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

// GoogleLogin exchanges a Google authorization code for an identity. On
// success it records the identity on the process session and returns the
// access level with a freshly minted token. On any failure it marks the
// session as a guest and returns the message the caller should answer with.
func GoogleLogin(code string) (authority, token, errorMessage string) {
	if code == "" {
		domain.Current().Reject()
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

	accessToken, err := infra.ExchangeGoogleCode(creds.ClientID, creds.ClientSecret, creds.RedirectURI, code)
	if err != nil {
		if errors.Is(err, infra.ErrTokenDecode) {
			return "", "", msgTokenDecode
		}
		return "", "", msgTokenRequest
	}

	email, name, err := infra.FetchGoogleUser(accessToken)
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

	domain.Current().Authenticate(username, userAccess)
	return userAccess, domain.GenerateUserToken(username, userAccess), ""
}
