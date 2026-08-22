package infra

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// The stages of the Google exchange that can fail. The callback answers with a
// different message for each, so they have to stay distinguishable.
var (
	ErrTokenRequest    = errors.New("google token request failed")
	ErrTokenDecode     = errors.New("google token response could not be parsed")
	ErrUserInfoRequest = errors.New("google user info request failed")
	ErrUserInfoDecode  = errors.New("google user info response could not be parsed")
)

// The endpoints are variables rather than constants so a test can point them at
// a server of its own. They used to be unexported constants, which left no seam
// at all: the only way to exercise the exchange was to call Google.
var (
	googleTokenURL    = "https://oauth2.googleapis.com/token"
	googleUserInfoURL = "https://www.googleapis.com/oauth2/v2/userinfo"
)

// ExchangeGoogleCode trades an authorization code for an access token.
//
// The status code is what says whether the trade happened. It used to be
// ignored: Google answers a code it does not recognise with 400 and an error
// document, PostForm reports no error for that, and the document decodes
// perfectly well into a struct of two strings — both empty. The caller then
// asked for the user's details with an empty bearer token, got a 401 and another
// error document, decoded that just as cleanly, and carried on with an empty
// email and an empty name. SaveGoogleUser wrote a row for that empty identity
// and a usable token came back. Anyone could post {"code":"x"} and be logged in.
func ExchangeGoogleCode(clientID, clientSecret, redirectURI, code string) (string, error) {
	data := url.Values{}
	data.Set("code", code)
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	data.Set("redirect_uri", redirectURI)
	data.Set("grant_type", "authorization_code")

	tokenResp, err := http.PostForm(googleTokenURL, data)
	if err != nil {
		return "", ErrTokenRequest
	}
	defer tokenResp.Body.Close()

	body, err := io.ReadAll(tokenResp.Body)
	if err != nil {
		return "", ErrTokenRequest
	}
	if tokenResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%w: %s: %s", ErrTokenRequest,
			tokenResp.Status, firstLine(body))
	}

	var tokenData struct {
		AccessToken string `json:"access_token"`
		IDToken     string `json:"id_token"`
	}
	if err := json.Unmarshal(body, &tokenData); err != nil {
		return "", ErrTokenDecode
	}
	if tokenData.AccessToken == "" {
		// A 200 with no token in it is not an exchange either.
		return "", fmt.Errorf("%w: the response carried no access token", ErrTokenDecode)
	}
	return tokenData.AccessToken, nil
}

// FetchGoogleUser reports the email and name Google holds for an access token.
func FetchGoogleUser(accessToken string) (email, name string, err error) {
	if strings.TrimSpace(accessToken) == "" {
		return "", "", fmt.Errorf("%w: no access token to ask with", ErrUserInfoRequest)
	}

	req, err := http.NewRequest("GET", googleUserInfoURL, nil)
	if err != nil {
		return "", "", ErrUserInfoRequest
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)

	client := &http.Client{}
	userInfoResp, err := client.Do(req)
	if err != nil {
		return "", "", ErrUserInfoRequest
	}
	defer userInfoResp.Body.Close()

	body, err := io.ReadAll(userInfoResp.Body)
	if err != nil {
		return "", "", ErrUserInfoRequest
	}
	if userInfoResp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("%w: %s: %s", ErrUserInfoRequest,
			userInfoResp.Status, firstLine(body))
	}

	var userData struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if err := json.Unmarshal(body, &userData); err != nil {
		return "", "", ErrUserInfoDecode
	}
	if userData.Email == "" {
		// An identity with no email is not one this can create an account for.
		return "", "", fmt.Errorf("%w: the response named no email address", ErrUserInfoDecode)
	}
	return userData.Email, userData.Name, nil
}

// firstLine trims a response body down to something a log line can carry.
func firstLine(body []byte) string {
	text := strings.TrimSpace(string(body))
	if index := strings.IndexAny(text, "\r\n"); index >= 0 {
		text = text[:index]
	}
	if len(text) > 200 {
		text = text[:200] + "…"
	}
	return text
}
