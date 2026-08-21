package infra

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
)

// The stages of the Google exchange that can fail. The callback answers with a
// different message for each, so they have to stay distinguishable.
var (
	ErrTokenRequest    = errors.New("google token request failed")
	ErrTokenDecode     = errors.New("google token response could not be parsed")
	ErrUserInfoRequest = errors.New("google user info request failed")
	ErrUserInfoDecode  = errors.New("google user info response could not be parsed")
)

const (
	googleTokenURL    = "https://oauth2.googleapis.com/token"
	googleUserInfoURL = "https://www.googleapis.com/oauth2/v2/userinfo"
)

// ExchangeGoogleCode trades an authorization code for an access token.
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

	tokenRespBody, _ := io.ReadAll(tokenResp.Body)
	tokenResp.Body = io.NopCloser(bytes.NewBuffer(tokenRespBody))

	var tokenData struct {
		AccessToken string `json:"access_token"`
		IDToken     string `json:"id_token"`
	}
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenData); err != nil {
		return "", ErrTokenDecode
	}
	return tokenData.AccessToken, nil
}

// FetchGoogleUser reports the email and name Google holds for an access token.
func FetchGoogleUser(accessToken string) (email, name string, err error) {
	req, _ := http.NewRequest("GET", googleUserInfoURL, nil)
	req.Header.Set("Authorization", "Bearer "+accessToken)

	client := &http.Client{}
	userInfoResp, err := client.Do(req)
	if err != nil {
		return "", "", ErrUserInfoRequest
	}
	defer userInfoResp.Body.Close()

	userInfoBody, _ := io.ReadAll(userInfoResp.Body)
	userInfoResp.Body = io.NopCloser(bytes.NewBuffer(userInfoBody))

	var userData struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if err := json.NewDecoder(userInfoResp.Body).Decode(&userData); err != nil {
		return "", "", ErrUserInfoDecode
	}
	return userData.Email, userData.Name, nil
}
