package infra

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// pointAt redirects the Google endpoints at a stand-in server for the duration
// of a test. The URLs used to be unexported constants, so the only way to
// exercise this at all was to call Google.
func pointAt(t *testing.T, token, userInfo string) {
	t.Helper()

	oldToken, oldUserInfo := googleTokenURL, googleUserInfoURL
	googleTokenURL, googleUserInfoURL = token, userInfo
	t.Cleanup(func() { googleTokenURL, googleUserInfoURL = oldToken, oldUserInfo })
}

// serve starts a server answering every request with one status and one body.
func serve(t *testing.T, status int, body string) string {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// TestARefusedCodeIsNotAnIdentity covers a way into the system with no
// credentials at all.
//
// Neither call looked at the HTTP status. Google answers an authorization code
// it does not recognise with 400 and an error document; http.PostForm reports no
// error for that, and the error document decodes cleanly into the token struct,
// leaving the access token empty. The user-info request then went out with an
// empty bearer token, Google answered 401 with another error document, and that
// decoded just as cleanly — leaving the email and the name empty. The flow
// carried on: a row was written for that empty identity, found again, and a
// token minted for it. Anyone who posted {"code":"x"} got an account and a
// token that validates.
func TestARefusedCodeIsNotAnIdentity(t *testing.T) {
	refused := serve(t, http.StatusBadRequest, `{"error":"invalid_grant"}`)
	pointAt(t, refused, refused)

	token, err := ExchangeGoogleCode("id", "secret", "https://example/callback", "x")
	if err == nil {
		t.Fatalf("ExchangeGoogleCode accepted a refused code and returned %q", token)
	}
	if !errors.Is(err, ErrTokenRequest) {
		t.Errorf("err = %v, want a token request failure", err)
	}
	if !strings.Contains(err.Error(), "invalid_grant") {
		t.Errorf("err = %v, want it to carry what Google said", err)
	}
}

// TestARejectedTokenIsNotAnIdentity is the second half: the user-info call
// answering 401 has to be a failure too.
func TestARejectedTokenIsNotAnIdentity(t *testing.T) {
	rejected := serve(t, http.StatusUnauthorized, `{"error":"invalid_credentials"}`)
	pointAt(t, rejected, rejected)

	email, name, err := FetchGoogleUser("not-a-token")
	if err == nil {
		t.Fatalf("FetchGoogleUser accepted a rejected token and returned %q/%q", email, name)
	}
	if !errors.Is(err, ErrUserInfoRequest) {
		t.Errorf("err = %v, want a user info request failure", err)
	}
}

// TestAnEmptyAccessTokenIsNotAsked covers the request that used to go out
// carrying nothing.
func TestAnEmptyAccessTokenIsNotAsked(t *testing.T) {
	asked := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		asked = true
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	pointAt(t, server.URL, server.URL)

	if _, _, err := FetchGoogleUser("  "); err == nil {
		t.Error("FetchGoogleUser asked with no token")
	}
	if asked {
		t.Error("a request went out with an empty bearer token")
	}
}

// TestASuccessfulExchangeIsAnIdentity is the path that has to keep working.
func TestASuccessfulExchangeIsAnIdentity(t *testing.T) {
	tokenServer := serve(t, http.StatusOK, `{"access_token":"ya29.a0","id_token":"eyJ"}`)
	userServer := serve(t, http.StatusOK, `{"email":"ada@example.com","name":"Ada"}`)
	pointAt(t, tokenServer, userServer)

	token, err := ExchangeGoogleCode("id", "secret", "https://example/callback", "code")
	if err != nil {
		t.Fatalf("ExchangeGoogleCode: %v", err)
	}
	if token != "ya29.a0" {
		t.Errorf("token = %q", token)
	}

	email, name, err := FetchGoogleUser(token)
	if err != nil {
		t.Fatalf("FetchGoogleUser: %v", err)
	}
	if email != "ada@example.com" || name != "Ada" {
		t.Errorf("identity = %q/%q", email, name)
	}
}

// TestA200WithNothingInItIsNotAnIdentity covers the shape of the error
// documents Google returns: they are valid JSON and they decode into the structs
// here without complaint, leaving every field empty.
func TestA200WithNothingInItIsNotAnIdentity(t *testing.T) {
	empty := serve(t, http.StatusOK, `{"error":"something else"}`)
	pointAt(t, empty, empty)

	if token, err := ExchangeGoogleCode("id", "secret", "uri", "code"); err == nil {
		t.Errorf("ExchangeGoogleCode returned %q for a response with no token", token)
	}
	if email, _, err := FetchGoogleUser("token"); err == nil {
		t.Errorf("FetchGoogleUser returned %q for a response with no email", email)
	}
}
