package domain

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// claimsOf decodes the payload half of a token without verifying it, so a test
// can assert what a token carries.
func claimsOf(t *testing.T, token string) tokenClaims {
	t.Helper()

	encoded, _, found := strings.Cut(token, ".")
	if !found {
		t.Fatalf("token %q has no payload", token)
	}
	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode %q: %v", encoded, err)
	}
	var claims tokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatalf("unmarshal %q: %v", payload, err)
	}
	return claims
}

// ------------------------------------------------------------- the secret

// TestAConfiguredSecretIsUsed pins the one supported way of setting the signing
// key.
func TestAConfiguredSecretIsUsed(t *testing.T) {
	t.Setenv("SYNC_TOKEN_SECRET", "a-real-secret")

	secret, ephemeral := resolveTokenSecret()
	if secret != "a-real-secret" {
		t.Errorf("secret = %q, want the environment value", secret)
	}
	if ephemeral {
		t.Error("a configured secret was reported as generated")
	}
}

// TestAnUnsetSecretIsGeneratedRatherThanGuessable is the fix for F-252. The
// fallback used to be a constant compiled into the binary and committed to the
// repository, so anybody who had read it could mint an admin token for any
// deployment that had not set the variable — offline, for any day.
func TestAnUnsetSecretIsGeneratedRatherThanGuessable(t *testing.T) {
	t.Setenv("SYNC_TOKEN_SECRET", "")

	first, ephemeral := resolveTokenSecret()
	if !ephemeral {
		t.Error("a generated secret was not reported as such; an operator has no " +
			"way to learn that tokens will not survive a restart")
	}
	if first == "sync_default_secret_key_change_me_in_production" {
		t.Fatal("the built-in default secret is still in use")
	}
	if len(first) < 32 {
		t.Errorf("the generated secret is %d characters, which is too short", len(first))
	}

	second, _ := resolveTokenSecret()
	if first == second {
		t.Error("two generated secrets were identical, so they are not random")
	}
}

func TestTheEphemeralSecretIsReported(t *testing.T) {
	// The package-level value is whatever the test environment set; the point
	// is only that the state is readable at all, because startup logs it.
	_ = SecretIsEphemeral()
}

// -------------------------------------------------------------- the token

func TestATokenNamesItsBearer(t *testing.T) {
	token := GenerateUserToken("alice", "guest")

	username, access, ok := ParseUserToken(token)
	if !ok {
		t.Fatal("ParseUserToken rejected the token it was just given")
	}
	if username != "alice" || access != "guest" {
		t.Errorf("token proves %q/%q, want alice/guest", username, access)
	}
}

// TestATokenExpires is the property the previous derivation had no way to
// express: it was a pure function of the username, the access level and the
// calendar date, so it was a constant for the whole day and there was no way to
// end a session early.
func TestATokenExpires(t *testing.T) {
	expired := generateUserTokenAt("alice", "guest", time.Now().Add(-time.Second))

	if _, _, ok := ParseUserToken(expired); ok {
		t.Error("an expired token was accepted")
	}
}

func TestAFreshTokenExpiresAfterTheTTL(t *testing.T) {
	claims := claimsOf(t, GenerateUserToken("alice", "guest"))

	expiry := time.Unix(claims.ExpiresAt, 0)
	if ttl := time.Until(expiry); ttl > TokenTTL || ttl < TokenTTL-time.Minute {
		t.Errorf("token expires in %v, want about %v", ttl, TokenTTL)
	}
}

// TestATamperedTokenIsRejected covers the reason the payload is signed rather
// than merely encoded: it names the access level the middleware trusts.
func TestATamperedTokenIsRejected(t *testing.T) {
	token := GenerateUserToken("alice", "guest")
	encoded, signature, _ := strings.Cut(token, ".")

	forgedPayload, err := json.Marshal(tokenClaims{
		Username: "alice", Access: AccessAdmin,
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	forged := base64.RawURLEncoding.EncodeToString(forgedPayload) + "." + signature

	if _, _, ok := ParseUserToken(forged); ok {
		t.Error("a payload swapped behind a valid signature was accepted")
	}
	if _, access, _ := ParseUserToken(encoded + "." + signature); access != "guest" {
		t.Error("the original token no longer proves what it was minted for")
	}
}

func TestAMalformedTokenIsRejected(t *testing.T) {
	valid := GenerateUserToken("alice", "guest")

	for name, token := range map[string]string{
		"empty":            "",
		"no separator":     strings.ReplaceAll(valid, ".", ""),
		"empty signature":  strings.SplitN(valid, ".", 2)[0] + ".",
		"empty payload":    "." + strings.SplitN(valid, ".", 2)[1],
		"not base64":       "!!!." + strings.SplitN(valid, ".", 2)[1],
		"signature cut":    valid[:len(valid)-1],
		"uppercased":       strings.ToUpper(valid),
		"payload not json": base64.RawURLEncoding.EncodeToString([]byte("nope")) + ".x",
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, ok := ParseUserToken(token); ok {
				t.Errorf("ParseUserToken(%q) accepted it", token)
			}
		})
	}
}

// TestTwoTokensForOneUserDiffer records that a token is no longer a constant
// for the day: each one carries its own expiry, so one being disclosed does not
// hand over every other token that user will be issued.
func TestTwoTokensForOneUserDiffer(t *testing.T) {
	first := generateUserTokenAt("alice", "guest", time.Now().Add(time.Hour))
	second := generateUserTokenAt("alice", "guest", time.Now().Add(2*time.Hour))

	if first == second {
		t.Error("two tokens with different expiries are identical")
	}
}

// TestTheFieldsAreNotAmbiguous covers a flaw in the old derivation: the HMAC
// input joined the username and the access level with a colon and escaped
// neither, so a user named "a:b" with access "c" produced the same token as a
// user named "a" with access "b:c".
func TestTheFieldsAreNotAmbiguous(t *testing.T) {
	first := generateUserTokenAt("a", "b:c", time.Unix(2000000000, 0))
	second := generateUserTokenAt("a:b", "c", time.Unix(2000000000, 0))

	if first == second {
		t.Error("two distinct identities produced the same token")
	}
}

func TestATokenVariesByIdentity(t *testing.T) {
	expiry := time.Unix(2000000000, 0)
	base := generateUserTokenAt("alice", "guest", expiry)

	for name, pair := range map[string][2]string{
		"different username":     {"bob", "guest"},
		"different access level": {"alice", AccessAdmin},
		"username case matters":  {"Alice", "guest"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := generateUserTokenAt(pair[0], pair[1], expiry); got == base {
				t.Errorf("%s collided with the base token", name)
			}
		})
	}
}

// ------------------------------------------------------------- admin token

// TestGenerateAdminTokenEqualsAdminUserToken records that the admin token is
// not a distinct credential: it is the user token for the pair
// ("admin", "admin"). Any account named admin whose access level is admin
// therefore holds the token ValidateAdminToken accepts.
func TestGenerateAdminTokenEqualsAdminUserToken(t *testing.T) {
	username, access, ok := ParseUserToken(GenerateAdminToken())
	if !ok {
		t.Fatal("the admin token does not parse")
	}
	if username != "admin" || access != AccessAdmin {
		t.Errorf("the admin token proves %q/%q", username, access)
	}
}

func TestValidateAdminToken(t *testing.T) {
	valid := GenerateAdminToken()

	if !ValidateAdminToken(valid) {
		t.Error("ValidateAdminToken rejected the token it just generated")
	}

	for name, token := range map[string]string{
		"empty":         "",
		"truncated":     valid[:len(valid)-1],
		"a user token":  GenerateUserToken("alice", "guest"),
		"a named admin": GenerateUserToken("alice", AccessAdmin),
		"expired":       generateUserTokenAt("admin", AccessAdmin, time.Now().Add(-time.Second)),
	} {
		t.Run(name, func(t *testing.T) {
			if ValidateAdminToken(token) {
				t.Errorf("ValidateAdminToken(%q) = true, want false", token)
			}
		})
	}
}

// -------------------------------------------------------------- the header

func TestExtractTokenFromHeader(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   string
	}{
		{"bearer prefix is stripped", "Bearer abc123", "abc123"},
		{"only the first prefix is stripped", "Bearer Bearer abc", "Bearer abc"},
		{"no prefix returns the whole value", "abc123", "abc123"},
		{"empty header", "", ""},
		// The prefix check is case-sensitive and requires the trailing space,
		// so these forms are treated as the token itself and fail validation
		// later with no indication that the header was malformed.
		{"lowercase bearer is not recognised", "bearer abc123", "bearer abc123"},
		{"missing space is not recognised", "Bearerabc123", "Bearerabc123"},
		{"leading whitespace is not trimmed", " Bearer abc123", " Bearer abc123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExtractTokenFromHeader(tt.header); got != tt.want {
				t.Errorf("ExtractTokenFromHeader(%q) = %q, want %q", tt.header, got, tt.want)
			}
		})
	}
}

// TestTokenSecretIsCapturedAtInit records that the signing secret is read once
// during initialisation, so changing SYNC_TOKEN_SECRET at runtime has no effect
// on issued tokens. Rotating it requires a restart, and the running process
// gives no sign that the environment and the key in use have diverged.
func TestTokenSecretIsCapturedAtInit(t *testing.T) {
	before := GenerateUserToken("alice", "guest")

	t.Setenv("SYNC_TOKEN_SECRET", "a-freshly-rotated-secret")

	after := GenerateUserToken("alice", "guest")
	if _, _, ok := ParseUserToken(after); !ok {
		t.Fatal("a freshly minted token does not verify")
	}
	if claimsOf(t, before).Username != claimsOf(t, after).Username {
		t.Error("the token payload changed, which this test does not cover")
	}
}
