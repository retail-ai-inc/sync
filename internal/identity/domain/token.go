package domain

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

// TokenTTL is how long a freshly minted token stays usable. The previous
// derivation had no expiry field at all: a token was a pure function of the
// username, the access level and the calendar date, so it was a constant for
// the whole day and there was no way to end a session early.
const TokenTTL = 12 * time.Hour

var (
	tokenSecret string
	// secretIsEphemeral records that no secret was configured and one was
	// generated for this process. Callers say so at startup: tokens will not
	// survive a restart and will not be accepted by another replica.
	secretIsEphemeral bool
)

func init() {
	tokenSecret, secretIsEphemeral = resolveTokenSecret()
}

// resolveTokenSecret reads the signing secret, generating a random one when
// none is configured. It used to fall back to a constant compiled into the
// binary, which is in a repository: anyone who had read it could mint an admin
// token for any day, against any deployment that had not set the variable.
func resolveTokenSecret() (secret string, ephemeral bool) {
	if configured := os.Getenv("SYNC_TOKEN_SECRET"); configured != "" {
		return configured, false
	}
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failing is not a condition a token can be minted under.
		panic("identity: no SYNC_TOKEN_SECRET is set and no random one could be generated: " + err.Error())
	}
	return hex.EncodeToString(buf), true
}

// SecretIsEphemeral reports whether the signing secret was generated for this
// process rather than configured. A deployment running more than one replica
// has to configure one, or a token minted by one replica is rejected by the
// next.
func SecretIsEphemeral() bool { return secretIsEphemeral }

// tokenClaims is what a token carries. The names are short because the payload
// is sent on every request.
type tokenClaims struct {
	Username  string `json:"u"`
	Access    string `json:"a"`
	ExpiresAt int64  `json:"e"`
}

func GenerateUserToken(username, accessLevel string) string {
	return generateUserTokenAt(username, accessLevel, time.Now().Add(TokenTTL))
}

// generateUserTokenAt mints a token with an explicit expiry, so the expiry
// behaviour can be exercised without waiting for it.
func generateUserTokenAt(username, accessLevel string, expiry time.Time) string {
	payload, err := json.Marshal(tokenClaims{
		Username:  username,
		Access:    accessLevel,
		ExpiresAt: expiry.Unix(),
	})
	if err != nil {
		return ""
	}
	encoded := base64.RawURLEncoding.EncodeToString(payload)
	return encoded + "." + sign(encoded)
}

func sign(encodedPayload string) string {
	mac := hmac.New(sha256.New, []byte(tokenSecret))
	mac.Write([]byte("user_token:" + encodedPayload))
	return hex.EncodeToString(mac.Sum(nil))
}

// ParseUserToken reports the identity a token proves, and whether it proves one
// at all. A token whose signature does not verify, whose payload cannot be read,
// or whose expiry has passed proves nothing.
func ParseUserToken(token string) (username, accessLevel string, ok bool) {
	encoded, signature, found := strings.Cut(token, ".")
	if !found {
		return "", "", false
	}
	if !hmac.Equal([]byte(signature), []byte(sign(encoded))) {
		return "", "", false
	}

	payload, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", "", false
	}
	var claims tokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return "", "", false
	}
	if time.Now().Unix() >= claims.ExpiresAt {
		return "", "", false
	}
	return claims.Username, claims.Access, true
}

func GenerateAdminToken() string {
	return GenerateUserToken("admin", AccessAdmin)
}

func ValidateAdminToken(token string) bool {
	username, access, ok := ParseUserToken(token)
	return ok && username == "admin" && access == AccessAdmin
}

// ExtractTokenFromHeader extracts token from HTTP request header.
//
// The scheme is matched without regard to case and without depending on the
// exact spacing. It used to require the literal "Bearer " — so "bearer abc" and
// a header with a leading space were handed on whole and then failed to verify,
// which reads to the caller as a bad token rather than a badly formed header.
func ExtractTokenFromHeader(authHeader string) string {
	header := strings.TrimSpace(authHeader)

	if scheme, rest, found := strings.Cut(header, " "); found && strings.EqualFold(scheme, "Bearer") {
		return strings.TrimSpace(rest)
	}
	// If no Bearer prefix, return entire value
	return header
}
