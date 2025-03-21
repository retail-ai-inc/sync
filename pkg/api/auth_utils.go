package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"
)

var (
	// Token secret for generating and validating tokens
	// Get from environment variables, use default if not set
	tokenSecret = getTokenSecret()
)

// getTokenSecret gets the token secret key
func getTokenSecret() string {
	secret := os.Getenv("SYNC_TOKEN_SECRET")
	if secret == "" {
		// If environment variable is not set, use default
		// Note: In production, set a strong key via environment variable
		return "sync_default_secret_key_change_me_in_production"
	}
	return secret
}

// GenerateUserToken generates a token for a specific user
// Based on username, access level, current date and secret key
func GenerateUserToken(username, accessLevel string) string {
	// Use current date as base, so token changes daily
	dateStr := time.Now().Format("2006-01-02")

	// Use HMAC-SHA256 algorithm to calculate token
	h := hmac.New(sha256.New, []byte(tokenSecret))
	h.Write([]byte("user_token:" + username + ":" + accessLevel + ":" + dateStr))

	// Return hex encoded token
	return hex.EncodeToString(h.Sum(nil))
}

// GenerateAdminToken generates admin verification token
// Based on current date and secret key, changes daily
func GenerateAdminToken() string {
	return GenerateUserToken("admin", "admin")
}

// ValidateAdminToken validates if admin token is valid
func ValidateAdminToken(token string) bool {
	// Generate current valid token
	validToken := GenerateAdminToken()

	// Compare provided token with valid token
	return hmac.Equal([]byte(token), []byte(validToken))
}

// ExtractTokenFromHeader extracts token from HTTP request header
func ExtractTokenFromHeader(authHeader string) string {
	// Check if header starts with Bearer
	if strings.HasPrefix(authHeader, "Bearer ") {
		return strings.TrimPrefix(authHeader, "Bearer ")
	}
	// If no Bearer prefix, return entire value
	return authHeader
}

// ValidateUserToken validates if user token is valid and returns username and access level
func ValidateUserToken(token string) (bool, string, string) {
	// Get all users from database
	users, err := GetAllUsers()
	if err != nil {
		fmt.Println("Error getting users:", err)
		return false, "", ""
	}

	// Check if token matches any user's valid token for today
	for _, user := range users {
		username := user["username"].(string)
		accessLevel := user["access"].(string)

		// Generate today's token for this user
		expectedToken := GenerateUserToken(username, accessLevel)

		// Compare tokens
		if hmac.Equal([]byte(token), []byte(expectedToken)) {
			return true, username, accessLevel
		}
	}

	fmt.Println("No matching token found for any user") // Debug
	return false, "", ""
}
