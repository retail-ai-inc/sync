package domain

import (
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Passwords were stored as the operator typed them, and checked with ==. A
// copy of the production database confirms it: the admin row holds sixteen
// printable characters, not a hash.

const (
	// hashPrefix marks a stored value as hashed, so a plaintext row from before
	// this existed can still be recognised.
	hashPrefix = "pbkdf2-sha256"
	// defaultIterations is what a login costs. OWASP's floor for
	// PBKDF2-HMAC-SHA256 at the time of writing; it is recorded in each stored
	// value, so raising it invalidates nothing — a hash made with fewer rounds
	// is simply replaced the next time that password is used.
	defaultIterations = 600_000
	hashLength        = 32
	saltLength        = 16
)

// hashIterations reports the work factor to use for a new hash.
//
// SYNC_PASSWORD_ITERATIONS exists because the right number depends on the
// hardware: the figure below should cost a noticeable fraction of a second on
// the machine that runs it, and a deployment on slower hardware has to be able
// to say so rather than making every login take five.
func hashIterations() int {
	raw := strings.TrimSpace(os.Getenv("SYNC_PASSWORD_ITERATIONS"))
	if raw == "" {
		return defaultIterations
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return defaultIterations
	}
	return n
}

func HashPassword(password string) (string, error) {
	salt := make([]byte, saltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("no salt could be generated: %w", err)
	}

	rounds := hashIterations()
	key, err := pbkdf2.Key(sha256.New, password, salt, rounds, hashLength)
	if err != nil {
		return "", fmt.Errorf("the password could not be hashed: %w", err)
	}

	return strings.Join([]string{
		hashPrefix,
		strconv.Itoa(rounds),
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	}, "$"), nil
}

// PasswordMatches reports whether an offered password matches what is stored,
// and whether the stored form should be replaced with a hash. A stored value
// that is not a hash is compared as plaintext, because that is what rows
// written before this existed hold.
func PasswordMatches(stored, offered string) (matches, needsRehash bool) {
	if !strings.HasPrefix(stored, hashPrefix+"$") {
		return subtle.ConstantTimeCompare([]byte(stored), []byte(offered)) == 1, true
	}

	parts := strings.Split(stored, "$")
	if len(parts) != 4 {
		return false, false
	}
	iterations, err := strconv.Atoi(parts[1])
	if err != nil || iterations < 1 {
		return false, false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[2])
	if err != nil {
		return false, false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[3])
	if err != nil {
		return false, false
	}

	got, err := pbkdf2.Key(sha256.New, offered, salt, iterations, len(want))
	if err != nil {
		return false, false
	}
	if subtle.ConstantTimeCompare(got, want) != 1 {
		return false, false
	}
	// A hash made with fewer rounds than are now required is worth replacing.
	return true, iterations < hashIterations()
}

// IsHashed reports whether a stored value has been hashed. It exists so an
// operator can be told how many rows are still in the clear.
func IsHashed(stored string) bool {
	return strings.HasPrefix(stored, hashPrefix+"$")
}
