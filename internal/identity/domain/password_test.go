package domain

import (
	"strconv"
	"strings"
	"testing"
)

// Password storage. Every one of these is a way for the wrong password to be
// accepted, or the right one refused, and none of it is visible in the calling
// code: the login handler sees a bool.

func TestHashPasswordProducesTheStoredForm(t *testing.T) {
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1")

	stored, err := HashPassword("a real password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	parts := strings.Split(stored, "$")
	if len(parts) != 4 {
		t.Fatalf("the stored form has %d parts: %q", len(parts), stored)
	}
	if parts[0] != hashPrefix {
		t.Errorf("the stored form is not marked as a hash: %q", parts[0])
	}
	if rounds, err := strconv.Atoi(parts[1]); err != nil || rounds != 1 {
		t.Errorf("the rounds are %q, want 1", parts[1])
	}
	if !IsHashed(stored) {
		t.Error("IsHashed said no to what HashPassword produced")
	}
	if strings.Contains(stored, "a real password") {
		t.Error("the stored form contains the password")
	}
}

// TestTwoHashesOfTheSamePasswordDiffer covers the salt. Without one, equal
// passwords store equal hashes, so the database says which accounts share a
// password.
func TestTwoHashesOfTheSamePasswordDiffer(t *testing.T) {
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1")

	first, err := HashPassword("same")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	second, err := HashPassword("same")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if first == second {
		t.Error("two hashes of the same password are identical, so it is unsalted")
	}
}

func TestTheRightPasswordMatchesAndTheWrongOneDoesNot(t *testing.T) {
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1")

	stored, err := HashPassword("correct horse")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	if matches, _ := PasswordMatches(stored, "correct horse"); !matches {
		t.Error("the right password was refused")
	}
	for _, wrong := range []string{"", "correct", "correct horse ", "Correct Horse"} {
		if matches, _ := PasswordMatches(stored, wrong); matches {
			t.Errorf("%q was accepted", wrong)
		}
	}
}

// TestAHashAtTheCurrentWorkFactorNeedsNoRehash, and one below it does. Raising
// the factor invalidates nothing -- the rounds are stored with the hash -- so
// the only way an old hash is replaced is this flag.
func TestAHashAtTheCurrentWorkFactorNeedsNoRehash(t *testing.T) {
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "2")
	stored, err := HashPassword("password")
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}

	if _, needsRehash := PasswordMatches(stored, "password"); needsRehash {
		t.Error("a hash at the current work factor was marked for replacement")
	}

	// The deployment raises the factor.
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "3")
	matches, needsRehash := PasswordMatches(stored, "password")
	if !matches {
		t.Error("raising the work factor stopped an existing hash from matching")
	}
	if !needsRehash {
		t.Error("a hash made with fewer rounds than are now required was not marked " +
			"for replacement")
	}
}

// TestAPlaintextRowIsComparedAndMarkedForRehash covers the rows written before
// hashing existed. They have to keep working -- otherwise turning this on locks
// everybody out -- and every one of them has to be marked so it is replaced on
// first use.
func TestAPlaintextRowIsComparedAndMarkedForRehash(t *testing.T) {
	matches, needsRehash := PasswordMatches("plaintextpw", "plaintextpw")
	if !matches {
		t.Error("a plaintext row stopped its owner logging in")
	}
	if !needsRehash {
		t.Error("a plaintext row was not marked for replacement")
	}

	if matches, _ := PasswordMatches("plaintextpw", "somethingelse"); matches {
		t.Error("a plaintext row accepted the wrong password")
	}
	if IsHashed("plaintextpw") {
		t.Error("IsHashed said yes to a plaintext row")
	}
}

// TestAMalformedHashMatchesNothing. Each of these is a stored value that
// carries the hash marker and cannot be read; the one wrong answer is to fall
// through to the plaintext comparison, which would let the stored text itself
// be used as the password.
func TestAMalformedHashMatchesNothing(t *testing.T) {
	for name, stored := range map[string]string{
		"too few parts":            hashPrefix + "$1$c2FsdA",
		"too many parts":           hashPrefix + "$1$c2FsdA$aGFzaA$extra",
		"rounds not a number":      hashPrefix + "$many$c2FsdA$aGFzaA",
		"zero rounds":              hashPrefix + "$0$c2FsdA$aGFzaA",
		"negative rounds":          hashPrefix + "$-1$c2FsdA$aGFzaA",
		"salt not base64":          hashPrefix + "$1$not base64!$aGFzaA",
		"hash not base64":          hashPrefix + "$1$c2FsdA$not base64!",
		"nothing after the marker": hashPrefix + "$",
	} {
		t.Run(name, func(t *testing.T) {
			matches, needsRehash := PasswordMatches(stored, "anything")
			if matches {
				t.Error("a malformed hash accepted a password")
			}
			if needsRehash {
				t.Error("a malformed hash was marked for replacement, which would " +
					"overwrite it with a hash of whatever was offered")
			}
			// The stored text itself must not be usable as the password, which
			// is what falling through to the plaintext comparison would allow.
			if matches, _ := PasswordMatches(stored, stored); matches {
				t.Error("the stored text was accepted as the password")
			}
		})
	}
}

func TestTheWorkFactorFallsBackToTheDefault(t *testing.T) {
	for name, value := range map[string]string{
		"unset":        "",
		"not a number": "many",
		"zero":         "0",
		"negative":     "-5",
		"blank":        "   ",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("SYNC_PASSWORD_ITERATIONS", value)
			if got := hashIterations(); got != defaultIterations {
				t.Errorf("hashIterations() = %d, want the default %d", got, defaultIterations)
			}
		})
	}
}

func TestTheWorkFactorIsTakenFromTheEnvironment(t *testing.T) {
	t.Setenv("SYNC_PASSWORD_ITERATIONS", "1234")
	if got := hashIterations(); got != 1234 {
		t.Errorf("hashIterations() = %d, want 1234", got)
	}
}

// TestTheDefaultWorkFactorIsNotTrivial: the figure is meant to cost a
// noticeable fraction of a second, which is the whole defence against an
// offline guess at a stolen table.
func TestTheDefaultWorkFactorIsNotTrivial(t *testing.T) {
	if defaultIterations < 100_000 {
		t.Errorf("defaultIterations = %d, which is not enough to slow down an "+
			"offline guessing attack", defaultIterations)
	}
}
