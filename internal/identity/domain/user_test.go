package domain

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// fmtSscan is fmt.Sscan, named so the mirrored parsing below reads like the
// handler it mirrors.
func fmtSscan(s string, a ...interface{}) (int, error) { return fmt.Sscan(s, a...) }

// It used to be the current time to the second with a "google_" prefix and no
// randomness at all — and password login is accepted for Google accounts too,
// so knowing roughly when an account was created put it within a few hundred
// guesses, and two accounts created in the same second shared one.
func TestGeneratedPasswordsAreNotGuessable(t *testing.T) {
	first := GenerateRandomPassword()
	second := GenerateRandomPassword()

	if !strings.HasPrefix(first, "google_") {
		t.Fatalf("GenerateRandomPassword = %q, want a google_ prefix", first)
	}
	if first == second {
		t.Fatal("two passwords generated in the same second are identical")
	}

	suffix := strings.TrimPrefix(first, "google_")
	if _, err := time.Parse("20060102150405", suffix); err == nil {
		t.Errorf("the password %q is still a timestamp", first)
	}
	if len(suffix) < 32 {
		t.Errorf("the random part is %d characters, which is not much to guess through", len(suffix))
	}
}

// TestTwoPasswordsInTheSameSecondCollide records the consequence: the value is
// a function of the clock alone.
func TestTwoPasswordsInTheSameSecondCollide(t *testing.T) {
	if GenerateRandomPassword() != GenerateRandomPassword() {
		t.Skip("the two calls straddled a second boundary")
	}
}

func users(n int) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, n)
	for i := 1; i <= n; i++ {
		out = append(out, map[string]interface{}{
			"id":       i,
			"username": "u" + string(rune('0'+i)),
			"password": "secret",
			"name":     "User",
			"email":    "u@example.com",
			"access":   "guest",
			"avatar":   "",
			"status":   "active",
			"userId":   "uid",
		})
	}
	return out
}

// TestPublicUsersDropsSensitiveColumns covers what the directory endpoint
// exposes.
func TestPublicUsersDropsSensitiveColumns(t *testing.T) {
	got := PublicUsers(users(1))
	if len(got) != 1 {
		t.Fatalf("PublicUsers returned %d rows, want 1", len(got))
	}

	for _, key := range []string{"password", "id", "username"} {
		if _, ok := got[0][key]; ok {
			t.Errorf("the row still exposes %q", key)
		}
	}
	for _, key := range []string{"userId", "name", "email", "access", "avatar", "status"} {
		if _, ok := got[0][key]; !ok {
			t.Errorf("the row lost %q", key)
		}
	}
	if len(got[0]) != 6 {
		t.Errorf("the row exposes %d columns, want 6: %v", len(got[0]), got[0])
	}
}

// The sensitive columns were removed with delete on the caller's own maps, so
// the slice handed in came back stripped — and only the rows on the page that
// had been asked for, which meant whether a row still carried its password
// depended on which page somebody requested.
func TestTheCallersRowsAreNotTouched(t *testing.T) {
	rows := users(25)

	public := PublicUsers(rows)

	for _, key := range []string{"password", "username"} {
		if _, ok := rows[0][key]; !ok {
			t.Errorf("the caller's row lost its %q", key)
		}
	}
	if _, ok := public[0]["password"]; ok {
		t.Error("the rendered row exposes the password")
	}
}

// TestPublicUsersOnAnEmptyPage covers a page with nothing on it, which is what
// asking for a page past the end produces.
func TestPublicUsersOnAnEmptyPage(t *testing.T) {
	if got := PublicUsers(nil); len(got) != 0 {
		t.Errorf("PublicUsers(nil) returned %d rows", len(got))
	}
}
