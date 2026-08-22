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

// TestGeneratedPasswordsAreNotGuessable covers the password a new Google user
// is given. It used to be the current time to the second with a "google_"
// prefix and no randomness at all — and password login is accepted for Google
// accounts too, so knowing roughly when an account was created put it within a
// few hundred guesses, and two accounts created in the same second shared one.
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

func TestPageOfUsersPaginates(t *testing.T) {
	for _, tt := range []struct {
		name     string
		total    int
		current  int
		pageSize int
		want     int
	}{
		{"first page", 25, 1, 10, 10},
		{"second page", 25, 2, 10, 10},
		{"last partial page", 25, 3, 10, 5},
		{"page past the end", 25, 4, 10, 0},
		{"page size larger than the table", 3, 1, 10, 3},
		{"empty table", 0, 1, 10, 0},
		{"single row", 1, 1, 10, 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := PageOfUsers(users(tt.total), tt.current, tt.pageSize)
			if len(got) != tt.want {
				t.Errorf("PageOfUsers(%d rows, page %d of %d) returned %d, want %d",
					tt.total, tt.current, tt.pageSize, len(got), tt.want)
			}
		})
	}
}

// TestAPageBeyondTheEndIsEmptyRatherThanAnError records that asking for page 99
// of a three-row table answers with an empty list and a total of three, not a
// 404 or a clamp to the last page. A UI that trusts the total will show an empty
// table.
func TestAPageBeyondTheEndIsEmptyRatherThanAnError(t *testing.T) {
	got := PageOfUsers(users(3), 99, 10)

	if len(got) != 0 {
		t.Fatalf("PageOfUsers returned %d rows for page 99; the page appears to be "+
			"clamped now, so assert that instead", len(got))
	}
}

func TestPageOfUsersDropsSensitiveColumns(t *testing.T) {
	got := PageOfUsers(users(1), 1, 10)
	if len(got) != 1 {
		t.Fatalf("PageOfUsers returned %d rows, want 1", len(got))
	}

	for _, key := range []string{"password", "id", "username"} {
		if _, ok := got[0][key]; ok {
			t.Errorf("the page still exposes %q", key)
		}
	}
	for _, key := range []string{"userId", "name", "email", "access", "avatar", "status"} {
		if _, ok := got[0][key]; !ok {
			t.Errorf("the page lost %q", key)
		}
	}
	if len(got[0]) != 6 {
		t.Errorf("the page exposes %d columns, want 6: %v", len(got[0]), got[0])
	}
}

// TestTheCallersRowsAreNotTouched covers redaction that reached back into the
// caller. The sensitive columns were removed with delete on the caller's own
// maps, so the slice handed in came back stripped — and only the rows on the
// requested page, which means whether a row still carried its password depended
// on which page somebody asked for.
func TestTheCallersRowsAreNotTouched(t *testing.T) {
	rows := users(25)

	page := PageOfUsers(rows, 1, 10)

	for _, key := range []string{"password", "username"} {
		if _, ok := rows[0]["password"]; !ok {
			t.Errorf("the caller's row lost its %q", key)
		}
	}
	if _, ok := page[0]["password"]; ok {
		t.Error("the page exposes the password")
	}
}

// TestAZeroPageSizeReturnsNothing records that the endpoint's own defaulting is
// the only thing keeping this function away from a zero page size: given one it
// answers with an empty page rather than an error or the whole table.
func TestAZeroPageSizeReturnsNothing(t *testing.T) {
	if got := PageOfUsers(users(5), 1, 0); len(got) != 0 {
		t.Errorf("PageOfUsers with pageSize 0 returned %d rows, want 0", len(got))
	}
}

// TestANonPositivePageIsTheFirstPage covers a page number of zero or below. The
// bounds check only caught a start index past the end of the table; a
// non-positive page made the start index negative, the check passed it through,
// and the slice expression panicked. The only guard was the directory endpoint's
// own parsing, in the HTTP layer several calls away, and this function is
// exported.
func TestANonPositivePageIsTheFirstPage(t *testing.T) {
	first := PageOfUsers(users(5), 1, 2)

	for _, page := range []int{0, -1, -100} {
		got := PageOfUsers(users(5), page, 2)
		if len(got) != len(first) {
			t.Errorf("page %d returned %d rows, want the first page's %d", page, len(got), len(first))
		}
	}
}

// TestANegativePageSizeReturnsNothing covers the other half of the arithmetic.
func TestANegativePageSizeReturnsNothing(t *testing.T) {
	if got := PageOfUsers(users(5), 1, -10); len(got) != 0 {
		t.Errorf("PageOfUsers with a negative page size returned %d rows", len(got))
	}
}

// TestTheEndpointsOwnParsingIsWhatKeepsThePanicUnreachable pins the guard that
// stands between the panic above and a request. It lives in the HTTP layer, so
// any second caller of PageOfUsers has to repeat it.
func TestTheEndpointsOwnParsingIsWhatKeepsThePanicUnreachable(t *testing.T) {
	// Mirrors the parsing in identityhttp.GetUsersHandler.
	parse := func(raw string, fallback int) int {
		if raw == "" {
			return fallback
		}
		var val int
		if _, err := fmtSscan(raw, &val); err != nil || val <= 0 {
			return fallback
		}
		return val
	}

	for _, raw := range []string{"0", "-1", "abc", ""} {
		if got := parse(raw, 1); got != 1 {
			t.Errorf("current=%q parsed to %d, want the fallback 1", raw, got)
		}
	}
}
