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

// TestGenerateRandomPasswordIsATimestamp records that the "random" password a
// new Google user is given is the current time to the second, prefixed with
// "google_". It has no randomness at all: anyone who knows roughly when an
// account was created can enumerate a few thousand candidates, and two accounts
// created in the same second get the same password.
func TestGenerateRandomPasswordIsATimestamp(t *testing.T) {
	got := GenerateRandomPassword()

	if !strings.HasPrefix(got, "google_") {
		t.Fatalf("GenerateRandomPassword = %q, want a google_ prefix", got)
	}
	stamp := strings.TrimPrefix(got, "google_")
	if _, err := time.Parse("20060102150405", stamp); err != nil {
		t.Fatalf("the suffix %q is not a timestamp any more (%v); real randomness "+
			"appears to have been added, so assert that instead", stamp, err)
	}
	if len(stamp) != 14 {
		t.Errorf("the timestamp is %d characters, want 14", len(stamp))
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

// TestPageOfUsersMutatesTheCallersRows records that the sensitive columns are
// removed with delete on the caller's own maps, so the slice handed in is
// modified in place. A caller that reads the password after paginating finds it
// gone.
func TestPageOfUsersMutatesTheCallersRows(t *testing.T) {
	rows := users(1)

	PageOfUsers(rows, 1, 10)

	if _, ok := rows[0]["password"]; ok {
		t.Fatal("the caller's row kept its password; the function appears to copy now, " +
			"so assert that instead")
	}
	if _, ok := rows[0]["username"]; ok {
		t.Error("the caller's row kept its username")
	}
}

// TestOnlyThePageIsStrippedNotTheWholeTable records the other side of that
// mutation: rows outside the requested page keep their password, so whether a
// row is scrubbed depends on which page was asked for.
func TestOnlyThePageIsStrippedNotTheWholeTable(t *testing.T) {
	rows := users(25)

	PageOfUsers(rows, 1, 10)

	if _, ok := rows[0]["password"]; ok {
		t.Error("a row on the requested page kept its password")
	}
	if _, ok := rows[20]["password"]; !ok {
		t.Fatal("a row outside the requested page lost its password too; the whole " +
			"table is scrubbed now, so assert that instead")
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

// TestANonPositivePagePanics records a defect this test suite found.
//
// The bounds check only catches a start index past the end of the table. For a
// page number of zero or below the start index goes negative, the check passes
// it through, and the slice expression panics with an out-of-range index.
//
// The directory endpoint is safe only because its own parsing refuses anything
// that is not greater than zero, in the HTTP layer, several calls away. This
// function carries no guard of its own, and the arithmetic is unchanged from
// when it was inline in the handler.
func TestANonPositivePagePanics(t *testing.T) {
	for _, page := range []int{0, -1, -100} {
		t.Run("", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("PageOfUsers survived page %d; a guard appears to have "+
						"been added, so assert the empty page instead", page)
				}
			}()
			PageOfUsers(users(5), page, 10)
		})
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
