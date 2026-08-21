package domain

import "testing"

func TestIsValidAccess(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want bool
	}{
		{"", true}, // empty means "leave it alone"
		{AccessAdmin, true},
		{AccessGuest, true},
		{"user", false},
		{"Admin", false},
		{"ADMIN", false},
		{"administrator", false},
		{" admin", false},
	} {
		t.Run(tt.in, func(t *testing.T) {
			if got := IsValidAccess(tt.in); got != tt.want {
				t.Errorf("IsValidAccess(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// TestUserIsNotAValidAccessLevel records that the only levels a user may be
// given are admin and guest, while the login path stores whatever the users
// table holds. A row whose access column says "user" therefore authenticates
// and is treated as authenticated, but no request can set that value.
func TestUserIsNotAValidAccessLevel(t *testing.T) {
	if IsValidAccess("user") {
		t.Fatal(`"user" is accepted now; the vocabulary appears to have grown, ` +
			"so assert the new set")
	}

	s := freshSession()
	s.Authenticate("alice", "user")
	if !s.IsAuthenticated() {
		t.Error("a session holding the unassignable level \"user\" is not authenticated")
	}
}

func TestIsValidStatus(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want bool
	}{
		{"", true},
		{StatusActive, true},
		{StatusInactive, true},
		{"Active", false},
		{"disabled", false},
		{"deleted", false},
	} {
		t.Run(tt.in, func(t *testing.T) {
			if got := IsValidStatus(tt.in); got != tt.want {
				t.Errorf("IsValidStatus(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestIsDeactivated(t *testing.T) {
	for in, want := range map[string]bool{
		StatusInactive: true,
		StatusActive:   false,
		"":             false,
		"Inactive":     false,
		"INACTIVE":     false,
	} {
		if got := IsDeactivated(in); got != want {
			t.Errorf("IsDeactivated(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestAnEmptyStatusIsNotDeactivated records that a users row with a NULL status
// signs in: the reader substitutes "active" for NULL, and IsDeactivated only
// refuses the literal "inactive". An account can therefore never be locked out
// by clearing its status.
func TestAnEmptyStatusIsNotDeactivated(t *testing.T) {
	if IsDeactivated("") {
		t.Fatal("an empty status is refused now; assert the new rule")
	}
	if IsDeactivated("Inactive") {
		t.Fatal("the comparison is case-insensitive now; assert that instead")
	}
}
