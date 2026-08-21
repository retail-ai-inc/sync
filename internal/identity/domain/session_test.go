package domain

import (
	"sync"
	"testing"
)

// freshSession returns a Session that is not the process-wide one, so the tests
// below can exercise the type without disturbing whatever else is running.
func freshSession() *Session { return &Session{} }

func TestANewSessionCarriesNoIdentity(t *testing.T) {
	s := freshSession()

	if s.Username() != "" || s.Access() != "" {
		t.Errorf("a fresh session holds %q/%q", s.Username(), s.Access())
	}
	if s.IsAuthenticated() {
		t.Error("IsAuthenticated = true for a fresh session")
	}
	if s.IsAdmin() {
		t.Error("IsAdmin = true for a fresh session")
	}
}

func TestAuthenticateRecordsTheIdentity(t *testing.T) {
	s := freshSession()
	s.Authenticate("alice", "admin")

	if s.Username() != "alice" || s.Access() != "admin" {
		t.Errorf("session = %q/%q, want alice/admin", s.Username(), s.Access())
	}
	if !s.IsAuthenticated() {
		t.Error("IsAuthenticated = false after Authenticate")
	}
}

func TestRejectMakesTheCallerAGuest(t *testing.T) {
	s := freshSession()
	s.Authenticate("alice", "admin")
	s.Reject()

	if s.Access() != AccessGuest {
		t.Errorf("Access = %q, want %q", s.Access(), AccessGuest)
	}
	if s.Username() != "" {
		t.Errorf("Username = %q, want empty", s.Username())
	}
	if s.IsAuthenticated() {
		t.Error("IsAuthenticated = true for a rejected session")
	}
}

func TestClearDiscardsTheIdentity(t *testing.T) {
	s := freshSession()
	s.Authenticate("alice", "admin")
	s.Clear()

	if s.Access() != "" || s.Username() != "" {
		t.Errorf("session = %q/%q after Clear, want empty/empty", s.Username(), s.Access())
	}
	if s.IsAuthenticated() {
		t.Error("IsAuthenticated = true after Clear")
	}
}

// TestRejectAndClearAreDistinguishable records that a failed login and a logout
// leave different state: Reject stores the guest access level, Clear stores
// nothing. Both report IsAuthenticated false, so only code that reads Access
// can tell them apart.
func TestRejectAndClearAreDistinguishable(t *testing.T) {
	rejected, cleared := freshSession(), freshSession()
	rejected.Reject()
	cleared.Clear()

	if rejected.Access() == cleared.Access() {
		t.Fatalf("Reject and Clear both leave Access = %q; they are indistinguishable "+
			"now, so assert the single state instead", rejected.Access())
	}
	if rejected.IsAuthenticated() || cleared.IsAuthenticated() {
		t.Error("a rejected or cleared session reports as authenticated")
	}
}

func TestIsAuthenticated(t *testing.T) {
	for _, tt := range []struct {
		name     string
		username string
		access   string
		want     bool
	}{
		{"admin", "admin", "admin", true},
		{"ordinary user", "alice", "user", true},
		{"guest access", "alice", AccessGuest, false},
		{"empty access", "alice", "", false},
		{"empty username", "", "admin", false},
		{"both empty", "", "", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := freshSession()
			s.Authenticate(tt.username, tt.access)
			if got := s.IsAuthenticated(); got != tt.want {
				t.Errorf("IsAuthenticated for %q/%q = %v, want %v",
					tt.username, tt.access, got, tt.want)
			}
		})
	}
}

func TestIsAdmin(t *testing.T) {
	for _, tt := range []struct {
		name     string
		username string
		access   string
		want     bool
	}{
		{"the admin", "admin", "admin", true},
		{"admin access, other name", "alice", "admin", false},
		{"admin name, other access", "admin", "user", false},
		{"neither", "alice", "user", false},
		{"case matters", "Admin", "admin", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := freshSession()
			s.Authenticate(tt.username, tt.access)
			if got := s.IsAdmin(); got != tt.want {
				t.Errorf("IsAdmin for %q/%q = %v, want %v", tt.username, tt.access, got, tt.want)
			}
		})
	}
}

// TestAdminRequiresBothTheNameAndTheLevel records that being granted admin
// access is not enough to reach the admin-only endpoints: the username has to be
// the literal "admin" as well. A second administrator account therefore cannot
// mint an admin token, however its access level is set.
func TestAdminRequiresBothTheNameAndTheLevel(t *testing.T) {
	s := freshSession()
	s.Authenticate("bob", AccessAdmin)

	if s.IsAdmin() {
		t.Fatal("a second admin account now passes IsAdmin; the rule appears to have " +
			"changed, so assert the new one")
	}
	if !s.IsAuthenticated() {
		t.Error("IsAuthenticated = false for an admin-level account")
	}
}

// TestCurrentIsProcessWide records T-070: there is exactly one Session for the
// whole process, so an identity established by one request is the identity every
// concurrent request is treated as.
func TestCurrentIsProcessWide(t *testing.T) {
	prevUser, prevAccess := Current().Username(), Current().Access()
	t.Cleanup(func() { Current().Authenticate(prevUser, prevAccess) })

	if Current() != Current() {
		t.Fatal("Current returns different sessions now; the process-wide session " +
			"appears to be gone, so assert the request-scoped behaviour instead")
	}

	Current().Authenticate("alice", "admin")
	if Current().Username() != "alice" {
		t.Errorf("a write through one handle is not visible through another")
	}
	Current().Clear()
}

// TestTheSessionIsUnsynchronised records the other half of T-070. The Session
// has no mutex, so concurrent handlers race on its two fields. This test is
// gated because it exists to be run under -race, where it reports the race that
// production has.
func TestTheSessionIsUnsynchronised(t *testing.T) {
	if testing.Short() {
		t.Skip("run without -short, and with -race, to demonstrate T-070")
	}

	s := freshSession()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Authenticate("alice", "admin")
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.IsAdmin()
		}()
	}
	wg.Wait()
}
