package domain

// Session is the identity the process last authenticated.
//
// It is deliberately process-wide rather than request-scoped, and deliberately
// unsynchronised: those are the two halves of a recorded defect (T-070). One
// Session is shared by every in-flight request, so a login on one connection
// changes who a concurrent request is treated as, and a logout on one
// connection signs everybody out. Concurrent handlers read and write these
// fields without a lock, which `go test -race` reports.
//
// Introducing this type does not change any of that. What it changes is
// ownership: the rules about the access level and the username now live on the
// type that holds them instead of being restated in whichever handler happened
// to touch the two package-level variables. Making the session request-scoped
// is a behaviour change and belongs to the aggregate work in #59; the tests
// that pin the current behaviour must fail loudly when it lands.
type Session struct {
	access   string
	username string
}

// current is the one session the whole process shares.
var current = &Session{}

// Current returns the process-wide session.
func Current() *Session { return current }

// Authenticate records a successful login.
func (s *Session) Authenticate(username, access string) {
	s.access = access
	s.username = username
}

// Reject records a failed login: the caller becomes a guest with no identity.
func (s *Session) Reject() {
	s.access = "guest"
	s.username = ""
}

// Clear discards the identity, as a logout does.
func (s *Session) Clear() {
	s.access = ""
	s.username = ""
}

// Username reports the authenticated username, empty when there is none.
func (s *Session) Username() string { return s.username }

// Access reports the access level, empty when nobody has logged in.
func (s *Session) Access() string { return s.access }

// IsAuthenticated reports whether the session carries a usable identity: an
// access level that is neither empty nor guest, together with a username.
func (s *Session) IsAuthenticated() bool {
	return s.access != "" && s.access != "guest" && s.username != ""
}

// IsAdmin reports whether the session is the admin identity. Both the access
// level and the username have to say admin, which is what the admin-only
// handlers check.
func (s *Session) IsAdmin() bool {
	return s.access == "admin" && s.username == "admin"
}
