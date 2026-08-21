package domain

// The access levels and account statuses this system recognises. They were
// previously restated as string literals inside the handler that validates
// them; naming them here gives the vocabulary one definition.
const (
	AccessAdmin = "admin"
	AccessGuest = "guest"

	StatusActive   = "active"
	StatusInactive = "inactive"
)

// IsValidAccess reports whether s names an access level a user may be given.
// An empty string is valid: it means "leave the access level alone".
func IsValidAccess(s string) bool {
	return s == "" || s == AccessAdmin || s == AccessGuest
}

// IsValidStatus reports whether s names an account status a user may be given.
// An empty string is valid: it means "leave the status alone".
func IsValidStatus(s string) bool {
	return s == "" || s == StatusActive || s == StatusInactive
}

// IsDeactivated reports whether an account status denies sign-in.
func IsDeactivated(s string) bool { return s == StatusInactive }
