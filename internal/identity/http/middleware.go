package identityhttp

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/retail-ai-inc/sync/internal/identity/app"
	"github.com/retail-ai-inc/sync/internal/identity/domain"
)

// Principal is the identity a request proved.
//
// It is carried on the request context rather than in a package variable. The
// package variable it replaces was shared by every request in the process, so
// one caller's login decided who a concurrent request was treated as (T-070).
type Principal struct {
	Username string
	Access   string
}

func (p Principal) IsAdmin() bool { return p.Access == domain.AccessAdmin }

type principalKey struct{}

// PrincipalFrom reports the identity a request proved, and whether it proved
// one. A handler behind RequireAuth can rely on the second value being true.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

func withPrincipal(r *http.Request, p Principal) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), principalKey{}, p))
}

func deny(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success":      false,
		"errorCode":    http.StatusText(status),
		"errorMessage": message,
	})
}

// RequireAuth rejects a request that does not carry a usable token.
//
// Before this existed, four handlers checked the Authorization header for
// themselves and the other twenty-six did not. Anyone who could reach the port
// could list every replication task — credentials included — create and delete
// tasks, read schemas, drive the connection prober against arbitrary hosts, and
// list the users.
func RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := r.Header.Get("Authorization")
		if header == "" {
			deny(w, http.StatusUnauthorized, "Authentication required")
			return
		}

		valid, username, access := app.ValidateUserToken(domain.ExtractTokenFromHeader(header))
		if !valid {
			deny(w, http.StatusUnauthorized, "Invalid or expired token")
			return
		}

		next.ServeHTTP(w, withPrincipal(r, Principal{Username: username, Access: access}))
	})
}

// RequireAdmin rejects a request whose identity is not an administrator. It is
// applied on top of RequireAuth, so a request that reaches it has an identity.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFrom(r.Context())
		if !ok {
			deny(w, http.StatusUnauthorized, "Authentication required")
			return
		}
		if !principal.IsAdmin() {
			deny(w, http.StatusForbidden, "Admin privileges required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
