package audit

import (
	"net/http"
	"time"
)

// Caller reports who made a request. It is supplied by the router rather than
// resolved here: identity is a bounded context of its own, and this package
// records what it is told.
type Caller func(*http.Request) (username, access string)

// Middleware records every request that reaches it.
//
// It is applied to the group of routes that change something, so the trail
// covers the writes without a per-handler change -- and a write added to that
// group later is recorded without anyone remembering to.
//
// The status is recorded, so a refused attempt is in the trail too: somebody
// who tried to delete a task and was denied is worth as much as somebody who
// succeeded.
func Middleware(caller Caller) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			recorder := &statusRecorder{ResponseWriter: w}
			started := time.Now()

			next.ServeHTTP(recorder, r)

			var username, access string
			if caller != nil {
				username, access = caller(r)
			}
			Record(Entry{
				At:         started,
				Username:   username,
				Access:     access,
				Method:     r.Method,
				Path:       r.URL.Path,
				Status:     recorder.status(),
				RemoteAddr: callerAddress(r),
			})
		})
	}
}

// statusRecorder remembers the status the handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	if s.code == 0 {
		s.code = code
	}
	s.ResponseWriter.WriteHeader(code)
}

// Write covers a handler that never calls WriteHeader, which is a 200.
func (s *statusRecorder) Write(b []byte) (int, error) {
	if s.code == 0 {
		s.code = http.StatusOK
	}
	return s.ResponseWriter.Write(b)
}

// status reports what was written, treating a handler that wrote nothing at all
// as the 200 net/http sends for it.
func (s *statusRecorder) status() int {
	if s.code == 0 {
		return http.StatusOK
	}
	return s.code
}
