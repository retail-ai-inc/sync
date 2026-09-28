package httpx

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

// ErrorJSON answers a request that failed.
//
// It sets a status code. It used to write only the body, so every failure this
// helper produced was a 200 — and anything that branches on the status rather
// than on a field of the body (a load balancer, a health probe, a generated
// client) read a failure as a success.
func ErrorJSON(w http.ResponseWriter, msg string, err error) {
	ErrorJSONStatus(w, http.StatusInternalServerError, msg, err)
}

// ErrorJSONStatus answers with a particular status, for the failures that are
// the caller's rather than this program's.
func ErrorJSONStatus(w http.ResponseWriter, status int, msg string, err error) {
	detail := ""
	if err != nil {
		// The error used to be dereferenced unconditionally, so a caller with
		// nothing to attach crashed the request.
		detail = err.Error()
	}
	logrus.Errorf("%s => %v", msg, err)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   msg,
		"detail":  detail,
	})
}

func WriteJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

// TimeNowStr returns the current time formatted as a string in UTC timezone
// for database storage purposes
func TimeNowStr() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05")
}

// ConvertTimeToJST converts a time string from UTC to JST timezone for SQL time format
// This handles the specific format used in the database "2006-01-02 15:04:05"
func ConvertTimeToJST(input string) string {
	if input == "" {
		return ""
	}

	// First try parsing with standard SQL format
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, input)
	if err == nil {
		jst := time.FixedZone("JST", 9*60*60)
		return t.In(jst).Format(layout)
	}

	// If that fails, try RFC3339 format
	t, err = time.Parse(time.RFC3339, input)
	if err == nil {
		jst := time.FixedZone("JST", 9*60*60)
		return t.In(jst).Format(layout)
	}

	return input
}

// RedactedPassword is what a stored password is replaced with on its way out.
// A fixed string rather than the real length, so it says nothing about the
// value it hides.
const RedactedPassword = "********"

// WithoutPassword copies a connection map with its password masked.
//
// Both list endpoints answer with the connection settings they hold, and those
// carry the passwords the process authenticates with. The replication side
// masked them and the backup side did not, so GET /api/backup handed the
// source and destination database passwords to anybody holding a token — in
// the clear, because they are also stored that way unless SYNC_CONFIG_KEY is
// set.
//
// It lives here rather than in either context so the next endpoint answering
// with a connection map has one obvious thing to call.
func WithoutPassword(conn map[string]interface{}) map[string]interface{} {
	if conn == nil {
		return nil
	}
	safe := make(map[string]interface{}, len(conn))
	for k, v := range conn {
		safe[k] = v
	}
	if text, ok := safe["password"].(string); ok && text != "" {
		safe["password"] = RedactedPassword
	}
	return safe
}
