// Package audit records who changed what through the API.
//
// Every write went unrecorded: a task could be stopped, its target
// repointed or deleted outright, and the only trace was the effect. For a
// deployment carrying payments between two regions, "when did this task stop
// and who stopped it" has to be answerable afterwards, and the process log is
// not that answer -- it rotates, and it says what happened without saying who
// asked.
package audit

import (
	"database/sql"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"
)

// Entry is one recorded change.
//
// Deliberately absent: the request body. An audit trail of task edits would
// otherwise hold the source and target credentials of every task ever saved,
// in a table that is read by a wider audience than the one that may see them.
// The method and path say what was changed; the task's own history says what it
// was changed to.
type Entry struct {
	At         time.Time
	Username   string
	Access     string
	Method     string
	Path       string
	Status     int
	RemoteAddr string
}

// Record stores one entry.
//
// A failure to record is logged and not returned. The alternative is refusing
// the operation the caller already performed, which would mean an unreachable
// control database could stop a task from being started -- the audit trail
// must not be able to take the system down.
func Record(entry Entry) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		logrus.Warnf("[Audit] %s %s by %q was not recorded: %v",
			entry.Method, entry.Path, entry.Username, err)
		return
	}
	defer db.Close()

	if err := insert(db, entry); err != nil {
		logrus.Warnf("[Audit] %s %s by %q was not recorded: %v",
			entry.Method, entry.Path, entry.Username, err)
	}
}

func insert(db *sql.DB, entry Entry) error {
	at := entry.At
	if at.IsZero() {
		at = time.Now()
	}
	_, err := db.Exec(`
INSERT INTO audit_log (at, username, access, method, path, status, remote_addr)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		at.UTC().Format("2006-01-02 15:04:05"), entry.Username, entry.Access,
		entry.Method, entry.Path, entry.Status, entry.RemoteAddr)
	return err
}

// callerAddress reports the address a request came from.
//
// X-Forwarded-For is taken over the socket address because the deployment is
// behind an ingress, where every socket address is the proxy's. Only the first
// entry is kept: the rest are appended by hops the caller could have written
// itself.
func callerAddress(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		if first, _, found := strings.Cut(forwarded, ","); found {
			return strings.TrimSpace(first)
		}
		return strings.TrimSpace(forwarded)
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}
