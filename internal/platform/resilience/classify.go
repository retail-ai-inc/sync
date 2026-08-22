package resilience

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"syscall"

	"go.mongodb.org/mongo-driver/mongo"
)

// Whether a failure is worth trying again is the one judgement this package
// makes, and it used to be a substring scan over the error text that was wrong
// in both directions. "connection" matched a malformed connection string, so a
// typo in the configuration was retried for seven seconds before being
// reported; meanwhile the failures a regional failover actually produces —
// "server selection error", "no reachable servers", "topology is closed",
// "Deadlock found when trying to get lock" — matched nothing at all and were
// given up on at the first attempt.
//
// The order below is what makes it work: a failure that cannot be fixed by
// waiting is recognised first, then the typed checks the drivers offer, and the
// text scan is only the last resort.

// permanentPhrases name failures that waiting cannot fix: the configuration is
// wrong, the statement is wrong, or the credentials are wrong. Retrying these
// wastes the backoff and buries the real message.
var permanentPhrases = []string{
	"invalid connection string",
	"error parsing uri",
	"scheme must be",
	"unknown driver",
	"unsupported",
	"malformed",
	"while parsing",
	"unexpected end of json",
	"invalid json",
	"syntax error",
	"access denied",
	"authentication failed",
	"auth error",
	"not authorized",
	"permission denied",
	"duplicate entry",
	"duplicate key",
	"unknown database",
	"no such table",
	"unknown column",
}

// transientPhrases name failures that a later attempt may well survive. Most of
// them are what a driver says while a replica set elects a new primary or a
// managed instance restarts for maintenance — the moments this tool exists for.
var transientPhrases = []string{
	"connection refused",
	"connection reset",
	"connection closed",
	"connection timed out",
	"connection handshake",
	"bad connection",
	"broken pipe",
	"i/o timeout",
	"network is unreachable",
	"no route to host",
	"no reachable servers",
	"server selection error",
	"topology is closed",
	"client is disconnected",
	"socket was unexpectedly closed",
	"context deadline exceeded",
	"database is locked",
	"deadlock found",
	"lock wait timeout",
	"too many connections",
	"lost connection",
	"server has gone away",
	"not master",
	"not primary",
	"node is recovering",
	"interruptedatshutdown",
	"shutdown in progress",
	"temporarily unavailable",
	"try again",
	"eof",
}

// IsConnectionError reports whether an error is worth another attempt.
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}

	// A cancelled context is this process deciding to stop. Retrying through it
	// is how a task asked to shut down went on writing to the target.
	if errors.Is(err, context.Canceled) {
		return false
	}

	text := strings.ToLower(err.Error())
	for _, phrase := range permanentPhrases {
		if strings.Contains(text, phrase) {
			return false
		}
	}

	if errors.Is(err, driver.ErrBadConn) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) {
		return true
	}

	for _, phrase := range transientPhrases {
		if strings.Contains(text, phrase) {
			return true
		}
	}
	return false
}
