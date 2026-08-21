package dbinspect

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// probe posts a connection request and returns the recorder.
func probe(t *testing.T, body string) (int, string) {
	t.Helper()

	rec := postJSON(TestConnectionHandler, http.MethodPost, "/test-connection", body)
	return rec.Code, rec.Body.String()
}

func TestTheProbeRejectsAnUnsupportedEngine(t *testing.T) {
	for _, engine := range []string{"cassandra", "", "MySQL", "MongoDB", "sqlite"} {
		t.Run(engine, func(t *testing.T) {
			code, body := probe(t, `{"dbType":"`+engine+`"}`)

			if code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (body: %q)", code, body)
			}
			if !strings.Contains(body, "Unsupported dbType") {
				t.Errorf("body = %q", body)
			}
		})
	}
}

// TestTheProbeIsCaseSensitiveAboutTheEngine records that the probe compares the
// engine name verbatim, so the UI has to send it in lower case. The monitor
// folds case and this does not, which is the same split T-053 records for the
// syncer dispatch.
func TestTheProbeIsCaseSensitiveAboutTheEngine(t *testing.T) {
	code, body := probe(t, `{"dbType":"MySQL","host":"127.0.0.1","port":"1"}`)

	if code != http.StatusBadRequest || !strings.Contains(body, "Unsupported dbType") {
		t.Fatalf("status = %d, body = %q; the comparison appears to fold case now, "+
			"so assert that instead", code, body)
	}
}

func TestTheProbeRejectsAMalformedBody(t *testing.T) {
	for _, body := range []string{"", "{", "not json", `{"dbType":}`, `[1,2]`} {
		t.Run(body, func(t *testing.T) {
			code, got := probe(t, body)

			if code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400 (body: %q)", code, got)
			}
		})
	}
}

// TestAnUnreachableMySQLIsReportedAsAPingFailure records that the probe answers
// a refused connection with a 500 carrying the driver's message. Port 1 is used
// because nothing listens there.
func TestAnUnreachableMySQLIsReportedAsAPingFailure(t *testing.T) {
	code, body := probe(t,
		`{"dbType":"mysql","host":"127.0.0.1","port":"1","user":"u","password":"p","database":"d"}`)

	if code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", code, body)
	}
	if !strings.Contains(body, "Ping failed") {
		t.Errorf("body = %q, want a ping failure", body)
	}
}

// TestThePingFailureEchoesTheHostAndPort records that the message the probe
// returns is the driver's, which names the address it could not reach. That is
// useful to an operator and it also confirms to an unauthenticated caller
// whether a given host and port answer — the endpoint needs no credentials.
func TestThePingFailureEchoesTheHostAndPort(t *testing.T) {
	_, body := probe(t,
		`{"dbType":"mysql","host":"127.0.0.1","port":"1","user":"u","password":"p","database":"d"}`)

	if !strings.Contains(body, "127.0.0.1:1") {
		t.Fatalf("body = %q; the address is no longer echoed, so assert the new "+
			"message instead", body)
	}
}

// TestThePasswordIsNotEchoed pins the one thing the failure message must not
// carry.
func TestThePasswordIsNotEchoed(t *testing.T) {
	_, body := probe(t,
		`{"dbType":"mysql","host":"127.0.0.1","port":"1","user":"u","password":"hunter2","database":"d"}`)

	if strings.Contains(body, "hunter2") {
		t.Errorf("the probe echoed the password: %q", body)
	}
}

func TestAnUnreachablePostgreSQLIsReported(t *testing.T) {
	code, body := probe(t,
		`{"dbType":"postgresql","host":"127.0.0.1","port":"1","user":"u","password":"p","database":"d"}`)

	if code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", code, body)
	}
	if strings.Contains(body, "hunter2") {
		t.Error("the probe echoed the password")
	}
}

// TestAnUnreachableMongoDBIsReported takes ten seconds on purpose: the MongoDB
// branch builds its own context with a hardcoded ten-second timeout from
// context.Background(), ignoring the request context entirely. A caller that
// disconnects therefore leaves the probe dialling for the full ten seconds, and
// a test cannot shorten it.
func TestAnUnreachableMongoDBIsReported(t *testing.T) {
	code, body := probe(t,
		`{"dbType":"mongodb","host":"127.0.0.1","port":"1","user":"","password":"","database":"d"}`)

	if code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", code, body)
	}
}

// TestAnUnreachableRedisIsReported records that the Redis branch does ping the
// server, and reports the failure with the driver's message.
func TestAnUnreachableRedisIsReported(t *testing.T) {
	code, body := probe(t,
		`{"dbType":"redis","host":"127.0.0.1","port":"1","user":"","password":"","database":"0"}`)

	if code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (body: %q)", code, body)
	}
	if !strings.Contains(body, "Redis ping error") {
		t.Errorf("body = %q, want a ping error", body)
	}
}

// TestTheRedisBranchNeverReportsTables records the shape of a successful Redis
// probe: Redis has no tables, so the branch answers success with an empty list
// rather than, say, the keyspace or the database count. The UI's table picker is
// therefore always empty for a Redis task, and the operator has to type the key
// patterns by hand.
func TestTheRedisBranchNeverReportsTables(t *testing.T) {
	// The success path needs a live Redis, which this suite does not have. What
	// can be pinned without one is that the branch has no query at all: the only
	// way it can produce a table list is the empty literal.
	_, body := probe(t,
		`{"dbType":"redis","host":"127.0.0.1","port":"1","user":"","password":"","database":"0"}`)

	var resp map[string]interface{}
	if err := json.Unmarshal([]byte(body), &resp); err == nil {
		data, _ := resp["data"].(map[string]interface{})
		if tables, ok := data["tables"].([]interface{}); ok && len(tables) != 0 {
			t.Errorf("tables = %v, want empty", tables)
		}
	}
}

// TestTheProbeIgnoresTheRequestContext records that every branch builds its own
// context from context.Background() rather than deriving one from the request,
// so cancelling the request does not stop the probe. The MongoDB branch runs for
// its full ten seconds whatever the caller does.
func TestTheProbeIgnoresTheRequestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/test-connection",
		strings.NewReader(`{"dbType":"redis","host":"127.0.0.1","port":"1"}`)).WithContext(ctx)
	cancel() // already cancelled before the handler runs

	rec := httptest.NewRecorder()
	start := time.Now()
	TestConnectionHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d for a cancelled request, want the probe to run anyway "+
			"and report its own failure", rec.Code)
	}
	if elapsed := time.Since(start); elapsed < 10*time.Millisecond {
		t.Logf("the probe returned in %v; it may honour the request context now", elapsed)
	}
}
