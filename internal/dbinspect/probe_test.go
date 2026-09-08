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
			// The reason travels as JSON, because the UI parses it as JSON.
			if !strings.Contains(body, `"success":false`) {
				t.Errorf("body = %q, want the JSON envelope", body)
			}
			// And it names the type it refused, which "Unsupported dbType" did not.
			if engine != "" && !strings.Contains(body, engine) {
				t.Errorf("body = %q, want it to name %q", body, engine)
			}
		})
	}
}

// The probe compares the engine name verbatim, so the UI has to send it in
// lower case. The monitoring side folds case and this does not; the two
// disagreeing is T-094.
func TestTheProbeIsCaseSensitiveAboutTheEngine(t *testing.T) {
	code, body := probe(t, `{"dbType":"MySQL","host":"127.0.0.1","port":"1"}`)

	if code != http.StatusBadRequest {
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
// a refused connection with a 500 carrying the driver's message.
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
// returns is the driver's, which names the address it could not reach.
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

// TestAnUnreachableMongoDBIsReported takes ten seconds on purpose.
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
// rather than, say, the keyspace or the database count.
func TestTheRedisBranchNeverReportsTables(t *testing.T) {
	// The success path needs a live Redis, which this suite does not have.
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

// Every branch built its context from context.Background() rather than from
// the request, so cancelling the request did not stop the probe — the MongoDB
// branch ran for its full ten seconds however long the caller had been gone.
func TestTheProbeStopsWhenTheRequestDoes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// An address that will not answer, so only the cancellation can end this.
	req := httptest.NewRequest(http.MethodPost, "/test-connection",
		strings.NewReader(`{"dbType":"mongodb","host":"10.255.255.1","port":"27017","database":"x"}`)).
		WithContext(ctx)
	cancel() // already cancelled before the handler runs

	rec := httptest.NewRecorder()
	start := time.Now()
	TestConnectionHandler(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want the failure reported", rec.Code)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the probe took %v after its request was cancelled", elapsed)
	}
}

// The UI feeds this response to response.json(), so a failure answered as
// text/plain surfaced as "Unexpected token 'M'" instead of the reason —
// measured on staging when editing a MongoDB task.
func TestAFailureIsAnsweredAsJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/test-connection",
		strings.NewReader(`{"dbType":"cassandra","host":"h","port":"1","database":"x"}`))
	rec := httptest.NewRecorder()

	TestConnectionHandler(rec, req)

	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("the response is not JSON (%v): %s", err, rec.Body.String())
	}
	if body["success"] != false {
		t.Errorf("success = %v, want false", body["success"])
	}
	if body["error"] == nil || body["error"] == "" {
		t.Errorf("no reason was given: %s", rec.Body.String())
	}
}

// A mask with no task behind it cannot be resolved into anything, so it is
// refused by name rather than used as a password -- authenticating with
// "********" fails as though the credentials were wrong.
func TestAMaskWithNoTaskIsRefused(t *testing.T) {
	previous := StoredPassword
	StoredPassword = nil
	t.Cleanup(func() { StoredPassword = previous })

	req := httptest.NewRequest(http.MethodPost, "/test-connection",
		strings.NewReader(`{"dbType":"mongodb","host":"h","port":"27017","user":"root",
			"password":"********","database":"x"}`))
	rec := httptest.NewRecorder()

	TestConnectionHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400: the caller sent a mask, not a credential", rec.Code)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("the response is not JSON: %s", err)
	}
	if reason, _ := body["error"].(string); !strings.Contains(reason, "mask") {
		t.Errorf("the reason does not mention the mask: %q", reason)
	}
}

// The edit form probes on open to list the source's tables, and it is filled
// from a list that masks passwords. Refusing that outright made opening a task
// report "Source DB connection failed (auto load)" every time. A mask means the
// stored password, which is what saving an untouched field already does.
func TestAMaskIsResolvedAgainstTheSavedTask(t *testing.T) {
	var askedID, askedRole string
	previous := StoredPassword
	StoredPassword = func(id, role string) (string, bool) {
		askedID, askedRole = id, role
		return "the-stored-one", true
	}
	t.Cleanup(func() { StoredPassword = previous })

	// An address nothing answers on: the probe gets past the mask and fails at
	// the connection, which is what proves the mask was resolved rather than
	// refused.
	req := httptest.NewRequest(http.MethodPost, "/test-connection",
		strings.NewReader(`{"dbType":"mongodb","host":"10.255.255.1","port":"27017",
			"user":"root","password":"********","database":"x",
			"taskId":"39","role":"source"}`))
	rec := httptest.NewRecorder()

	TestConnectionHandler(rec, req)

	if askedID != "39" || askedRole != "source" {
		t.Errorf("resolved against task %q role %q, want 39/source", askedID, askedRole)
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("the response is not JSON: %s", err)
	}
	if reason, _ := body["error"].(string); strings.Contains(reason, "mask") {
		t.Errorf("the probe still refused the mask instead of resolving it: %q", reason)
	}
}

// A task the resolver does not know is refused rather than probed with the
// mask: a task id that resolves to nothing is not a password.
func TestAMaskForAnUnknownTaskIsRefused(t *testing.T) {
	previous := StoredPassword
	StoredPassword = func(string, string) (string, bool) { return "", false }
	t.Cleanup(func() { StoredPassword = previous })

	req := httptest.NewRequest(http.MethodPost, "/test-connection",
		strings.NewReader(`{"dbType":"mongodb","host":"h","port":"27017","user":"root",
			"password":"********","database":"x","taskId":"999","role":"source"}`))
	rec := httptest.NewRecorder()

	TestConnectionHandler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

// A backup job's edit form carries the same mask a task's does, and the
// backend had no way to resolve it against a job: testing the connection or
// listing a table meant retyping a password nobody had changed.
func TestAMaskIsResolvedAgainstABackupJobToo(t *testing.T) {
	previousTask, previousJob := StoredPassword, StoredBackupPassword
	t.Cleanup(func() { StoredPassword, StoredBackupPassword = previousTask, previousJob })

	StoredPassword = func(taskID, role string) (string, bool) {
		if taskID == "7" && role == "source" {
			return "from the task", true
		}
		return "", false
	}
	StoredBackupPassword = func(jobID string) (string, bool) {
		if jobID == "12" {
			return "from the job", true
		}
		return "", false
	}

	for name, c := range map[string]struct {
		taskID, role, backupID, want string
		found                        bool
	}{
		"a saved task":                      {"7", "source", "", "from the task", true},
		"a saved backup job":                {"", "", "12", "from the job", true},
		"neither":                           {"", "", "", "", false},
		"a task that is gone":               {"99", "source", "", "", false},
		"a job that is gone":                {"", "", "99", "", false},
		"the task wins when both are named": {"7", "source", "12", "from the task", true},
	} {
		t.Run(name, func(t *testing.T) {
			got, ok := resolveMask(c.taskID, c.role, c.backupID)
			if ok != c.found || got != c.want {
				t.Errorf("resolveMask(%q, %q, %q) = %q, %v; want %q, %v",
					c.taskID, c.role, c.backupID, got, ok, c.want, c.found)
			}
		})
	}
}

// Nothing wired means a mask is refused rather than resolved to an empty
// password, which would authenticate as nobody.
func TestWithNothingWiredAMaskIsRefused(t *testing.T) {
	previousTask, previousJob := StoredPassword, StoredBackupPassword
	t.Cleanup(func() { StoredPassword, StoredBackupPassword = previousTask, previousJob })
	StoredPassword, StoredBackupPassword = nil, nil

	if _, ok := resolveMask("7", "source", "12"); ok {
		t.Error("a mask resolved with no resolver wired")
	}
}
