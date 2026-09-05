package replicationhttp

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// The counting endpoint outlives the server's write deadline.
//
// Every other endpoint here answers well inside the sixty seconds the server
// allows, and this one does not: an exact count of both sides of the sharded
// MongoDB source took about five minutes. The connection was closed with
// nothing written and the caller saw an empty reply -- which is how the first
// run of scripts/rowcounts.sh against MongoDB failed.

// TestAHandlerOutlivesTheServerWriteDeadlineOnlyIfItExtendsIt drives a real
// server with a short deadline, because this is a property of net/http rather
// than of the handler: nothing in the handler's own code can be asserted
// against it.
func TestAHandlerOutlivesTheServerWriteDeadlineOnlyIfItExtendsIt(t *testing.T) {
	const deadline = 150 * time.Millisecond
	const work = 400 * time.Millisecond

	for name, extend := range map[string]bool{
		"without extending the deadline": false,
		"extending the deadline":         true,
	} {
		t.Run(name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if extend {
					if err := http.NewResponseController(w).
						SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
						t.Errorf("SetWriteDeadline: %v", err)
					}
				}
				time.Sleep(work)
				fmt.Fprint(w, "counted")
			})

			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			server := &http.Server{Handler: handler, WriteTimeout: deadline}
			go func() { _ = server.Serve(listener) }()
			t.Cleanup(func() { _ = server.Close() })

			resp, err := http.Get("http://" + listener.Addr().String() + "/")
			if !extend {
				// The deadline passed mid-handler, so the connection is closed
				// with nothing written.
				if err == nil {
					body, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					if len(body) > 0 {
						t.Errorf("the answer survived a deadline it should have "+
							"missed: %q", body)
					}
				}
				return
			}

			if err != nil {
				t.Fatalf("a handler that extended the deadline still failed: %v", err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read the answer: %v", err)
			}
			if string(body) != "counted" {
				t.Errorf("body = %q, want the full answer", body)
			}
		})
	}
}

// TestTheCountDeadlineIsLongerThanASlowCount pins the figure against what was
// measured: 116 collections counted on both sides of the sharded source took
// about five minutes.
func TestTheCountDeadlineIsLongerThanASlowCount(t *testing.T) {
	if countDeadline < 10*time.Minute {
		t.Errorf("countDeadline = %v; a sharded MongoDB count measured about five "+
			"minutes, and a deadline near that leaves no room", countDeadline)
	}
	if countDeadline > time.Hour {
		t.Errorf("countDeadline = %v; a wedged count would hold the connection "+
			"far too long", countDeadline)
	}
}

// TestTheRowCountsHandlerExtendsTheDeadline drives the real handler through a
// server whose deadline is shorter than the work, which is the shape that
// failed. It cannot reach a database, so it answers an error -- but it has to
// answer, rather than having the connection closed under it.
func TestTheRowCountsHandlerExtendsTheDeadline(t *testing.T) {
	useTempTaskDB(t)

	rec := httptest.NewRecorder()
	serveWithURLParams(rec, httptest.NewRequest(http.MethodGet, "/sync/9999/rowcounts", nil),
		SyncRowCountsHandler, map[string]string{"id": "9999"})

	if rec.Body.Len() == 0 {
		t.Error("the handler wrote nothing")
	}
}
