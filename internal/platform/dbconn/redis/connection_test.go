package redis

import (
	"strings"
	"testing"
	"time"
)

func TestGetRedisClientRejectsAMalformedDSN(t *testing.T) {
	for _, dsn := range []string{
		"",
		"127.0.0.1:6379",         // no scheme
		"http://127.0.0.1:6379",  // wrong scheme
		"redis://:bad-port-here", // unparseable
		"not a url at all",
	} {
		t.Run(dsn, func(t *testing.T) {
			client, err := GetRedisClient(dsn)
			if err == nil {
				_ = client.Close()
				t.Fatalf("GetRedisClient(%q) succeeded", dsn)
			}
			if !strings.Contains(err.Error(), "failed to parse redis DSN") {
				t.Errorf("error = %q, want the parse failure", err)
			}
			if client != nil {
				t.Error("a client was returned alongside the error")
			}
		})
	}
}

// TestGetRedisClientPingsBeforeReturning records that this helper does connect.
func TestGetRedisClientPingsBeforeReturning(t *testing.T) {
	start := time.Now()

	client, err := GetRedisClient("redis://127.0.0.1:1/0")
	if err == nil {
		_ = client.Close()
		t.Fatal("something answered on port 1")
	}
	if !strings.Contains(err.Error(), "failed to ping redis") {
		t.Errorf("error = %q, want the ping failure", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("GetRedisClient took %v, want under the three-second ceiling", elapsed)
	}
}

// TestThePasswordIsNotEchoedInTheError pins the one thing the failure must not
// carry: a DSN with credentials in it must not appear in the message a caller
// might log or return.
func TestThePasswordIsNotEchoedInTheError(t *testing.T) {
	_, err := GetRedisClient("redis://:hunter2@127.0.0.1:1/0")
	if err == nil {
		t.Fatal("something answered on port 1")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("the error echoed the password: %q", err)
	}
}

// A URL has room for one authority, so the builder joins the seeds with commas.
func TestAMultiSeedClusterDSNReachesEverySeed(t *testing.T) {
	got := splitSeeds([]string{"127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"})
	want := []string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}

	if len(got) != len(want) {
		t.Fatalf("splitSeeds gave %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("seed %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestASingleSeedIsLeftAlone(t *testing.T) {
	got := splitSeeds([]string{"10.0.0.1:6379"})
	if len(got) != 1 || got[0] != "10.0.0.1:6379" {
		t.Errorf("splitSeeds gave %v, want one unchanged seed", got)
	}
}

func TestBlankSeedsAreDropped(t *testing.T) {
	got := splitSeeds([]string{"a:1, ,b:2,"})
	if len(got) != 2 {
		t.Errorf("splitSeeds gave %v, want the empty entries dropped", got)
	}
}
