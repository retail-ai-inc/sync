package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

func cmd(parts ...string) *Command {
	args := make([][]byte, 0, len(parts))
	for _, p := range parts {
		args = append(args, []byte(p))
	}
	return &Command{Args: args}
}

// table is the command specification a server would have given us.
func table() *commandTable {
	return &commandTable{
		keyAt:   map[string]int{"set": 1, "del": 1, "hset": 1, "eval": 0, "get": 1},
		movable: map[string]bool{"eval": true},
	}
}

// FLUSHALL on the source and FLUSHALL typed by mistake are indistinguishable
// from here, and replicating it would mean one fat-fingered command in Tokyo
// destroys the disaster-recovery copy in Osaka at the same moment.
func TestEmptyingTheSourceIsNeverReplicated(t *testing.T) {
	for _, name := range []string{"FLUSHALL", "flushall", "FLUSHDB", "SWAPDB"} {
		t.Run(name, func(t *testing.T) {
			class, key, err := table().classify(context.Background(), nil, cmd(name))
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if class != classRefused {
				t.Errorf("%s classified as %v, want refused — replicating it "+
					"would empty the disaster-recovery copy", name, class)
			}
			if key != nil {
				t.Errorf("%s produced key %q, want none", name, key)
			}
		})
	}
}

// A PING is proof the link is alive; REPLCONF is the protocol talking to
// itself; PUBLISH is not state at all — forwarding it would deliver the
// message twice to anything listening in both regions.
func TestTheCommandsThatCarryNoDataAreRecognised(t *testing.T) {
	for _, c := range []struct {
		name string
		want classification
	}{
		{"PING", classHeartbeat},
		{"MULTI", classTransactionBegin},
		{"EXEC", classTransactionEnd},
		{"REPLCONF", classIgnored},
		{"PUBLISH", classIgnored},
		{"SPUBLISH", classIgnored},
		// SELECT is not ignored: it says which database follows it.
		{"SELECT", classSelect},
	} {
		t.Run(c.name, func(t *testing.T) {
			class, _, err := table().classify(context.Background(), nil, cmd(c.name, "x"))
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if class != c.want {
				t.Errorf("%s classified as %v, want %v", c.name, class, c.want)
			}
		})
	}
}

// TestAWriteReportsTheKeyThatNamesItsSlot is what decides which slot's marker
// moves.
func TestAWriteReportsTheKeyThatNamesItsSlot(t *testing.T) {
	class, key, err := table().classify(context.Background(), nil, cmd("SET", "user:1", "v"))
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if class != classWrite {
		t.Errorf("SET classified as %v, want write", class)
	}
	if string(key) != "user:1" {
		t.Errorf("key = %q, want user:1", key)
	}
}

// TestACommandTheTargetDoesNotKnowIsRefusedLoudly: guessing which key an
// unknown command touches would put its marker in the wrong slot, and applying
// it would fail anyway.
func TestACommandTheTargetDoesNotKnowIsRefusedLoudly(t *testing.T) {
	class, _, err := table().classify(context.Background(), nil, cmd("JSON.SET", "doc", "$", "1"))
	if class != classRefused {
		t.Errorf("classified as %v, want refused", class)
	}
	if err == nil {
		t.Fatal("an unknown command was refused silently")
	}
	if !domain.IsUnrecoverable(err) {
		t.Errorf("error is %v, want an unrecoverable one — retrying cannot teach "+
			"the target a command it does not have", err)
	}
	if !strings.Contains(err.Error(), "JSON.SET") {
		t.Errorf("error %q does not name the command", err)
	}
}

// TestACommandMissingItsKeyIsRefused guards against a truncated or malformed
// command silently becoming a no-op.
func TestACommandMissingItsKeyIsRefused(t *testing.T) {
	class, _, err := table().classify(context.Background(), nil, cmd("SET"))
	if class != classRefused {
		t.Errorf("classified as %v, want refused", class)
	}
	if err == nil {
		t.Fatal("a command with no key was accepted")
	}
}

// TestAnEmptyCommandIsIgnored: the stream carries protocol noise, and an empty
// command is not something to stop replication over.
func TestAnEmptyCommandIsIgnored(t *testing.T) {
	class, key, err := table().classify(context.Background(), nil, cmd())
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if class != classIgnored {
		t.Errorf("classified as %v, want ignored", class)
	}
	if key != nil {
		t.Errorf("key = %q, want none", key)
	}
}

// TestTheDatabaseIsReadFromSelect: a standalone server interleaves every
// database into one replication stream, so which one a command belongs to is
// only knowable from the SELECT before it.
func TestTheDatabaseIsReadFromSelect(t *testing.T) {
	for _, c := range []struct {
		arg  string
		want int
		bad  bool
	}{
		{"0", 0, false},
		{"1", 1, false},
		{"11", 11, false},
		{"-1", 0, true},
		{"x", 0, true},
	} {
		got, err := selectedDB(cmd("SELECT", c.arg))
		if c.bad {
			if err == nil {
				t.Errorf("SELECT %q was accepted as database %d", c.arg, got)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("SELECT %q = %d, %v; want %d", c.arg, got, err, c.want)
		}
	}
	if _, err := selectedDB(cmd("SELECT")); err == nil {
		t.Error("SELECT with no database was accepted")
	}
}
