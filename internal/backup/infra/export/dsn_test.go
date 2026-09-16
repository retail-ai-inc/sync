package export

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAMongoPasswordNeverReachesTheCommandLine covers the leak the MySQL path
// was already fixed for.
//
// Anything in argv is in /proc/<pid>/cmdline for the life of the process,
// readable by anyone on the node, and this is the password to the payment
// database. Masking only ever applied to the copy that was logged.
func TestAMongoPasswordNeverReachesTheCommandLine(t *testing.T) {
	uri, password := splitMongoPassword("mongodb://backup:hunter2@mongos:27017/?authSource=admin")

	if password != "hunter2" {
		t.Errorf("password = %q, want it taken out of the URI", password)
	}
	if strings.Contains(uri, "hunter2") {
		t.Errorf("uri = %q, still carries the password", uri)
	}
	if !strings.Contains(uri, "backup@") {
		t.Errorf("uri = %q, want the username kept", uri)
	}

	// A source with no authentication is left exactly as it was.
	plain, none := splitMongoPassword("mongodb://mongos:27017/?directConnection=true")
	if none != "" || plain != "mongodb://mongos:27017/?directConnection=true" {
		t.Errorf("a URI with no credentials came back as %q / %q", plain, none)
	}
}

func TestTheMongoCredentialsFileIsPrivateAndTemporary(t *testing.T) {
	path, remove, err := mongoConfigFile(`pa"ss\word`)
	if err != nil {
		t.Fatalf("mongoConfigFile: %v", err)
	}
	defer remove()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("mode = %o, want 0600", mode)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(body), `password: "pa\"ss\\word"`) {
		t.Errorf("file = %q, want the password quoted", body)
	}

	remove()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("the credentials file outlived the export")
	}

	// No password, no file.
	empty, removeEmpty, err := mongoConfigFile("")
	defer removeEmpty()
	if err != nil || empty != "" {
		t.Errorf("mongoConfigFile(\"\") = %q (%v), want no file", empty, err)
	}
}

// A credentials file that cannot be written is an error, not a backup that
// quietly runs without the password and exports nothing.
func TestACredentialsFileThatCannotBeWrittenIsReported(t *testing.T) {
	// TMPDIR at a path that is not a directory: CreateTemp cannot work there.
	blocker := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	t.Setenv("TMPDIR", blocker)

	path, remove, err := mongoConfigFile("hunter2")
	defer remove()
	if err == nil {
		t.Errorf("mongoConfigFile wrote %q under a TMPDIR that is a file", path)
	}
	if path != "" {
		t.Errorf("a failed write still returned a path: %q", path)
	}
}
