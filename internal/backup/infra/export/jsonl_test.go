package export

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeFile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "export.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func merge(t *testing.T, path string) string {
	t.Helper()
	var out bytes.Buffer
	writer := bufio.NewWriter(&out)
	if err := appendJSONLines(path, writer); err != nil {
		t.Fatalf("appendJSONLines: %v", err)
	}
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return out.String()
}

func TestEveryDocumentReachesTheMergedFile(t *testing.T) {
	path := writeFile(t, "{\"_id\":1}\n{\"_id\":2}\n{\"_id\":3}\n")

	got := merge(t, path)
	if got != "{\"_id\":1}\n{\"_id\":2}\n{\"_id\":3}\n" {
		t.Errorf("merged = %q", got)
	}
}

// A file that does not end in a newline still ends in a document. Reading the
// whole file and splitting on newlines happened to handle this; a reader that
// stops at the last newline would drop it.
func TestTheLastDocumentSurvivesAMissingNewline(t *testing.T) {
	path := writeFile(t, "{\"_id\":1}\n{\"_id\":2}")

	got := merge(t, path)
	if strings.Count(got, "\n") != 2 {
		t.Errorf("merged = %q, want both documents each followed by a newline", got)
	}
	if !strings.Contains(got, `{"_id":2}`) {
		t.Errorf("the last document was dropped: %q", got)
	}
}

// Blank lines and anything that is not an object are dropped, which is what
// the merge did before: mongoexport writes nothing else, and a line that is
// not an object would make the merged file unreadable as JSONL.
func TestOnlyObjectsAreCarriedThrough(t *testing.T) {
	path := writeFile(t, "\n{\"_id\":1}\n\n   \nnot json\n[1,2]\n{\"_id\":2}\n\n")

	got := merge(t, path)
	if got != "{\"_id\":1}\n{\"_id\":2}\n" {
		t.Errorf("merged = %q", got)
	}
}

func TestAnEmptyExportContributesNothing(t *testing.T) {
	if got := merge(t, writeFile(t, "")); got != "" {
		t.Errorf("merged = %q, want nothing", got)
	}
	if got := merge(t, writeFile(t, "\n\n  \n")); got != "" {
		t.Errorf("merged = %q, want nothing", got)
	}
}

// A BSON document may be 16MB and its JSON form is larger, so a line well past
// any read buffer has to come through whole.
func TestADocumentLargerThanTheReadBufferIsNotSplit(t *testing.T) {
	big := strings.Repeat("x", 4<<20)
	path := writeFile(t, fmt.Sprintf("{\"_id\":1}\n{\"blob\":%q}\n{\"_id\":3}\n", big))

	got := merge(t, path)
	if lines := strings.Count(got, "\n"); lines != 3 {
		t.Fatalf("merged holds %d lines, want 3", lines)
	}
	if !strings.Contains(got, big) {
		t.Error("the large document did not survive")
	}
}

// A file that cannot be read is reported rather than skipped: a collection
// silently missing from a backup is the failure this is guarding.
func TestAnUnreadableExportIsReported(t *testing.T) {
	var out bytes.Buffer
	writer := bufio.NewWriter(&out)
	if err := appendJSONLines(filepath.Join(t.TempDir(), "absent.json"), writer); err == nil {
		t.Error("a file that is not there was merged without complaint")
	}
}

func TestAFileIsCopiedThroughVerbatim(t *testing.T) {
	body := "INSERT INTO t VALUES (1);\n-- a comment\n\nINSERT INTO t VALUES (2);\n"
	path := writeFile(t, body)

	var out bytes.Buffer
	if err := appendFile(path, &out); err != nil {
		t.Fatalf("appendFile: %v", err)
	}
	// Verbatim: the SQL merge writes what mysqldump produced, comments,
	// blank lines and all.
	if out.String() != body {
		t.Errorf("copied = %q, want %q", out.String(), body)
	}
}

func TestAnUnreadableFileIsReported(t *testing.T) {
	var out bytes.Buffer
	if err := appendFile(filepath.Join(t.TempDir(), "absent.sql"), &out); err == nil {
		t.Error("a file that is not there was copied without complaint")
	}
}
