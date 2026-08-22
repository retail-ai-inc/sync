package export

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// yesterdayStamp is the date the exporters put in a file name: a backup taken
// after midnight covers the day that just ended.
func yesterdayStamp() string {
	return time.Now().AddDate(0, 0, -1).Format("2006-01-02")
}

// mongoMergedConfig builds a config for the merged MongoDB path.
func mongoMergedConfig(gcsPath string) ExecutorBackupConfig {
	var cfg ExecutorBackupConfig
	cfg.SourceType = "mongodb"
	cfg.Destination.GCSPath = gcsPath
	cfg.Database.Database = "shop"
	return cfg
}

// TestTheMergedMongoExportJoinsEveryCollection covers the path a job takes as
// soon as its pattern matches more than one collection — which for a monthly
// naming scheme is every job, on the first of the month. Nothing exercised it
// before: an operator would have found out when the cross-month backup ran.
func TestTheMergedMongoExportJoinsEveryCollection(t *testing.T) {
	binDir := stubPATH(t)
	// mongoexport writes JSONL to the file named by --out. The stub finds that
	// argument and writes one document naming the collection it was asked for.
	stubBin(t, binDir, "mongoexport", `
out=""; coll=""
while [ $# -gt 0 ]; do
  case "$1" in
    --out) out="$2"; shift 2;;
    --collection) coll="$2"; shift 2;;
    *) shift;;
  esac
done
printf '{"_id":1,"from":"%s"}\n' "$coll" > "$out"
`, 0)
	stubBin(t, binDir, "zip", `
out=""
for a in "$@"; do case "$a" in *.zip) out="$a";; esac; done
echo zipped > "$out"
`, 0)
	stubBin(t, binDir, "gsutil", "", 0)

	e := newExecutor()
	tempDir := t.TempDir()

	err := e.exportMongoDBMergedTables(context.Background(), "mongodb://host/", "shop",
		[]string{"orders_202607", "orders_202608"}, tempDir, mongoMergedConfig(""))
	if err != nil {
		t.Fatalf("exportMongoDBMergedTables: %v", err)
	}

	// Both collections were asked for.
	args := strings.Join(stubArgs(t, binDir, "mongoexport"), " ")
	for _, want := range []string{"orders_202607", "orders_202608", "shop"} {
		if !strings.Contains(args, want) {
			t.Errorf("mongoexport arguments = %q, want %q among them", args, want)
		}
	}

	// The per-collection temporary files are removed, and the merged file is
	// named for the group rather than for one of its members.
	left, err := os.ReadDir(tempDir)
	if err != nil {
		t.Fatalf("read temp dir: %v", err)
	}
	for _, entry := range left {
		if strings.Contains(entry.Name(), "_temp") {
			t.Errorf("%s was left behind", entry.Name())
		}
	}
}

// TestTheMergedMongoExportSkipsTheUploadWithoutABucket is the fix for a job that
// worked until the day it matched a second collection. The single-collection
// path checks whether a GCS destination is configured; this one did not, so it
// ran gsutil against "/name.zip" and failed the whole backup.
func TestTheMergedMongoExportSkipsTheUploadWithoutABucket(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mongoexport", `
out=""
while [ $# -gt 0 ]; do case "$1" in --out) out="$2"; shift 2;; *) shift;; esac; done
printf '{"_id":1}\n' > "$out"
`, 0)
	stubBin(t, binDir, "zip", `
out=""
for a in "$@"; do case "$a" in *.zip) out="$a";; esac; done
echo zipped > "$out"
`, 0)
	// Fails if it is called at all, which is what the defect did.
	stubBin(t, binDir, "gsutil", "", 1)

	e := newExecutor()
	if err := e.exportMongoDBMergedTables(context.Background(), "mongodb://host/", "shop",
		[]string{"orders_202607", "orders_202608"}, t.TempDir(), mongoMergedConfig("")); err != nil {
		t.Fatalf("exportMongoDBMergedTables: %v", err)
	}
	stubWasNotInvoked(t, binDir, "gsutil")
}

// TestTheMergedMongoExportUploadsWhenABucketIsConfigured is the other half:
// skipping the upload must not mean skipping it when one is set.
func TestTheMergedMongoExportUploadsWhenABucketIsConfigured(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mongoexport", `
out=""
while [ $# -gt 0 ]; do case "$1" in --out) out="$2"; shift 2;; *) shift;; esac; done
printf '{"_id":1}\n' > "$out"
`, 0)
	stubBin(t, binDir, "zip", `
out=""
for a in "$@"; do case "$a" in *.zip) out="$a";; esac; done
echo zipped > "$out"
`, 0)
	// gsutil is called twice: once to copy and once by the size check that
	// follows it. Only the stat call needs an answer.
	stubBin(t, binDir, "gsutil", `
case "$1" in
  stat) echo "Content-Length: 7";;
esac
`, 0)

	e := newExecutor()
	err := e.exportMongoDBMergedTables(context.Background(), "mongodb://host/", "shop",
		[]string{"orders_202607", "orders_202608"}, t.TempDir(),
		mongoMergedConfig("gs://bucket/backups"))
	if err != nil {
		t.Fatalf("exportMongoDBMergedTables: %v", err)
	}

	args := strings.Join(stubArgs(t, binDir, "gsutil"), " ")
	want := "gs://bucket/backups/orders" + ZIPFilenameSeparator + yesterdayStamp() + ".zip"
	if !strings.Contains(args, want) {
		t.Errorf("gsutil arguments = %q, want the object at %q", args, want)
	}
}

// TestAnEmptyTableGroupIsRefused records that the merged paths report an empty
// group rather than panicking on tables[0]. The file name is derived from the
// first table, so an index out of range would take the process down — every
// other sync task with it — instead of failing one backup.
func TestAnEmptyTableGroupIsRefused(t *testing.T) {
	stubPATH(t)
	e := newExecutor()

	if err := e.exportMongoDBMergedTables(context.Background(), "mongodb://host/", "shop",
		nil, t.TempDir(), mongoMergedConfig("")); err == nil {
		t.Error("the MongoDB merged export accepted an empty group")
	}
	if err := e.exportMySQLMergedTables(context.Background(), "tokyo:3306", "shop",
		nil, t.TempDir(), mysqlBackupConfig("sql", "", "")); err == nil {
		t.Error("the MySQL merged export accepted an empty group")
	}
}

// TestTheMergedMySQLExportJoinsEveryTable covers the MySQL half of the same
// path: several monthly tables dumped into one file.
func TestTheMergedMySQLExportJoinsEveryTable(t *testing.T) {
	binDir := stubPATH(t)
	stubBin(t, binDir, "mysqldump", `
last=""
for a in "$@"; do last="$a"; done
echo "-- dump of $last"
`, 0)
	// zip is given -j <output> <input>; the stub keeps a copy of the input,
	// because the export removes the merged file once it is done with it.
	stubBin(t, binDir, "zip", `
cp "$3" `+filepath.Join(binDir, "merged.captured")+`
echo zipped > "$2"
`, 0)
	stubBin(t, binDir, "gsutil", "", 1)

	e := newExecutor()
	tempDir := t.TempDir()

	err := e.exportMySQLMergedTables(context.Background(), "tokyo:3306", "shop",
		[]string{"orders_202607", "orders_202608"}, tempDir,
		mysqlBackupConfig("sql", "", ""))
	if err != nil {
		t.Fatalf("exportMySQLMergedTables: %v", err)
	}

	args := strings.Join(stubArgs(t, binDir, "mysqldump"), " ")
	for _, want := range []string{"orders_202607", "orders_202608"} {
		if !strings.Contains(args, want) {
			t.Errorf("mysqldump arguments = %q, want %q among them", args, want)
		}
	}
	// No bucket is configured, so nothing is uploaded.
	stubWasNotInvoked(t, binDir, "gsutil")

	// One file carrying both dumps, which is the point of the merged path.
	data, err := os.ReadFile(filepath.Join(binDir, "merged.captured"))
	if err != nil {
		t.Fatalf("the merged file was never zipped: %v", err)
	}
	for _, want := range []string{"orders_202607", "orders_202608"} {
		if !strings.Contains(string(data), want) {
			t.Errorf("merged dump = %q, want the dump of %s in it", data, want)
		}
	}
}

// TestNoCredentialsFileWithoutAPassword records that a job with no password
// does not leave an empty defaults file behind on every run.
func TestNoCredentialsFileWithoutAPassword(t *testing.T) {
	path, remove, err := mysqlCredentialsFile("")
	if err != nil {
		t.Fatalf("mysqlCredentialsFile: %v", err)
	}
	defer remove()

	if path != "" {
		t.Errorf("path = %q, want no file for an empty password", path)
	}
}

// TestTheCredentialsFileQuotesThePassword covers what mysqldump reads. A
// password holding a quote or a backslash would otherwise truncate the option
// file and the dump would fail to authenticate — with the password already out
// of the argument list, that failure would have no obvious cause.
func TestTheCredentialsFileQuotesThePassword(t *testing.T) {
	path, remove, err := mysqlCredentialsFile(`pa"ss\word`)
	if err != nil {
		t.Fatalf("mysqlCredentialsFile: %v", err)
	}
	defer remove()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if got := string(data); got != "[client]\npassword=\"pa\\\"ss\\\\word\"\n" {
		t.Errorf("file = %q, want the quote and the backslash escaped", got)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// The file exists so the password is not in the process list; it is only
	// better than the argument list while nobody else can read it.
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("mode = %o, want 600", perm)
	}

	remove()
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the credentials file outlived the dump: %v", err)
	}
}

// TestAnUnwritableCredentialsFileIsReported records that a temporary directory
// that cannot be written to fails the dump rather than falling back to the
// password on the command line.
func TestAnUnwritableCredentialsFileIsReported(t *testing.T) {
	t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "does-not-exist"))

	path, remove, err := mysqlCredentialsFile("hunter2")
	if err == nil {
		remove()
		t.Fatalf("mysqlCredentialsFile succeeded with an unusable TMPDIR (path %q)", path)
	}
	if path != "" {
		t.Errorf("path = %q, want none alongside the error", path)
	}
	if remove == nil {
		t.Error("no cleanup function was returned, so the caller's defer would panic")
	}
}
