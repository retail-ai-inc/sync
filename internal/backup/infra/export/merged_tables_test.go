package export

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// yesterdayStamp is the date the exporters put in a file name.
func yesterdayStamp() string {
	return time.Now().AddDate(0, 0, -1).Format("2006-01-02")
}

func mongoMergedConfig(gcsPath string) ExecutorBackupConfig {
	var cfg ExecutorBackupConfig
	cfg.SourceType = "mongodb"
	cfg.Destination.GCSPath = gcsPath
	cfg.Database.Database = "shop"
	return cfg
}

// TestTheMergedMongoExportJoinsEveryCollection covers the path a job takes as
// soon as its pattern matches more than one collection — which for a monthly
// naming scheme is every job, on the first of the month.
func TestTheMergedMongoExportJoinsEveryCollection(t *testing.T) {
	binDir := stubPATH(t)
	// mongoexport writes JSONL to the file named by --out.
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

// TestTheMergedMongoExportSkipsTheUploadWithoutABucket is the fix for a job
// that worked until the day it matched a second collection.
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

// The file name is derived from the first table, so an index out of range
// would take the process down — every other sync task with it — instead of
// failing one backup.
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

// A job with no password still gets a defaults file, because the transport
// settings live in it too. It used to be written only for a password, and
// leaving the client to its own TLS defaults is what stopped every MySQL
// backup.
func TestTheOptionsFileIsWrittenWithoutAPassword(t *testing.T) {
	path, remove, err := mysqlCredentialsFile("")
	if err != nil {
		t.Fatalf("mysqlCredentialsFile: %v", err)
	}
	defer remove()

	if path == "" {
		t.Fatal("no options file was written, so the client would use its own TLS defaults")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if strings.Contains(string(data), "password") {
		t.Errorf("file = %q, want no password line", string(data))
	}
}

// A password holding a quote or a backslash would otherwise truncate the
// option file and the dump would fail to authenticate — with the password
// already out of the argument list, that failure would have no obvious cause.
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
	if got := string(data); !strings.Contains(got, "password=\"pa\\\"ss\\\\word\"\n") {
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

// What the client does about TLS, which it used to decide for itself.
//
// MariaDB's client from 11.4 negotiates TLS whenever the server offers it and
// verifies the certificate by default. Cloud SQL's certificate carries
// CN=project:instance and no subjectAltName, so that verification cannot pass
// over an address of any kind: every MySQL backup failed on "unable to get
// local issuer certificate" the morning the servers began advertising TLS,
// while the MongoDB ones went on working.

func TestTheConnectionIsEncryptedAndSaysSoInTheOptionsFile(t *testing.T) {
	path, remove, err := mysqlCredentialsFile("secret")
	if err != nil {
		t.Fatalf("mysqlCredentialsFile: %v", err)
	}
	defer remove()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	// Encryption stays on; only the verification the certificate cannot satisfy
	// is turned off. skip-ssl would drop the encryption with it.
	if got := string(data); !strings.Contains(got, "ssl-verify-server-cert=0\n") {
		t.Errorf("file = %q, want verification off", got)
	}
	if got := string(data); strings.Contains(got, "skip-ssl") {
		t.Errorf("file = %q, want the transport still encrypted", got)
	}
}

func TestACertificateAuthorityIsVerifiedAgainstWhenOneIsNamed(t *testing.T) {
	t.Setenv("SYNC_MYSQL_SSL_CA", "/etc/sync/cloudsql/server-ca.pem")

	path, remove, err := mysqlCredentialsFile("secret")
	if err != nil {
		t.Fatalf("mysqlCredentialsFile: %v", err)
	}
	defer remove()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	got := string(data)
	if !strings.Contains(got, "ssl-ca=/etc/sync/cloudsql/server-ca.pem\n") {
		t.Errorf("file = %q, want the CA named", got)
	}
	// Naming a CA and then not verifying against it would be theatre: the
	// setting disables the check the CA exists for.
	if strings.Contains(got, "ssl-verify-server-cert=0") {
		t.Errorf("file = %q, want verification left on when a CA is named", got)
	}
}

func TestTLSCanBeTurnedOffForAServerThatDoesNotOfferIt(t *testing.T) {
	t.Setenv("SYNC_MYSQL_TLS", "off")

	path, remove, err := mysqlCredentialsFile("secret")
	if err != nil {
		t.Fatalf("mysqlCredentialsFile: %v", err)
	}
	defer remove()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if got := string(data); !strings.Contains(got, "skip-ssl\n") {
		t.Errorf("file = %q, want TLS off", got)
	}
}

// The option file is where the password is, so its settings must not widen who
// can read it.
func TestTheOptionsFileStaysPrivateWithTheTransportSettings(t *testing.T) {
	path, remove, err := mysqlCredentialsFile("secret")
	if err != nil {
		t.Fatalf("mysqlCredentialsFile: %v", err)
	}
	defer remove()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("mode = %o, want 600", mode)
	}
}

// mongoexportWritesItsCollection writes one document naming its collection to
// the file given by --out.
const mongoexportWritesItsCollection = `
out=""; coll=""
while [ $# -gt 0 ]; do
  case "$1" in
    --out) out="$2"; shift 2;;
    --collection) coll="$2"; shift 2;;
    *) shift;;
  esac
done
printf '{"_id":1,"from":"%s"}\n' "$coll" > "$out"
`

// gsutilThatStoresSeven answers the size check for the seven bytes zipThatKeeps
// writes.
const gsutilThatStoresSeven = `case "$1" in stat) echo "Content-Length: 7";; esac`

// zipThatKeeps copies the file it is given to kept before writing a seven-byte
// archive, because the export removes the file once it has been zipped.
func zipThatKeeps(kept string) string {
	return `cp "$3" ` + kept + `
echo zipped > "$2"
`
}

// A merge left in its buffer would upload an empty backup while every other test stays green.
func TestTheMergedMongoFileHoldsEveryDocument(t *testing.T) {
	binDir := stubPATH(t)
	kept := filepath.Join(binDir, "merged.captured")
	stubBin(t, binDir, "mongoexport", mongoexportWritesItsCollection, 0)
	stubBin(t, binDir, "zip", zipThatKeeps(kept), 0)
	stubBin(t, binDir, "gsutil", gsutilThatStoresSeven, 0)

	e := newExecutor()
	if err := e.exportMongoDBMergedTables(context.Background(), "mongodb://host/", "shop",
		[]string{"orders_202607", "orders_202608"}, t.TempDir(),
		mongoMergedConfig("gs://bucket/backups")); err != nil {
		t.Fatalf("exportMongoDBMergedTables: %v", err)
	}

	data, err := os.ReadFile(kept)
	if err != nil {
		t.Fatalf("the merged file was never zipped: %v", err)
	}
	got := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	want := []string{
		`{"_id":1,"from":"orders_202607"}`,
		`{"_id":1,"from":"orders_202608"}`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged file = %q, want one document from each collection in order", data)
	}
}
