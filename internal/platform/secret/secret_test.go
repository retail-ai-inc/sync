package secret

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func keeper(t *testing.T) *Keeper {
	t.Helper()

	t.Setenv("SYNC_CONFIG_KEY", base64.StdEncoding.EncodeToString([]byte(
		"0123456789abcdef0123456789abcdef")))
	k, err := KeeperFromEnv()
	if err != nil {
		t.Fatalf("KeeperFromEnv: %v", err)
	}
	if k == nil {
		t.Fatal("KeeperFromEnv returned no keeper for a configured key")
	}
	return k
}

// withDefault points the package-level keeper at a test key for the duration of
// one test, since the document helpers use it.
func withDefault(t *testing.T, k *Keeper) {
	t.Helper()

	previous := Default
	Default = k
	t.Cleanup(func() { Default = previous })
}

// ------------------------------------------------------------------- keys

func TestNoKeyMeansNoKeeper(t *testing.T) {
	t.Setenv("SYNC_CONFIG_KEY", "")

	k, err := KeeperFromEnv()
	if err != nil {
		t.Fatalf("KeeperFromEnv: %v", err)
	}
	if k != nil {
		t.Error("a keeper was built with no key configured")
	}
}

// TestTheKeyMayBeBase64OrHex covers the two forms an operator is likely to have
// it in, which is worth accepting because getting this wrong looks like a
// corrupt database rather than a wrong key.
func TestTheKeyMayBeBase64OrHex(t *testing.T) {
	raw := []byte("0123456789abcdef0123456789abcdef")

	for name, encoded := range map[string]string{
		"standard base64": base64.StdEncoding.EncodeToString(raw),
		"raw base64":      base64.RawStdEncoding.EncodeToString(raw),
		"hex":             hex.EncodeToString(raw),
		"with whitespace": "  " + base64.StdEncoding.EncodeToString(raw) + "\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("SYNC_CONFIG_KEY", encoded)
			k, err := KeeperFromEnv()
			if err != nil {
				t.Fatalf("KeeperFromEnv: %v", err)
			}
			if k == nil {
				t.Fatal("no keeper was built")
			}
		})
	}
}

// TestAShortKeyIsRefused pins that a key is not stretched into one. Deriving a
// 32-byte key from a four-character string would look exactly like encryption
// and provide much less of it.
func TestAShortKeyIsRefused(t *testing.T) {
	for name, value := range map[string]string{
		"a passphrase":  "hunter2",
		"too few bytes": base64.StdEncoding.EncodeToString([]byte("short")),
		"too many":      hex.EncodeToString(make([]byte, 64)),
		"not encoded":   "!!!not base64 or hex!!!",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("SYNC_CONFIG_KEY", value)
			if _, err := KeeperFromEnv(); err == nil {
				t.Error("the key was accepted")
			}
		})
	}
}

// ------------------------------------------------------------ seal and open

func TestAValueRoundTrips(t *testing.T) {
	k := keeper(t)

	sealed, err := k.Seal("hunter2")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if strings.Contains(sealed, "hunter2") {
		t.Fatalf("the sealed value carries the plaintext: %q", sealed)
	}
	if !IsSealed(sealed) {
		t.Errorf("the sealed value is not recognisable as one: %q", sealed)
	}

	opened, err := k.Open(sealed)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if opened != "hunter2" {
		t.Errorf("opened %q", opened)
	}
}

// TestTwoSealsOfTheSameValueDiffer pins that the nonce is fresh each time. Equal
// ciphertexts would tell a reader of the file which two tasks share a password.
func TestTwoSealsOfTheSameValueDiffer(t *testing.T) {
	k := keeper(t)

	first, err := k.Seal("hunter2")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	second, err := k.Seal("hunter2")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if first == second {
		t.Error("two seals of the same value are identical")
	}
}

// TestSealingIsIdempotent matters because a configuration is rewritten by paths
// that only meant to change something else — starting a task rewrites its
// stored document.
func TestSealingIsIdempotent(t *testing.T) {
	k := keeper(t)

	once, err := k.Seal("hunter2")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	twice, err := k.Seal(once)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if twice != once {
		t.Error("sealing an already sealed value changed it")
	}
	if opened, _ := k.Open(twice); opened != "hunter2" {
		t.Errorf("the doubly sealed value opened to %q", opened)
	}
}

// TestAPlaintextValueOpensUnchanged is how a database written before a key was
// configured keeps working.
func TestAPlaintextValueOpensUnchanged(t *testing.T) {
	k := keeper(t)

	opened, err := k.Open("hunter2")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if opened != "hunter2" {
		t.Errorf("opened %q", opened)
	}
}

func TestAnEmptyValueIsLeftAlone(t *testing.T) {
	k := keeper(t)

	sealed, err := k.Seal("")
	if err != nil || sealed != "" {
		t.Errorf("Seal(\"\") = %q, %v", sealed, err)
	}
}

// TestASealedValueWithNoKeyIsReported is the case that must not pass silently:
// handing the ciphertext back as the password would have the syncer authenticate
// with base64 and fail with an error naming neither the task nor the reason.
func TestASealedValueWithNoKeyIsReported(t *testing.T) {
	sealed, err := keeper(t).Seal("hunter2")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	var none *Keeper
	if _, err := none.Open(sealed); !errors.Is(err, ErrNoKey) {
		t.Errorf("Open with no key = %v, want ErrNoKey", err)
	}
	// A value that was never sealed still passes through.
	if opened, err := none.Open("hunter2"); err != nil || opened != "hunter2" {
		t.Errorf("Open of a plaintext value with no key = %q, %v", opened, err)
	}
}

// TestTheWrongKeyIsReported covers a rotated or mistyped key, which must be
// distinguishable from a corrupt file.
func TestTheWrongKeyIsReported(t *testing.T) {
	sealed, err := keeper(t).Seal("hunter2")
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}

	t.Setenv("SYNC_CONFIG_KEY", hex.EncodeToString(make([]byte, 32)))
	other, err := KeeperFromEnv()
	if err != nil {
		t.Fatalf("KeeperFromEnv: %v", err)
	}

	_, err = other.Open(sealed)
	if err == nil {
		t.Fatal("a value sealed with another key opened")
	}
	if !strings.Contains(err.Error(), "key") {
		t.Errorf("error = %v, want it to point at the key", err)
	}
}

func TestAMalformedSealedValueIsReported(t *testing.T) {
	k := keeper(t)

	for name, value := range map[string]string{
		"not base64": prefix + "!!!!",
		"too short":  prefix + base64.RawStdEncoding.EncodeToString([]byte("ab")),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := k.Open(value); err == nil {
				t.Error("a malformed sealed value opened")
			}
		})
	}
}

// -------------------------------------------------------- task documents

const taskConfig = `{
  "type": "mysql",
  "taskName": "tokyo-to-osaka",
  "sourceConn": {"host": "tokyo", "port": "3306", "user": "repl", "password": "tokyo-secret", "database": "shop"},
  "targetConn": {"host": "osaka", "port": "3306", "user": "repl", "password": "osaka-secret", "database": "shop"},
  "mappings": [{"tables": [{"sourceTable": "orders", "targetTable": "orders"}]}]
}`

func passwordsIn(t *testing.T, configJSON string) (source, target string) {
	t.Helper()

	var document struct {
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
	}
	if err := json.Unmarshal([]byte(configJSON), &document); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return document.SourceConn["password"], document.TargetConn["password"]
}

// TestBothPasswordsAreSealed is the point of the whole thing: the file holds the
// credentials for both regions, and masking them at the API did nothing about
// that.
func TestBothPasswordsAreSealed(t *testing.T) {
	withDefault(t, keeper(t))

	sealed, err := SealTaskConfig(taskConfig)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}

	for _, plaintext := range []string{"tokyo-secret", "osaka-secret"} {
		if strings.Contains(sealed, plaintext) {
			t.Errorf("the stored document still carries %q", plaintext)
		}
	}
	source, target := passwordsIn(t, sealed)
	if !IsSealed(source) || !IsSealed(target) {
		t.Errorf("passwords = %q / %q, want both sealed", source, target)
	}
}

// TestEverythingElseIsLeftReadable matters because an operator looking at the
// file has to be able to tell which task is which.
func TestEverythingElseIsLeftReadable(t *testing.T) {
	withDefault(t, keeper(t))

	sealed, err := SealTaskConfig(taskConfig)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}

	for _, kept := range []string{"tokyo", "osaka", "repl", "shop", "orders", "tokyo-to-osaka"} {
		if !strings.Contains(sealed, kept) {
			t.Errorf("the stored document no longer carries %q", kept)
		}
	}
}

func TestATaskDocumentRoundTrips(t *testing.T) {
	withDefault(t, keeper(t))

	sealed, err := SealTaskConfig(taskConfig)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}
	opened, err := OpenTaskConfig(sealed)
	if err != nil {
		t.Fatalf("OpenTaskConfig: %v", err)
	}

	source, target := passwordsIn(t, opened)
	if source != "tokyo-secret" || target != "osaka-secret" {
		t.Errorf("passwords = %q / %q", source, target)
	}
}

// TestAFieldNeitherPackageModelsSurvives is why this works on the JSON rather
// than a typed value: the same document is parsed by two packages with two
// different shapes, and a field neither of them knows about must not be dropped
// on the way through.
func TestAFieldNeitherPackageModelsSurvives(t *testing.T) {
	withDefault(t, keeper(t))
	document := `{"sourceConn":{"password":"p","somethingNew":"keep me"},"unknownTopLevel":42}`

	sealed, err := SealTaskConfig(document)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}

	for _, kept := range []string{"somethingNew", "keep me", "unknownTopLevel", "42"} {
		if !strings.Contains(sealed, kept) {
			t.Errorf("the stored document lost %q: %s", kept, sealed)
		}
	}
}

// TestADocumentWithNoKeyIsUnchanged is the state of every deployment that has
// not configured one: the store keeps working exactly as it did.
func TestADocumentWithNoKeyIsUnchanged(t *testing.T) {
	withDefault(t, nil)

	sealed, err := SealTaskConfig(taskConfig)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}
	if sealed != taskConfig {
		t.Error("the document was rewritten with no key configured")
	}

	opened, err := OpenTaskConfig(taskConfig)
	if err != nil {
		t.Fatalf("OpenTaskConfig: %v", err)
	}
	if opened != taskConfig {
		t.Error("the document was rewritten on the way out")
	}
}

// TestASealedDocumentWithNoKeyIsReported covers the key being removed or lost:
// the task must not be started with ciphertext for a password.
func TestASealedDocumentWithNoKeyIsReported(t *testing.T) {
	withDefault(t, keeper(t))
	sealed, err := SealTaskConfig(taskConfig)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}

	withDefault(t, nil)
	if _, err := OpenTaskConfig(sealed); !errors.Is(err, ErrNoKey) {
		t.Errorf("OpenTaskConfig with no key = %v, want ErrNoKey", err)
	}
}

// TestAnUnparseableDocumentIsLeftAlone records that a broken document is not
// rewritten: it is already broken, and rewriting it would replace one problem
// with a less obvious one.
func TestAnUnparseableDocumentIsLeftAlone(t *testing.T) {
	withDefault(t, keeper(t))

	for _, document := range []string{"", "   ", "{not json", "[1,2,3]"} {
		sealed, err := SealTaskConfig(document)
		if err != nil {
			t.Errorf("SealTaskConfig(%q) = %v", document, err)
		}
		if sealed != document {
			t.Errorf("SealTaskConfig(%q) = %q", document, sealed)
		}
	}
}

// TestADocumentWithNoConnectionsIsUnchanged covers a task saved before its
// connections were filled in.
func TestADocumentWithNoConnectionsIsUnchanged(t *testing.T) {
	withDefault(t, keeper(t))
	document := `{"type":"mysql","taskName":"new"}`

	sealed, err := SealTaskConfig(document)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}
	if sealed != document {
		t.Errorf("the document was rewritten: %s", sealed)
	}
}

func TestAConnectionWithNoPasswordIsUnchanged(t *testing.T) {
	withDefault(t, keeper(t))
	document := `{"sourceConn":{"host":"tokyo","password":""}}`

	sealed, err := SealTaskConfig(document)
	if err != nil {
		t.Fatalf("SealTaskConfig: %v", err)
	}
	if sealed != document {
		t.Errorf("the document was rewritten: %s", sealed)
	}
}
