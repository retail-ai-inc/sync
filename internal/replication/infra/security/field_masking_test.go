package security

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func enabled(fields ...FieldSecurityConfig) TableSecurity {
	return TableSecurity{SecurityEnabled: true, FieldSecurity: fields}
}

// decrypt mirrors encryptAES using the same package-level key, so the tests can
// TestMain gives the package a field encryption key.
//
// There is no fallback key any more — a deployment that asks for encryption and
// configures none is refused — so the tests that exercise encryption have to
// supply one, exactly as a deployment does. The tests that cover the refusal
// clear it for themselves.
func TestMain(m *testing.M) {
	if err := os.Setenv("SYNC_FIELD_KEY", "abcdefghijklmnopqrstuvwxyz012345"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// assert that ciphertext really carries the plaintext.
func decrypt(t *testing.T, encoded string) string {
	t.Helper()

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("ciphertext is not base64: %v", err)
	}
	block, err := aes.NewCipher(fieldKey())
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("new gcm: %v", err)
	}
	if len(raw) < gcm.NonceSize() {
		t.Fatalf("ciphertext shorter than nonce: %d bytes", len(raw))
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	return string(plain)
}

func TestProcessValuePassesThroughWhenDisabled(t *testing.T) {
	cfg := TableSecurity{
		SecurityEnabled: false,
		FieldSecurity:   []FieldSecurityConfig{{Field: "email", SecurityType: "masked"}},
	}

	if got := ProcessValue("john@example.com", "email", cfg); got != "john@example.com" {
		t.Errorf("ProcessValue with security disabled = %v, want the original value", got)
	}
}

func TestProcessValuePassesThroughUnconfiguredField(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "email", SecurityType: "masked"})

	if got := ProcessValue("Tokyo", "city", cfg); got != "Tokyo" {
		t.Errorf("ProcessValue for an unconfigured field = %v, want the original value", got)
	}
}

func TestProcessValueMasked(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "email", SecurityType: "masked"})

	tests := []struct {
		name  string
		value interface{}
		want  interface{}
	}{
		// Strings are replaced with one asterisk per byte, so the length leaks.
		{"string", "john@example.com", strings.Repeat("*", len("john@example.com"))},
		{"empty string", "", ""},
		// A value that is not text keeps its type. It used to become the literal
		// string "****" whatever it was, which for a numeric or boolean column on
		// the target is either an error or a truncation.
		{"int", 12345, 0},
		{"int64", int64(12345), int64(0)},
		{"float", 1.5, float64(0)},
		{"bool", true, false},
		// A null field has nothing to hide, and masking it would make an absent
		// value look like a present one.
		{"nil", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ProcessValue(tt.value, "email", cfg); got != tt.want {
				t.Errorf("ProcessValue(%#v) = %#v, want %#v", tt.value, got, tt.want)
			}
		})
	}
}

// TestProcessValueMaskedCountsBytesNotRunes records that masking uses byte
// length, so a multibyte value is replaced by more asterisks than it has
// characters. Harmless on its own, but it means the mask leaks the encoded
// size rather than a uniform placeholder.
func TestProcessValueMaskedCountsBytesNotRunes(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "name", SecurityType: "masked"})

	got := ProcessValue("日本語", "name", cfg)

	if got == strings.Repeat("*", 3) {
		t.Fatalf("masking now counts runes; update this test to assert the new behaviour")
	}
	if want := strings.Repeat("*", 9); got != want {
		t.Errorf("ProcessValue(%q) = %v, want %q (9 bytes)", "日本語", got, want)
	}
}

func TestProcessValueEncrypted(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "phone", SecurityType: "encrypted"})

	t.Run("string", func(t *testing.T) {
		got, ok := ProcessValue("090-1234-5678", "phone", cfg).(string)
		if !ok {
			t.Fatalf("encrypted value is not a string")
		}
		if plain := decrypt(t, got); plain != "090-1234-5678" {
			t.Errorf("decrypted to %q, want %q", plain, "090-1234-5678")
		}
	})

	t.Run("byte slice", func(t *testing.T) {
		got, ok := ProcessValue([]byte("secret"), "phone", cfg).(string)
		if !ok {
			t.Fatalf("encrypted value is not a string")
		}
		if plain := decrypt(t, got); plain != "secret" {
			t.Errorf("decrypted to %q, want %q", plain, "secret")
		}
	})

	t.Run("other types go through fmt", func(t *testing.T) {
		got, ok := ProcessValue(42, "phone", cfg).(string)
		if !ok {
			t.Fatalf("encrypted value is not a string")
		}
		if plain := decrypt(t, got); plain != "42" {
			t.Errorf("decrypted to %q, want %q", plain, "42")
		}
	})
}

// TestProcessValueEncryptedIsNonDeterministic records a property that matters
// for replication: AES-GCM uses a fresh random nonce, so the same source value
// encrypts to different ciphertext on every call. Consequences:
//
//   - re-running an initial sync rewrites every encrypted field even when the
//     source has not changed
//   - source and target can never be compared on encrypted fields, so the
//     row-count and checksum verification planned for the DR work cannot cover
//     them
func TestProcessValueEncryptedIsNonDeterministic(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "phone", SecurityType: "encrypted"})

	first := ProcessValue("090-1234-5678", "phone", cfg)
	second := ProcessValue("090-1234-5678", "phone", cfg)

	if first == second {
		t.Errorf("encryption became deterministic (%v); if that was intentional, "+
			"update this test and the consistency-check design", first)
	}
	if decrypt(t, first.(string)) != decrypt(t, second.(string)) {
		t.Error("the two ciphertexts do not decrypt to the same plaintext")
	}
}

// TestAnUnknownSecurityTypeDoesNotDestroyTheValue covers a defect that wrote
// NULL over real data. The switch left `processed` at its zero value for
// anything outside {masked, encrypted} and returned it unconditionally, so a
// securityType of "Masked" — the comparison was case-sensitive — or anything a
// client had made up replicated the field as NULL. The mapping store accepts
// whatever string it is given as long as it is non-empty.
func TestAnUnknownSecurityTypeDoesNotDestroyTheValue(t *testing.T) {
	const value = "john@example.com"

	t.Run("a known kind spelled differently still works", func(t *testing.T) {
		for _, secType := range []string{"Masked", "MASKED", " masked "} {
			cfg := enabled(FieldSecurityConfig{Field: "email", SecurityType: secType})
			if got := ProcessValue(value, "email", cfg); got != strings.Repeat("*", len(value)) {
				t.Errorf("ProcessValue with securityType %q = %#v, want it masked", secType, got)
			}
		}
	})

	t.Run("an unknown kind leaves the value alone", func(t *testing.T) {
		for _, secType := range []string{"hashed", "redacted", "unknown"} {
			cfg := enabled(FieldSecurityConfig{Field: "email", SecurityType: secType})
			if got := ProcessValue(value, "email", cfg); got != value {
				t.Errorf("ProcessValue with securityType %q = %#v, want the value unchanged", secType, got)
			}
		}
	})
}

// TestAFieldNameIsMatchedWithoutRegardToCase covers the same case-sensitivity on
// the other half of the rule.
func TestAFieldNameIsMatchedWithoutRegardToCase(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "Email", SecurityType: "masked"})

	if got := ProcessValue("john@example.com", "email", cfg); got == "john@example.com" {
		t.Error("the field was not matched, so it was replicated in the clear")
	}
}

func TestProcessValueNestedObject(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.email", SecurityType: "masked"})

	value := map[string]interface{}{
		"email": "john@example.com",
		"city":  "Tokyo",
	}

	got, ok := ProcessValue(value, "profile", cfg).(map[string]interface{})
	if !ok {
		t.Fatalf("nested processing did not return a map")
	}
	if want := strings.Repeat("*", len("john@example.com")); got["email"] != want {
		t.Errorf("profile.email = %v, want %q", got["email"], want)
	}
	if got["city"] != "Tokyo" {
		t.Errorf("profile.city = %v, want it untouched", got["city"])
	}
	// The input map must not be mutated; the syncers reuse the source document.
	if value["email"] != "john@example.com" {
		t.Errorf("input map was mutated: %v", value["email"])
	}
}

func TestProcessValueNestedBSON(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.email", SecurityType: "masked"})

	got, ok := ProcessValue(bson.M{"email": "a@b.com"}, "profile", cfg).(map[string]interface{})
	if !ok {
		t.Fatalf("bson.M was not handled as a nested object")
	}
	if want := strings.Repeat("*", len("a@b.com")); got["email"] != want {
		t.Errorf("profile.email = %v, want %q", got["email"], want)
	}
}

// The nested paths are covered in nested_test.go, against one implementation.
// There used to be four overlapping ones — ProcessNestedFieldValue,
// processNestedObjectValue, getNestedValue and processNestedFieldSafe — of which
// two had no callers and the third could not be reached, because ProcessValue
// only handed it values that were not documents while its first act was to
// require one.

func TestFindTableSecurityFromMappings(t *testing.T) {
	mappings := []config.DatabaseMapping{{
		Tables: []config.TableMapping{{
			SourceTable:     "users",
			TargetTable:     "users_copy",
			SecurityEnabled: true,
			FieldSecurity: []interface{}{
				map[string]interface{}{"field": "email", "securityType": "masked"},
				map[string]interface{}{"field": "profile.phone", "securityType": "encrypted"},
			},
		}},
	}}

	for _, name := range []string{"users", "users_copy"} {
		t.Run("matches "+name, func(t *testing.T) {
			got := FindTableSecurityFromMappings(name, mappings)

			if !got.SecurityEnabled {
				t.Error("SecurityEnabled = false, want true")
			}
			if len(got.FieldSecurity) != 2 {
				t.Fatalf("FieldSecurity has %d entries, want 2", len(got.FieldSecurity))
			}
			if got.FieldSecurity[0] != (FieldSecurityConfig{Field: "email", SecurityType: "masked"}) {
				t.Errorf("FieldSecurity[0] = %+v", got.FieldSecurity[0])
			}
			if got.FieldSecurity[1] != (FieldSecurityConfig{Field: "profile.phone", SecurityType: "encrypted"}) {
				t.Errorf("FieldSecurity[1] = %+v", got.FieldSecurity[1])
			}
		})
	}
}

func TestFindTableSecurityFromMappingsNotFound(t *testing.T) {
	mappings := []config.DatabaseMapping{{
		Tables: []config.TableMapping{{SourceTable: "users", TargetTable: "users", SecurityEnabled: true}},
	}}

	got := FindTableSecurityFromMappings("orders", mappings)

	// A miss must yield a disabled config, never a partially filled one.
	if got.SecurityEnabled || len(got.FieldSecurity) != 0 {
		t.Errorf("miss returned %+v, want the zero value", got)
	}
	if got := FindTableSecurityFromMappings("users", nil); got.SecurityEnabled {
		t.Errorf("nil mappings returned %+v, want the zero value", got)
	}
}

func TestFindTableSecurityFromMappingsSkipsIncompleteEntries(t *testing.T) {
	mappings := []config.DatabaseMapping{{
		Tables: []config.TableMapping{{
			SourceTable:     "users",
			TargetTable:     "users",
			SecurityEnabled: true,
			FieldSecurity: []interface{}{
				map[string]interface{}{"field": "email", "securityType": "masked"},
				map[string]interface{}{"field": "", "securityType": "masked"}, // no field name
				map[string]interface{}{"field": "phone", "securityType": ""},  // no type
				map[string]interface{}{"field": "city"},                       // type missing
				"not-a-map",                                                   // wrong shape entirely
			},
		}},
	}}

	got := FindTableSecurityFromMappings("users", mappings)

	// Only the complete entry survives; the rest are dropped without a trace in
	// the returned config, which is why a mistyped UI entry looks like it worked.
	if len(got.FieldSecurity) != 1 {
		t.Fatalf("FieldSecurity has %d entries, want 1: %+v", len(got.FieldSecurity), got.FieldSecurity)
	}
	if got.FieldSecurity[0].Field != "email" {
		t.Errorf("surviving entry = %+v, want the email rule", got.FieldSecurity[0])
	}
}

// TestTheKeyComesFromTheEnvironment covers where the AES-256 key is read from.
// It was a literal in this file — in a public repository, identical in every
// deployment — so anything encrypted under it could be read by anyone who had
// the source: the configuration said the field was protected and it was not.
// There is no fallback now.
func TestTheKeyComesFromTheEnvironment(t *testing.T) {
	for name, given := range map[string]string{
		"hex":    "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
		"base64": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)),
		"raw":    "abcdefghijklmnopqrstuvwxyz012345",
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("SYNC_FIELD_KEY", given)

			if key := fieldKey(); len(key) != 32 {
				t.Fatalf("key length = %d, want 32", len(key))
			}
			if !KeyConfigured() {
				t.Error("a usable key was not recognised")
			}
		})
	}

	t.Run("the credential key is accepted too", func(t *testing.T) {
		t.Setenv("SYNC_FIELD_KEY", "")
		t.Setenv("SYNC_CONFIG_KEY", "abcdefghijklmnopqrstuvwxyz012345")

		if !KeyConfigured() {
			t.Error("SYNC_CONFIG_KEY was ignored")
		}
	})

	t.Run("a key that cannot be used stops the process", func(t *testing.T) {
		t.Setenv("SYNC_FIELD_KEY", "too short")

		defer func() {
			if recover() == nil {
				t.Error("an unusable key was accepted, so fields would be sealed " +
					"with something other than the operator's key")
			}
		}()
		fieldKey()
	})
}

// TestWithNoKeyNothingIsEncrypted covers the case that used to be answered with
// the published key. There is nowhere to get one, so encrypting is refused
// rather than performed with a key everyone has.
func TestWithNoKeyNothingIsEncrypted(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", "")
	t.Setenv("SYNC_CONFIG_KEY", "")

	if KeyConfigured() {
		t.Fatal("a key was found with neither variable set")
	}
	if _, err := encryptAES([]byte("secret")); !errors.Is(err, ErrNoFieldKey) {
		t.Errorf("encryptAES = %v, want ErrNoFieldKey", err)
	}
}

// TestATaskThatCannotEncryptIsRefused covers what a syncer does about it. A task
// naming an encrypted field with no key would replicate that field in whatever
// form the failed encryption left, while the interface went on reporting it as
// protected — so it is refused at startup instead.
func TestATaskThatCannotEncryptIsRefused(t *testing.T) {
	encrypted := []config.DatabaseMapping{{
		Tables: []config.TableMapping{{
			SourceTable:     "users",
			SecurityEnabled: true,
			FieldSecurity: []interface{}{
				map[string]interface{}{"field": "email", "securityType": "encrypted"},
			},
		}},
	}}

	t.Run("with no key", func(t *testing.T) {
		t.Setenv("SYNC_FIELD_KEY", "")
		t.Setenv("SYNC_CONFIG_KEY", "")

		err := CheckKeyForMappings(encrypted)
		if !errors.Is(err, ErrNoFieldKey) {
			t.Fatalf("CheckKeyForMappings = %v, want ErrNoFieldKey", err)
		}
		if !strings.Contains(err.Error(), "users.email") {
			t.Errorf("err = %v, want it to name the field", err)
		}
	})

	t.Run("with a key", func(t *testing.T) {
		t.Setenv("SYNC_FIELD_KEY", "abcdefghijklmnopqrstuvwxyz012345")

		if err := CheckKeyForMappings(encrypted); err != nil {
			t.Errorf("CheckKeyForMappings = %v", err)
		}
	})

	t.Run("a task that encrypts nothing does not need one", func(t *testing.T) {
		t.Setenv("SYNC_FIELD_KEY", "")
		t.Setenv("SYNC_CONFIG_KEY", "")

		masked := []config.DatabaseMapping{{
			Tables: []config.TableMapping{{
				SourceTable:     "users",
				SecurityEnabled: true,
				FieldSecurity: []interface{}{
					map[string]interface{}{"field": "name", "securityType": "masked"},
				},
			}},
		}}
		if err := CheckKeyForMappings(masked); err != nil {
			t.Errorf("CheckKeyForMappings = %v for a task that only masks", err)
		}
	})
}
