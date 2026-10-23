package verify

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/internal/platform/logging"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const testFieldKey = "abcdefghijklmnopqrstuvwxyz012345"

func policyOf(rules ...string) security.TableSecurity {
	policy := security.TableSecurity{SecurityEnabled: true}
	for i := 0; i+1 < len(rules); i += 2 {
		policy.FieldSecurity = append(policy.FieldSecurity,
			security.FieldSecurityConfig{Field: rules[i], SecurityType: rules[i+1]})
	}
	return policy
}

// usersEnd creates one side over users; each row is id, email, card, name, score.
func usersEnd(t *testing.T, name string, rows ...[5]interface{}) *SQLEnd {
	t.Helper()

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), name+".db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`CREATE TABLE users (
		id INTEGER PRIMARY KEY, email TEXT, card TEXT, name TEXT, score INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO users VALUES (?, ?, ?, ?, ?)`, r[:]...); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return &SQLEnd{DB: db, Table: "users", Keys: []string{"id"},
		Columns: []string{"id", "email", "card", "name", "score"}}
}

// protectedPair wires the two ends and a repairer the way the consistency check does.
func protectedPair(source, target *SQLEnd, policy security.TableSecurity) *SQLRepairer {
	protection := ProtectionOf(policy)
	all := source.Columns
	source.Columns = protection.Comparable(all)
	target.Columns = protection.Comparable(all)
	source.Protect = protection
	return &SQLRepairer{Source: source, Target: target, Upsert: upsertFor, Columns: all, Protect: protection}
}

// again is a fresh end over the same table, since an end streams its rows once.
func again(e *SQLEnd) *SQLEnd {
	return &SQLEnd{DB: e.DB, Table: e.Table, Keys: e.Keys, Columns: e.Columns, Protect: e.Protect}
}

func userOf(t *testing.T, end *SQLEnd, id int) (email, card sql.NullString, found bool) {
	t.Helper()

	err := end.DB.QueryRow(`SELECT email, card FROM users WHERE id = ?`, id).Scan(&email, &card)
	if err == sql.ErrNoRows {
		return email, card, false
	}
	if err != nil {
		t.Fatalf("read %d: %v", id, err)
	}
	return email, card, true
}

func decryptField(t *testing.T, encoded string) string {
	t.Helper()

	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("ciphertext is not base64: %v", err)
	}
	block, err := aes.NewCipher([]byte(testFieldKey))
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("new gcm: %v", err)
	}
	plain, err := gcm.Open(nil, raw[:gcm.NonceSize()], raw[gcm.NonceSize():], nil)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	return string(plain)
}

func TestAMaskedColumnMatchesATargetHoldingItsMask(t *testing.T) {
	source := usersEnd(t, "source", [5]interface{}{1, "ann@example.com", nil, "Ann", 7})
	target := usersEnd(t, "target", [5]interface{}{1, strings.Repeat("*", 15), nil, "Ann", 0})
	protectedPair(source, target, policyOf("email", "masked", "score", "masked"))

	if got := compare(t, source, target, 0); !got.Identical() {
		t.Errorf("a target holding what replication writes was reported: %s", got.Summary())
	}
}

func TestATargetHoldingTheRawValueOfAMaskedColumnIsReported(t *testing.T) {
	source := usersEnd(t, "source", [5]interface{}{1, "ann@example.com", nil, "Ann", 7})
	target := usersEnd(t, "target", [5]interface{}{1, "ann@example.com", nil, "Ann", 7})
	protectedPair(source, target, policyOf("email", "masked"))

	if got := compare(t, source, target, 0); got.Differing != 1 {
		t.Errorf("unmasked data on the target went unreported: %s", got.Summary())
	}
}

func TestARepairWritesTheMaskNotTheRawValue(t *testing.T) {
	source := usersEnd(t, "source",
		[5]interface{}{1, "ann@example.com", nil, "Ann", 7},
		[5]interface{}{2, "bob@example.com", nil, "Bob", 9})
	target := usersEnd(t, "target", [5]interface{}{1, "ann@example.com", nil, "Ann", 7})
	r := protectedPair(source, target, policyOf("email", "masked"))

	if _, err := r.Repair(context.Background(), []Difference{
		{Key: keyOf("1"), Kind: Differing}, {Key: keyOf("2"), Kind: Missing},
	}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	for _, id := range []int{1, 2} {
		email, _, found := userOf(t, target, id)
		if !found || email.String != strings.Repeat("*", 15) {
			t.Errorf("row %d holds email %q after the repair, want the mask", id, email.String)
		}
	}
	if got := compare(t, source, target, 0); !got.Identical() {
		t.Errorf("the repaired target does not match: %s", got.Summary())
	}
}

func TestAnEncryptedColumnIsLeftOutOfTheComparison(t *testing.T) {
	source := usersEnd(t, "source", [5]interface{}{1, "a", "4111111111111111", "Ann", 7})
	target := usersEnd(t, "target", [5]interface{}{1, "a", "c2VhbGVkIGJ5IGFub3RoZXIgbm9uY2U=", "Ann", 7})
	protectedPair(source, target, policyOf("card", "encrypted"))

	if got := compare(t, source, target, 0); !got.Identical() {
		t.Errorf("an encrypted column was compared by value: %s", got.Summary())
	}

	if _, err := target.DB.Exec(`UPDATE users SET name = 'Eve' WHERE id = 1`); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got := compare(t, again(source), again(target), 0); got.Differing != 1 {
		t.Errorf("the rest of the row is no longer compared: %s", got.Summary())
	}
}

func TestARepairEncryptsRatherThanCopies(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", testFieldKey)
	source := usersEnd(t, "source", [5]interface{}{1, "a", "4111111111111111", "Ann", 7})
	target := usersEnd(t, "target")
	r := protectedPair(source, target, policyOf("card", "encrypted"))

	if _, err := r.Repair(context.Background(), []Difference{{Key: keyOf("1"), Kind: Missing}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	_, card, found := userOf(t, target, 1)
	if !found || !card.Valid || strings.Contains(card.String, "4111111111111111") {
		t.Fatalf("the target holds card %q/%v after the repair", card.String, found)
	}
	if plain := decryptField(t, card.String); plain != "4111111111111111" {
		t.Errorf("the ciphertext carries %q", plain)
	}
}

// A failure means a repair writes a ciphertext, or nothing, where the source holds NULL.
func TestARepairKeepsANullEncryptedColumnNull(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", testFieldKey)
	source := usersEnd(t, "source", [5]interface{}{1, "a", nil, "Ann", 7})
	target := usersEnd(t, "target")
	r := protectedPair(source, target, policyOf("card", "encrypted"))

	if _, err := r.Repair(context.Background(), []Difference{{Key: keyOf("1"), Kind: Missing}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	if _, card, found := userOf(t, target, 1); !found || card.Valid {
		t.Errorf("the target holds card %q/%v after the repair, want a NULL", card.String, found)
	}
}

func TestARepairWithNoFieldKeyWritesNothing(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", "")
	t.Setenv("SYNC_CONFIG_KEY", "")
	source := usersEnd(t, "source", [5]interface{}{1, "a", "4111111111111111", "Ann", 7})
	target := usersEnd(t, "target")
	r := protectedPair(source, target, policyOf("card", "encrypted"))

	if _, err := r.Repair(context.Background(), []Difference{{Key: keyOf("1"), Kind: Missing}}); err == nil {
		t.Error("a repair with no key to encrypt with succeeded")
	}
	if _, _, found := userOf(t, target, 1); found {
		t.Error("a row was written with no key to encrypt its card with")
	}
}

func TestAPolicyThatRewritesNothingIsNoProtection(t *testing.T) {
	for name, policy := range map[string]security.TableSecurity{
		"none":     {},
		"disabled": {FieldSecurity: []security.FieldSecurityConfig{{Field: "email", SecurityType: "masked"}}},
		"unknown":  policyOf("email", "hashed"),
	} {
		if p := ProtectionOf(policy); p != nil {
			t.Errorf("%s: got a protection for a policy replication does not apply", name)
		}
	}
}

func TestProtectedColumnsMatchAsReplicationMatchesThem(t *testing.T) {
	p := ProtectionOf(policyOf("EMAIL", "masked", "card", " Encrypted ", "email", "encrypted"))

	if got := p.Protected([]string{"id", "email", "card"}); strings.Join(got, ",") != "email,card" {
		t.Errorf("Protected = %v", got)
	}
	if got := p.Comparable([]string{"id", "email", "card"}); strings.Join(got, ",") != "id,email" {
		t.Errorf("Comparable = %v, want the first rule naming a column to decide", got)
	}
}

func documentOf(t *testing.T, doc bson.M) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return raw
}

func TestAMaskedFieldMatchesATargetHoldingItsMask(t *testing.T) {
	p := ProtectionOf(policyOf("profile.contact.email", "masked", "age", "masked"))
	source := documentOf(t, bson.M{"_id": 1, "name": "Ann", "age": int32(41),
		"profile": bson.M{"contact": bson.M{"email": "ann@example.com", "tier": "gold"}}})
	target := documentOf(t, bson.M{"_id": 1, "name": "Ann", "age": int32(0),
		"profile": bson.M{"contact": bson.M{"tier": "gold", "email": strings.Repeat("*", 15)}}})

	sourceRow, err := (&MongoEnd{Protect: p}).row(source)
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	targetRow, err := (&MongoEnd{}).row(target)
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	if sourceRow.Digest != targetRow.Digest {
		t.Error("a target holding what replication writes hashes differently")
	}
}

func TestAnEncryptedFieldIsLeftOutOfTheDigest(t *testing.T) {
	p := ProtectionOf(policyOf("payment.card", "encrypted"))
	source := documentOf(t, bson.M{"_id": 1, "name": "Ann", "payment": bson.M{"card": "4111", "brand": "visa"}})
	target := documentOf(t, bson.M{"_id": 1, "name": "Ann", "payment": bson.M{"card": "c2VhbGVk", "brand": "visa"}})
	changed := documentOf(t, bson.M{"_id": 1, "name": "Eve", "payment": bson.M{"card": "c2VhbGVk", "brand": "visa"}})

	sourceRow, err := (&MongoEnd{Protect: p, Skip: p.Encrypted()}).row(source)
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	targetRow, err := (&MongoEnd{Skip: p.Encrypted()}).row(target)
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	changedRow, err := (&MongoEnd{Skip: p.Encrypted()}).row(changed)
	if err != nil {
		t.Fatalf("row: %v", err)
	}

	if sourceRow.Digest != targetRow.Digest {
		t.Error("an encrypted field was compared by value")
	}
	if sourceRow.Digest == changedRow.Digest {
		t.Error("the rest of the document is no longer compared")
	}
}

func TestADocumentRepairWritesWhatReplicationWrites(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", testFieldKey)
	r := &MongoRepairer{Protect: ProtectionOf(policyOf("contact.email", "masked", "card", "encrypted"))}
	raw := documentOf(t, bson.M{"_id": 1, "name": "Ann", "card": "4111",
		"contact": bson.M{"email": "ann@example.com"}})

	replacement, err := r.replacement(raw)
	if err != nil {
		t.Fatalf("replacement: %v", err)
	}
	doc, ok := replacement.(bson.M)
	if !ok {
		t.Fatalf("replacement is a %T", replacement)
	}
	if email, _ := valueAt(doc, []string{"contact", "email"}); email != strings.Repeat("*", 15) {
		t.Errorf("email = %v, want the mask", email)
	}
	card, _ := doc["card"].(string)
	if card == "" || card == "4111" || decryptField(t, card) != "4111" {
		t.Errorf("card = %q, want fresh ciphertext of the value", card)
	}
	if doc["name"] != "Ann" {
		t.Errorf("an unprotected field became %v", doc["name"])
	}
}

// A failure means a document repair writes a ciphertext, or nothing, where the source holds null.
func TestADocumentRepairKeepsANullEncryptedFieldNull(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", testFieldKey)
	r := &MongoRepairer{Protect: ProtectionOf(policyOf("card", "encrypted"))}

	replacement, err := r.replacement(documentOf(t, bson.M{"_id": 1, "card": nil}))
	if err != nil {
		t.Fatalf("replacement: %v", err)
	}
	doc, ok := replacement.(bson.M)
	if !ok {
		t.Fatalf("replacement is a %T", replacement)
	}
	if card, present := doc["card"]; !present || card != nil {
		t.Errorf("card = %#v (present %v), want a null", card, present)
	}
}

func TestAnEncryptedSubdocumentIsComparedAsReplicationWritesIt(t *testing.T) {
	p := ProtectionOf(policyOf("address", "encrypted"))
	source := documentOf(t, bson.M{"_id": 1, "address": bson.M{"city": "Oslo", "zip": "0150"}})
	moved := documentOf(t, bson.M{"_id": 1, "address": bson.M{"city": "Bergen", "zip": "0150"}})

	sourceRow, err := (&MongoEnd{Protect: p, Skip: p.Encrypted()}).row(source)
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	sameRow, err := (&MongoEnd{Skip: p.Encrypted()}).row(source)
	if err != nil {
		t.Fatalf("row: %v", err)
	}
	movedRow, err := (&MongoEnd{Skip: p.Encrypted()}).row(moved)
	if err != nil {
		t.Fatalf("row: %v", err)
	}

	if sourceRow.Digest != sameRow.Digest {
		t.Error("a target holding the subdocument replication writes hashes differently")
	}
	if sourceRow.Digest == movedRow.Digest {
		t.Error("a divergence inside a subdocument an encrypted rule names went unreported")
	}
}

func TestADocumentRepairRefusesASubdocumentARuleNames(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", testFieldKey)
	raw := documentOf(t, bson.M{"_id": 1, "profile": bson.M{"address": bson.M{"city": "Oslo"}}})

	for _, kind := range []string{"masked", "encrypted"} {
		r := &MongoRepairer{Protect: ProtectionOf(policyOf("profile.address", kind))}
		replacement, err := r.replacement(raw)
		if err == nil {
			t.Errorf("%s: a subdocument replication leaves in the clear was prepared for writing: %v",
				kind, replacement)
			continue
		}
		if strings.Contains(err.Error(), "Oslo") {
			t.Errorf("%s: the refusal carries the value: %v", kind, err)
		}
	}
}

func TestNoProtectedValueIsLogged(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", testFieldKey)
	var logged bytes.Buffer
	saved := logging.Log
	logging.Log = logrus.New()
	logging.Log.SetOutput(&logged)
	logging.Log.SetLevel(logrus.DebugLevel)
	t.Cleanup(func() { logging.Log = saved })

	source := usersEnd(t, "source", [5]interface{}{1, "ann@example.com", "4111111111111111", "Ann", 7})
	target := usersEnd(t, "target")
	r := protectedPair(source, target, policyOf("email", "masked", "card", "encrypted"))
	compare(t, source, target, 0)
	if _, err := r.Repair(context.Background(), []Difference{{Key: keyOf("1"), Kind: Missing}}); err != nil {
		t.Fatalf("Repair: %v", err)
	}

	p := ProtectionOf(policyOf("contact.email", "masked", "card", "encrypted"))
	doc := documentOf(t, bson.M{"_id": 1, "card": "4111111111111111",
		"contact": bson.M{"email": "ann@example.com"}})
	if _, err := (&MongoEnd{Protect: p, Skip: p.Encrypted()}).row(doc); err != nil {
		t.Fatalf("row: %v", err)
	}
	if _, err := (&MongoRepairer{Protect: p}).replacement(doc); err != nil {
		t.Fatalf("replacement: %v", err)
	}

	if !strings.Contains(logged.String(), "email") {
		t.Fatalf("nothing was logged at debug level, so this proves nothing:\n%s", logged.String())
	}
	for _, secret := range []string{"ann@example.com", "4111111111111111"} {
		if strings.Contains(logged.String(), secret) {
			t.Errorf("%q was logged:\n%s", secret, logged.String())
		}
	}
}

func TestADocumentRepairWithNoFieldKeyWritesNothing(t *testing.T) {
	t.Setenv("SYNC_FIELD_KEY", "")
	t.Setenv("SYNC_CONFIG_KEY", "")
	r := &MongoRepairer{Protect: ProtectionOf(policyOf("card", "encrypted"))}

	if _, err := r.replacement(documentOf(t, bson.M{"_id": 1, "card": "4111"})); err == nil {
		t.Error("a document was prepared for writing with no key to encrypt its card with")
	}
}

func TestAnUnprotectedDocumentIsRepairedAsItWasRead(t *testing.T) {
	raw := documentOf(t, bson.M{"_id": 1, "email": "ann@example.com"})

	replacement, err := (&MongoRepairer{}).replacement(raw)
	if err != nil {
		t.Fatalf("replacement: %v", err)
	}
	if got, ok := replacement.(bson.Raw); !ok || string(got) != string(raw) {
		t.Errorf("replacement = %v, want the document as read", replacement)
	}
}

func TestSkippingAFieldLeavesTheDocumentItCameFromAlone(t *testing.T) {
	doc := bson.M{"_id": 1, "a": bson.D{{Key: "b", Value: "x"}, {Key: "c", Value: "y"}}}

	out := without(doc, []string{"a.b", "missing.path", "_id.x"})

	if _, present := valueAt(out, []string{"a", "b"}); present {
		t.Error("a.b is still there")
	}
	if c, _ := valueAt(out, []string{"a", "c"}); c != "y" {
		t.Errorf("a.c = %v", c)
	}
	if b, _ := valueAt(doc, []string{"a", "b"}); b != "x" {
		t.Error("the input document was written to")
	}
}
