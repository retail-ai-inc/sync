package security

import (
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// A field's path may name something several levels down, and the rule has to
// reach it. It used to reach exactly one: the parent prefix was stripped and the
// remainder looked up as a literal key, so "profile.contact.phone" went looking
// for a key called "contact.phone", did not find one, and left the phone number
// in the clear on the target.

func TestARuleReachesAsDeepAsItsPathNames(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.contact.phone", SecurityType: "masked"})

	document := map[string]interface{}{
		"profile": map[string]interface{}{
			"name": "Ada",
			"contact": map[string]interface{}{
				"phone": "555-0100",
				"email": "ada@example.com",
			},
		},
	}

	processed, ok := ProcessValue(document, "", cfg).(map[string]interface{})
	if !ok {
		t.Fatalf("ProcessValue returned %T, want a document", ProcessValue(document, "", cfg))
	}

	profile := processed["profile"].(map[string]interface{})
	contact := profile["contact"].(map[string]interface{})

	if contact["phone"] == "555-0100" {
		t.Errorf("phone = %v, want it masked", contact["phone"])
	}
	if contact["phone"] != strings.Repeat("*", len("555-0100")) {
		t.Errorf("phone = %v", contact["phone"])
	}
	// Its neighbours are untouched.
	if contact["email"] != "ada@example.com" {
		t.Errorf("email = %v, want it left alone", contact["email"])
	}
	if profile["name"] != "Ada" {
		t.Errorf("name = %v, want it left alone", profile["name"])
	}
}

// TestOneLevelStillWorks is the case that did work, kept so the rewrite is not
// trading one depth for another.
func TestOneLevelStillWorks(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.email", SecurityType: "masked"})

	processed := ProcessValue(map[string]interface{}{
		"profile": map[string]interface{}{"email": "ada@example.com", "name": "Ada"},
	}, "", cfg).(map[string]interface{})

	profile := processed["profile"].(map[string]interface{})
	if profile["email"] == "ada@example.com" {
		t.Errorf("email = %v, want it masked", profile["email"])
	}
	if profile["name"] != "Ada" {
		t.Errorf("name = %v, want it left alone", profile["name"])
	}
}

// TestABSONDocumentIsHandledLikeAMap covers what the MongoDB driver produces.
func TestABSONDocumentIsHandledLikeAMap(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.contact.phone", SecurityType: "masked"})

	processed, ok := ProcessValue(bson.M{
		"profile": bson.M{"contact": bson.M{"phone": "555-0100"}},
	}, "", cfg).(map[string]interface{})
	if !ok {
		t.Fatal("a bson.M was not processed as a document")
	}

	profile := processed["profile"].(map[string]interface{})
	contact := profile["contact"].(map[string]interface{})
	if contact["phone"] == "555-0100" {
		t.Errorf("phone = %v, want it masked", contact["phone"])
	}
}

// TestTheCallersDocumentIsNotTouched covers a row a syncer read from the source:
// it has to keep the value it read, at every level, not just the top one.
func TestTheCallersDocumentIsNotTouched(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.contact.phone", SecurityType: "masked"})

	contact := map[string]interface{}{"phone": "555-0100"}
	profile := map[string]interface{}{"contact": contact}
	document := map[string]interface{}{"profile": profile}

	ProcessValue(document, "", cfg)

	if contact["phone"] != "555-0100" {
		t.Errorf("the caller's nested map now reads %v", contact["phone"])
	}
}

// TestAPathThroughSomethingThatIsNotADocumentIsRefused covers a configuration
// that does not match the data: it leaves the value alone rather than guessing.
func TestAPathThroughSomethingThatIsNotADocumentIsRefused(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.contact.phone", SecurityType: "masked"})

	processed := ProcessValue(map[string]interface{}{
		"profile": map[string]interface{}{"contact": "not a document"},
	}, "", cfg).(map[string]interface{})

	profile := processed["profile"].(map[string]interface{})
	if profile["contact"] != "not a document" {
		t.Errorf("contact = %v, want it left alone", profile["contact"])
	}
}

// TestAFieldThatIsNotThereIsNotInvented covers a rule naming something the
// document does not carry.
func TestAFieldThatIsNotThereIsNotInvented(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.contact.phone", SecurityType: "masked"})

	processed := ProcessValue(map[string]interface{}{
		"profile": map[string]interface{}{"name": "Ada"},
	}, "", cfg).(map[string]interface{})

	profile := processed["profile"].(map[string]interface{})
	if _, invented := profile["contact"]; invented {
		t.Errorf("a contact was added: %v", profile)
	}
}

// TestARuleInsideANamedDocumentIsApplied covers the other entry point: the
// syncer passes a field's own name as the prefix when it hands over a subtree.
func TestARuleInsideANamedDocumentIsApplied(t *testing.T) {
	cfg := enabled(FieldSecurityConfig{Field: "profile.contact.phone", SecurityType: "masked"})

	processed := ProcessValue(map[string]interface{}{
		"contact": map[string]interface{}{"phone": "555-0100"},
	}, "profile", cfg).(map[string]interface{})

	contact := processed["contact"].(map[string]interface{})
	if contact["phone"] == "555-0100" {
		t.Errorf("phone = %v, want it masked", contact["phone"])
	}
}
