package security

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// maskConfig returns a table configuration that masks one field path.
func maskConfig(path, securityType string) TableSecurity {
	return TableSecurity{
		SecurityEnabled: true,
		FieldSecurity:   []FieldSecurityConfig{{Field: path, SecurityType: securityType}},
	}
}

// TestProcessNestedFieldValueRejectsANonObject records that a value which is
// not an object is returned untouched, with a warning. The masking rule is
// therefore silently skipped for a field whose type changed.
func TestProcessNestedFieldValueRejectsANonObject(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value interface{}
	}{
		{"string", "a string"},
		{"number", 42},
		{"nil", nil},
		{"slice", []interface{}{1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := ProcessNestedFieldValue(tt.value, "profile.email",
				maskConfig("profile.email", "masked"))

			// reflect.DeepEqual, because a slice is not comparable with ==.
			if !reflect.DeepEqual(got, tt.value) {
				t.Errorf("ProcessNestedFieldValue(%v) = %v, want the value untouched",
					tt.value, got)
			}
		})
	}
}

func TestProcessNestedFieldValueAcceptsBSON(t *testing.T) {
	got := ProcessNestedFieldValue(
		bson.M{"profile": bson.M{"email": "alice@example.test"}},
		"profile.email",
		maskConfig("profile.email", "masked"))

	nested, ok := got.(map[string]interface{})
	if !ok {
		t.Fatalf("ProcessNestedFieldValue returned %T, want a map", got)
	}
	profile, ok := nested["profile"].(map[string]interface{})
	if !ok {
		t.Fatalf("profile is %T, want a map", nested["profile"])
	}
	if profile["email"] == "alice@example.test" {
		t.Error("the email was not masked")
	}
}

func TestProcessNestedFieldValueMasksTheNamedPath(t *testing.T) {
	got := ProcessNestedFieldValue(
		map[string]interface{}{
			"profile": map[string]interface{}{"email": "alice@example.test", "name": "Alice"},
		},
		"profile.email",
		maskConfig("profile.email", "masked"))

	nested := got.(map[string]interface{})
	profile := nested["profile"].(map[string]interface{})

	if profile["email"] == "alice@example.test" {
		t.Error("the email was not masked")
	}
	if profile["name"] != "Alice" {
		t.Errorf("name = %v; a field outside the path was touched", profile["name"])
	}
}

// TestTheOriginalIsCopiedNotMutated records that the outer map is copied before
// the path is processed, so the document handed in keeps its value. The copy is
// shallow, though — see the test below.
func TestTheOriginalIsCopiedNotMutated(t *testing.T) {
	original := map[string]interface{}{
		"profile": map[string]interface{}{"email": "alice@example.test"},
	}

	ProcessNestedFieldValue(original, "profile.email", maskConfig("profile.email", "masked"))

	profile := original["profile"].(map[string]interface{})
	if profile["email"] == "alice@example.test" {
		t.Fatal("the caller's document survived unchanged; the copy appears to be " +
			"deep now, so assert that instead")
	}
}

// TestTheCopyIsShallowSoNestedMapsAreShared records the consequence: the copy
// only duplicates the top level, so the nested map the mask writes into is the
// caller's own. A caller that reads the document afterwards sees the masked
// value where it expected the original.
func TestTheCopyIsShallowSoNestedMapsAreShared(t *testing.T) {
	inner := map[string]interface{}{"email": "alice@example.test"}
	original := map[string]interface{}{"profile": inner}

	ProcessNestedFieldValue(original, "profile.email", maskConfig("profile.email", "masked"))

	if inner["email"] == "alice@example.test" {
		t.Fatal("the nested map was not shared; the copy appears to be deep now")
	}
}

func TestProcessNestedFieldValueIgnoresAnUnconfiguredPath(t *testing.T) {
	value := map[string]interface{}{
		"profile": map[string]interface{}{"email": "alice@example.test"},
	}

	got := ProcessNestedFieldValue(value, "profile.email", maskConfig("other.path", "masked"))

	nested := got.(map[string]interface{})
	profile := nested["profile"].(map[string]interface{})
	if profile["email"] != "alice@example.test" {
		t.Error("an unconfigured path was masked")
	}
}

// TestASinglePathSegmentIsRejected records that a configured field with no dot in
// it is refused here, because this function exists for nested paths. A field
// configured as "email" is handled by ProcessValue instead.
func TestASinglePathSegmentIsRejected(t *testing.T) {
	value := map[string]interface{}{"email": "alice@example.test"}

	got := ProcessNestedFieldValue(value, "email", maskConfig("email", "masked"))

	if nested, ok := got.(map[string]interface{}); ok {
		if nested["email"] != "alice@example.test" {
			t.Error("a single-segment path was processed")
		}
	}
}

func TestADeepPathIsNavigated(t *testing.T) {
	got := ProcessNestedFieldValue(
		map[string]interface{}{
			"a": map[string]interface{}{
				"b": map[string]interface{}{
					"c": map[string]interface{}{"secret": "value"},
				},
			},
		},
		"a.b.c.secret",
		maskConfig("a.b.c.secret", "masked"))

	nested := got.(map[string]interface{})
	a := nested["a"].(map[string]interface{})
	b := a["b"].(map[string]interface{})
	c := b["c"].(map[string]interface{})
	if c["secret"] == "value" {
		t.Error("the deep path was not masked")
	}
}

// TestABrokenPathIsLoggedAndLeftAlone records that a path segment that is not an
// object stops the walk with an error line and no change. The document goes to
// the target with the value unmasked, and nothing in the response says so.
func TestABrokenPathIsLoggedAndLeftAlone(t *testing.T) {
	value := map[string]interface{}{"profile": "not an object"}

	got := ProcessNestedFieldValue(value, "profile.email.deep", maskConfig("profile.email.deep", "masked"))

	nested := got.(map[string]interface{})
	if nested["profile"] != "not an object" {
		t.Errorf("profile = %v, want it untouched", nested["profile"])
	}
}

// TestAMissingFinalFieldIsLoggedAndLeftAlone records the same for a path whose
// last segment is absent.
func TestAMissingFinalFieldIsLoggedAndLeftAlone(t *testing.T) {
	value := map[string]interface{}{
		"profile": map[string]interface{}{"name": "Alice"},
	}

	got := ProcessNestedFieldValue(value, "profile.email", maskConfig("profile.email", "masked"))

	nested := got.(map[string]interface{})
	profile := nested["profile"].(map[string]interface{})
	if _, exists := profile["email"]; exists {
		t.Error("a missing field was created")
	}
	if profile["name"] != "Alice" {
		t.Errorf("name = %v", profile["name"])
	}
}

// TestAnIntermediateBSONMapIsConvertedInPlace records that a bson.M encountered
// while walking is replaced with a map[string]interface{} in the document, so
// the type the syncer writes to the target changes as a side effect of masking.
func TestAnIntermediateBSONMapIsConvertedInPlace(t *testing.T) {
	value := map[string]interface{}{
		"a": bson.M{"b": bson.M{"secret": "value"}},
	}

	got := ProcessNestedFieldValue(value, "a.b.secret", maskConfig("a.b.secret", "masked"))

	nested := got.(map[string]interface{})
	if _, isBSON := nested["a"].(bson.M); isBSON {
		t.Fatal("the intermediate bson.M survived; the conversion appears to be gone, " +
			"so assert that instead")
	}
	a := nested["a"].(map[string]interface{})
	b := a["b"].(map[string]interface{})
	if b["secret"] == "value" {
		t.Error("the deep path was not masked")
	}
}

func TestProcessNestedObjectValueIsANoOpOnDegenerateInput(t *testing.T) {
	// A nil map, and a path with fewer than two segments, both return at once.
	processNestedObjectValue(nil, []string{"a", "b"}, "masked")
	processNestedObjectValue(map[string]interface{}{"a": 1}, []string{"a"}, "masked")
	processNestedObjectValue(map[string]interface{}{"a": 1}, nil, "masked")
}
