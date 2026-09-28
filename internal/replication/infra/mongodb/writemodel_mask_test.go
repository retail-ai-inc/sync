package mongodb

import (
	"fmt"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

const (
	clearCard  = "4111111111111111"
	clearPhone = "555-0100"
)

func protectedSyncer() *MongoDBSyncer {
	return &MongoDBSyncer{logger: quietLog(), cfg: config.SyncConfig{
		Mappings: []config.DatabaseMapping{{SourceDatabase: "shop", Tables: []config.TableMapping{{
			SourceTable:     "customers",
			SecurityEnabled: true,
			FieldSecurity: []interface{}{
				map[string]interface{}{"field": "card", "securityType": "masked"},
				map[string]interface{}{"field": "profile.contact.phone", "securityType": "masked"},
			},
		}}}},
	}}
}

func protectedDocument() bson.D {
	return bson.D{
		{Key: "_id", Value: 1},
		{Key: "card", Value: clearCard},
		{Key: "profile", Value: bson.D{{Key: "contact", Value: bson.D{{Key: "phone", Value: clearPhone}}}}},
	}
}

// writtenBy renders what a write model would send to the target.
func writtenBy(t *testing.T, model interface{}) string {
	t.Helper()
	switch m := model.(type) {
	case *mongo.ReplaceOneModel:
		return fmt.Sprint(m.Replacement)
	case *mongo.UpdateOneModel:
		return fmt.Sprint(m.Update)
	}
	t.Fatalf("model is a %T, want a replace or an update", model)
	return ""
}

func assertMasked(t *testing.T, written string) {
	t.Helper()
	for _, clear := range []string{clearCard, clearPhone} {
		if strings.Contains(written, clear) {
			t.Errorf("the target is sent %q in the clear: %s", clear, written)
		}
	}
	if got, want := strings.Count(written, "*"), len(clearCard)+len(clearPhone); got != want {
		t.Errorf("the target is sent %d masked characters, want %d: %s", got, want, written)
	}
}

// A failure means a streamed insert, replace or looked-up update writes a protected field in the clear.
func TestAStreamedDocumentHasItsProtectedFieldsMasked(t *testing.T) {
	key := bson.D{{Key: "_id", Value: 1}}
	for _, op := range []string{"insert", "replace", "update"} {
		t.Run(op, func(t *testing.T) {
			raw := rawEvent(t, append(changeDoc("shop", "customers", op, key),
				bson.E{Key: "fullDocument", Value: protectedDocument()}))

			model, err := protectedSyncer().convertRawBSONToWriteModel(raw, "shop", "customers")
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			assertMasked(t, writtenBy(t, model))
		})
	}
}

// A failure means a delta writes a protected field in the clear, whichever path its $set keys name.
func TestADeltaToAProtectedFieldIsMasked(t *testing.T) {
	for name, updated := range map[string]bson.D{
		"top-level fields": {
			{Key: "card", Value: clearCard},
			{Key: "profile", Value: bson.D{{Key: "contact", Value: bson.D{{Key: "phone", Value: clearPhone}}}}},
		},
		"the field's own dotted path": {
			{Key: "card", Value: clearCard},
			{Key: "profile.contact.phone", Value: clearPhone},
		},
		"a dotted path to a document above it": {
			{Key: "card", Value: clearCard},
			{Key: "profile.contact", Value: bson.D{{Key: "phone", Value: clearPhone}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			raw := rawEvent(t, append(changeDoc("shop", "customers", "update", bson.D{{Key: "_id", Value: 1}}),
				bson.E{Key: "updateDescription", Value: bson.D{{Key: "updatedFields", Value: updated}}}))

			model, err := protectedSyncer().convertRawBSONToWriteModel(raw, "shop", "customers")
			if err != nil {
				t.Fatalf("convert: %v", err)
			}
			if _, ok := model.(*mongo.UpdateOneModel); !ok {
				t.Fatalf("model is a %T, want the change written as a delta", model)
			}
			assertMasked(t, writtenBy(t, model))
		})
	}
}

// A failure means a delta rebuilds a subdocument no rule reaches, reordering its fields on the target.
func TestADeltaLeavesASubdocumentNoRuleReachesAsItIs(t *testing.T) {
	shipping := bson.D{{Key: "to", Value: "Osaka"}, {Key: "from", Value: "Tokyo"}}
	raw := rawEvent(t, append(changeDoc("shop", "customers", "update", bson.D{{Key: "_id", Value: 1}}),
		bson.E{Key: "updateDescription", Value: bson.D{{Key: "updatedFields", Value: bson.D{
			{Key: "card", Value: clearCard},
			{Key: "shipping", Value: shipping},
		}}}}))

	model, err := protectedSyncer().convertRawBSONToWriteModel(raw, "shop", "customers")
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	update, ok := model.(*mongo.UpdateOneModel)
	if !ok {
		t.Fatalf("model is a %T, want the change written as a delta", model)
	}
	set := update.Update.(bson.M)["$set"].(bson.M)
	if got, ok := set["shipping"].(bson.D); !ok || fmt.Sprint(got) != fmt.Sprint(shipping) {
		t.Errorf("shipping = %#v, want %v in its order", set["shipping"], shipping)
	}
}
