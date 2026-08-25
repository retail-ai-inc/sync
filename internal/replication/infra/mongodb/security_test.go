package mongodb

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// ------------------------------------------- the shape the driver decodes into

// TestMaskingReachesADocumentTheDriverDecodedAsABsonD is a regression test for a
// security setting that stopped working without saying so.
//
// Unmarshalling into a bson.M gives nested documents as bson.M under the
// MongoDB driver's v1 and as bson.D under its v2. A change stream event's
// fullDocument therefore changed shape when the driver was upgraded, fell
// through the type switch, and was returned untouched — so a task configured to
// mask a field replicated it in the clear, with nothing anywhere to show it.
func TestMaskingReachesADocumentTheDriverDecodedAsABsonD(t *testing.T) {
	syncer := &MongoDBSyncer{cfg: config.SyncConfig{
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{{
			SourceTable:     "customers",
			TargetTable:     "customers",
			SecurityEnabled: true,
			FieldSecurity: []interface{}{
				map[string]interface{}{"field": "card", "securityType": "masked"},
			},
		}}}},
	}}

	masked := syncer.maskValue("customers", bson.D{
		{Key: "_id", Value: 1},
		{Key: "card", Value: "4111111111111111"},
	})

	document := documentOf(masked)
	if document == nil {
		t.Fatalf("maskValue returned a %T, which carries no fields", masked)
	}
	if got := document["card"]; got == "4111111111111111" {
		t.Fatal("the card number was replicated in the clear")
	} else if got != "****************" {
		t.Errorf("card = %v, want it masked", got)
	}
}

// TestMaskingABsonMStillWorks keeps the shape the driver's v1 produced working,
// so the fix above is an addition and not a swap.
func TestMaskingABsonMStillWorks(t *testing.T) {
	syncer := &MongoDBSyncer{cfg: config.SyncConfig{
		Mappings: []config.DatabaseMapping{{Tables: []config.TableMapping{{
			SourceTable:     "customers",
			SecurityEnabled: true,
			FieldSecurity: []interface{}{
				map[string]interface{}{"field": "card", "securityType": "masked"},
			},
		}}}},
	}}

	masked := syncer.maskValue("customers", bson.M{"_id": 1, "card": "4111"})
	document := documentOf(masked)
	if document["card"] != "****" {
		t.Errorf("card = %v, want it masked", document["card"])
	}
}
