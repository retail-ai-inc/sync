package mongodb

import (
	"bytes"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// TestMaskingReachesADocumentTheDriverDecodedAsABsonD is a regression test for
// a security setting that stopped working without saying so.
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

	masked := syncer.maskValue("shop", "customers", bson.D{
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

	masked := syncer.maskValue("shop", "customers", bson.M{"_id": 1, "card": "4111"})
	document := documentOf(masked)
	if document["card"] != "****" {
		t.Errorf("card = %v, want it masked", document["card"])
	}
}

// A failure means one database's documents were written under another database's policy.
func TestTheSameCollectionInTwoDatabasesIsMaskedByItsOwnMapping(t *testing.T) {
	syncer := &MongoDBSyncer{logger: logrus.New(), cfg: config.SyncConfig{
		Mappings: []config.DatabaseMapping{
			{SourceDatabase: "shop", Tables: []config.TableMapping{{SourceTable: "customers"}}},
			{SourceDatabase: "ledger", Tables: []config.TableMapping{{
				SourceTable:     "customers",
				SecurityEnabled: true,
				FieldSecurity: []interface{}{
					map[string]interface{}{"field": "card", "securityType": "masked"},
				},
			}}},
		},
	}}

	for database, want := range map[string]string{"shop": "4111", "ledger": "****"} {
		raw := rawEvent(t, append(changeDoc(database, "customers", "insert", bson.D{{Key: "_id", Value: 1}}),
			bson.E{Key: "fullDocument", Value: bson.D{{Key: "_id", Value: 1}, {Key: "card", Value: "4111"}}}))

		model, err := syncer.convertRawBSONToWriteModel(raw, database, "customers")
		if err != nil {
			t.Fatalf("convert %s: %v", database, err)
		}
		replace, ok := model.(*mongo.ReplaceOneModel)
		if !ok {
			t.Fatalf("%s: model is a %T, want a replace", database, model)
		}
		if got := documentOf(replace.Replacement)["card"]; got != want {
			t.Errorf("%s.customers card = %v, want %q", database, got, want)
		}
	}
}

// The warning is one a collection, not one an event: a busy collection would
// otherwise fill the log with the same line, which is how a warning stops
// being read.
func TestDroppedDeletesAreReportedOncePerCollection(t *testing.T) {
	var out bytes.Buffer
	logger := logrus.New()
	logger.SetOutput(&out)
	logger.SetLevel(logrus.InfoLevel)

	syncer := &MongoDBSyncer{logger: logger}
	for i := 0; i < 5; i++ {
		syncer.warnAboutDroppedDeletes("shop", "orders")
	}
	syncer.warnAboutDroppedDeletes("shop", "customers")

	text := out.String()
	if n := strings.Count(text, "shop.orders"); n != 1 {
		t.Errorf("shop.orders was reported %d times, want 1", n)
	}
	if n := strings.Count(text, "shop.customers"); n != 1 {
		t.Errorf("shop.customers was reported %d times, want 1", n)
	}
	// At warning level, because Debug is not shown in production and the target
	// silently keeping deleted documents is what this is for.
	if !strings.Contains(text, "level=warning") {
		t.Errorf("the report is not a warning: %s", text)
	}
	if !strings.Contains(text, "ignoreDeleteOps") {
		t.Error("the report does not name the setting that caused it")
	}
}
