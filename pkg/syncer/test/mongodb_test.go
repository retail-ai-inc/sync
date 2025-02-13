package test

import (
	"fmt"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/syncer/common"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
	"testing"
	"time"
)

func testTC03MongoDBSync(t *testing.T, syncConfigs []config.SyncConfig) {
	var mongoTasks []config.SyncConfig
	for _, syncConfig := range syncConfigs {
		if strings.ToLower(syncConfig.Type) == "mongodb" && syncConfig.Enable {
			mongoTasks = append(mongoTasks, syncConfig)
		}
	}
	if len(mongoTasks) == 0 {
		t.Skip("[TC03] no enabled MongoDB tasks found.")
	}

	syncConfig := mongoTasks[0]
	srcDSN := syncConfig.SourceConnection
	tgtDSN := syncConfig.TargetConnection
	if len(syncConfig.Mappings) == 0 || len(syncConfig.Mappings[0].Tables) == 0 {
		t.Skip("[TC03] no table mapping.")
	}
	sourceTable := syncConfig.Mappings[0].Tables[0].SourceTable
	targetTable := syncConfig.Mappings[0].Tables[0].TargetTable

	srcCli, err := connectMongo(srcDSN)
	if err != nil {
		t.Fatalf("[TC03] connect src fail: %v", err)
	}
	defer srcCli.Disconnect(ctx)

	tgtCli, err := connectMongo(tgtDSN)
	if err != nil {
		t.Fatalf("[TC03] connect tgt fail: %v", err)
	}
	defer tgtCli.Disconnect(ctx)

	sourceDBName := common.GetDatabaseName(syncConfig.Type, srcDSN)
	targetDBName := common.GetDatabaseName(syncConfig.Type, tgtDSN)

	srcColl := srcCli.Database(sourceDBName).Collection(sourceTable)
	// tgtColl := tgtCli.Database(tk.Parsed.TargetConn.Database).Collection(targetTable)

	time.Sleep(1 * time.Second)
	// Insert initial documents
	docs := make([]interface{}, 0, 5)
	for i := 1; i <= 5; i++ {
		docs = append(docs, bson.M{"name": fmt.Sprintf("User_%d", i), "email": fmt.Sprintf("user%d@mail.com", i)})
	}
	_, err = srcColl.InsertMany(ctx, docs)
	if err != nil {
		t.Fatalf("[TC03] InsertMany fail: %v", err)
	}
	time.Sleep(5 * time.Second)

	// Compare data consistency between source and target
	compareMongoDataConsistency(t, srcCli, tgtCli, sourceDBName, targetDBName, sourceTable, targetTable)

	// Perform updates and verify the data consistency again
	_, err = srcColl.InsertOne(ctx, bson.M{"name": "ExtraUser", "email": "extra@mail.com"})
	if err != nil {
		t.Errorf("[TC03] insertOne fail => %v", err)
	}
	time.Sleep(2 * time.Second)
	_, err = srcColl.UpdateOne(ctx, bson.M{"name": "ExtraUser"}, bson.M{"$set": bson.M{"name": "UpdatedName", "email": "updated@mail.com"}})
	if err != nil {
		t.Errorf("[TC03] updateOne fail => %v", err)
	}
	time.Sleep(2 * time.Second)
	_, err = srcColl.DeleteOne(ctx, bson.M{"name": "UpdatedName"})
	if err != nil {
		t.Errorf("[TC03] deleteOne fail => %v", err)
	}
	time.Sleep(2 * time.Second)

	// Final data consistency check
	compareMongoDataConsistency(t, srcCli, tgtCli, sourceDBName, targetDBName, sourceTable, targetTable)
}
