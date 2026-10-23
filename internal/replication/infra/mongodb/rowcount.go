package mongodb

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

// RowCounts counts every replicated collection on both sides.
//
// CountDocuments and not EstimatedDocumentCount. The estimate reads the
// collection metadata, and on a sharded cluster that metadata is per shard and
// stale -- it was checked against a real count here and disagreed. The exact
// count is what makes this worth asking for, and it is also why it is asked for
// rather than collected: 116 collections on both sides took about five minutes
// against this source.
func RowCounts(ctx context.Context, cfg config.SyncConfig) (domain.RowCounts, error) {
	source, err := mongo.Connect(options.Client().ApplyURI(cfg.SourceConnection))
	if err != nil {
		return domain.RowCounts{}, fmt.Errorf("connect to the source: %w", err)
	}
	defer func() { _ = source.Disconnect(ctx) }()

	target, err := mongo.Connect(options.Client().ApplyURI(cfg.TargetConnection))
	if err != nil {
		return domain.RowCounts{}, fmt.Errorf("connect to the target: %w", err)
	}
	defer func() { _ = target.Disconnect(ctx) }()

	sourceDB := source.Database(dsn.GetDatabaseName(cfg.Type, cfg.SourceConnection))
	targetDB := target.Database(dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection))

	pairs, discovered := collectionPairs(cfg)
	counts := domain.RowCounts{Engine: "mongodb", Discovered: discovered}
	if discovered {
		names, err := discovery.MongoCollections(ctx, sourceDB)
		if err != nil {
			return domain.RowCounts{}, err
		}
		for _, name := range names {
			pairs = append(pairs, discovery.Pair{Source: name, Target: name})
		}
	}

	for _, pair := range pairs {
		object := domain.ObjectCount{Source: pair.Source, Target: pair.Target}

		var err error
		object.SourceRows, err = countDocuments(ctx, sourceDB, pair.Source)
		if err != nil {
			object.SourceRows = -1
			object.Note = fmt.Sprintf("source: %v", err)
		}
		object.TargetRows, err = countDocuments(ctx, targetDB, pair.Target)
		if err != nil {
			object.TargetRows = -1
			if object.Note != "" {
				object.Note += "; "
			}
			object.Note += fmt.Sprintf("target: %v", err)
		}
		counts.Objects = append(counts.Objects, object)
	}
	return counts, nil
}

func collectionPairs(cfg config.SyncConfig) (pairs []discovery.Pair, discovered bool) {
	pairs = discovery.ConfiguredPairs(cfg.Mappings)
	return pairs, len(pairs) == 0
}

func countDocuments(ctx context.Context, db *mongo.Database, name string) (int64, error) {
	return db.Collection(name).CountDocuments(ctx, bson.D{})
}
