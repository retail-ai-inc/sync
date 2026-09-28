package infra

import (
	"context"
	"database/sql"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

// What to compare when the task names nothing.
//
// The counters looped over the configured mappings, so a task replicating a
// whole database -- which is every task in production -- compared nothing and
// reported nothing at all. The row-count panel was empty for them and had been
// since they were created. The rule itself lives in discovery,
// with the row-count endpoint and the consistency check; only the listing
// differs per engine.

func sqlPairs(ctx context.Context, sc config.SyncConfig, source discovery.Querier,
	sourceDB string) ([]discovery.Pair, error) {

	if pairs := discovery.ConfiguredPairs(sc.Mappings); len(pairs) > 0 {
		return pairs, nil
	}
	names, err := discovery.MySQLTables(ctx, source, sourceDB)
	if err != nil {
		return nil, err
	}
	return discovery.SamePairs(names), nil
}

// postgresPairs takes one mapping rather than the task: each mapping names a
// schema, and pairing every configured table with every schema would count each
// one twice.
func postgresPairs(ctx context.Context, mapping config.DatabaseMapping, source *sql.DB,
	schema string) ([]discovery.Pair, error) {

	if pairs := discovery.ConfiguredPairs([]config.DatabaseMapping{mapping}); len(pairs) > 0 {
		return pairs, nil
	}
	names, err := discovery.PostgresTables(ctx, source, schema)
	if err != nil {
		return nil, err
	}
	return discovery.SamePairs(names), nil
}

// mongoTables reports the collections to compare, as whole mappings: a
// configured collection can carry a count query and a discovered one cannot.
func mongoTables(ctx context.Context, sc config.SyncConfig,
	source *mongo.Database) ([]config.TableMapping, error) {

	var configured []config.TableMapping
	for _, mapping := range sc.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			if table.TargetTable == "" {
				table.TargetTable = table.SourceTable
			}
			configured = append(configured, table)
		}
	}
	if len(configured) > 0 {
		return configured, nil
	}

	names, err := discovery.MongoCollections(ctx, source)
	if err != nil {
		return nil, err
	}
	discovered := make([]config.TableMapping, 0, len(names))
	for _, pair := range discovery.SamePairs(names) {
		discovered = append(discovered, config.TableMapping{
			SourceTable: pair.Source, TargetTable: pair.Target,
		})
	}
	return discovered, nil
}
