package infra

import (
	"context"
	"database/sql"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

// What to compare when the task names nothing.
//
// The counters looped over the configured mappings, so a task replicating a
// whole database -- which is every task in production -- compared nothing and
// wrote no monitoring_log row at all. The row-count panel was empty for them
// and had been since they were created. Discovery is the same rule replication
// itself follows, so the comparison covers the same objects.

type countPair struct{ source, target string }

// configuredPairs is what the task lists, with an unnamed target meaning the
// same name on both sides.
func configuredPairs(sc config.SyncConfig) []countPair {
	var pairs []countPair
	for _, mapping := range sc.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			target := table.TargetTable
			if target == "" {
				target = table.SourceTable
			}
			pairs = append(pairs, countPair{table.SourceTable, target})
		}
	}
	return pairs
}

func samePairs(names []string) []countPair {
	pairs := make([]countPair, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, countPair{name, name})
	}
	return pairs
}

// sqlPairs reports the tables to compare for a SQL task.
func sqlPairs(ctx context.Context, sc config.SyncConfig, source discovery.Querier,
	sourceDB string) ([]countPair, error) {

	if pairs := configuredPairs(sc); len(pairs) > 0 {
		return pairs, nil
	}
	names, err := discovery.MySQLTables(ctx, source, sourceDB)
	if err != nil {
		return nil, err
	}
	return samePairs(names), nil
}

// postgresPairs reports the tables to compare in one schema.
//
// Its own query rather than discovery.MySQLTables: that one binds with ?, which
// PostgreSQL does not accept, and a schema is what identifies a table here.
func postgresPairs(ctx context.Context, mapping config.DatabaseMapping, source *sql.DB,
	schema string) ([]countPair, error) {

	// This mapping's own tables, not the task's: each mapping names a schema,
	// and pairing every table with every schema would count each one twice.
	if pairs := configuredPairs(config.SyncConfig{
		Mappings: []config.DatabaseMapping{mapping},
	}); len(pairs) > 0 {
		return pairs, nil
	}

	rows, err := source.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		 ORDER BY table_name`, schema)
	if err != nil {
		return nil, fmt.Errorf("list the tables in %s: %w", schema, err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("list the tables in %s: %w", schema, err)
		}
		if discovery.IsInternal(name) {
			continue
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return samePairs(names), nil
}

// mongoTables reports the collections to compare for a MongoDB task.
//
// Whole mappings rather than name pairs, because a configured collection can
// carry a count query and a discovered one cannot.
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
	for _, name := range names {
		discovered = append(discovered, config.TableMapping{
			SourceTable: name, TargetTable: name,
		})
	}
	return discovered, nil
}
