// Package discovery finds the tables and collections a task should replicate
// when its configuration does not list them.
//
// Until now every table had to be typed into the UI one at a time, and a table
// created at the source afterwards was simply not replicated — silently, with
// no warning anywhere, until somebody noticed it missing from the
// disaster-recovery copy. For a payment schema that grows a table for a new
// settlement type, "silently not replicated" is the worst possible failure: it
// looks exactly like everything working.
package discovery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// internalPrefixes name the tables and collections this tool creates for
// itself. Replicating them would copy one side's replication state onto the
// other, which for the direction lock means telling the target it is a source.
var internalPrefixes = []string{"_sync_"}

// IsInternal reports whether a name belongs to the syncer rather than to the
// data being replicated.
func IsInternal(name string) bool {
	for _, prefix := range internalPrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	// MongoDB's own bookkeeping collections.
	return strings.HasPrefix(name, "system.")
}

// Querier is the part of database/sql this needs, so the caller may pass either
// a pool or the pinned connection the consistent snapshot reads through.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// MySQLTables reports the base tables in a MySQL database.
//
// Views are excluded: replicating one would need the view's definition rather
// than its rows, and the binlog carries no events for it anyway.
func MySQLTables(ctx context.Context, db Querier, database string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = ? AND table_type = 'BASE TABLE'
		 ORDER BY table_name`, database)
	if err != nil {
		return nil, fmt.Errorf("list the tables in %s: %w", database, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("list the tables in %s: %w", database, err)
		}
		if IsInternal(name) {
			continue
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// MongoCollections reports the collections in a MongoDB database.
func MongoCollections(ctx context.Context, db *mongo.Database) ([]string, error) {
	names, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("list the collections in %s: %w", db.Name(), err)
	}

	var collections []string
	for _, name := range names {
		if IsInternal(name) {
			continue
		}
		collections = append(collections, name)
	}
	return collections, nil
}

// Unlisted reports the names the source holds that a task does not name, and
// records them as seen so a name is reported once rather than every scan.
//
// Internal tables are excluded: the checkpoint and the direction lock are the
// syncer's own, and reporting them as unreplicated would be noise that trains
// people to ignore the warning.
func Unlisted(listed, reported map[string]bool, current []string) []string {
	var missing []string
	for _, name := range current {
		key := strings.ToLower(name)
		if listed[key] || reported[key] || IsInternal(name) {
			continue
		}
		reported[key] = true
		missing = append(missing, name)
	}
	return missing
}

// Added reports the names in current that were not in known, so a caller can
// act on what has appeared since it last looked.
func Added(known map[string]bool, current []string) []string {
	var added []string
	for _, name := range current {
		if !known[name] {
			added = append(added, name)
		}
	}
	return added
}

// Poll runs scan straight away and then once per interval until the context is
// cancelled.
//
// Three copies of this loop existed, one per thing a syncer rescans. Running
// the first scan before the ticker is the part worth having in one place: a
// loop that only ever runs on the tick does nothing at all for the length of
// the interval, which for the newly-created-table scan means a table added
// just before the syncer started is not replicated for a minute and nothing
// says so.
func Poll(ctx context.Context, interval time.Duration, scan func()) {
	scan()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scan()
		}
	}
}
