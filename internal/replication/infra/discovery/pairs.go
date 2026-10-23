package discovery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// What a task covers, in one place.
//
// The rule is the same everywhere it is asked: a task that names tables covers
// those, a table with no target named goes to the same name, and a task that
// names none covers whatever the source holds. It was written out four times --
// the row-count endpoint for each of two engines, the consistency check, and
// the monitoring counters -- and the copies had already drifted: two of them
// reported whether the list was discovered and two dropped that fact.

// Pair is one object and where it goes.
type Pair struct {
	Source string
	Target string
}

// ConfiguredPairs is what the mappings name, with an unnamed target meaning the
// same name on both sides. Empty means the task named nothing, which is what
// makes the caller discover.
func ConfiguredPairs(mappings []config.DatabaseMapping) []Pair {
	var pairs []Pair
	for _, mapping := range mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			target := table.TargetTable
			if target == "" {
				target = table.SourceTable
			}
			pairs = append(pairs, Pair{Source: table.SourceTable, Target: target})
		}
	}
	return pairs
}

// SamePairs pairs each discovered name with itself, which is where a task that
// lists nothing replicates it to.
func SamePairs(names []string) []Pair {
	pairs := make([]Pair, 0, len(names))
	for _, name := range names {
		pairs = append(pairs, Pair{Source: name, Target: name})
	}
	return pairs
}

// PostgresTables reports the base tables in one schema.
//
// Its own query rather than MySQLTables': that one binds with ?, which
// PostgreSQL does not accept, and a schema identifies a table here where a
// database does there.
func PostgresTables(ctx context.Context, db *sql.DB, schema string) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = $1 AND table_type = 'BASE TABLE'
		 ORDER BY table_name`, schema)
	if err != nil {
		return nil, fmt.Errorf("list the tables in %s: %w", schema, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("list the tables in %s: %w", schema, err)
		}
		if IsInternal(name) {
			continue
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}
