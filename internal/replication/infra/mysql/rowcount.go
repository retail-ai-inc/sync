package mysql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

// RowCounts counts every replicated table on both sides.
//
// Exact counts, not information_schema.table_rows: that column is an estimate
// derived from the index statistics and is routinely out by whole percentages
// on InnoDB, which makes it useless for the one question this answers.
func RowCounts(ctx context.Context, cfg config.SyncConfig) (domain.RowCounts, error) {
	source, err := sql.Open("mysql", cfg.SourceConnection)
	if err != nil {
		return domain.RowCounts{}, fmt.Errorf("connect to the source: %w", err)
	}
	defer source.Close()

	target, err := sql.Open("mysql", cfg.TargetConnection)
	if err != nil {
		return domain.RowCounts{}, fmt.Errorf("connect to the target: %w", err)
	}
	defer target.Close()

	sourceDB := dsn.GetDatabaseName(cfg.Type, cfg.SourceConnection)
	targetDB := dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection)

	pairs, discovered := tablePairs(cfg)
	if !discovered {
		counts := domain.RowCounts{Engine: cfg.Type}
		return fill(ctx, counts, pairs, source, target, sourceDB, targetDB), nil
	}

	// The task names no tables, so it replicates whatever the source holds --
	// the same rule the reader follows, asked here so the comparison covers the
	// same objects rather than nothing at all.
	names, err := discovery.MySQLTables(ctx, source, sourceDB)
	if err != nil {
		return domain.RowCounts{}, err
	}
	for _, name := range names {
		pairs = append(pairs, [2]string{name, name})
	}
	counts := domain.RowCounts{Engine: cfg.Type, Discovered: true}
	return fill(ctx, counts, pairs, source, target, sourceDB, targetDB), nil
}

// tablePairs reports the source/target pairs the task names, and whether it
// named none.
func tablePairs(cfg config.SyncConfig) (pairs [][2]string, discovered bool) {
	for _, mapping := range cfg.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable == "" {
				continue
			}
			target := table.TargetTable
			if target == "" {
				target = table.SourceTable
			}
			pairs = append(pairs, [2]string{table.SourceTable, target})
		}
	}
	return pairs, len(pairs) == 0
}

func fill(ctx context.Context, counts domain.RowCounts, pairs [][2]string,
	source, target *sql.DB, sourceDB, targetDB string) domain.RowCounts {

	for _, pair := range pairs {
		object := domain.ObjectCount{Source: pair[0], Target: pair[1]}

		var err error
		object.SourceRows, err = countRows(ctx, source, sourceDB, pair[0])
		if err != nil {
			object.SourceRows = -1
			object.Note = fmt.Sprintf("source: %v", err)
		}
		object.TargetRows, err = countRows(ctx, target, targetDB, pair[1])
		if err != nil {
			object.TargetRows = -1
			if object.Note != "" {
				object.Note += "; "
			}
			object.Note += fmt.Sprintf("target: %v", err)
		}
		counts.Objects = append(counts.Objects, object)
	}
	return counts
}

// countRows counts one table. The name is quoted rather than bound because a
// table name cannot be a parameter, and it comes from the source's own
// catalogue or the task's configuration.
func countRows(ctx context.Context, db *sql.DB, schema, table string) (int64, error) {
	var n int64
	err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", quoteName(schema), quoteName(table))).Scan(&n)
	return n, err
}

// quoteName renders an identifier, doubling any backtick in it so a name
// cannot end the quoting and become part of the statement.
func quoteName(name string) string {
	out := make([]rune, 0, len(name)+2)
	out = append(out, '`')
	for _, r := range name {
		if r == '`' {
			out = append(out, '`')
		}
		out = append(out, r)
	}
	return string(append(out, 0x60))
}
