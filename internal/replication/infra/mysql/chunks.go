package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Chunks reads a table in primary key order, for a re-copy that runs alongside
// the stream.
//
// The source's own clock is read before the rows, not this machine's: the
// pipeline holds the chunk until the stream has passed that moment, and the
// comparison has to be in the source's terms or the two clocks' skew decides
// whether a repair is safe.
type Chunks struct {
	Source *sql.DB
	// Database is the source database.
	Database string
	// Dialect spells the upsert for the target.
	Dialect dialect
	// TargetDatabase and TargetOf resolve where the rows are written.
	TargetDatabase string
	TargetOf       func(source string) string
}

// NextChunk reads the rows after a key.
func (c *Chunks) NextChunk(ctx context.Context, ns domain.Namespace, after string, size int) (pipeline.Chunk, error) {
	var chunk pipeline.Chunk

	// The source's clock first, so it is never later than the rows it describes.
	// Taken the other way round, a row changed between the read and the clock
	// would look older than it is and could be applied over a newer value.
	var seconds int64
	if err := c.Source.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP()").Scan(&seconds); err != nil {
		return chunk, fmt.Errorf("read the source's clock: %w", err)
	}
	chunk.ReadAt = time.Unix(seconds, 0)

	key, err := c.primaryKey(ctx, ns.Object)
	if err != nil {
		return chunk, err
	}
	if key == "" {
		return chunk, domain.Unrecoverable(
			"%s has no single-column primary key, so a re-copy cannot walk it in key "+
				"order. Re-copying it means clearing the position and copying everything",
			ns)
	}

	columns, err := c.columns(ctx, ns.Object)
	if err != nil {
		return chunk, err
	}

	query := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(columns, ","), c.Database, ns.Object)
	args := []interface{}{}
	if after != "" {
		query += fmt.Sprintf(" WHERE %s > ?", key)
		args = append(args, after)
	}
	query += fmt.Sprintf(" ORDER BY %s LIMIT %d", key, size)

	rows, err := c.Source.QueryContext(ctx, query, args...)
	if err != nil {
		return chunk, fmt.Errorf("read %s: %w", ns, err)
	}
	defer rows.Close()

	keyAt := indexOf(columns, key)
	target := ns.Object
	if c.TargetOf != nil {
		target = c.TargetOf(ns.Object)
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return chunk, fmt.Errorf("read a row of %s: %w", ns, err)
		}

		chunk.Events = append(chunk.Events, &domain.Event{
			NS:  ns,
			Op:  domain.OpInsert,
			Key: fmt.Sprintf("%v\x00", values[keyAt]),
			Payload: statement{
				query: upsertStatement(c.Dialect, c.TargetDatabase, target, columns, 1),
				args:  values,
			},
		})
		chunk.After = fmt.Sprintf("%v", values[keyAt])
	}
	if err := rows.Err(); err != nil {
		return chunk, fmt.Errorf("read %s: %w", ns, err)
	}

	chunk.Done = len(chunk.Events) < size
	return chunk, nil
}

// primaryKey reports the table's single key column, empty when it has none or
// has more than one.
func (c *Chunks) primaryKey(ctx context.Context, table string) (string, error) {
	rows, err := c.Source.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		 ORDER BY ORDINAL_POSITION`, c.Database, table)
	if err != nil {
		return "", fmt.Errorf("read the primary key of %s.%s: %w", c.Database, table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	if len(columns) != 1 {
		return "", nil
	}
	return columns[0], nil
}

// columns lists the table's columns in ordinal order.
func (c *Chunks) columns(ctx context.Context, table string) ([]string, error) {
	rows, err := c.Source.QueryContext(ctx,
		`SELECT COLUMN_NAME FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
		c.Database, table)
	if err != nil {
		return nil, fmt.Errorf("read the columns of %s.%s: %w", c.Database, table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%s.%s has no columns", c.Database, table)
	}
	return columns, nil
}

func indexOf(list []string, want string) int {
	for i, s := range list {
		if strings.EqualFold(s, want) {
			return i
		}
	}
	return 0
}
