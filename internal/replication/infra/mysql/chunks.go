package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// Chunks reads a table in primary key order, for a re-copy that runs alongside
// the stream. The source's own clock is read before the rows, not this
// machine's: the pipeline holds the chunk until the stream has passed that
// moment, and the comparison has to be in the source's terms or the two
// clocks' skew decides whether a repair is safe.
type Chunks struct {
	Source *sql.DB
	// Database is the source database.
	Database string
	// Dialect spells the upsert for the target.
	Dialect dialect
	// TargetDatabase and TargetOf resolve where the rows are written.
	TargetDatabase string
	TargetOf       func(source string) string
	// Mappings carry the task's field security. Without them a re-copy wrote raw
	// source values over fields the stream masks or encrypts, so asking for one
	// replaced protected data on the target with plaintext -- the stream path
	// has applied this from the start, and this path was reading the same rows
	// and skipping it.
	Mappings []config.DatabaseMapping
}

// mask applies the task's field security to one row, returning a new slice.
//
// A copy, not the row: the caller's slice is the scan buffer and is reused, and
// the stream path builds a new slice for the same reason.
func (c *Chunks) mask(table, target string, columns []string, values []interface{}) []interface{} {
	policy := security.FindTableSecurityFromMappings(security.TableRef{
		Database: c.Database, Table: table, Target: target,
	}, c.Mappings)
	if !policy.SecurityEnabled || len(policy.FieldSecurity) == 0 {
		return values
	}

	out := make([]interface{}, len(values))
	for i, value := range values {
		if i < len(columns) {
			out[i] = security.ProcessValue(value, columns[i], policy)
			continue
		}
		out[i] = value
	}
	return out
}

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

	query := fmt.Sprintf("SELECT %s FROM %s.%s", strings.Join(quoteAll(columns), ","),
		quoteName(c.Database), quoteName(ns.Object))
	args := []interface{}{}
	if after != "" {
		query += fmt.Sprintf(" WHERE %s > ?", quoteName(key))
		args = append(args, after)
	}
	query += fmt.Sprintf(" ORDER BY %s LIMIT %d", quoteName(key), size)

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

		key := cursorKey(values[keyAt])
		chunk.Events = append(chunk.Events, &domain.Event{
			NS:  ns,
			Op:  domain.OpInsert,
			Key: key + "\x00",
			Payload: statement{
				query: upsertStatement(c.Dialect, c.TargetDatabase, target, columns, 1),
				args:  c.mask(ns.Object, target, columns, values),
			},
		})
		chunk.After = key
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

func (c *Chunks) columns(ctx context.Context, table string) ([]string, error) {
	// EXTRA carries the generation clause. A generated column is computed by the
	// server, which refuses a write that supplies one, so a re-copy of a table
	// holding one failed outright -- the stream path filters them through
	// writableColumns and this path did not.
	rows, err := c.Source.QueryContext(ctx,
		`SELECT COLUMN_NAME, EXTRA FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
		c.Database, table)
	if err != nil {
		return nil, fmt.Errorf("read the columns of %s.%s: %w", c.Database, table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var name string
		var extra sql.NullString
		if err := rows.Scan(&name, &extra); err != nil {
			return nil, err
		}
		if generatedColumn(extra.String) {
			continue
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("%s.%s has no columns that may be written", c.Database, table)
	}
	return columns, nil
}

// cursorKey renders a primary key so it can be bound back into the next
// chunk's WHERE clause.
//
// The driver hands VARCHAR and binary columns back as []byte, and %v on those
// prints the bytes: a key of "abc" became "[97 98 99]", which was then bound to
// "WHERE key > ?" and matched nothing like the row it came from. Once a table
// spanned more than one chunk that repeated pages or skipped the rest of the
// table.
func cursorKey(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case []byte:
		return string(typed)
	case string:
		return typed
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func indexOf(list []string, want string) int {
	for i, s := range list {
		if strings.EqualFold(s, want) {
			return i
		}
	}
	return 0
}
