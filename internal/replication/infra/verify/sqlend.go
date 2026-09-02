package verify

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SQLEnd reads one SQL table.
//
// It streams the rows in key order and looks individual keys up, which is all
// the comparison asks of it. Nothing relies on the two sides ordering keys the
// same way — the paging order only has to be consistent with itself.
type SQLEnd struct {
	DB *sql.DB
	// Schema may be empty, in which case the table is addressed unqualified.
	Schema string
	Table  string
	// Keys are the columns that identify a row, in order. A payment ledger's
	// tables are commonly keyed by a pair — an account and an entry — so this is
	// a list rather than one column.
	Keys []string
	// Columns are the columns whose contents are hashed. The keys are included,
	// so a row whose key was rewritten shows up as two differences rather than
	// none.
	Columns []string

	// last is the key of the last row streamed, which is where the next page
	// starts.
	last []sql.NullString
	done bool
}

func (e *SQLEnd) Name() string {
	if e.Schema == "" {
		return e.Table
	}
	return e.Schema + "." + e.Table
}

// quote renders an identifier. Backticks are what MySQL uses and SQLite
// accepts, which is what the hermetic suite drives this against.
func quote(name string) string { return "`" + name + "`" }

func quoteAll(names []string) []string {
	out := make([]string, len(names))
	for i, name := range names {
		out[i] = quote(name)
	}
	return out
}

func marks(n int) string {
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

// selectList is the projection every read uses: the key columns first, then the
// columns to hash.
func (e *SQLEnd) selectList() string {
	return strings.Join(append(quoteAll(e.Keys), quoteAll(e.Columns)...), ", ")
}

func (e *SQLEnd) scanRow(rows *sql.Rows) (Row, []sql.NullString, error) {
	cells := make([]sql.NullString, len(e.Keys)+len(e.Columns))
	scan := make([]interface{}, len(cells))
	for i := range cells {
		scan[i] = &cells[i]
	}
	if err := rows.Scan(scan...); err != nil {
		return Row{}, nil, err
	}
	key := cells[:len(e.Keys)]
	return Row{Key: encodeKey(key), Digest: Digest(cells[len(e.Keys):])}, key, nil
}

func (e *SQLEnd) Next(ctx context.Context, limit int) ([]Row, error) {
	if e.done {
		return nil, nil
	}

	query := fmt.Sprintf("SELECT %s FROM %s", e.selectList(), e.Name())
	var args []interface{}
	if e.last != nil {
		// A row constructor, so a composite key pages correctly: (a, b) > (?, ?)
		// is one comparison rather than a where-clause that has to enumerate the
		// carry cases. MySQL and SQLite both take it.
		query += fmt.Sprintf(" WHERE (%s) > (%s)",
			strings.Join(quoteAll(e.Keys), ", "), marks(len(e.Keys)))
		for _, v := range e.last {
			args = append(args, nullable(v))
		}
	}
	query += fmt.Sprintf(" ORDER BY %s LIMIT %d", strings.Join(quoteAll(e.Keys), ", "), limit)

	rows, err := e.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Row
	var lastKey []sql.NullString
	for rows.Next() {
		row, key, err := e.scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
		lastKey = key
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(out) == 0 {
		e.done = true
		return nil, nil
	}
	e.last = lastKey
	return out, nil
}

func (e *SQLEnd) Lookup(ctx context.Context, keys []string) (map[string]Row, error) {
	if len(keys) == 0 {
		return map[string]Row{}, nil
	}

	tuples := make([]string, 0, len(keys))
	var args []interface{}
	for _, key := range keys {
		values, err := decodeKey(key)
		if err != nil {
			return nil, err
		}
		if len(values) != len(e.Keys) {
			return nil, fmt.Errorf("the comparison key %q has %d parts and %s is keyed "+
				"by %d columns", key, len(values), e.Name(), len(e.Keys))
		}
		tuples = append(tuples, "("+marks(len(e.Keys))+")")
		for _, v := range values {
			args = append(args, nullable(v))
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE (%s) IN (%s)",
		e.selectList(), e.Name(),
		strings.Join(quoteAll(e.Keys), ", "), strings.Join(tuples, ", "))

	rows, err := e.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	found := make(map[string]Row, len(keys))
	for rows.Next() {
		row, _, err := e.scanRow(rows)
		if err != nil {
			return nil, err
		}
		found[row.Key] = row
	}
	return found, rows.Err()
}

// nullable renders a key value as a driver argument, so a NULL is passed as one
// rather than as an empty string.
func nullable(v sql.NullString) interface{} {
	if !v.Valid {
		return nil
	}
	return v.String
}

// ---------------------------------------------------------------- repair

type SQLRepairer struct {
	Source *SQLEnd
	Target *SQLEnd
	// Upsert renders the statement that writes one row. It is supplied rather
	// than built here because the two flavours spell an upsert differently, and
	// the syncer already has a builder for its own dialect.
	Upsert func(schema, table string, columns []string) string
}

// Repair makes the named rows right and reports how many it fixed.
//
// It is deliberately row by row. A repair runs after something has already gone
// wrong, so being slow and obvious beats being fast and hard to reason about.
func (r *SQLRepairer) Repair(ctx context.Context, differences []Difference) (int, error) {
	fixed := 0
	for _, d := range differences {
		values, err := decodeKey(d.Key)
		if err != nil {
			return fixed, err
		}

		switch d.Kind {
		case Extra:
			err = r.deleteRow(ctx, values)
		case Missing, Differing:
			err = r.copyRow(ctx, values)
		}
		if err != nil {
			return fixed, fmt.Errorf("repair %s %s: %w", d.Kind, d.Key, err)
		}
		fixed++
	}
	return fixed, nil
}

func (e *SQLEnd) where() string {
	parts := make([]string, len(e.Keys))
	for i, k := range e.Keys {
		parts[i] = quote(k) + " = ?"
	}
	return strings.Join(parts, " AND ")
}

func (r *SQLRepairer) deleteRow(ctx context.Context, key []sql.NullString) error {
	args := make([]interface{}, len(key))
	for i, v := range key {
		args[i] = nullable(v)
	}
	_, err := r.Target.DB.ExecContext(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s", r.Target.Name(), r.Target.where()), args...)
	return err
}

func (r *SQLRepairer) copyRow(ctx context.Context, key []sql.NullString) error {
	columns := r.Source.Columns
	keyArgs := make([]interface{}, len(key))
	for i, v := range key {
		keyArgs[i] = nullable(v)
	}

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(quoteAll(columns), ", "), r.Source.Name(), r.Source.where())

	cells := make([]sql.NullString, len(columns))
	scan := make([]interface{}, len(cells))
	for i := range cells {
		scan[i] = &cells[i]
	}
	switch err := r.Source.DB.QueryRowContext(ctx, query, keyArgs...).Scan(scan...); {
	case err == sql.ErrNoRows:
		// The source has lost the row since the comparison, so the target
		// should not have it either.
		return r.deleteRow(ctx, key)
	case err != nil:
		return err
	}

	values := make([]interface{}, len(cells))
	for i, cell := range cells {
		values[i] = nullable(cell)
	}
	_, err := r.Target.DB.ExecContext(ctx,
		r.Upsert(r.Target.Schema, r.Target.Table, columns), values...)
	return err
}
