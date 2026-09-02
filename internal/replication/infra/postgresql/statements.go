package postgresql

import (
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/lib/pq"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// The statements this file builds used to be assembled by pasting the row's
// values into SQL text, with a doubled single quote as the only escaping. Every
// value in a replicated row came from the source database, so the correctness of
// the target depended on a setting of the source (standard_conforming_strings)
// that nothing here checks. The values are now bound as parameters, which also
// makes NULL and the empty string different things again.

type column struct {
	Name string
	// Value is nil for SQL NULL.
	Value interface{}
}

// readTuple reads the columns a tuple carries, in the order the relation
// declares them. A column pgoutput marks unchanged — 'u', a TOASTed value the
// server deliberately did not resend — is left out rather than read as NULL.
func readTuple(rel *pglogrepl.RelationMessageV2, cols []*pglogrepl.TupleDataColumn) ([]column, error) {
	var present []column

	for i, col := range cols {
		if i >= len(rel.Columns) {
			// The relation message is older than the tuple: the source has added
			// a column and has not re-announced the table. Guessing which value
			// belongs to which column would write the row wrong.
			return nil, fmt.Errorf("%s.%s: the row has %d columns but the last "+
				"relation message described %d; waiting for the source to re-announce it",
				rel.Namespace, rel.RelationName, len(cols), len(rel.Columns))
		}
		name := rel.Columns[i].Name

		switch col.DataType {
		case 'n':
			present = append(present, column{Name: name})
		case 't':
			present = append(present, column{Name: name, Value: string(col.Data)})
		case 'u':
			// Unchanged and not resent. Omitted on purpose.
		case 'b':
			return nil, fmt.Errorf("%s.%s: column %s arrived in binary format, "+
				"which this replication stream cannot write", rel.Namespace, rel.RelationName, name)
		default:
			return nil, fmt.Errorf("%s.%s: column %s arrived as an unknown format %q",
				rel.Namespace, rel.RelationName, name, col.DataType)
		}
	}
	return present, nil
}

// mask applies the task's field security to a column's value.
//
// It used to be applied on the insert path alone, so a row arrived masked and
// then every update overwrote it with the value from the source: a table
// configured to hide an email address held the address in plain text as soon as
// the row changed once.
func mask(c column, table security.TableSecurity) column {
	if !table.SecurityEnabled || c.Value == nil {
		return c
	}
	text, ok := c.Value.(string)
	if !ok {
		return c
	}
	if processed, ok := security.ProcessValue(text, c.Name, table).(string); ok {
		c.Value = processed
	}
	return c
}

func qualified(rel *pglogrepl.RelationMessageV2) string {
	return pq.QuoteIdentifier(rel.Namespace) + "." + pq.QuoteIdentifier(rel.RelationName)
}

func buildInsert(rel *pglogrepl.RelationMessageV2, tuple *pglogrepl.TupleData,
	table security.TableSecurity) (string, []interface{}, error) {

	cols, err := readTuple(rel, tuple.Columns)
	if err != nil {
		return "", nil, err
	}
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("%s: the row carries no columns", qualified(rel))
	}

	names := make([]string, 0, len(cols))
	holders := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))
	for _, c := range cols {
		c = mask(c, table)
		names = append(names, pq.QuoteIdentifier(c.Name))
		args = append(args, c.Value)
		holders = append(holders, fmt.Sprintf("$%d", len(args)))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		qualified(rel), strings.Join(names, ", "), strings.Join(holders, ", ")), args, nil
}

// buildUpdate renders an UPDATE for one row.
//
// The WHERE clause addresses the row by its key alone. It used to be built from
// every column of the old tuple, so a target row that differed anywhere — drifted
// once, missed an earlier update, or had a field masked on the way in — matched
// nothing and the update was silently a no-op.
func buildUpdate(rel *pglogrepl.RelationMessageV2, oldTuple, newTuple *pglogrepl.TupleData,
	keys []string, table security.TableSecurity) (string, []interface{}, error) {

	setCols, err := readTuple(rel, newTuple.Columns)
	if err != nil {
		return "", nil, err
	}
	if len(setCols) == 0 {
		return "", nil, fmt.Errorf("%s: the update carries no columns", qualified(rel))
	}

	// With REPLICA IDENTITY DEFAULT the old tuple is only sent when the key
	// changed; when it did not, the key in the new tuple is the same key.
	identity := newTuple
	if oldTuple != nil {
		identity = oldTuple
	}
	whereCols, err := readTuple(rel, identity.Columns)
	if err != nil {
		return "", nil, err
	}
	whereCols = keyed(whereCols, keys)
	if len(whereCols) == 0 {
		return "", nil, fmt.Errorf("%s: no key column to address the row by", qualified(rel))
	}

	var args []interface{}
	assignments := make([]string, 0, len(setCols))
	for _, c := range setCols {
		c = mask(c, table)
		args = append(args, c.Value)
		assignments = append(assignments, fmt.Sprintf("%s = $%d", pq.QuoteIdentifier(c.Name), len(args)))
	}

	clauses, args := whereClause(whereCols, args)

	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		qualified(rel), strings.Join(assignments, ", "), strings.Join(clauses, " AND ")), args, nil
}

// buildDelete renders a DELETE for one row.
func buildDelete(rel *pglogrepl.RelationMessageV2, oldTuple *pglogrepl.TupleData,
	keys []string) (string, []interface{}, error) {

	cols, err := readTuple(rel, oldTuple.Columns)
	if err != nil {
		return "", nil, err
	}
	cols = keyed(cols, keys)
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("%s: no column to address the row by", qualified(rel))
	}

	clauses, args := whereClause(cols, nil)

	return fmt.Sprintf("DELETE FROM %s WHERE %s",
		qualified(rel), strings.Join(clauses, " AND ")), args, nil
}

// keyed narrows a row to the columns named as its key. An empty key list leaves
// the row as it is, which is the only thing left to match on when the table has
// no primary key.
func keyed(cols []column, keys []string) []column {
	if len(keys) == 0 {
		return cols
	}

	wanted := make(map[string]bool, len(keys))
	for _, key := range keys {
		wanted[key] = true
	}

	var narrowed []column
	for _, c := range cols {
		if wanted[c.Name] {
			narrowed = append(narrowed, c)
		}
	}
	return narrowed
}

// whereClause renders the comparisons that address a row, appending the bound
// values to args.
func whereClause(cols []column, args []interface{}) ([]string, []interface{}) {
	clauses := make([]string, 0, len(cols))
	for _, c := range cols {
		if c.Value == nil {
			clauses = append(clauses, pq.QuoteIdentifier(c.Name)+" IS NULL")
			continue
		}
		args = append(args, c.Value)
		clauses = append(clauses, fmt.Sprintf("%s = $%d", pq.QuoteIdentifier(c.Name), len(args)))
	}
	return clauses, args
}
