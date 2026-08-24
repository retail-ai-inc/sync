package mysql

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// The one way a schema change corrupts rows silently, and how it is caught.
//
// A ROW binlog event carries values and ordinal positions, never column names.
// canal resolves the names by asking the source for the table's current shape,
// so a stream resumed from a position that predates a schema change decodes the
// rows between the two against the wrong shape.
//
// Most of the time that is loud. A column added or dropped changes how many
// values an event holds, the statement built from it has a different number of
// placeholders than arguments, and the write fails — the task stops, which is
// the right outcome. A pure rename is harmless, because the values are still in
// the positions the driver reads them from.
//
// One shape is neither: a change that moves a column without changing how many
// there are. ALTER TABLE ... MODIFY c INT AFTER a keeps the count and changes the
// order, so every row read before that statement is seen decodes one or more
// columns out of place, writes cleanly, and is wrong. The row counts agree
// afterwards.
//
// binlog_row_metadata=FULL would put the names in the event and end the whole
// problem, but canal does not read them: RowsEvent exposes the table it fetched
// from the source and not the binlog's own description of it. Requiring the
// setting therefore buys nothing and costs binlog space, which is why the check
// that required it was removed.
//
// What is left is to notice. The reader knows where it resumed from and whether
// it has applied any rows for a table since; if the first reordering statement
// for that table arrives after some have, those rows were decoded against the
// shape the statement produced rather than the one they were written under.
// Nothing can repair them from here — the correct values are only in the source —
// so the task stops and says which table needs copying again.

// reordersColumns reports whether a statement moves a column without changing
// how many the table has.
func reordersColumns(stmt ast.StmtNode) bool {
	alter, ok := stmt.(*ast.AlterTableStmt)
	if !ok {
		return false
	}
	for _, spec := range alter.Specs {
		if spec == nil || spec.Position == nil {
			continue
		}
		if spec.Position.Tp == ast.ColumnPositionNone {
			continue
		}
		switch spec.Tp {
		case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
			// The column already exists and is being moved: the count is
			// unchanged and the order is not.
			return true
		case ast.AlterTableAddColumns:
			// Adding one FIRST or AFTER moves everything below it. The count
			// changes too, so a row read before this fails to apply rather than
			// applying wrongly — but only while the count is what disagrees. It
			// is reported for the same reason.
			return true
		}
	}
	return false
}
