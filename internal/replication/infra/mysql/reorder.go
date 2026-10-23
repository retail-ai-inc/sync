package mysql

import (
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// The one way a schema change corrupts rows silently, and how it is caught. A
// ROW binlog event carries values and ordinal positions, never column names.
// canal resolves the names by asking the source for the table's current shape,
// so a stream resumed from a position that predates a schema change decodes
// the rows between the two against the wrong shape.

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
