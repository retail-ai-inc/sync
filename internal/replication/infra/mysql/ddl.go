package mysql

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"

	// The parser builds literal values through a driver that has to be
	// registered. Without one every literal restores as nothing at all, so a
	// column declared DEFAULT 'new' was rewritten for the target as "DEFAULT"
	// with no value — a syntax error that stopped replication on the first
	// schema change and could not be got past without editing the source.
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"

	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

type ddlAction int

const (
	// ddlSkip: the statement touches nothing this task replicates.
	ddlSkip ddlAction = iota
	// ddlApply: the statement was rewritten for the target and should run.
	ddlApply
	// ddlBlock: the statement would destroy replicated data on the target.
	ddlBlock
	// ddlNotSchema: the statement is not a schema change at all. It is dropped
	// like a skip and, unlike a skip, is not counted as one: the binlog carries
	// a BEGIN and a COMMIT per transaction as query events, and counting those
	// as schema changes deliberately not carried through reported tens of them
	// a second on a busy source. A refusal metric that says that is a metric
	// nobody can read.
	ddlNotSchema
)

type ddlDecision struct {
	action ddlAction
	// query is the statement rewritten with the target's names, set when the
	// action is ddlApply.
	query string
	// reason explains a skip or a block, for the log and for the error an
	// operator will read.
	reason string
	// kind is the same thing in one of a fixed set of words, for a metric
	// label. reason carries a database name, and a label whose values are
	// unbounded is a series count nobody planned for.
	kind string
}

// notASchemaChange reports whether a statement changes no schema at all.
//
// The binlog carries these as query events beside the real DDL: a BEGIN and a
// COMMIT for every transaction, and whatever the session set along the way.
// They parse, they name no table, and they used to be counted as schema changes
// this task refused to carry -- twenty-five a second on a shared server, which
// is what an operator would have read as the two schemas coming apart.
func notASchemaChange(stmt ast.StmtNode) bool {
	switch stmt.(type) {
	case *ast.BeginStmt, *ast.CommitStmt, *ast.RollbackStmt,
		*ast.SavepointStmt, *ast.ReleaseSavepointStmt,
		*ast.SetStmt, *ast.SetSessionStatesStmt, *ast.UseStmt:
		return true
	}
	return false
}

// tableRefs reports every table name a DDL statement names, as pointers into
// the tree so they can be rewritten in place.
func tableRefs(stmt ast.StmtNode) []*ast.TableName {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		refs := []*ast.TableName{s.Table}
		if s.ReferTable != nil {
			refs = append(refs, s.ReferTable)
		}
		return refs
	case *ast.AlterTableStmt:
		return []*ast.TableName{s.Table}
	case *ast.DropTableStmt:
		return s.Tables
	case *ast.TruncateTableStmt:
		return []*ast.TableName{s.Table}
	case *ast.RenameTableStmt:
		var refs []*ast.TableName
		for _, pair := range s.TableToTables {
			refs = append(refs, pair.OldTable, pair.NewTable)
		}
		return refs
	case *ast.CreateIndexStmt:
		return []*ast.TableName{s.Table}
	case *ast.DropIndexStmt:
		return []*ast.TableName{s.Table}
	case *ast.CreateViewStmt:
		return []*ast.TableName{s.ViewName}
	}
	return nil
}

// isDestructive reports whether a statement removes data or the structure that
// holds it. Such a statement is never applied to the target automatically: a
// mistaken DROP at the source would otherwise take the disaster-recovery copy
// with it, and that copy is the only thing left to recover from.
func isDestructive(stmt ast.StmtNode) (bool, string) {
	switch s := stmt.(type) {
	case *ast.DropTableStmt:
		return true, "drops a replicated table"
	case *ast.TruncateTableStmt:
		return true, "truncates a replicated table"
	case *ast.DropDatabaseStmt:
		return true, "drops a database"
	case *ast.RenameTableStmt:
		return true, "renames a replicated table, which the task's mappings still address by its old name"
	case *ast.AlterTableStmt:
		for _, spec := range s.Specs {
			switch spec.Tp {
			case ast.AlterTableDropColumn:
				return true, "drops a column"
			case ast.AlterTableDropPrimaryKey:
				return true, "drops the primary key the replicated updates and deletes are addressed by"
			case ast.AlterTableDropPartition:
				return true, "drops a partition"
			}
		}
	}
	return false, ""
}

// replicatesSchema reports whether a database a DDL statement names is the one
// this task reads. Matching on the table name alone, with the database
// discarded and replaced by the target's, meant an ALTER TABLE against some
// other database on the same server that happened to hold a table of the same
// name was rewritten and applied to the disaster-recovery copy.
func (h *MyEventHandler) replicatesSchema(schema string) bool {
	if h.sourceDatabase == "" || schema == "" {
		// There is nothing to compare against: either the handler was built
		// without a source database, or the statement is unqualified and its
		// event carried no default schema. Falling back to the name-only match
		// keeps propagating what was propagated before; refusing here would
		// silently stop replicating schema changes altogether, which is the
		// failure this whole path exists to prevent.
		return true
	}
	return strings.EqualFold(schema, h.sourceDatabase)
}

// targetTableFor reports the target name a source table is replicated to, and
// whether the task replicates it at all. The lookup matches on the table name
// alone; the database the name was qualified with is checked separately, by
// replicatesSchema.
func (h *MyEventHandler) targetTableFor(source string) (string, bool) {
	for _, mapping := range h.mappings {
		for _, table := range mapping.Tables {
			if strings.EqualFold(table.SourceTable, source) {
				return table.TargetTable, true
			}
		}
	}
	if h.discovering && !discovery.IsInternal(source) {
		// The task lists no tables, so every table is replicated under its own
		// name — schema changes to it included.
		return source, true
	}
	return "", false
}

// planDDL decides what to do with each statement of a query event: one naming
// no replicated table is skipped, one that would destroy replicated data is
// blocked so an operator decides, and everything else is rewritten with the
// target's names and applied — so a column added at the source exists before
// the first row that uses it arrives.
func (h *MyEventHandler) planDDL(defaultSchema, query string) ([]ddlDecision, error) {
	stmts, _, err := parser.New().Parse(query, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", query, err)
	}

	targetDBName := dsn.GetDatabaseName("mysql", h.TargetConnection)

	decisions := make([]ddlDecision, 0, len(stmts))
	for _, stmt := range stmts {
		if notASchemaChange(stmt) {
			decisions = append(decisions, ddlDecision{
				action: ddlNotSchema,
				reason: "is not a schema change",
			})
			continue
		}

		refs := tableRefs(stmt)
		if len(refs) == 0 {
			decisions = append(decisions, ddlDecision{
				action: ddlSkip,
				reason: "names no table",
				kind:   "no_table",
			})
			continue
		}

		replicated := false
		elsewhere := ""
		for _, ref := range refs {
			// An unqualified name means the database the event was issued
			// against, which is what the source server itself would resolve it
			// to.
			schema := ref.Schema.O
			if schema == "" {
				schema = defaultSchema
			}
			if !h.replicatesSchema(schema) {
				elsewhere = schema
				continue
			}
			if target, ok := h.targetTableFor(ref.Name.O); ok {
				replicated = true
				ref.Schema = model.NewCIStr(targetDBName)
				ref.Name = model.NewCIStr(target)
			}
		}
		if !replicated {
			reason, kind := "names no replicated table", "not_replicated"
			if elsewhere != "" {
				reason = fmt.Sprintf("names a table in %s, which this task does not read", elsewhere)
				kind = "another_database"
			}
			decisions = append(decisions, ddlDecision{action: ddlSkip, reason: reason, kind: kind})
			continue
		}

		if destructive, why := isDestructive(stmt); destructive {
			decisions = append(decisions, ddlDecision{action: ddlBlock, reason: why})
			continue
		}

		var sb strings.Builder
		// RestoreStringWithoutCharset keeps the introducer off the literal:
		// the driver renders _UTF8MB4'new', which MySQL accepts but which
		// makes every rewritten statement differ from the one it came from.
		ctx := format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb)
		if err := stmt.Restore(ctx); err != nil {
			return nil, fmt.Errorf("rewrite %q for the target: %w", query, err)
		}
		decisions = append(decisions, ddlDecision{action: ddlApply, query: sb.String()})
	}
	return decisions, nil
}
