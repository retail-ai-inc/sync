package mysql

import (
	"context"
	"fmt"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"strings"
	"sync/atomic"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
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
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
)

// ddlAction is what the handler decided to do with one parsed statement.
type ddlAction int

const (
	// ddlSkip: the statement touches nothing this task replicates.
	ddlSkip ddlAction = iota
	// ddlApply: the statement was rewritten for the target and should run.
	ddlApply
	// ddlBlock: the statement would destroy replicated data on the target.
	ddlBlock
)

// ddlDecision is the outcome for one statement of a query event.
type ddlDecision struct {
	action ddlAction
	// query is the statement rewritten with the target's names, set when the
	// action is ddlApply.
	query string
	// reason explains a skip or a block, for the log and for the error an
	// operator will read.
	reason string
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
// this task reads.
//
// A table reference used to be matched on its name alone, with the database it
// named discarded and overwritten with the target's. A source server hosting
// more than one database was all it took: an ALTER TABLE run against some other
// database that happened to hold a table of the same name was rewritten and
// applied to the replication target, changing the schema of the disaster-
// recovery copy from a statement that had nothing to do with it. The row path
// never had this problem — canal filters row events on database and table both.
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

// planDDL decides what to do with each statement of a query event.
//
// A statement that names no replicated table is skipped: the source runs plenty
// of schema changes this task has no business copying. A statement that would
// destroy replicated data is blocked, which stops replication so an operator
// decides. Everything else is rewritten with the target's database and table
// names and applied, so a column added at the source exists on the target
// before the first row that uses it arrives.
func (h *MyEventHandler) planDDL(defaultSchema, query string) ([]ddlDecision, error) {
	stmts, _, err := parser.New().Parse(query, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse %q: %w", query, err)
	}

	targetDBName := dsn.GetDatabaseName("mysql", h.TargetConnection)

	decisions := make([]ddlDecision, 0, len(stmts))
	for _, stmt := range stmts {
		refs := tableRefs(stmt)
		if len(refs) == 0 {
			decisions = append(decisions, ddlDecision{
				action: ddlSkip,
				reason: "names no table",
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
			reason := "names no replicated table"
			if elsewhere != "" {
				reason = fmt.Sprintf("names a table in %s, which this task does not read", elsewhere)
			}
			decisions = append(decisions, ddlDecision{action: ddlSkip, reason: reason})
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

// OnDDL propagates a schema change to the target.
//
// Without it a column added at the source never reached the target, and every
// row event that used the new column failed to apply from then on — which,
// before the offset was made to wait on successful writes, meant the task went
// on silently discarding rows.
func (h *MyEventHandler) OnDDL(_ *replication.EventHeader, _ mysql.Position, e *replication.QueryEvent) error {
	// A schema change closes whatever transaction was open.
	if err := h.flush(); err != nil {
		return err
	}
	if e == nil {
		return nil
	}
	query := string(e.Query)

	decisions, err := h.planDDL(string(e.Schema), query)
	if err != nil {
		// The statement cannot be read, which is also true of the BEGIN and
		// COMMIT markers that arrive as query events. Carrying on is right for
		// those; for anything else the schemas may drift, so say so loudly.
		h.logger.Warnf("[MySQL][DDL] Not propagating a statement that could not be "+
			"parsed: %v", err)
		return nil
	}

	for _, decision := range decisions {
		switch decision.action {
		case ddlSkip:
			metrics.CountSchemaRefused(h.labels, "skipped")
			h.logger.Debugf("[MySQL][DDL] Skipping %q: %s", query, decision.reason)

		case ddlBlock:
			// A refusal is a decision, and a decision nobody can see is a
			// decision nobody can audit. This is the path a TRUNCATE or a DROP
			// takes, so it is the one that has to be counted.
			metrics.CountSchemaRefused(h.labels, "blocked")
			atomic.StoreInt32(&h.lastExecError, 1)
			return fmt.Errorf("refusing to replicate %q: it %s. Replication has "+
				"stopped so the change can be made on the target deliberately; "+
				"clear the stored position to resume", query, decision.reason)

		case ddlApply:
			metrics.CountSchemaChange(h.labels, 1)
			metrics.SetSchemaChangeAge(h.labels, 0)
			h.logger.Infof("[MySQL][DDL] Applying %q", decision.query)
			if err := h.execDDL(decision.query); err != nil {
				atomic.StoreInt32(&h.lastExecError, 1)
				return fmt.Errorf("apply DDL %q: %w", decision.query, err)
			}
		}
	}
	return nil
}

// execDDL runs one rewritten statement on the target.
func (h *MyEventHandler) execDDL(query string) error {
	h.mu.Lock()
	db := h.targetDB
	h.mu.Unlock()

	if db == nil {
		return fmt.Errorf("no target connection")
	}
	return resilience.RetryDBOperation(context.Background(), h.logger, "DDL",
		func() error {
			_, err := db.Exec(query)
			return err
		})
}

// OnTableChanged records that canal has seen a table's definition change. The
// statement itself arrives separately at OnDDL; this is the boundary, so
// anything still buffered belongs to the old definition and is applied first.
func (h *MyEventHandler) OnTableChanged(_ *replication.EventHeader, schema, table string) error {
	h.logger.Infof("[MySQL][DDL] Source table %s.%s changed", schema, table)
	return h.flush()
}
