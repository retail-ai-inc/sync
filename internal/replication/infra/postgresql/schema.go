package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// Preparing the target's shape, and reading the source's.
//
// These were methods on the syncer and reached straight for its *pgx.Conn,
// which is a concrete type with no interface behind it -- so none of this could
// be exercised without a PostgreSQL to hand, and none of it was. What they need
// from the source is one method.

// sourceQuerier is the source connection, narrowed to the one thing these ask
// of it.
type sourceQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// schemaWork carries what the schema and copy paths read and write.
type schemaWork struct {
	Source sourceQuerier
	Target *sql.DB
	Config config.SyncConfig
	Logger logrus.FieldLogger
}

func (s *schemaWork) prepare(ctx context.Context) error {
	for _, dbmap := range s.Config.Mappings {
		srcSchema := dbmap.SourceSchema
		if srcSchema == "" {
			srcSchema = "public"
		}
		tgtSchema := dbmap.TargetSchema
		if tgtSchema == "" {
			tgtSchema = "public"
		}
		if len(dbmap.Tables) > 0 {
			for _, tbl := range dbmap.Tables {
				exist, errCheck := s.tableExists(ctx, tgtSchema, tbl.TargetTable)
				if errCheck != nil {
					s.Logger.Warnf("[PostgreSQL] check table exist error: %v", errCheck)
					continue
				}
				if !exist {
					createSQL, seqs, errGen := s.createTableSQL(ctx, srcSchema, tbl.SourceTable, tgtSchema, tbl.TargetTable)
					if errGen != nil {
						s.Logger.Warnf("[PostgreSQL] generateCreateTableSQL fail => %v", errGen)
						continue
					}
					for _, seqSQL := range seqs {
						s.Logger.Debugf("[PostgreSQL] Creating sequence => %s", seqSQL)
						if _, errSeq := s.Target.ExecContext(ctx, seqSQL); errSeq != nil {
							s.Logger.Warnf("[PostgreSQL] Create sequence fail => %v", errSeq)
							continue
						}
					}
					s.Logger.Infof("[PostgreSQL] Creating table => %s", createSQL)
					if _, err2 := s.Target.ExecContext(ctx, createSQL); err2 != nil {
						s.Logger.Errorf("[PostgreSQL] Create table fail => %v", err2)
						continue
					}
					if err3 := s.copyIndexes(ctx, srcSchema, tbl.SourceTable, tgtSchema, tbl.TargetTable); err3 != nil {
						s.Logger.Warnf("[PostgreSQL] copyIndexes fail => %v", err3)
					} else {
						s.Logger.Infof("[PostgreSQL] Created table and indexes for %s.%s", tgtSchema, tbl.TargetTable)
					}
				}
			}
		} else {
			s.Logger.Warn("[PostgreSQL] Table mappings are empty, skipping processing")
			continue
		}
	}
	return nil
}

func (s *schemaWork) createTableSQL(
	ctx context.Context,
	srcSchema, srcTable, tgtSchema, tgtTable string,
) (createTableSQL string, sequences []string, err error) {

	query := `
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default,
    character_maximum_length,
    numeric_precision,
    numeric_scale
FROM information_schema.columns
WHERE table_schema=$1
  AND table_name=$2
ORDER BY ordinal_position
`
	rows, errQ := s.Source.Query(ctx, query, srcSchema, srcTable)
	if errQ != nil {
		return "", nil, fmt.Errorf("query source table columns fail: %w", errQ)
	}
	defer rows.Close()

	var columns []string
	sequencesMap := make(map[string]bool)

	for rows.Next() {
		var (
			columnName    string
			dataType      string
			isNullable    string
			columnDefault sql.NullString
			charMaxLen    sql.NullInt64
			numPrecision  sql.NullInt64
			numScale      sql.NullInt64
		)
		if errScan := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault,
			&charMaxLen, &numPrecision, &numScale); errScan != nil {
			return "", nil, fmt.Errorf("scan column info fail: %w", errScan)
		}

		colDef := fmt.Sprintf("%s %s", columnName, dataType)

		if (dataType == "character varying" || dataType == "varchar" ||
			dataType == "character" || dataType == "char") && charMaxLen.Valid {
			colDef += fmt.Sprintf("(%d)", charMaxLen.Int64)
		} else if (dataType == "numeric" || dataType == "decimal") && numPrecision.Valid {
			colDef += fmt.Sprintf("(%d", numPrecision.Int64)
			if numScale.Valid {
				colDef += fmt.Sprintf(",%d", numScale.Int64)
			}
			colDef += ")"
		}

		if columnDefault.Valid {
			colDef += fmt.Sprintf(" DEFAULT %s", columnDefault.String)
			if strings.Contains(columnDefault.String, "nextval(") {
				seqName := extractSequenceName(columnDefault.String)
				if seqName != "" {
					sequencesMap[seqName] = true
				}
			}
		}
		if isNullable == "NO" {
			colDef += " NOT NULL"
		}
		columns = append(columns, colDef)
	}
	if errClose := rows.Err(); errClose != nil {
		return "", nil, fmt.Errorf("iterate columns fail: %w", errClose)
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
  %s
);`, tgtSchema, tgtTable, strings.Join(columns, ",\n  "))

	var seqSlice []string
	for seqName := range sequencesMap {
		seq := fmt.Sprintf(`CREATE SEQUENCE IF NOT EXISTS "%s"`, seqName)
		seqSlice = append(seqSlice, seq)
	}
	return createSQL, seqSlice, nil
}

func extractSequenceName(defaultVal string) string {
	reg := regexp.MustCompile(`nextval\('([^']+)'::regclass\)`)
	matches := reg.FindStringSubmatch(defaultVal)
	if len(matches) == 2 {
		return matches[1]
	}
	return ""
}

func (s *schemaWork) tableExists(ctx context.Context, schemaName, tableName string) (bool, error) {
	query := `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2`
	var cnt int
	err := s.Target.QueryRowContext(ctx, query, schemaName, tableName).Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt > 0, nil
}

func (s *schemaWork) copyIndexes(ctx context.Context, srcSchema, srcTable, tgtSchema, tgtTable string) error {
	sqlIdx := `
	SELECT indexname, indexdef
	FROM pg_indexes
	WHERE schemaname=$1 
	  AND tablename=$2
	`
	rows, err := s.Source.Query(ctx, sqlIdx, srcSchema, srcTable)
	if err != nil {
		return fmt.Errorf("query source indexes fail: %w", err)
	}
	defer rows.Close()

	// First check existing indexes in target
	sqlExistingIdx := `
	SELECT indexname 
	FROM pg_indexes
	WHERE schemaname=$1 
	  AND tablename=$2
	`
	existingRows, err := s.Target.QueryContext(ctx, sqlExistingIdx, tgtSchema, tgtTable)
	if err != nil {
		s.Logger.Warnf("[PostgreSQL] Failed to query existing indexes: %v", err)
		// Continue anyway to attempt creation
	}

	existingIndexes := make(map[string]bool)
	if existingRows != nil {
		for existingRows.Next() {
			var idxName string
			if err := existingRows.Scan(&idxName); err == nil {
				existingIndexes[idxName] = true
			}
		}
		existingRows.Close()
	}

	indexesCreated := 0
	indexesSkipped := 0

	for rows.Next() {
		var idxName, idxDef string
		if err2 := rows.Scan(&idxName, &idxDef); err2 != nil {
			return fmt.Errorf("scan idx fail: %w", err2)
		}

		newIdxDef := idxDef
		oldName := fmt.Sprintf("%s.%s", srcSchema, srcTable)
		newName := fmt.Sprintf("%s.%s", tgtSchema, tgtTable)
		newIdxDef = strings.ReplaceAll(newIdxDef, oldName, newName)
		newIdxName := fmt.Sprintf("%s_%s", tgtTable, idxName)
		newIdxDef = strings.Replace(newIdxDef, idxName, newIdxName, 1)

		if existingIndexes[newIdxName] {
			s.Logger.Debugf("[PostgreSQL] Index %s already exists, skipping", newIdxName)
			indexesSkipped++
			continue
		}

		s.Logger.Debugf("[PostgreSQL] Creating index => %s", newIdxDef)
		_, errExec := s.Target.ExecContext(ctx, newIdxDef)
		if errExec != nil {
			if strings.Contains(errExec.Error(), "already exists") {
				s.Logger.Debugf("[PostgreSQL] Index %s already exists", newIdxName)
				indexesSkipped++
			} else {
				s.Logger.Warnf("[PostgreSQL] Create index %s fail => %v", newIdxName, errExec)
			}
		} else {
			s.Logger.Infof("[PostgreSQL] Successfully created index %s", newIdxName)
			indexesCreated++
		}
	}

	s.Logger.Infof("[PostgreSQL] Index creation summary for %s.%s: created=%d, skipped=%d",
		tgtSchema, tgtTable, indexesCreated, indexesSkipped)

	return rows.Err()
}

func (s *schemaWork) primaryKey(schema, tableName string) ([]string, error) {
	// Quoted, or a mixed-case name folds to lower case and names no table.
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = (quote_ident($1) || '.' || quote_ident($2))::regclass
		AND i.indisprimary
	`

	var primaryKeys []string
	rows, err := s.Source.Query(context.Background(), query, schema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		primaryKeys = append(primaryKeys, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return primaryKeys, nil
}
