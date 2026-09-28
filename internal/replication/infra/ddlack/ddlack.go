// Package ddlack records an operator's decision to let one refused schema
// change past.
//
// A statement that would destroy replicated data stops the task, so that the
// change is made on the target deliberately rather than by a stream nobody was
// watching. What was missing is the other half: there was no way to say "I have
// dealt with this one, carry on". A task halted on a DROP COLUMN stayed halted,
// and the only ways out were to copy the whole task again or to move the stored
// position past the statement by hand -- which skips every transaction in
// between, on every table.
//
// An acknowledgement names one statement of one task, is used once, and means
// skip it, never apply it: whatever the target needed, the operator has already
// done.
package ddlack

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

type Acknowledgement struct {
	ID        int64
	TaskID    int
	Statement string
	Digest    string
	CreatedBy string
	CreatedAt time.Time
	// UsedAt is when replication passed the statement over. Zero while it is
	// still outstanding.
	UsedAt time.Time
}

func (a Acknowledgement) Used() bool { return !a.UsedAt.IsZero() }

// Digest identifies a statement independently of how it was copied out of a
// log line: the leading and trailing space and the line breaks a terminal adds
// are not part of what was refused.
func Digest(statement string) string {
	sum := sha256.Sum256([]byte(normalise(statement)))
	return hex.EncodeToString(sum[:])
}

func normalise(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

// Add records that a statement may be passed over the next time the task
// reaches it. An identical outstanding acknowledgement is returned as it
// stands rather than duplicated, so asking twice does not buy two skips.
func Add(ctx context.Context, taskID int, statement, by string) (Acknowledgement, error) {
	if strings.TrimSpace(statement) == "" {
		return Acknowledgement{}, fmt.Errorf("an acknowledgement has to name the statement it allows")
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return Acknowledgement{}, err
	}
	defer db.Close()

	digest := Digest(statement)
	if existing, found, err := outstanding(ctx, db, taskID, digest); err != nil {
		return Acknowledgement{}, err
	} else if found {
		return existing, nil
	}

	result, err := db.ExecContext(ctx,
		`INSERT INTO ddl_acknowledgements (task_id, digest, statement, created_by)
		 VALUES (?, ?, ?, ?)`, taskID, digest, statement, by)
	if err != nil {
		return Acknowledgement{}, fmt.Errorf("record the acknowledgement: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Acknowledgement{}, fmt.Errorf("record the acknowledgement: %w", err)
	}
	return Acknowledgement{ID: id, TaskID: taskID, Statement: statement,
		Digest: digest, CreatedBy: by, CreatedAt: time.Now()}, nil
}

// List reports a task's acknowledgements, outstanding ones first, so the
// question "what is this task allowed to skip" has an answer that does not
// depend on reading the log.
func List(ctx context.Context, taskID int) ([]Acknowledgement, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx,
		`SELECT id, task_id, digest, statement, COALESCE(created_by, ''), created_at, used_at
		   FROM ddl_acknowledgements WHERE task_id = ?
		  ORDER BY used_at IS NOT NULL, id DESC`, taskID)
	if err != nil {
		return nil, fmt.Errorf("read the acknowledgements: %w", err)
	}
	defer rows.Close()

	var all []Acknowledgement
	for rows.Next() {
		a, err := scan(rows)
		if err != nil {
			return nil, err
		}
		all = append(all, a)
	}
	return all, rows.Err()
}

// Revoke removes one, for the case where it was recorded against the wrong
// task or the operator changed their mind before the stream reached it.
func Revoke(ctx context.Context, taskID int, id int64) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return err
	}
	defer db.Close()

	result, err := db.ExecContext(ctx,
		`DELETE FROM ddl_acknowledgements WHERE id = ? AND task_id = ? AND used_at IS NULL`,
		id, taskID)
	if err != nil {
		return fmt.Errorf("remove the acknowledgement: %w", err)
	}
	if affected, err := result.RowsAffected(); err == nil && affected == 0 {
		return fmt.Errorf("task %d has no outstanding acknowledgement %d", taskID, id)
	}
	return nil
}

// Consume reports whether this statement is allowed past, and spends the
// acknowledgement that allows it.
//
// Spending it before the statement is passed over rather than after is
// deliberate. A process that dies in between leaves the task blocked on the
// same statement with the acknowledgement used up, which an operator sees and
// can record again; the other order leaves a standing permission to skip a
// destructive change, which nobody would see at all.
func Consume(ctx context.Context, taskID int, statement string) (bool, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return false, err
	}
	defer db.Close()

	result, err := db.ExecContext(ctx,
		`UPDATE ddl_acknowledgements SET used_at = CURRENT_TIMESTAMP
		  WHERE id = (SELECT id FROM ddl_acknowledgements
		               WHERE task_id = ? AND digest = ? AND used_at IS NULL
		               ORDER BY id LIMIT 1)`, taskID, Digest(statement))
	if err != nil {
		return false, fmt.Errorf("spend the acknowledgement: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("spend the acknowledgement: %w", err)
	}
	return affected == 1, nil
}

func outstanding(ctx context.Context, db *sql.DB, taskID int, digest string) (Acknowledgement, bool, error) {
	row := db.QueryRowContext(ctx,
		`SELECT id, task_id, digest, statement, COALESCE(created_by, ''), created_at, used_at
		   FROM ddl_acknowledgements
		  WHERE task_id = ? AND digest = ? AND used_at IS NULL ORDER BY id LIMIT 1`,
		taskID, digest)
	a, err := scan(row)
	if err == sql.ErrNoRows {
		return Acknowledgement{}, false, nil
	}
	if err != nil {
		return Acknowledgement{}, false, err
	}
	return a, true, nil
}

type scanner interface {
	Scan(dest ...interface{}) error
}

func scan(row scanner) (Acknowledgement, error) {
	var (
		a      Acknowledgement
		used   sql.NullTime
		madeAt sql.NullTime
	)
	if err := row.Scan(&a.ID, &a.TaskID, &a.Digest, &a.Statement, &a.CreatedBy,
		&madeAt, &used); err != nil {
		if err == sql.ErrNoRows {
			return Acknowledgement{}, err
		}
		return Acknowledgement{}, fmt.Errorf("read an acknowledgement: %w", err)
	}
	a.CreatedAt = madeAt.Time
	a.UsedAt = used.Time
	return a, nil
}
