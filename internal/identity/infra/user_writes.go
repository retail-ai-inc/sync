package infra

import (
	"database/sql"
	"strings"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// ErrNoSuchUser is returned when a write names a userId the table does not
// hold. The handlers answer that with a 200 and success:false, not a 404.
var ErrNoSuchUser = &Fault{Stage: StageCheck, Err: sql.ErrNoRows}

// ErrNothingToUpdate is returned when a request changes neither the access
// level nor the status.
var ErrNothingToUpdate = &Fault{Stage: StageUpdate, Err: sql.ErrNoRows}

// UpdateUserAccessAndStatus applies an access level and a status to a user in
// one transaction, and returns the user's profile as it now stands. Empty
// values leave the corresponding column alone.
func UpdateUserAccessAndStatus(userID, access, status string) (map[string]interface{}, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return nil, faultAt(StageConnect, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return nil, faultAt(StageBegin, err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	var dbID int
	var username, name, currentAccess string
	var statusCol, avatar, userIDCol, email sql.NullString

	err = tx.QueryRow("SELECT id, username, name, avatar, userId, email, access, status FROM users WHERE userId = ?", userID).Scan(
		&dbID, &username, &name, &avatar, &userIDCol, &email, &currentAccess, &statusCol)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, ErrNoSuchUser
		}
		return nil, faultAt(StageQuery, err)
	}

	userData := map[string]interface{}{
		"id":       dbID,
		"username": username,
		"name":     name,
		"access":   currentAccess,
		"avatar":   nullOr(avatar, ""),
		"userId":   nullOr(userIDCol, ""),
		"email":    nullOr(email, ""),
		"status":   nullOr(statusCol, "active"),
	}

	var updateFields []string
	var updateParams []interface{}
	if access != "" {
		updateFields = append(updateFields, "access = ?")
		updateParams = append(updateParams, access)
	}
	if status != "" {
		updateFields = append(updateFields, "status = ?")
		updateParams = append(updateParams, status)
	}
	if len(updateFields) == 0 {
		return nil, ErrNothingToUpdate
	}

	updateSQL := "UPDATE users SET " + strings.Join(updateFields, ", ") + " WHERE userId = ?"
	updateParams = append(updateParams, userID)

	if _, err := tx.Exec(updateSQL, updateParams...); err != nil {
		return nil, faultAt(StageUpdate, err)
	}
	if err = tx.Commit(); err != nil {
		return nil, faultAt(StageCommit, err)
	}
	tx = nil

	if access != "" {
		userData["access"] = access
	}
	if status != "" {
		userData["status"] = status
	}
	return userData, nil
}

// DeleteUser removes a user by userId in one transaction.
func DeleteUser(userID string) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return faultAt(StageConnect, err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		return faultAt(StageBegin, err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	var exists bool
	if err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE userId = ?)", userID).Scan(&exists); err != nil {
		return faultAt(StageCheck, err)
	}
	if !exists {
		return ErrNoSuchUser
	}

	if _, err := tx.Exec("DELETE FROM users WHERE userId = ?", userID); err != nil {
		return faultAt(StageDelete, err)
	}
	if err = tx.Commit(); err != nil {
		return faultAt(StageCommit, err)
	}
	tx = nil
	return nil
}

// nullOr returns the string a nullable column holds, or fallback when it is
// NULL. The handlers spelled this out once per column.
func nullOr(s sql.NullString, fallback string) string {
	if s.Valid {
		return s.String
	}
	return fallback
}
