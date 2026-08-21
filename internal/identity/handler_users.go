package identity

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
)

// GetUsersHandler GET /api/users
func GetUsersHandler(w http.ResponseWriter, r *http.Request) {
	// Get pagination parameters
	currentStr := r.URL.Query().Get("current")
	pageSizeStr := r.URL.Query().Get("pageSize")

	current := 1
	pageSize := 10

	if currentStr != "" {
		if val, err := strconv.Atoi(currentStr); err == nil && val > 0 {
			current = val
		}
	}

	if pageSizeStr != "" {
		if val, err := strconv.Atoi(pageSizeStr); err == nil && val > 0 {
			pageSize = val
		}
	}

	// Get all users
	users, err := GetAllUsers()
	if err != nil {
		resp := map[string]interface{}{
			"success": false,
			"data":    []interface{}{},
			"message": "Failed to get user list",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Calculate total and paginate data
	total := len(users)
	startIndex := (current - 1) * pageSize
	endIndex := startIndex + pageSize

	if startIndex >= total {
		startIndex = 0
		endIndex = 0
	}

	if endIndex > total {
		endIndex = total
	}

	// Paginate data
	var pagedUsers []map[string]interface{}
	if startIndex < endIndex {
		pagedUsers = users[startIndex:endIndex]
	} else {
		pagedUsers = []map[string]interface{}{}
	}

	// Process return data format
	var formattedUsers []map[string]interface{}
	for _, user := range pagedUsers {
		// Remove sensitive information
		delete(user, "password")
		delete(user, "id")
		delete(user, "username")

		// Ensure data is returned in the required format
		formattedUsers = append(formattedUsers, map[string]interface{}{
			"userId": user["userId"],
			"name":   user["name"],
			"email":  user["email"],
			"access": user["access"],
			"avatar": user["avatar"],
			"status": user["status"],
		})
	}

	// Build response
	resp := map[string]interface{}{
		"success": true,
		"data":    formattedUsers,
		"total":   total,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// UpdateUserAccessHandler PUT /api/users/access
func UpdateUserAccessHandler(w http.ResponseWriter, r *http.Request) {

	// Parse request body
	var req struct {
		Access string `json:"access"`
		UserId string `json:"userId"`
		Status string `json:"status,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request data", http.StatusBadRequest)
		return
	}

	// Validate userId is not empty
	if req.UserId == "" {
		http.Error(w, "User ID cannot be empty", http.StatusBadRequest)
		return
	}

	// Validate access is valid
	if req.Access != "" && req.Access != "admin" && req.Access != "guest" {
		resp := map[string]interface{}{
			"success": false,
			"message": "Invalid permission type, must be admin or guest",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	if req.Status != "" && req.Status != "active" && req.Status != "inactive" {
		resp := map[string]interface{}{
			"success": false,
			"message": "Invalid status, must be active or inactive",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to connect to database: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	defer db.Close()

	// Use transaction for operations
	tx, err := db.Begin()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to start transaction: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	// Ensure transaction is appropriately committed or rolled back
	defer func() {
		if tx != nil {
			_ = tx.Rollback() // Rollback if not committed
		}
	}()

	// First query user existence
	var userData map[string]interface{}

	var dbId int
	var username, name, access string
	var status sql.NullString
	var avatar, userId, email sql.NullString // Use sql.NullString for fields that can be NULL

	// Use transaction query
	err = tx.QueryRow("SELECT id, username, name, avatar, userId, email, access, status FROM users WHERE userId = ?", req.UserId).Scan(
		&dbId, &username, &name, &avatar, &userId, &email, &access, &status)

	if err != nil {
		if err == sql.ErrNoRows {
			resp := map[string]interface{}{
				"success": false,
				"message": "User does not exist",
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}

		errMsg := fmt.Sprintf("Query user failed: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	// User exists, build user data
	userData = map[string]interface{}{
		"id":       dbId,
		"username": username,
		"name":     name,
		"access":   access,
	}

	// Handle possible NULL fields
	if avatar.Valid {
		userData["avatar"] = avatar.String
	} else {
		userData["avatar"] = ""
	}

	if userId.Valid {
		userData["userId"] = userId.String
	} else {
		userData["userId"] = ""
	}

	if email.Valid {
		userData["email"] = email.String
	} else {
		userData["email"] = ""
	}

	if status.Valid {
		userData["status"] = status.String
	} else {
		userData["status"] = "active"
	}

	updateSQL := "UPDATE users SET "
	var updateParams []interface{}
	var updateFields []string

	if req.Access != "" {
		updateFields = append(updateFields, "access = ?")
		updateParams = append(updateParams, req.Access)
	}

	if req.Status != "" {
		updateFields = append(updateFields, "status = ?")
		updateParams = append(updateParams, req.Status)
	}

	if len(updateFields) == 0 {
		resp := map[string]interface{}{
			"success": false,
			"message": "No fields to update",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	updateSQL += strings.Join(updateFields, ", ")
	updateSQL += " WHERE userId = ?"
	updateParams = append(updateParams, req.UserId)

	// Update user
	result, err := tx.Exec(updateSQL, updateParams...)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to update user: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	// Check update result
	rowsAffected, err := result.RowsAffected()
	if err != nil {
	} else {
		if rowsAffected == 0 {
		}
	}

	// Commit transaction
	if err = tx.Commit(); err != nil {
		errMsg := fmt.Sprintf("Failed to commit transaction: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	tx = nil // Transaction committed, prevent rollback in defer

	// Update values in userData
	if req.Access != "" {
		userData["access"] = req.Access
	}

	if req.Status != "" {
		userData["status"] = req.Status
	}

	// Build response
	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"userId": userData["userId"],
			"name":   userData["name"],
			"email":  userData["email"],
			"access": userData["access"],
			"status": userData["status"],
			"avatar": userData["avatar"],
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// DeleteUserHandler DELETE /api/users
func DeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	// Get user ID to delete
	var req struct {
		UserId string `json:"userId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request data", http.StatusBadRequest)
		return
	}

	// Validate user ID is not empty
	if req.UserId == "" {
		http.Error(w, "User ID cannot be empty", http.StatusBadRequest)
		return
	}

	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to connect to database: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to start transaction: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	// Check if user exists
	var exists bool
	err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE userId = ?)", req.UserId).Scan(&exists)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to check user existence: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	if !exists {
		resp := map[string]interface{}{
			"success": false,
			"message": "User does not exist",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Delete user
	result, err := tx.Exec("DELETE FROM users WHERE userId = ?", req.UserId)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to delete user: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
	} else {
		if rowsAffected == 0 {
		}
	}

	if err = tx.Commit(); err != nil {
		errMsg := fmt.Sprintf("Failed to commit transaction: %v", err)
		http.Error(w, errMsg, http.StatusInternalServerError)
		return
	}

	tx = nil

	resp := map[string]interface{}{
		"success": true,
		"message": "User deleted successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
