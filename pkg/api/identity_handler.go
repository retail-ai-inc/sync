package api

import (
	"encoding/json"
	"net/http"
)

var (
	// Store current user's access level
	access = ""
	// Store current username, used to get user information
	currentUsername = ""
)

// AuthLoginHandler  POST /api/login
func AuthLoginHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username  string `json:"username"`
		Password  string `json:"password"`
		AutoLogin bool   `json:"autoLogin"`
		Type      string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	valid, userAccess, err := ValidateUser(req.Username, req.Password)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if valid {
		access = userAccess
		currentUsername = req.Username

		// Generate user token
		token := GenerateUserToken(req.Username, userAccess)

		resp := map[string]interface{}{
			"status":           "ok",
			"type":             req.Type,
			"currentAuthority": userAccess,
			"accessToken":      token,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	} else {
		access = "guest"
		currentUsername = ""
		resp := map[string]interface{}{
			"status":           "error",
			"type":             req.Type,
			"currentAuthority": "guest",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// AuthCurrentUserHandler  GET /api/currentUser
func AuthCurrentUserHandler(w http.ResponseWriter, r *http.Request) {
	username := ""
	authHeader := r.Header.Get("Authorization")

	if authHeader != "" {
		token := ExtractTokenFromHeader(authHeader)
		valid, tokenUsername, _ := ValidateUserToken(token)
		if valid {
			username = tokenUsername
		}
	}

	if username == "" {
		w.WriteHeader(http.StatusUnauthorized)
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"isLogin": false,
			},
			"errorCode":    "401",
			"errorMessage": "Please log in first or provide a valid token!",
			"success":      true,
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	userData, err := GetUserData(username)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"isLogin": false,
			},
			"errorCode":    "500",
			"errorMessage": "Failed to obtain user data.",
			"success":      false,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"name":   userData["name"],
			"avatar": userData["avatar"],
			"userId": userData["userId"],
			"email":  userData["email"],
			"access": userData["access"],
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// AuthLogoutHandler POST /api/logout
func AuthLogoutHandler(w http.ResponseWriter, r *http.Request) {
	access = ""
	currentUsername = ""

	resp := map[string]interface{}{
		"data":    map[string]interface{}{},
		"success": true,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// UpdatePasswordHandler PUT /api/updatePassword
func UpdatePasswordHandler(w http.ResponseWriter, r *http.Request) {
	// First try to get token from Authorization header
	username := ""
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		token := ExtractTokenFromHeader(authHeader)
		valid, tokenUsername, _ := ValidateUserToken(token)
		if valid {
			username = tokenUsername
		}
	}

	// If token validation fails, check if user is logged in
	if username == "" {
		if access == "" || access == "guest" || currentUsername == "" {
			w.WriteHeader(http.StatusUnauthorized)
			resp := map[string]interface{}{
				"success":      false,
				"errorCode":    "401",
				"errorMessage": "Please login or provide a valid token",
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
		username = currentUsername
	}

	var req struct {
		OldPassword string `json:"oldPassword"`
		NewPassword string `json:"newPassword"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "400",
			"errorMessage": "Invalid request parameters",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Verify if old password is correct
	valid, _, err := ValidateUser(username, req.OldPassword)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "500",
			"errorMessage": "Internal server error",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	if !valid {
		w.WriteHeader(http.StatusBadRequest)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "400",
			"errorMessage": "Old password is incorrect",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Update password
	if err := UpdateUserPassword(username, req.NewPassword); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "500",
			"errorMessage": "Failed to update password: " + err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Update successful
	resp := map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// UpdateAdminPasswordHandler PUT /api/updateAdminPassword
func UpdateAdminPasswordHandler(w http.ResponseWriter, r *http.Request) {
	// Get token from Authorization header
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		w.WriteHeader(http.StatusUnauthorized)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "401",
			"errorMessage": "No authentication token provided",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Extract token
	token := ExtractTokenFromHeader(authHeader)

	// Validate token
	if !ValidateAdminToken(token) {
		w.WriteHeader(http.StatusUnauthorized)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "401",
			"errorMessage": "Invalid token",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	var req struct {
		OldPassword string `json:"oldPassword"`
		NewPassword string `json:"newPassword"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "400",
			"errorMessage": "Invalid request parameters",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Verify if old password is correct
	valid, _, err := ValidateUser("admin", req.OldPassword)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "500",
			"errorMessage": "Internal server error",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	if !valid {
		w.WriteHeader(http.StatusBadRequest)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "400",
			"errorMessage": "Old password is incorrect",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Update admin password
	if err := UpdateUserPassword("admin", req.NewPassword); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "500",
			"errorMessage": "Failed to update password: " + err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Update successful
	resp := map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// GetAdminTokenHandler GET /api/getAdminToken
func GetAdminTokenHandler(w http.ResponseWriter, r *http.Request) {
	// Check if user is logged in and is admin
	if access != "admin" || currentUsername != "admin" {
		w.WriteHeader(http.StatusUnauthorized)
		resp := map[string]interface{}{
			"success":      false,
			"errorCode":    "401",
			"errorMessage": "Admin privileges required",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Generate current valid token
	token := GenerateAdminToken()

	// Return token
	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"accessToken": token,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
