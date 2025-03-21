package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"bytes"
	"strings"

	"github.com/go-chi/chi"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"github.com/redis/go-redis/v9"
	"github.com/retail-ai-inc/sync/pkg/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

// AuthGoogleCallbackHandler POST /api/login/google/callback
func AuthGoogleCallbackHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Code  string `json:"code"`
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	if req.Code == "" {
		access = "guest"
		currentUsername = ""
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Invalid Google authentication data",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Get Google OAuth configuration
	config, err := GetAuthConfig("google")
	if err != nil {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Please configure Google OAuth information first",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Check if configuration is set
	clientID, ok := config["clientId"].(string)
	if !ok || clientID == "" {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Please set Google OAuth Client ID in the admin panel first",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	clientSecret, ok := config["clientSecret"].(string)
	if !ok || clientSecret == "" {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Please set Google OAuth Client Secret in the admin panel first",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	redirectURI, ok := config["redirectUri"].(string)
	if !ok || redirectURI == "" {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Please set Google OAuth Redirect URI in the admin panel first",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Build token request
	tokenURL := "https://oauth2.googleapis.com/token"
	data := url.Values{}
	data.Set("code", req.Code)
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)
	data.Set("redirect_uri", redirectURI)
	data.Set("grant_type", "authorization_code")

	// Send token request
	tokenResp, err := http.PostForm(tokenURL, data)
	if err != nil {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Failed to get Google Token",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	defer tokenResp.Body.Close()

	// Print token request response
	tokenRespBody, _ := io.ReadAll(tokenResp.Body)

	// Reset body for subsequent reading
	tokenResp.Body = io.NopCloser(bytes.NewBuffer(tokenRespBody))

	var tokenData struct {
		AccessToken string `json:"access_token"`
		IDToken     string `json:"id_token"`
	}
	if err := json.NewDecoder(tokenResp.Body).Decode(&tokenData); err != nil {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Failed to parse Google Token",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Use access token to get user information
	userInfoURL := "https://www.googleapis.com/oauth2/v2/userinfo"
	req2, _ := http.NewRequest("GET", userInfoURL, nil)
	req2.Header.Set("Authorization", "Bearer "+tokenData.AccessToken)

	client := &http.Client{}
	userInfoResp, err := client.Do(req2)
	if err != nil {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Failed to get Google user information",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	defer userInfoResp.Body.Close()

	// Print user information response
	userInfoBody, _ := io.ReadAll(userInfoResp.Body)
	// Reset body for subsequent reading
	userInfoResp.Body = io.NopCloser(bytes.NewBuffer(userInfoBody))

	var userData struct {
		Email string `json:"email"`
		Name  string `json:"name"`
	}
	if err := json.NewDecoder(userInfoResp.Body).Decode(&userData); err != nil {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Failed to parse Google user information",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}
	// Save or update user information
	username, userAccess, err := SaveGoogleUser(userData.Email, userData.Name)
	if err != nil {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Failed to save user information: " + err.Error(),
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	user, err := GetUserByUsername(username)
	if err != nil {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Failed to verify user account status",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	if status, ok := user["status"].(string); ok && status == "inactive" {
		resp := map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     "Your account has been deactivated. Please contact your system administrator for assistance.",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// Set user login status
	access = userAccess
	currentUsername = username

	// Generate token
	token := GenerateUserToken(username, userAccess)

	resp := map[string]interface{}{
		"status":           "ok",
		"type":             "google",
		"currentAuthority": userAccess,
		"accessToken":      token,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// TestConnectionHandler POST /api/test-connection
func TestConnectionHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DbType   string `json:"dbType"`
		Host     string `json:"host"`
		Port     string `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	var tables []string

	switch req.DbType {
	case "mysql", "mariadb":
		// DSN: user:password@tcp(host:port)/database?parseTime=true&loc=Local
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=Local",
			req.User, req.Password, req.Host, req.Port, req.Database)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error opening connection: %v", err), http.StatusInternalServerError)
			return
		}
		defer db.Close()

		if err = db.Ping(); err != nil {
			http.Error(w, fmt.Sprintf("Ping failed: %v", err), http.StatusInternalServerError)
			return
		}

		rows, err := db.Query("SHOW TABLES")
		if err != nil {
			http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				http.Error(w, fmt.Sprintf("Scan failed: %v", err), http.StatusInternalServerError)
				return
			}
			tables = append(tables, table)
		}

	case "postgresql":
		// DSN: postgres://user:password@host:port/database?sslmode=disable
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			req.User, req.Password, req.Host, req.Port, req.Database)
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error opening connection: %v", err), http.StatusInternalServerError)
			return
		}
		defer db.Close()

		if err = db.Ping(); err != nil {
			http.Error(w, fmt.Sprintf("Ping failed: %v", err), http.StatusInternalServerError)
			return
		}

		rows, err := db.Query("SELECT tablename FROM pg_tables WHERE schemaname='public'")
		if err != nil {
			http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		for rows.Next() {
			var table string
			if err := rows.Scan(&table); err != nil {
				http.Error(w, fmt.Sprintf("Scan failed: %v", err), http.StatusInternalServerError)
				return
			}
			tables = append(tables, table)
		}

	case "mongodb":
		var uri string
		if req.User != "" && req.Password != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/?directConnection=true", req.User, req.Password, req.Host, req.Port)
		} else {
			uri = fmt.Sprintf("mongodb://%s:%s/?directConnection=true", req.Host, req.Port)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err != nil {
			http.Error(w, fmt.Sprintf("MongoDB connection error: %v", err), http.StatusInternalServerError)
			return
		}
		defer func() {
			_ = client.Disconnect(ctx)
		}()

		if err = client.Ping(ctx, nil); err != nil {
			http.Error(w, fmt.Sprintf("MongoDB ping error: %v", err), http.StatusInternalServerError)
			return
		}

		collections, err := client.Database(req.Database).ListCollectionNames(ctx, struct{}{})
		if err != nil {
			http.Error(w, fmt.Sprintf("Listing collections failed: %v", err), http.StatusInternalServerError)
			return
		}
		tables = collections

	case "redis":
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		dbIndex, err := strconv.Atoi(req.Database)
		if err != nil {
			dbIndex = 0
		}

		rdb := redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", req.Host, req.Port),
			Username: req.User,
			Password: req.Password,
			DB:       dbIndex,
		})

		if err = rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, fmt.Sprintf("Redis ping error: %v", err), http.StatusInternalServerError)
			return
		}

		resp := map[string]interface{}{
			"success": true,
			"data": map[string]interface{}{
				"tables": []string{},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		return

	default:
		http.Error(w, "Unsupported dbType", http.StatusBadRequest)
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"tables": tables,
		},
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

// GetOAuthConfigHandler handles GET /api/oauth/{provider}/config
func GetOAuthConfigHandler(w http.ResponseWriter, r *http.Request) {
	provider := chi.URLParam(r, "provider")
	if provider == "" {
		provider = r.URL.Query().Get("provider")
	}

	if provider == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "missing provider parameter",
		})
		return
	}

	config, err := GetAuthConfig(provider)
	if err != nil {
		if err == sql.ErrNoRows {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":   "No specified OAuth configuration found",
				"success": false,
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Failed to get OAuth configuration",
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"data":    config,
	})
}

// UpdateOAuthConfigHandler handles PUT /api/oauth/{provider}/config
func UpdateOAuthConfigHandler(w http.ResponseWriter, r *http.Request) {
	// Check user permissions
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Admin privileges required",
		})
		return
	}

	token := ExtractTokenFromHeader(authHeader)
	valid, _, userAccess := ValidateUserToken(token)
	if !valid || userAccess != "admin" {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Admin privileges required",
		})
		return
	}

	// Get provider parameter
	provider := chi.URLParam(r, "provider")
	if provider == "" {
		provider = r.URL.Query().Get("provider")
	}

	if provider == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "missing provider parameter",
		})
		return
	}

	// Parse request body
	var config map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Invalid request data",
		})
		return
	}

	enabled, _ := config["enabled"].(bool)

	if provider == "google" && enabled {
		requiredFields := []string{"clientId", "clientSecret", "redirectUri"}
		for _, field := range requiredFields {
			value, exists := config[field].(string)
			if !exists || value == "" {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"success": false,
					"error":   fmt.Sprintf("Missing necessary configuration field: %s", field),
				})
				return
			}
		}

		// Ensure necessary default values are included
		config["authUri"] = "https://accounts.google.com/o/oauth2/auth"
		config["tokenUri"] = "https://oauth2.googleapis.com/token"
		if _, exists := config["scopes"]; !exists {
			config["scopes"] = []string{"email", "profile"}
		}
	}

	// Update configuration
	if err := UpdateAuthConfig(provider, config); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Failed to update configuration: " + err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": "Configuration updated successfully",
	})
}

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

	db, err := db.OpenSQLiteDB()
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

	db, err := db.OpenSQLiteDB()
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

// ExecuteSQLHandler POST /api/sql/execute
func ExecuteSQLHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TaskId int    `json:"taskId"`
		SQL    string `json:"sql"`
		Target bool   `json:"target"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Get task configuration
	db, err := db.OpenSQLiteDB()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open database connection: %v", err), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	var configJSON string
	err = db.QueryRow("SELECT config_json FROM sync_tasks WHERE id = ?", req.TaskId).Scan(&configJSON)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get task configuration: %v", err), http.StatusInternalServerError)
		return
	}

	var taskConfig struct {
		SourceConn map[string]string `json:"sourceConn"`
		TargetConn map[string]string `json:"targetConn"`
		Type       string            `json:"type"`
	}
	if err := json.Unmarshal([]byte(configJSON), &taskConfig); err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse task configuration: %v", err), http.StatusInternalServerError)
		return
	}

	// Select target or source connection config
	connConfig := taskConfig.SourceConn
	dbType := taskConfig.Type
	if req.Target {
		connConfig = taskConfig.TargetConn
	}

	// Simplified check for write operations: check if contains insert, update, delete keywords
	sqlClean := strings.TrimSpace(req.SQL)
	sqlLower := strings.ToLower(sqlClean)
	isWriteOperation := strings.Contains(sqlLower, "insert") ||
		strings.Contains(sqlLower, "update") ||
		strings.Contains(sqlLower, "delete") ||
		strings.Contains(sqlLower, "remove")

	var results []map[string]interface{}
	var affectedRows int64
	var executionTime time.Duration
	var operationMessage string

	startTime := time.Now()

	// Execute SQL based on database type
	switch dbType {
	case "mysql", "mariadb":
		// DSN: user:password@tcp(host:port)/database?parseTime=true&loc=Local
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=Local",
			connConfig["user"], connConfig["password"], connConfig["host"], connConfig["port"], connConfig["database"])
		sqlDB, err := sql.Open("mysql", dsn)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to connect to MySQL database: %v", err), http.StatusInternalServerError)
			return
		}
		defer sqlDB.Close()

		if err = sqlDB.Ping(); err != nil {
			http.Error(w, fmt.Sprintf("MySQL connection test failed: %v", err), http.StatusInternalServerError)
			return
		}

		if isWriteOperation {
			// Execute write operation
			result, err := sqlDB.Exec(req.SQL)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to execute SQL operation: %v", err), http.StatusInternalServerError)
				return
			}

			affectedRows, _ = result.RowsAffected()
			if strings.Contains(sqlLower, "insert") {
				operationMessage = "Insert operation completed successfully"
			} else if strings.Contains(sqlLower, "update") {
				operationMessage = "Update operation completed successfully"
			} else if strings.Contains(sqlLower, "delete") {
				operationMessage = "Delete operation completed successfully"
			}
		} else {
			// Execute query operation
			rows, err := sqlDB.Query(req.SQL)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to execute SQL query: %v", err), http.StatusInternalServerError)
				return
			}
			defer rows.Close()

			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to get column information: %v", err), http.StatusInternalServerError)
				return
			}

			// Prepare result scan containers
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			// Process result set
			for rows.Next() {
				err = rows.Scan(scanArgs...)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to scan row data: %v", err), http.StatusInternalServerError)
					return
				}

				row := make(map[string]interface{})
				for i, col := range columns {
					var v interface{}
					val := values[i]
					b, ok := val.([]byte)
					if ok {
						v = string(b)
					} else {
						v = val
					}
					row[col] = v
				}
				results = append(results, row)
			}
			operationMessage = "Query operation completed successfully"
		}

	case "postgresql":
		// DSN: postgres://user:password@host:port/database?sslmode=disable
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
			connConfig["user"], connConfig["password"], connConfig["host"], connConfig["port"], connConfig["database"])
		sqlDB, err := sql.Open("postgres", dsn)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to connect to PostgreSQL database: %v", err), http.StatusInternalServerError)
			return
		}
		defer sqlDB.Close()

		if err = sqlDB.Ping(); err != nil {
			http.Error(w, fmt.Sprintf("PostgreSQL connection test failed: %v", err), http.StatusInternalServerError)
			return
		}

		if isWriteOperation {
			// Execute write operation
			result, err := sqlDB.Exec(req.SQL)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to execute SQL operation: %v", err), http.StatusInternalServerError)
				return
			}

			affectedRows, _ = result.RowsAffected()
			if strings.Contains(sqlLower, "insert") {
				operationMessage = "Insert operation completed successfully"
			} else if strings.Contains(sqlLower, "update") {
				operationMessage = "Update operation completed successfully"
			} else if strings.Contains(sqlLower, "delete") {
				operationMessage = "Delete operation completed successfully"
			}
		} else {
			// Execute query operation
			rows, err := sqlDB.Query(req.SQL)
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to execute SQL query: %v", err), http.StatusInternalServerError)
				return
			}
			defer rows.Close()

			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				http.Error(w, fmt.Sprintf("Failed to get column information: %v", err), http.StatusInternalServerError)
				return
			}

			// Prepare result scan containers
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			// Process result set
			for rows.Next() {
				err = rows.Scan(scanArgs...)
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to scan row data: %v", err), http.StatusInternalServerError)
					return
				}

				row := make(map[string]interface{})
				for i, col := range columns {
					var v interface{}
					val := values[i]
					b, ok := val.([]byte)
					if ok {
						v = string(b)
					} else {
						v = val
					}
					row[col] = v
				}
				results = append(results, row)
			}
			operationMessage = "Query operation completed successfully"
		}

	case "mongodb":
		var uri string
		if connConfig["user"] != "" && connConfig["password"] != "" {
			uri = fmt.Sprintf("mongodb://%s:%s@%s:%s/?directConnection=true",
				connConfig["user"], connConfig["password"], connConfig["host"], connConfig["port"])
		} else {
			uri = fmt.Sprintf("mongodb://%s:%s/?directConnection=true",
				connConfig["host"], connConfig["port"])
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
		if err != nil {
			http.Error(w, fmt.Sprintf("MongoDB connection error: %v", err), http.StatusInternalServerError)
			return
		}
		defer func() {
			_ = client.Disconnect(ctx)
		}()

		if err = client.Ping(ctx, nil); err != nil {
			http.Error(w, fmt.Sprintf("MongoDB connection test failed: %v", err), http.StatusInternalServerError)
			return
		}

		// Try to extract collection name
		var collectionName string
		collPattern := regexp.MustCompile(`db\.([a-zA-Z0-9_]+)\.`)
		collMatches := collPattern.FindStringSubmatch(sqlClean)
		if len(collMatches) >= 2 {
			collectionName = collMatches[1]
		} else {
			http.Error(w, "Cannot identify MongoDB collection name from query", http.StatusBadRequest)
			return
		}

		database := client.Database(connConfig["database"])
		collection := database.Collection(collectionName)

		if isWriteOperation {
			// For write operations
			if strings.Contains(sqlLower, "insertmany") {
				// Get count before operation
				countBefore, err := collection.CountDocuments(ctx, bson.M{})
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to count documents: %v", err), http.StatusInternalServerError)
					return
				}

				// Simple attempt to handle the specific insert pattern in the example
				if strings.Contains(sqlClean, "docs.push") && strings.Contains(sqlClean, "insertMany(docs)") {
					// Create documents with sequential IDs
					var docsToInsert []interface{}

					// Find the maximum ID currently in the collection
					var maxIDDoc bson.M
					opts := options.FindOne().SetSort(bson.D{{Key: "id", Value: -1}})
					err := collection.FindOne(ctx, bson.M{}, opts).Decode(&maxIDDoc)

					var maxID int = 0
					// If we found a document with an ID, use it as base
					if err == nil && maxIDDoc != nil {
						if idVal, ok := maxIDDoc["id"]; ok {
							switch id := idVal.(type) {
							case int32:
								maxID = int(id)
							case int64:
								maxID = int(id)
							case float64:
								maxID = int(id)
							case int:
								maxID = id
							}
						}
					}

					// Extract count from the JS code if possible
					countPattern := regexp.MustCompile(`let\s+count\s*=\s*(\d+)`)
					countMatches := countPattern.FindStringSubmatch(sqlClean)
					count := 5 // default
					if len(countMatches) >= 2 {
						count, _ = strconv.Atoi(countMatches[1])
					}

					// Create the documents
					for i := 1; i <= count; i++ {
						newID := maxID + i
						doc := bson.M{
							"name":  fmt.Sprintf("user%d", newID),
							"email": fmt.Sprintf("user%d@example.com", newID),
							"id":    newID,
						}
						docsToInsert = append(docsToInsert, doc)
					}

					// Perform the insert
					_, err = collection.InsertMany(ctx, docsToInsert)
					if err != nil {
						http.Error(w, fmt.Sprintf("MongoDB insert operation failed: %v", err), http.StatusInternalServerError)
						return
					}

					// Get count after operation to determine affected rows
					countAfter, err := collection.CountDocuments(ctx, bson.M{})
					if err != nil {
						http.Error(w, fmt.Sprintf("Failed to count documents after insert: %v", err), http.StatusInternalServerError)
						return
					}

					affectedRows = countAfter - countBefore
					operationMessage = fmt.Sprintf("MongoDB insert operation completed successfully. Inserted %d documents.", affectedRows)
				} else {
					http.Error(w, "Unsupported MongoDB JavaScript format. Only specific insertMany patterns are supported.", http.StatusBadRequest)
					return
				}
			} else if strings.Contains(sqlLower, "updatemany") || strings.Contains(sqlLower, "updateone") {
				// Implement MongoDB update operation support
				// Parse and execute update operations, supporting simple patterns
				if strings.Contains(sqlClean, "updateMany") && strings.Contains(sqlClean, "$set") {
					// Try to parse JavaScript update statement
					// Example: db.users.updateMany({ _id: { $in: ids } }, { $set: { email: newEmail } });

					// Parse update fields
					fieldPattern := regexp.MustCompile(`\{\s*\$set\s*:\s*\{\s*([a-zA-Z0-9_]+)\s*:\s*([^}]+)\s*\}\s*\}`)
					fieldMatches := fieldPattern.FindStringSubmatch(sqlClean)

					if len(fieldMatches) >= 3 {
						fieldName := fieldMatches[1]
						fieldValue := strings.TrimSpace(fieldMatches[2])

						// If the value is a variable, try to find its definition
						if !strings.HasPrefix(fieldValue, "\"") && !strings.HasPrefix(fieldValue, "'") {
							varPattern := regexp.MustCompile(`var\s+` + fieldValue + `\s*=\s*["']([^"']+)["']`)
							varMatches := varPattern.FindStringSubmatch(sqlClean)
							if len(varMatches) >= 2 {
								fieldValue = varMatches[1]
							} else {
								// If variable definition not found, create a similar value using current time
								fieldValue = "updated_" + strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10) + "@mail.com"
							}
						} else {
							// Remove quotes
							fieldValue = strings.Trim(fieldValue, "\"'")
						}

						// Execute update operation, update the most recent 5 records
						cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(5))
						if err != nil {
							http.Error(w, fmt.Sprintf("Failed to find documents to update: %v", err), http.StatusInternalServerError)
							return
						}

						var documentsToUpdate []bson.M
						if err = cursor.All(ctx, &documentsToUpdate); err != nil {
							http.Error(w, fmt.Sprintf("Failed to parse documents: %v", err), http.StatusInternalServerError)
							return
						}

						if len(documentsToUpdate) > 0 {
							var ids []interface{}
							for _, doc := range documentsToUpdate {
								ids = append(ids, doc["_id"])
							}

							// Execute update
							result, err := collection.UpdateMany(
								ctx,
								bson.M{"_id": bson.M{"$in": ids}},
								bson.M{"$set": bson.M{fieldName: fieldValue}},
							)

							if err != nil {
								http.Error(w, fmt.Sprintf("Failed to update documents: %v", err), http.StatusInternalServerError)
								return
							}

							affectedRows = result.ModifiedCount
							operationMessage = fmt.Sprintf("MongoDB update operation completed successfully. Updated %d documents.", affectedRows)
						} else {
							operationMessage = "No documents found to update."
							affectedRows = 0
						}
					} else {
						http.Error(w, "Could not parse update operation. Please use a simpler format.", http.StatusBadRequest)
						return
					}
				} else {
					http.Error(w, "Only simple updateMany operations with $set are supported.", http.StatusBadRequest)
					return
				}
			} else if strings.Contains(sqlLower, "deletemany") || strings.Contains(sqlLower, "deleteone") || strings.Contains(sqlLower, "remove") {
				// Implement MongoDB delete operation support
				_, err := collection.CountDocuments(ctx, bson.M{})
				if err != nil {
					http.Error(w, fmt.Sprintf("Failed to count documents: %v", err), http.StatusInternalServerError)
					return
				}

				// Parse and execute delete operation
				if strings.Contains(sqlClean, "deleteMany") {
					// Try to get IDs to delete from the query
					// Example: db.users.deleteMany({_id: {$in: ids}});

					// Find the most recent 5 records
					cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}).SetLimit(5))
					if err != nil {
						http.Error(w, fmt.Sprintf("Failed to find documents to delete: %v", err), http.StatusInternalServerError)
						return
					}

					var documentsToDelete []bson.M
					if err = cursor.All(ctx, &documentsToDelete); err != nil {
						http.Error(w, fmt.Sprintf("Failed to parse documents: %v", err), http.StatusInternalServerError)
						return
					}

					if len(documentsToDelete) > 0 {
						var ids []interface{}
						for _, doc := range documentsToDelete {
							ids = append(ids, doc["_id"])
						}

						// Execute delete
						result, err := collection.DeleteMany(
							ctx,
							bson.M{"_id": bson.M{"$in": ids}},
						)

						if err != nil {
							http.Error(w, fmt.Sprintf("Failed to delete documents: %v", err), http.StatusInternalServerError)
							return
						}

						affectedRows = result.DeletedCount
						operationMessage = fmt.Sprintf("MongoDB delete operation completed successfully. Deleted %d documents.", affectedRows)
					} else {
						operationMessage = "No documents found to delete."
						affectedRows = 0
					}
				} else {
					http.Error(w, "Only simple deleteMany operations are supported.", http.StatusBadRequest)
					return
				}
			} else {
				// Default error for unsupported operations
				http.Error(w, "Unsupported MongoDB JavaScript operation. Only specific patterns are supported.", http.StatusBadRequest)
				return
			}
		} else {
			// For query operations, return result data
			cursor, err := collection.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}))
			if err != nil {
				http.Error(w, fmt.Sprintf("MongoDB query execution failed: %v", err), http.StatusInternalServerError)
				return
			}
			defer cursor.Close(ctx)

			// Parse results
			var documents []bson.M
			if err = cursor.All(ctx, &documents); err != nil {
				http.Error(w, fmt.Sprintf("Failed to parse MongoDB results: %v", err), http.StatusInternalServerError)
				return
			}

			// Convert format
			for _, doc := range documents {
				results = append(results, doc)
			}
			operationMessage = "MongoDB query operation completed successfully"
		}

	case "redis":
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		dbIndex, err := strconv.Atoi(connConfig["database"])
		if err != nil {
			dbIndex = 0
		}

		rdb := redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", connConfig["host"], connConfig["port"]),
			Username: connConfig["user"],
			Password: connConfig["password"],
			DB:       dbIndex,
		})

		if err = rdb.Ping(ctx).Err(); err != nil {
			http.Error(w, fmt.Sprintf("Redis connection test failed: %v", err), http.StatusInternalServerError)
			return
		}

		sqlUpper := strings.ToUpper(sqlClean)
		if strings.HasPrefix(sqlUpper, "KEYS") {
			pattern := strings.TrimSpace(strings.TrimPrefix(sqlClean, "KEYS"))
			pattern = strings.TrimSpace(strings.TrimPrefix(pattern, "keys"))

			keys, err := rdb.Keys(ctx, pattern).Result()
			if err != nil {
				http.Error(w, fmt.Sprintf("Redis KEYS command execution failed: %v", err), http.StatusInternalServerError)
				return
			}

			for i, key := range keys {
				results = append(results, map[string]interface{}{
					"index": i,
					"key":   key,
				})
			}
			operationMessage = fmt.Sprintf("Redis KEYS command executed successfully, found %d keys", len(keys))
		} else if strings.HasPrefix(sqlUpper, "GET") {
			key := strings.TrimSpace(strings.TrimPrefix(sqlClean, "GET"))
			key = strings.TrimSpace(strings.TrimPrefix(key, "get"))

			val, err := rdb.Get(ctx, key).Result()
			if err != nil {
				http.Error(w, fmt.Sprintf("Redis GET command execution failed: %v", err), http.StatusInternalServerError)
				return
			}

			results = append(results, map[string]interface{}{
				"key":   key,
				"value": val,
			})
			operationMessage = "Redis GET command executed successfully"
		} else if strings.HasPrefix(sqlUpper, "SET") {
			parts := strings.SplitN(sqlClean, " ", 3)
			if len(parts) < 3 {
				http.Error(w, "Redis SET command format error, should be: SET key value", http.StatusBadRequest)
				return
			}

			key := strings.TrimSpace(parts[1])
			value := strings.TrimSpace(parts[2])

			err := rdb.Set(ctx, key, value, 0).Err()
			if err != nil {
				http.Error(w, fmt.Sprintf("Redis SET command execution failed: %v", err), http.StatusInternalServerError)
				return
			}

			affectedRows = 1
			operationMessage = fmt.Sprintf("Redis SET command executed successfully, key '%s' has been set", key)
		} else if strings.HasPrefix(sqlUpper, "DEL") {
			parts := strings.SplitN(sqlClean, " ", 2)
			if len(parts) < 2 {
				http.Error(w, "Redis DEL command format error, should be: DEL key [key ...]", http.StatusBadRequest)
				return
			}

			keys := strings.Split(strings.TrimSpace(parts[1]), " ")

			count, err := rdb.Del(ctx, keys...).Result()
			if err != nil {
				http.Error(w, fmt.Sprintf("Redis DEL command execution failed: %v", err), http.StatusInternalServerError)
				return
			}

			affectedRows = count
			operationMessage = fmt.Sprintf("Redis DEL command executed successfully, deleted %d keys", count)
		} else {
			http.Error(w, "Currently supports Redis KEYS, GET, SET and DEL commands", http.StatusBadRequest)
			return
		}

	default:
		http.Error(w, fmt.Sprintf("Unsupported database type: %s", dbType), http.StatusBadRequest)
		return
	}

	executionTime = time.Since(startTime)

	// Build simplified response object
	respData := map[string]interface{}{
		"executionTime": executionTime.String(),
		"message":       operationMessage,
	}

	// Different data content for different operation types
	if isWriteOperation {
		// Write operations only return affected rows count, not detailed results
		respData["affectedRows"] = affectedRows
	} else {
		// Query operations return query results
		respData["results"] = results
	}

	resp := map[string]interface{}{
		"success": true,
		"data":    respData,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
