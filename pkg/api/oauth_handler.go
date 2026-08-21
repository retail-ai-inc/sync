package api

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/go-chi/chi"
)

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
