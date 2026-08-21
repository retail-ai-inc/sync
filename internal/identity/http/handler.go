// Package identityhttp adapts the identity use cases to HTTP.
//
// Every handler here keeps the request parsing and the response writing that it
// has always done, down to the order in which the status code and the
// Content-Type header are set — several handlers write the status first, which
// makes the later Set a no-op, and that is observable. The decisions the
// handlers used to make inline now live in the app layer.
package identityhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/retail-ai-inc/sync/internal/identity/app"
	"github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/identity/infra"
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

	ok, userAccess, token, err := app.Login(req.Username, req.Password)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	if ok {
		resp := map[string]interface{}{
			"status":           "ok",
			"type":             req.Type,
			"currentAuthority": userAccess,
			"accessToken":      token,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	} else {
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
	username := app.IdentifyFromHeader(r.Header.Get("Authorization"))

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

	userData, err := app.CurrentUser(username)
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
	app.Logout()

	resp := map[string]interface{}{
		"data":    map[string]interface{}{},
		"success": true,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// writeFailure answers with the {success,errorCode,errorMessage} shape the
// password handlers use, writing the status before the Content-Type exactly as
// they always have.
func writeFailure(w http.ResponseWriter, status int, code, message string) {
	w.WriteHeader(status)
	resp := map[string]interface{}{
		"success":      false,
		"errorCode":    code,
		"errorMessage": message,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// writeSuccessEnvelope answers with the {success,data} shape.
func writeSuccessEnvelope(w http.ResponseWriter) {
	resp := map[string]interface{}{
		"success": true,
		"data":    map[string]interface{}{},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// UpdatePasswordHandler PUT /api/updatePassword
func UpdatePasswordHandler(w http.ResponseWriter, r *http.Request) {
	username, ok := app.ResolvePasswordChangeIdentity(r.Header.Get("Authorization"))
	if !ok {
		writeFailure(w, http.StatusUnauthorized, "401", "Please login or provide a valid token")
		return
	}

	var req struct {
		OldPassword string `json:"oldPassword"`
		NewPassword string `json:"newPassword"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFailure(w, http.StatusBadRequest, "400", "Invalid request parameters")
		return
	}

	switch err := app.ChangePassword(username, req.OldPassword, req.NewPassword); {
	case err == nil:
		writeSuccessEnvelope(w)
	case errors.Is(err, app.ErrPasswordLookup):
		writeFailure(w, http.StatusInternalServerError, "500", "Internal server error")
	case errors.Is(err, app.ErrPasswordMismatch):
		writeFailure(w, http.StatusBadRequest, "400", "Old password is incorrect")
	default:
		writeFailure(w, http.StatusInternalServerError, "500", "Failed to update password: "+err.Error())
	}
}

// UpdateAdminPasswordHandler PUT /api/updateAdminPassword
func UpdateAdminPasswordHandler(w http.ResponseWriter, r *http.Request) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		writeFailure(w, http.StatusUnauthorized, "401", "No authentication token provided")
		return
	}

	if !domain.ValidateAdminToken(domain.ExtractTokenFromHeader(authHeader)) {
		writeFailure(w, http.StatusUnauthorized, "401", "Invalid token")
		return
	}

	var req struct {
		OldPassword string `json:"oldPassword"`
		NewPassword string `json:"newPassword"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeFailure(w, http.StatusBadRequest, "400", "Invalid request parameters")
		return
	}

	switch err := app.ChangePassword("admin", req.OldPassword, req.NewPassword); {
	case err == nil:
		writeSuccessEnvelope(w)
	case errors.Is(err, app.ErrPasswordLookup):
		writeFailure(w, http.StatusInternalServerError, "500", "Internal server error")
	case errors.Is(err, app.ErrPasswordMismatch):
		writeFailure(w, http.StatusBadRequest, "400", "Old password is incorrect")
	default:
		writeFailure(w, http.StatusInternalServerError, "500", "Failed to update password: "+err.Error())
	}
}

// GetAdminTokenHandler GET /api/getAdminToken
func GetAdminTokenHandler(w http.ResponseWriter, r *http.Request) {
	token, ok := app.AdminToken()
	if !ok {
		writeFailure(w, http.StatusUnauthorized, "401", "Admin privileges required")
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"accessToken": token,
		},
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

	authority, token, errorMessage := app.GoogleLogin(req.Code)

	var resp map[string]interface{}
	if errorMessage != "" {
		resp = map[string]interface{}{
			"status":           "error",
			"type":             "google",
			"currentAuthority": "guest",
			"errorMessage":     errorMessage,
		}
	} else {
		resp = map[string]interface{}{
			"status":           "ok",
			"type":             "google",
			"currentAuthority": authority,
			"accessToken":      token,
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// provider reads the provider name from the route, falling back to the query
// string. The route parameter is read with chi v1 while the router registers
// its routes with chi v5, so the route value never arrives and only the query
// string works — a recorded defect (T-106), preserved here.
func provider(r *http.Request) string {
	p := chi.URLParam(r, "provider")
	if p == "" {
		p = r.URL.Query().Get("provider")
	}
	return p
}

// GetOAuthConfigHandler GET /api/oauth/{provider}/config
func GetOAuthConfigHandler(w http.ResponseWriter, r *http.Request) {
	name := provider(r)
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "missing provider parameter",
		})
		return
	}

	config, err := app.ReadOAuthConfig(name)
	if err != nil {
		if errors.Is(err, app.ErrNoOAuthConfig) {
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

// UpdateOAuthConfigHandler PUT /api/oauth/{provider}/config
func UpdateOAuthConfigHandler(w http.ResponseWriter, r *http.Request) {
	supplied, isAdmin := app.AuthoriseAdmin(r.Header.Get("Authorization"))
	if !supplied {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Admin privileges required",
		})
		return
	}
	if !isAdmin {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Admin privileges required",
		})
		return
	}

	name := provider(r)
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "missing provider parameter",
		})
		return
	}

	var config map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Invalid request data",
		})
		return
	}

	if err := app.WriteOAuthConfig(name, config); err != nil {
		var missing *app.MissingOAuthFieldError
		if errors.As(err, &missing) {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": false,
				"error":   missing.Error(),
			})
			return
		}
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
	current := 1
	pageSize := 10

	if currentStr := r.URL.Query().Get("current"); currentStr != "" {
		if val, err := strconv.Atoi(currentStr); err == nil && val > 0 {
			current = val
		}
	}
	if pageSizeStr := r.URL.Query().Get("pageSize"); pageSizeStr != "" {
		if val, err := strconv.Atoi(pageSizeStr); err == nil && val > 0 {
			pageSize = val
		}
	}

	page, total, err := app.ListUsers(current, pageSize)
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

	resp := map[string]interface{}{
		"success": true,
		"data":    page,
		"total":   total,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// writeRejection answers with the {success:false,message} shape at HTTP 200,
// which is what the user-management endpoints do for a refused request.
func writeRejection(w http.ResponseWriter, message string) {
	resp := map[string]interface{}{
		"success": false,
		"message": message,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// storeFault answers with the plain-text 500 the endpoints have always used,
// choosing the message from the stage the store failed at.
func storeFault(w http.ResponseWriter, err error, messages map[string]string) {
	var fault *infra.Fault
	stage := ""
	if errors.As(err, &fault) {
		stage = fault.Stage
	}
	format, ok := messages[stage]
	if !ok {
		format = "%v"
	}
	http.Error(w, fmt.Sprintf(format, errors.Unwrap(err)), http.StatusInternalServerError)
}

var accessFaultMessages = map[string]string{
	infra.StageConnect: "Failed to connect to database: %v",
	infra.StageBegin:   "Failed to start transaction: %v",
	infra.StageQuery:   "Query user failed: %v",
	infra.StageUpdate:  "Failed to update user: %v",
	infra.StageCommit:  "Failed to commit transaction: %v",
}

// UpdateUserAccessHandler PUT /api/users/access
func UpdateUserAccessHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Access string `json:"access"`
		UserId string `json:"userId"`
		Status string `json:"status,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request data", http.StatusBadRequest)
		return
	}

	if req.UserId == "" {
		http.Error(w, "User ID cannot be empty", http.StatusBadRequest)
		return
	}

	userData, rejection, err := app.ChangeUserAccess(req.UserId, req.Access, req.Status)
	if rejection != "" {
		writeRejection(w, string(rejection))
		return
	}
	if err != nil {
		storeFault(w, err, accessFaultMessages)
		return
	}

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

var deleteFaultMessages = map[string]string{
	infra.StageConnect: "Failed to connect to database: %v",
	infra.StageBegin:   "Failed to start transaction: %v",
	infra.StageCheck:   "Failed to check user existence: %v",
	infra.StageDelete:  "Failed to delete user: %v",
	infra.StageCommit:  "Failed to commit transaction: %v",
}

// DeleteUserHandler DELETE /api/users
func DeleteUserHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserId string `json:"userId"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request data", http.StatusBadRequest)
		return
	}

	if req.UserId == "" {
		http.Error(w, "User ID cannot be empty", http.StatusBadRequest)
		return
	}

	rejection, err := app.RemoveUser(req.UserId)
	if rejection != "" {
		writeRejection(w, string(rejection))
		return
	}
	if err != nil {
		storeFault(w, err, deleteFaultMessages)
		return
	}

	resp := map[string]interface{}{
		"success": true,
		"message": "User deleted successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
