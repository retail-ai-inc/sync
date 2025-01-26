// pkg/api/auth_handler.go
package api

import (
	"encoding/json"
	"net/http"

	"github.com/sirupsen/logrus"
)

// Store user login status and permissions in memory. For demonstration purposes only.
var (
	access        = "" // If "", it means not logged in; otherwise, it stores "admin" etc.
	currentUserDB = map[string]interface{}{
		"name":   "Serati Ma",
		"avatar": "https://gw.alipayobjects.com/zos/antfincdn/XAosXuNZyF/BiazfanxmamNRoxxVxka.png",
		"userid": "00000001",
		"email":  "antdesign@alipay.com",
		"access": "admin",
	}
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

	logrus.Infof("AuthLoginHandler => username=%s, password=%s", req.Username, req.Password)

	// 简单示例：假设只有 admin/admin 为正确账号
	if req.Username == "admin" && req.Password == "admin" {
		access = "admin" // 表示当前用户已登录，权限=admin
		resp := map[string]interface{}{
			"status":           "ok",
			"type":             req.Type,
			"currentAuthority": "admin",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	} else {
		access = "guest"
		resp := map[string]interface{}{
			"status":           "error",
			"type":             req.Type,
			"currentAuthority": "guest",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// AuthCurrentUserHandler 处理 GET /api/currentUser
func AuthCurrentUserHandler(w http.ResponseWriter, r *http.Request) {
	// 若未登录，返回401
	if access == "" || access == "guest" {
		w.WriteHeader(http.StatusUnauthorized)
		resp := map[string]interface{}{
			"data": map[string]interface{}{
				"isLogin": false,
			},
			"errorCode":    "401",
			"errorMessage": "请先登录！",
			"success":      true,
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
	}

	// 已登录 => 返回 mock 用户信息
	resp := map[string]interface{}{
		"success": true,
		"data": map[string]interface{}{
			"name":   currentUserDB["name"],
			"avatar": currentUserDB["avatar"],
			"userid": currentUserDB["userid"],
			"email":  currentUserDB["email"],
			"access": access, // "admin"
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// AuthLogoutHandler 处理 POST /api/logout
func AuthLogoutHandler(w http.ResponseWriter, r *http.Request) {
	// 清除登录状态
	access = ""
	resp := map[string]interface{}{
		"data":    map[string]interface{}{},
		"success": true,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
