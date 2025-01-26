// pkg/api/router.go
package api

import (
    "net/http"

    "github.com/go-chi/chi/v5"
)

// NewRouter 创建并返回整个 /api 的路由配置
func NewRouter() http.Handler {
    r := chi.NewRouter()

    // 如果有通用的中间件，比如鉴权，可以在这里 use

    // 1) Auth 相关
    r.Post("/login", AuthLoginHandler)    // POST /api/login
    r.Get("/currentUser", AuthCurrentUserHandler) // GET /api/currentUser
    r.Post("/logout", AuthLogoutHandler)          // POST /api/logout

    // 2) Sync 相关
    r.Get("/sync", SyncListHandler)                                 // GET /api/sync
    r.Put("/sync/{id}/stop", SyncStopHandler)                   // PUT /api/sync/{taskID}/stop
    r.Put("/sync/{id}/start", SyncStartHandler)                 // PUT /api/sync/{taskID}/start
    r.Put("/sync/{id}", SyncUpdateHandler)                      // PUT /api/sync/{taskID}
    r.Post("/sync", SyncCreateHandler)                     // POST /api/sync
    r.Delete("/sync/{id}", SyncDeleteHandler)                   // DELETE /api/sync/{taskID}
    r.Get("/sync/{id}/monitor", SyncMonitorHandler)             // GET /api/sync/{taskID}/monitor
    r.Get("/sync/{id}/metrics", SyncMetricsHandler)             // GET /api/sync/{taskID}/metrics
    r.Get("/sync/{id}/logs", SyncLogsHandler)                   // GET /api/sync/{taskID}/logs

    return r
}
