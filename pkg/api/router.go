// pkg/api/router.go
package api

import (
    "net/http"

    "github.com/go-chi/chi/v5"
)

// NewRouter creates and returns the routing configuration for the entire /api
func NewRouter() http.Handler {
    r := chi.NewRouter()

    // If there are common middlewares, such as authentication, they can be used here

    // 1) Auth related
    r.Post("/login", AuthLoginHandler)    // POST /api/login
    r.Get("/currentUser", AuthCurrentUserHandler) // GET /api/currentUser
    r.Post("/logout", AuthLogoutHandler)          // POST /api/logout

    // 2) Sync related
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
