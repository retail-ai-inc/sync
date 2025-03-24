package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NewRouter creates and returns the routing configuration for the entire /api
func NewRouter() http.Handler {
	r := chi.NewRouter()

	// 1) Auth related
	r.Post("/login", AuthLoginHandler)                // POST /api/login
	r.Get("/currentUser", AuthCurrentUserHandler)     // GET /api/currentUser
	r.Post("/logout", AuthLogoutHandler)              // POST /api/logout
	r.Post("/test-connection", TestConnectionHandler) // GET /api/test-connection

	// 2) Monitor
	r.Get("/sync/{id}/monitor", SyncMonitorHandler) // GET /api/sync/{taskID}/monitor
	r.Get("/sync/{id}/metrics", SyncMetricsHandler) // GET /api/sync/{taskID}/metrics
	r.Get("/sync/{id}/logs", SyncLogsHandler)       // GET /api/sync/{taskID}/logs
	r.Get("/sync/{id}/tables", SyncTablesHandler)   // GET /api/sync/{taskID}/tables

	// 3) api_oauth_test
	r.Get("/oauth/{provider}/config", GetOAuthConfigHandler)    // GET /api/oauth/{provider}/config
	r.Put("/oauth/{provider}/config", UpdateOAuthConfigHandler) // PUT /oauth/{provider}/config
	r.Post("/login/google/callback", AuthGoogleCallbackHandler)

	// 4) Sync related
	r.Get("/sync", SyncListHandler)             // GET /api/sync
	r.Put("/sync/{id}/stop", SyncStopHandler)   // PUT /api/sync/{taskID}/stop
	r.Put("/sync/{id}/start", SyncStartHandler) // PUT /api/sync/{taskID}/start
	r.Put("/sync/{id}", SyncUpdateHandler)      // PUT /api/sync/{taskID}
	r.Post("/sync", SyncCreateHandler)          // POST /api/sync
	r.Delete("/sync/{id}", SyncDeleteHandler)   // DELETE /api/sync/{taskID}

	// 5) User management
	r.Get("/users", GetUsersHandler)                          // GET /api/users
	r.Put("/users/access", UpdateUserAccessHandler)           // PUT /api/users/access
	r.Delete("/users", DeleteUserHandler)                     // DELETE /api/users
	r.Put("/updatePassword", UpdatePasswordHandler)           // PUT /api/updatePassword
	r.Put("/updateAdminPassword", UpdateAdminPasswordHandler) // PUT /api/updateAdminPassword
	r.Get("/getAdminToken", GetAdminTokenHandler)             // GET /api/getAdminToken

	// 6) Schema related
	r.Post("/tables/schema", GetTableSchemaHandler)
	r.Post("/sql/execute", ExecuteSQLHandler) // POST /api/sql/execute

	return r
}
