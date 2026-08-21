package httpapi

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/internal/backup"
	"github.com/retail-ai-inc/sync/internal/dbinspect"
	identityhttp "github.com/retail-ai-inc/sync/internal/identity/http"
	monitoringhttp "github.com/retail-ai-inc/sync/internal/monitoring/http"
	"github.com/retail-ai-inc/sync/internal/replication"
)

// NewRouter creates and returns the routing configuration for the entire /api
func NewRouter() http.Handler {
	r := chi.NewRouter()

	// 1) Auth related
	r.Post("/login", identityhttp.AuthLoginHandler)             // POST /api/login
	r.Get("/currentUser", identityhttp.AuthCurrentUserHandler)  // GET /api/currentUser
	r.Post("/logout", identityhttp.AuthLogoutHandler)           // POST /api/logout
	r.Post("/test-connection", dbinspect.TestConnectionHandler) // GET /api/test-connection

	// 2) Monitor
	r.Get("/sync/{id}/monitor", monitoringhttp.SyncMonitorHandler)            // GET /api/sync/{taskID}/monitor
	r.Get("/sync/{id}/metrics", monitoringhttp.SyncMetricsHandler)            // GET /api/sync/{taskID}/metrics
	r.Get("/sync/{id}/logs", monitoringhttp.SyncLogsHandler)                  // GET /api/sync/{taskID}/logs
	r.Get("/sync/{id}/tables", replication.SyncTablesHandler)                 // GET /api/sync/{taskID}/tables
	r.Get("/changestreams/status", monitoringhttp.ChangeStreamsStatusHandler) // GET /api/changestreams/status

	// 3) api_oauth_test
	r.Get("/oauth/{provider}/config", identityhttp.GetOAuthConfigHandler)    // GET /api/oauth/{provider}/config
	r.Put("/oauth/{provider}/config", identityhttp.UpdateOAuthConfigHandler) // PUT /oauth/{provider}/config
	r.Post("/login/google/callback", identityhttp.AuthGoogleCallbackHandler)

	// 4) Sync related
	r.Get("/sync", replication.SyncListHandler)             // GET /api/sync
	r.Put("/sync/{id}/stop", replication.SyncStopHandler)   // PUT /api/sync/{taskID}/stop
	r.Put("/sync/{id}/start", replication.SyncStartHandler) // PUT /api/sync/{taskID}/start
	r.Put("/sync/{id}", replication.SyncUpdateHandler)      // PUT /api/sync/{taskID}
	r.Post("/sync", replication.SyncCreateHandler)          // POST /api/sync
	r.Delete("/sync/{id}", replication.SyncDeleteHandler)   // DELETE /api/sync/{taskID}

	// 5) User management
	r.Get("/users", identityhttp.GetUsersHandler)                          // GET /api/users
	r.Put("/users/access", identityhttp.UpdateUserAccessHandler)           // PUT /api/users/access
	r.Delete("/users", identityhttp.DeleteUserHandler)                     // DELETE /api/users
	r.Put("/updatePassword", identityhttp.UpdatePasswordHandler)           // PUT /api/updatePassword
	r.Put("/updateAdminPassword", identityhttp.UpdateAdminPasswordHandler) // PUT /api/updateAdminPassword
	r.Get("/getAdminToken", identityhttp.GetAdminTokenHandler)             // GET /api/getAdminToken

	// 6) Schema related
	r.Post("/tables/schema", dbinspect.GetTableSchemaHandler)

	// 7) Backup related
	r.Get("/backup", backup.BackupListHandler)
	r.Post("/backup", backup.BackupCreateHandler)
	r.Delete("/backup/{id}", backup.BackupDeleteHandler)
	r.Put("/backup/{id}/pause", backup.BackupPauseHandler)
	r.Put("/backup/{id}/resume", backup.BackupResumeHandler)
	r.Post("/backup/{id}/run", backup.BackupRunHandler)
	r.Put("/backup/{id}", backup.BackupUpdateHandler)

	// 8) Cronjob related
	r.Post("/backup/execute/{id}", backup.BackupExecuteHandler)  // POST /api/backup/execute/{id}
	r.Get("/backup/status/{taskId}", backup.BackupStatusHandler) // GET /api/backup/status/{taskId}

	return r
}
