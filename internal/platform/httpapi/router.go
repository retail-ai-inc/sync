package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	backuphttp "github.com/retail-ai-inc/sync/internal/backup/http"
	"github.com/retail-ai-inc/sync/internal/dbinspect"
	identityhttp "github.com/retail-ai-inc/sync/internal/identity/http"
	monitoringhttp "github.com/retail-ai-inc/sync/internal/monitoring/http"
	replicationhttp "github.com/retail-ai-inc/sync/internal/replication/http"
)

// NewRouter creates and returns the routing configuration for the entire /api
//
// The routes are in three groups, and which group a route is in is the whole of
// its access control:
//
//   - public: signing in, and the OAuth client details the sign-in page needs
//     before anybody has a token
//   - authenticated: everything that reads
//   - administrative: everything that changes a replication task, a backup
//     job, a user or the OAuth configuration
//
// Before this split, four handlers checked the Authorization header for
// themselves and the rest did not, so anyone who could reach the port could
// list every replication task with its credentials, create and delete tasks,
// and drive the connection prober at arbitrary hosts.
func NewRouter() http.Handler {
	r := chi.NewRouter()

	// 1) Public: what a caller with no token legitimately needs.
	r.Post("/login", identityhttp.AuthLoginHandler)
	r.Post("/logout", identityhttp.AuthLogoutHandler)
	r.Post("/login/google/callback", identityhttp.AuthGoogleCallbackHandler)
	r.Get("/oauth/{provider}/config", identityhttp.GetOAuthConfigHandler)

	// 2) Authenticated: reads.
	r.Group(func(r chi.Router) {
		r.Use(identityhttp.RequireAuth)

		r.Get("/currentUser", identityhttp.AuthCurrentUserHandler)
		r.Put("/updatePassword", identityhttp.UpdatePasswordHandler)

		r.Get("/sync", replicationhttp.SyncListHandler)
		r.Get("/sync/{id}/monitor", monitoringhttp.SyncMonitorHandler)
		r.Get("/sync/{id}/metrics", monitoringhttp.SyncMetricsHandler)
		r.Get("/sync/{id}/logs", monitoringhttp.SyncLogsHandler)
		r.Get("/sync/{id}/tables", replicationhttp.SyncTablesHandler)
		r.Get("/changestreams/status", monitoringhttp.ChangeStreamsStatusHandler)

		r.Get("/backup", backuphttp.BackupListHandler)
		r.Get("/backup/status/{taskId}", backuphttp.BackupStatusHandler)
	})

	// 3) Administrative: everything that changes something, plus the two
	//    endpoints that reach out to a database an operator names. The prober
	//    and the schema reader take a host, a port and credentials and connect
	//    to them, which is a scanner unless it is held to administrators.
	r.Group(func(r chi.Router) {
		r.Use(identityhttp.RequireAuth, identityhttp.RequireAdmin)

		r.Post("/test-connection", dbinspect.TestConnectionHandler)
		r.Post("/tables/schema", dbinspect.GetTableSchemaHandler)

		r.Put("/oauth/{provider}/config", identityhttp.UpdateOAuthConfigHandler)

		r.Post("/sync", replicationhttp.SyncCreateHandler)
		r.Put("/sync/{id}", replicationhttp.SyncUpdateHandler)
		r.Put("/sync/{id}/stop", replicationhttp.SyncStopHandler)
		r.Put("/sync/{id}/start", replicationhttp.SyncStartHandler)
		r.Delete("/sync/{id}", replicationhttp.SyncDeleteHandler)

		r.Get("/users", identityhttp.GetUsersHandler)
		r.Put("/users/access", identityhttp.UpdateUserAccessHandler)
		r.Delete("/users", identityhttp.DeleteUserHandler)
		r.Put("/updateAdminPassword", identityhttp.UpdateAdminPasswordHandler)
		r.Get("/getAdminToken", identityhttp.GetAdminTokenHandler)

		r.Post("/backup", backuphttp.BackupCreateHandler)
		r.Delete("/backup/{id}", backuphttp.BackupDeleteHandler)
		r.Put("/backup/{id}/pause", backuphttp.BackupPauseHandler)
		r.Put("/backup/{id}/resume", backuphttp.BackupResumeHandler)
		r.Post("/backup/{id}/run", backuphttp.BackupRunHandler)
		r.Put("/backup/{id}", backuphttp.BackupUpdateHandler)
		r.Post("/backup/execute/{id}", backuphttp.BackupExecuteHandler)
	})

	return r
}

// Health answers the liveness probe: the process is running and serving.
func Health(w http.ResponseWriter, r *http.Request) {
	writeStatus(w, http.StatusOK, map[string]interface{}{"status": "ok"})
}

// Ready answers the readiness probe.
//
// A rolling update has no way to know when a replica can take traffic without
// one, so Kubernetes sends requests to a container that is still starting. The
// check is deliberately cheap: it reports that the process is serving, not that
// every replication task is caught up, because a task that is behind is a
// reason to alert rather than to take the whole control plane out of service.
func Ready(w http.ResponseWriter, r *http.Request) {
	writeStatus(w, http.StatusOK, map[string]interface{}{"status": "ready"})
}

func writeStatus(w http.ResponseWriter, status int, body map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
