package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	backuphttp "github.com/retail-ai-inc/sync/internal/backup/http"
	"github.com/retail-ai-inc/sync/internal/dbinspect"
	identityhttp "github.com/retail-ai-inc/sync/internal/identity/http"
	monitoringhttp "github.com/retail-ai-inc/sync/internal/monitoring/http"
	"github.com/retail-ai-inc/sync/internal/platform/audit"
	replicationapp "github.com/retail-ai-inc/sync/internal/replication/app"
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
// Before this split each handler decided for itself, and most did not, so
// anyone who could reach the port could read every task's credentials.
func NewRouter() http.Handler {
	// The probe is a leaf: it connects to whatever it is handed and knows
	// nothing about tasks. This lets it resolve the mask an edit form carries
	// against the task that form was filled from.
	dbinspect.StoredPassword = replicationapp.StoredEndpointPassword

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
		r.Get("/sync/{id}/position", replicationhttp.SyncPositionHandler)
		r.Get("/sync/{id}/rowcounts", replicationhttp.SyncRowCountsHandler)
		r.Get("/changestreams/status", monitoringhttp.ChangeStreamsStatusHandler)
		r.Get("/settings", SettingsHandler)

		r.Get("/backup", backuphttp.BackupListHandler)
		r.Get("/backup/status/{taskId}", backuphttp.BackupStatusHandler)
	})

	// 3) Administrative: everything that changes something, plus the two
	//    endpoints that reach out to a database an operator names. The prober
	//    and the schema reader take a host, a port and credentials and connect
	//    to them, which is a scanner unless it is held to administrators.
	r.Group(func(r chi.Router) {
		// The audit trail sits between the two checks deliberately. After
		// RequireAuth, so the caller is known; before RequireAdmin, so an
		// authenticated caller who tried a write and was refused is in the
		// trail with the 403 -- somebody probing the administrative endpoints
		// is worth recording. Not before RequireAuth: a caller with no token
		// would then be able to fill the control database from outside.
		r.Use(identityhttp.RequireAuth, audit.Middleware(principal), identityhttp.RequireAdmin)

		r.Post("/test-connection", dbinspect.TestConnectionHandler)
		r.Post("/tables/schema", dbinspect.GetTableSchemaHandler)

		r.Put("/settings", UpdateSettingsHandler)
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

		r.Post("/backup", backuphttp.BackupCreateHandler)
		r.Delete("/backup/{id}", backuphttp.BackupDeleteHandler)
		r.Put("/backup/{id}/pause", backuphttp.BackupPauseHandler)
		r.Put("/backup/{id}/resume", backuphttp.BackupResumeHandler)
		r.Put("/backup/{id}", backuphttp.BackupUpdateHandler)
		r.Post("/backup/execute/{id}", backuphttp.BackupExecuteHandler)
	})

	return r
}

// principal tells the audit trail who a request belongs to. The resolution
// lives here, with the rest of the wiring, so the audit package does not import
// the identity context to record what it is handed.
func principal(r *http.Request) (username, access string) {
	caller, ok := identityhttp.PrincipalFrom(r.Context())
	if !ok {
		return "", ""
	}
	return caller.Username, caller.Access
}

func Health(w http.ResponseWriter, r *http.Request) {
	writeStatus(w, http.StatusOK, map[string]interface{}{"status": "ok"})
}

// Ready answers the readiness probe. A rolling update has no way to know when
// a replica can take traffic without one, so Kubernetes sends requests to a
// container that is still starting.
//
// check reports whether this process can serve the control plane. Deliberately
// not replication health: a probe that failed because Tokyo was unreachable
// would have Kubernetes restart the one process that is still working -- the
// one holding the task list, the positions and the switch-over reports, which
// is what an operator needs most at that moment. Whether the replica is caught
// up is sync_task_up and the progress endpoint; this is whether the control
// plane can answer.
func Ready(check func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := check(); err != nil {
			writeStatus(w, http.StatusServiceUnavailable, map[string]interface{}{
				"status": "not ready",
				"reason": err.Error(),
			})
			return
		}
		writeStatus(w, http.StatusOK, map[string]interface{}{"status": "ready"})
	}
}

func writeStatus(w http.ResponseWriter, status int, body map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
