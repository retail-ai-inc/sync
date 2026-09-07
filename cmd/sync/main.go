package main

import (
	"context"
	"errors"
	"fmt"
	replicationapp "github.com/retail-ai-inc/sync/internal/replication/app"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	backupapp "github.com/retail-ai-inc/sync/internal/backup/app"
	identity "github.com/retail-ai-inc/sync/internal/identity/domain"
	identityStore "github.com/retail-ai-inc/sync/internal/identity/infra"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/httpapi"
	"github.com/retail-ai-inc/sync/internal/platform/logging"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/secret"
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/retail-ai-inc/sync/internal/platform/webui"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/sirupsen/logrus"
)

// httpAddr is where the control plane listens, which SYNC_HTTP_ADDR overrides.
//
// It was hardcoded, so a second process on one host died on the bind before it
// reached anything else — including the check that would have told it another
// instance was already running this task. Two syncers on one host is a
// reasonable thing to want when they carry different tasks.
func httpAddr() string {
	if addr := strings.TrimSpace(os.Getenv("SYNC_HTTP_ADDR")); addr != "" {
		return addr
	}
	return ":8080"
}

func main() {
	cfg, err := config.NewConfig()
	if err != nil {
		logrus.Fatalf("Failed to read the configuration: %v", err)
	}
	log := logging.InitLogger(cfg.LogLevel)

	if _, err := os.Stat("ui/dist"); os.IsNotExist(err) {
		log.Info("ui/dist directory does not exist, extracting ui/dist.zip...")
		if err := webui.UnzipDistFile("ui/dist.zip", "ui/"); err != nil {
			log.Errorf("Error unzipping dist.zip: %v", err)
			return
		}
		log.Info("ui/dist directory extracted successfully.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	if !secret.Configured() {
		log.Error("SYNC_CONFIG_KEY is not set, so the database passwords in the " +
			"configuration store are held in the clear. Anybody who can read the " +
			"file — a backup, a volume snapshot — has the credentials for both " +
			"regions.")
	}

	// A fresh control database has no accounts at all, because the file is no
	// longer shipped with one in it. Without this the UI would be reachable and
	// unusable.
	switch created, err := identityStore.EnsureAdmin(); {
	case errors.Is(err, identityStore.ErrNoBootstrapPassword):
		log.Error(err)
	case err != nil:
		log.Errorf("Failed to check for an administrator account: %v", err)
	case created:
		log.Infof("Created the first administrator %q from SYNC_ADMIN_PASSWORD. "+
			"Change the password after signing in; the variable is ignored from now on.",
			identityStore.BootstrapUsername)
	}

	if identity.SecretIsEphemeral() {
		log.Error("SYNC_TOKEN_SECRET is not set, so a random signing secret was " +
			"generated for this process: tokens will not survive a restart and will " +
			"not be accepted by another replica. Set it before running more than one.")
	}

	router := newRouter()

	// Every timeout is set. Without them a connection that opens and then sends
	// nothing holds a goroutine and a file descriptor for as long as it likes,
	// which is all it takes to exhaust the control plane from one host.
	server := &http.Server{
		Addr:              httpAddr(),
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Infof("UI is running at http://localhost%s", httpAddr())
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("HTTP server error: %v", err)
			cancel()
		}
	}()

	// The backup schedule is this process's own, rebuilt from the job table on
	// every start. It used to be the system crontab, written only when a job was
	// edited through the API — so a new container had none, and every backup
	// stopped until somebody happened to touch a job.
	stopBackups := backupapp.StartBackupScheduler(ctx, log)

	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		// A deleted task's positions are removed by the engine that wrote them,
		// and this is where the engines are known.
		replicationapp.PurgeCheckpoints = purgeCheckpointsFor

		// A batch's bounds come from the settings, so an operator can cap the
		// memory a batch of large rows takes without a rebuild. The pipeline
		// asks rather than importing the control database.
		pipeline.StoredLimits = storedBatchLimits

		runSyncTasks(ctx, log, cfg)
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Errorf("HTTP server Shutdown error: %v", err)
	} else {
		log.Info("HTTP server gracefully stopped")
	}

	// Wait for the replication tasks to finish applying what they had in hand.
	// This used to be a two-second sleep, which discarded whatever a syncer was
	// midway through; the deadline is the container's grace period to use.
	select {
	case <-syncDone:
		log.Info("Replication stopped cleanly")
	case <-time.After(drainTimeout + 5*time.Second):
		log.Warn("Replication did not stop within the drain timeout")
	}
	stopBackups()
	log.Info("Program exited")
}

// newRouter builds the whole HTTP surface: the API under /api, the probes and
// the metrics beside it, and the single-page application under everything
// else. A function rather than a block inside main so it can be exercised:
// what is reachable without a credential is a security property, and "the
// probes answer before anything is configured" is the property a rolling
// deployment depends on.
func newRouter() *chi.Mux {
	router := chi.NewRouter()
	router.Mount("/api", httpapi.NewRouter())

	// Probes, outside /api because they must answer before anything is
	// configured and must never require a credential.
	//
	// Readiness is whether the control database can be read, not whether
	// replication is caught up: a probe that failed because Tokyo was
	// unreachable would restart the one process still able to answer what the
	// tasks are and where their positions stand.
	router.Get("/healthz", httpapi.Health)
	router.Get("/readyz", httpapi.Ready(controlPlaneReady))

	// The exposition a scraper reads. It sits outside /api and takes no
	// credential, which is what every scraper expects; keeping the port off the
	// public network is the requirement that replaces the token.
	router.Get("/metrics", metrics.Handler)

	router.Get("/*", serveUI)
	return router
}

// serveUI answers with a built file when one exists and with the application's
// entry point otherwise, because the routes belong to the single-page
// application rather than to this server.
func serveUI(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	filePath := filepath.Join("ui/dist", path)

	_, err := os.Stat(filePath)
	fileExists := !os.IsNotExist(err)

	if fileExists {
		http.StripPrefix("/", http.FileServer(http.Dir("ui/dist"))).ServeHTTP(w, r)
		return
	}

	http.ServeFile(w, r, "ui/dist/index.html")
}

// controlPlaneReady reports whether the control database answers.
//
// It is opened rather than kept, because the failure this is looking for is the
// file being unreadable -- a volume that did not mount, a database replaced
// while the process ran -- and a connection opened at start-up would not notice
// either.
func controlPlaneReady() error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return fmt.Errorf("open the control database: %w", err)
	}
	defer db.Close()

	// One row, not a count. Both prove the same things -- the file is readable,
	// the table is there, a page decodes -- and a count walks every leaf page of
	// a table whose rows carry the whole task configuration.
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM (SELECT 1 FROM sync_tasks LIMIT 1)").Scan(new(int)); err != nil {
		return fmt.Errorf("read the control database: %w", err)
	}
	return nil
}

// storedBatchLimits reports the batch bounds a deployment has set, or zeroes,
// which leave the built-in defaults in place.
func storedBatchLimits() pipeline.Limits {
	stored, err := config.LoadSettings()
	if err != nil {
		return pipeline.Limits{}
	}
	return pipeline.Limits{
		MaxEvents: stored.BatchMaxEvents,
		MaxBytes:  stored.BatchMaxBytes,
	}
}
