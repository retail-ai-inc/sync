package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	identity "github.com/retail-ai-inc/sync/internal/identity/domain"
	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/httpapi"
	"github.com/retail-ai-inc/sync/internal/platform/logging"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/webui"
	"github.com/sirupsen/logrus"
)

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

	if identity.SecretIsEphemeral() {
		log.Error("SYNC_TOKEN_SECRET is not set, so a random signing secret was " +
			"generated for this process: tokens will not survive a restart and will " +
			"not be accepted by another replica. Set it before running more than one.")
	}

	router := chi.NewRouter()
	router.Mount("/api", httpapi.NewRouter())

	// Probes, outside /api because they must answer before anything is
	// configured and must never require a credential.
	router.Get("/healthz", httpapi.Health)
	router.Get("/readyz", httpapi.Ready)

	// The exposition a scraper reads. It sits outside /api and takes no
	// credential, which is what every scraper expects; keeping the port off the
	// public network is the requirement that replaces the token.
	router.Get("/metrics", metrics.Handler)

	router.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		filePath := filepath.Join("ui/dist", path)

		_, err := os.Stat(filePath)
		fileExists := !os.IsNotExist(err)

		if fileExists {
			http.StripPrefix("/", http.FileServer(http.Dir("ui/dist"))).ServeHTTP(w, r)
			return
		}

		http.ServeFile(w, r, "ui/dist/index.html")
	})

	// Every timeout is set. Without them a connection that opens and then sends
	// nothing holds a goroutine and a file descriptor for as long as it likes,
	// which is all it takes to exhaust the control plane from one host.
	server := &http.Server{
		Addr:              ":8080",
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	go func() {
		log.Info("UI is running at http://localhost:8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("HTTP server error: %v", err)
			cancel()
		}
	}()

	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
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
	log.Info("Program exited")
}
