package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL / MariaDB driver
	_ "github.com/lib/pq"              // PostgreSQL driver
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/syncer"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"net/http"
	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/pkg/api"
)

// Interval for row count monitoring every minute
const monitorInterval = time.Second * 60

func main() {
	// Initialize context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture system interrupt signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger.Log.Info("Received interrupt signal, exiting...")
		cancel()
	}()

	// Load configuration
	cfg := config.NewConfig()
	log := logger.InitLogger(cfg.LogLevel)

	uiDistPath := "ui/dist"
	unzipDistPath := "ui"
	zipFilePath := "ui/dist.zip"
	if _, err := os.Stat(uiDistPath); os.IsNotExist(err) {
		log.Info("ui/dist does not exist. Unzipping dist.zip...")
		if err := utils.UnzipDistFile(zipFilePath, unzipDistPath); err != nil {
			log.Errorf("Failed to unzip dist.zip: %v", err)
			cancel()
			return
		}
		log.Info("dist.zip has been successfully unzipped.")
	}

	// Start backend synchronization
	var wg sync.WaitGroup
	for _, syncCfg := range cfg.SyncConfigs {
		if !syncCfg.Enable {
			continue
		}
		wg.Add(1)
		switch syncCfg.Type {
		case "mongodb":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMongoDBSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "mysql":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMySQLSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "mariadb":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewMariaDBSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "postgresql":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewPostgreSQLSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		case "redis":
			go func(syncCfg config.SyncConfig) {
				defer wg.Done()
				syncer := syncer.NewRedisSyncer(syncCfg, log)
				syncer.Start(ctx)
			}(syncCfg)
		default:
			log.Errorf("Unknown sync type: %s", syncCfg.Type)
			wg.Done()
		}
	}

	// Start monitoring goroutine: output row counts every minute for each mapped table (source/target)
	if cfg.EnableTableRowCountMonitoring {
		utils.StartRowCountMonitoring(ctx, cfg, log, monitorInterval)
	}

	// cmd/sync/main.go
	router := chi.NewRouter()
	router.Mount("/api", api.NewRouter())

	// Serve static files (index.html, etc.) from ui/dist folder
	// Serving static files from ui/dist
	router.Handle("/*", http.StripPrefix("/", http.FileServer(http.Dir("ui/dist"))))

	// Define HTTP server
	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	// Start HTTP server in a separate goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
	logger.Log.Info("Starting server on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("HTTP server ListenAndServe error: %v", err)
			cancel()
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Gracefully shutdown HTTP server
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Errorf("HTTP server Shutdown error: %v", err)
	} else {
		log.Info("HTTP server gracefully stopped")
	}

	// Wait for all goroutines to finish
	wg.Wait()
	log.Info("All synchronization tasks have completed.")

	// Final log
	log.Info("Program has exited")
}
