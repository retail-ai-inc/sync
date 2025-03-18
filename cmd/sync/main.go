package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/retail-ai-inc/sync/pkg/api"
	"github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/logger"
	"github.com/retail-ai-inc/sync/pkg/syncer"
	"github.com/retail-ai-inc/sync/pkg/utils"
	"github.com/sirupsen/logrus"
)

const monitorInterval = time.Second * 60

func main() {
	cfg := config.NewConfig()
	log := logger.InitLogger(cfg.LogLevel)

	if _, err := os.Stat("ui/dist"); os.IsNotExist(err) {
		log.Info("ui/dist directory does not exist, extracting ui/dist.zip...")
		if err := utils.UnzipDistFile("ui/dist.zip", "ui/"); err != nil {
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

	if cfg.EnableTableRowCountMonitoring {
		utils.StartRowCountMonitoring(ctx, cfg, log, monitorInterval)
	}

	router := chi.NewRouter()
	router.Mount("/api", api.NewRouter())

	router.Get("/*", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path

		cleanPath := filepath.Clean(path)
		if strings.Contains(cleanPath, "..") || strings.HasPrefix(cleanPath, "/") {
			http.Error(w, "invaild path", http.StatusBadRequest)
			return
		}
		filePath := filepath.Join("ui/dist", cleanPath)

		_, err := os.Stat(filePath)
		fileExists := !os.IsNotExist(err)

		if fileExists {
			http.StripPrefix("/", http.FileServer(http.Dir("ui/dist"))).ServeHTTP(w, r)
			return
		}

		http.ServeFile(w, r, "ui/dist/index.html")
	})

	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}
	go func() {
		log.Info("UI is running at http://localhost:8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("HTTP server error: %v", err)
			cancel()
		}
	}()

	go runSyncTasks(ctx, log, cfg)

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Errorf("HTTP server Shutdown error: %v", err)
	} else {
		log.Info("HTTP server gracefully stopped")
	}

	time.Sleep(2 * time.Second)
	log.Info("Program exited")
}

func runSyncTasks(parentCtx context.Context, log *logrus.Logger, cfg *config.Config) {
	configReloadInterval := 10 * time.Second
	currentConfig := cfg
	var wg sync.WaitGroup
	syncCtx, syncCancel := context.WithCancel(parentCtx)
	startSyncTasks(syncCtx, currentConfig, &wg, log)
	ticker := time.NewTicker(configReloadInterval)
	defer ticker.Stop()

	// Monitor for config changes and restart row count monitoring if needed
	var rowCountMonitorCancel context.CancelFunc
	var rowCountMonitorCtx context.Context // Declare the context variable here

	for {
		select {
		case <-parentCtx.Done():
			syncCancel()
			wg.Wait()
			if rowCountMonitorCancel != nil {
				rowCountMonitorCancel()
			}
			return
		case <-ticker.C:
			newConfig := config.NewConfig()
			if !configsEqual(currentConfig, newConfig) {
				log.Info("Config change detected, restarting sync tasks and row count monitoring...")
				syncCancel()
				wg.Wait()

				// Stop the row count monitoring if it was started
				if rowCountMonitorCancel != nil {
					rowCountMonitorCancel()
				}

				currentConfig = newConfig
				syncCtx, syncCancel = context.WithCancel(parentCtx)
				wg = sync.WaitGroup{}
				startSyncTasks(syncCtx, currentConfig, &wg, log)

				// Restart row count monitoring if needed
				if currentConfig.EnableTableRowCountMonitoring {
					// Cancel the previous row count monitoring context (if any)
					if rowCountMonitorCancel != nil {
						rowCountMonitorCancel()
					}
					// Start a new row count monitoring with a fresh context
					rowCountMonitorCtx, rowCountMonitorCancel = context.WithCancel(parentCtx)
					utils.StartRowCountMonitoring(rowCountMonitorCtx, currentConfig, log, monitorInterval)
				}
			}
		}
	}
}

func startSyncTasks(ctx context.Context, cfg *config.Config, wg *sync.WaitGroup, log *logrus.Logger) {
	for _, syncCfg := range cfg.SyncConfigs {
		if !syncCfg.Enable {
			continue
		}
		wg.Add(1)
		switch syncCfg.Type {
		case "mongodb":
			go func(sc config.SyncConfig) {
				defer wg.Done()
				syncer.NewMongoDBSyncer(sc, log).Start(ctx)
			}(syncCfg)
		case "mysql", "mariadb":
			go func(sc config.SyncConfig) {
				defer wg.Done()
				syncer.NewMySQLSyncer(sc, log).Start(ctx)
			}(syncCfg)
		case "postgresql":
			go func(sc config.SyncConfig) {
				defer wg.Done()
				syncer.NewPostgreSQLSyncer(sc, log).Start(ctx)
			}(syncCfg)
		case "redis":
			go func(sc config.SyncConfig) {
				defer wg.Done()
				syncer.NewRedisSyncer(sc, log).Start(ctx)
			}(syncCfg)
		default:
			log.Errorf("Unknown sync type: %s", syncCfg.Type)
			wg.Done()
		}
	}
}

func configsEqual(c1, c2 *config.Config) bool {
	b1, err1 := json.Marshal(c1.SyncConfigs)
	b2, err2 := json.Marshal(c2.SyncConfigs)
	if err1 != nil || err2 != nil {
		return false
	}
	return bytes.Equal(b1, b2)
}
