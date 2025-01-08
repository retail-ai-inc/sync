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

	// Wait for all sync to complete
	wg.Wait()
	logger.Log.Info("All synchronization tasks have completed.")

	// Wait for program to end
	<-ctx.Done()
	logger.Log.Info("Program has exited")
}