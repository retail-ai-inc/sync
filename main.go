package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"

    "go.mongodb.org/mongo-driver/mongo"
    "mongodb_sync/config"
    "mongodb_sync/syncer"
    "mongodb_sync/utils"
)

func main() {
    // Create a context
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Capture system interrupt signal
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c
        utils.Log.Info("Received interrupt signal, exiting...")
        cancel()
    }()

    // Load configuration
    cfg := config.NewConfig()
    utils.InitLogger(cfg.Logger)

    // Connect to MongoDB cluster A
    clientA, err := mongo.Connect(ctx, cfg.GetClientOptions(cfg.ClusterAURI))
    if err != nil {
        utils.Log.Fatalf("Failed to connect to cluster A: %v", err)
    }
    defer clientA.Disconnect(ctx)

    // Connect to standalone MongoDB B
    clientB, err := mongo.Connect(ctx, cfg.GetClientOptions(cfg.StandaloneBURI))
    if err != nil {
        utils.Log.Fatalf("Failed to connect to standalone B: %v", err)
    }
    defer clientB.Disconnect(ctx)

    utils.Log.Info("Successfully connected to MongoDB cluster A and standalone B")

    // Start syncer
    mySyncer := syncer.NewSyncer(clientA, clientB, cfg.SyncMappings, cfg.Logger)
    mySyncer.Start(ctx)

    // Wait for program to end
    <-ctx.Done()
    utils.Log.Info("Program has exited")
}