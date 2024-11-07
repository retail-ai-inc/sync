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
    // 创建上下文
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // 捕获系统中断信号
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c
        utils.Log.Info("收到中断信号，正在退出...")
        cancel()
    }()

    // 加载配置
    cfg := config.NewConfig()
    utils.InitLogger(cfg.Logger)

    // 连接到 MongoDB 集群 A
    clientA, err := mongo.Connect(ctx, cfg.GetClientOptions(cfg.ClusterAURI))
    if err != nil {
        utils.Log.Fatalf("连接到集群 A 失败：%v", err)
    }
    defer clientA.Disconnect(ctx)

    // 连接到 MongoDB 单机 B
    clientB, err := mongo.Connect(ctx, cfg.GetClientOptions(cfg.StandaloneBURI))
    if err != nil {
        utils.Log.Fatalf("连接到单机 B 失败：%v", err)
    }
    defer clientB.Disconnect(ctx)

    utils.Log.Info("成功连接到 MongoDB 集群 A 和单机 B")

    // 启动同步器
    mySyncer := syncer.NewSyncer(clientA, clientB, cfg.SyncMappings, cfg.Logger)
    mySyncer.Start(ctx)

    // 等待程序结束
    <-ctx.Done()
    utils.Log.Info("程序已退出")
}
