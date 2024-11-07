package config

import (
    "github.com/sirupsen/logrus"
    "go.mongodb.org/mongo-driver/mongo/options"
)

// CollectionMapping 定义源集合和目标集合的映射关系
type CollectionMapping struct {
    SourceCollection string
    TargetCollection string
}

// SyncMapping 定义源数据库和目标数据库的映射关系，以及集合映射列表
type SyncMapping struct {
    SourceDatabase   string
    TargetDatabase   string
    Collections      []CollectionMapping // 集合映射列表
}

// 配置结构体
type Config struct {
    ClusterAURI    string
    StandaloneBURI string
    SyncMappings   []SyncMapping
    Logger         *logrus.Logger
}

// NewConfig 返回配置实例
func NewConfig() *Config {
    return &Config{
        ClusterAURI: "mongodb://root:WcLOIVWfG8I4owE0@localhost:27017/admin",
        StandaloneBURI: "mongodb://root:WcLOIVWfG8I4owE0@localhost:27017/admin",
        // 需要同步的数据库和集合映射
        SyncMappings: []SyncMapping{
            {
                SourceDatabase: "test",
                TargetDatabase: "test",
                Collections: []CollectionMapping{
                    {SourceCollection: "test", TargetCollection: "test2"},
                    {SourceCollection: "test3", TargetCollection: "test4"},
                    // 添加更多的集合映射
                },
            },
            {
                SourceDatabase: "test2",
                TargetDatabase: "test2",
                Collections: []CollectionMapping{
                    {SourceCollection: "test", TargetCollection: "test2"},
                    // 添加更多的集合映射
                },
            },
            // 添加其他数据库的映射，共计 7 个
        },

        // 日志器
        Logger: logrus.New(),
    }
}

// MongoDB 客户端选项
func (cfg *Config) GetClientOptions(uri string) *options.ClientOptions {
    return options.Client().ApplyURI(uri)
}
