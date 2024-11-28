package config

import (
    "encoding/json"
    "io/ioutil"
    "log"
    "os"

    "github.com/sirupsen/logrus"
    "go.mongodb.org/mongo-driver/mongo/options"
)

// CollectionMapping defines the mapping between source and target collections
type CollectionMapping struct {
    SourceCollection string `json:"source_collection"`
    TargetCollection string `json:"target_collection"`
}

// SyncMapping defines the source and target databases, and the collection mappings
type SyncMapping struct {
    SourceDatabase   string             `json:"source_database"`
    TargetDatabase   string             `json:"target_database"`
    Collections      []CollectionMapping `json:"collections"` // List of collection mappings
}

// Configuration struct
type Config struct {
    ClusterAURI    string        `json:"cluster_a_uri"`
    StandaloneBURI string        `json:"standalone_b_uri"`
    SyncMappings   []SyncMapping `json:"sync_mappings"`
    Logger         *logrus.Logger
}

// NewConfig returns a new configuration instance
func NewConfig() *Config {
    // Try to get the configuration file path from the environment variable
    configPath := os.Getenv("CONFIG_PATH")
    if configPath == "" {
        configPath = "config.json" // Default configuration file path
    }

    // Read the configuration file
    data, err := ioutil.ReadFile(configPath)
    if err != nil {
        log.Fatalf("Failed to read configuration file: %v", err)
    }

    // Parse the configuration file
    var cfg Config
    if err := json.Unmarshal(data, &cfg); err != nil {
        log.Fatalf("Failed to parse configuration file: %v", err)
    }

    // Initialize logger
    cfg.Logger = logrus.New()
    
    return &cfg
}

// MongoDB client options
func (cfg *Config) GetClientOptions(uri string) *options.ClientOptions {
    return options.Client().ApplyURI(uri)
}
