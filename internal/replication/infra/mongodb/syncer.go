package mongodb

import (
	"context"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// Snapshotter makes the first copy of a MongoDB source.
//
// One position covers the whole copy, not one per collection. A stream opened on
// the deployment resumes from a single token, so the copy has a single starting
// point — the cluster time pinned before a document is read.
type Snapshotter struct {
	Syncer *MongoDBSyncer
	Config config.SyncConfig
	Logger logrus.FieldLogger

	sourceDB string
	targetDB string
}

// Pin records the cluster time the copy reads at.
//
// It runs before a document is copied. Reading it afterwards loses every write
// made while the copy was running, and there is nothing to show that it happened
// — the collections are all present and the counts are plausible.
func (s *Snapshotter) Pin(ctx context.Context) (domain.Position, error) {
	at, err := s.Syncer.clusterTime(ctx)
	if err != nil {
		return domain.Position{}, fmt.Errorf("pin the snapshot's cluster time: %w", err)
	}
	payload, err := checkpoint.Encode(startPosition{Cluster: at.T, Increment: at.I})
	if err != nil {
		return domain.Position{}, fmt.Errorf("encode the pinned cluster time: %w", err)
	}
	s.Logger.Infof("[MongoDB] Snapshot pinned at cluster time %d.%d", at.T, at.I)
	return domain.Position{Payload: payload}, nil
}

// Copy fills the target with every mapped collection.
//
// A collection that cannot be copied does not stop the others, but it is
// reported: the caller must not record the position for an incomplete copy,
// because the stream would then carry on from a point that assumes a complete
// base and the gap would stay for good.
func (s *Snapshotter) Copy(ctx context.Context) error {
	source := s.Syncer.sourceClient.Database(s.sourceDB)
	target := s.Syncer.targetClient.Database(s.targetDB)

	var failures []string
	for _, mapping := range s.Config.Mappings {
		for _, table := range mapping.Tables {
			targetName := table.TargetTable
			if targetName == "" {
				targetName = table.SourceTable
			}
			if err := s.Syncer.doInitialSync(ctx,
				source.Collection(table.SourceTable),
				target.Collection(targetName),
				s.sourceDB, s.targetDB); err != nil {
				failures = append(failures,
					fmt.Sprintf("%s.%s: %v", s.sourceDB, table.SourceTable, err))
			}
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("the initial copy is incomplete, so the stream must not start "+
			"from it: %s", strings.Join(failures, "; "))
	}
	return nil
}

// startPosition is a pinned cluster time, stored the way the change stream
// resumes from one.
type startPosition struct {
	Cluster   uint32 `json:"cluster"`
	Increment uint32 `json:"increment"`
}

// Syncer replicates one MongoDB deployment through the shared pipeline.
type Syncer struct {
	cfg    config.SyncConfig
	global *config.Config
	logger *logrus.Logger
}

// NewSyncer builds the syncer for one task.
func NewSyncer(cfg config.SyncConfig, global *config.Config, logger *logrus.Logger) *Syncer {
	return &Syncer{cfg: cfg, global: global, logger: logger}
}

// Start replicates until the context is cancelled or the stream cannot carry on.
func (s *Syncer) Start(ctx context.Context) error {
	if err := security.CheckKeyForMappings(s.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	labels := metrics.Labels{"task": fmt.Sprint(s.cfg.ID), "engine": "mongodb"}
	sourceDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	targetDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection)

	inner := NewMongoDBSyncer(s.cfg, s.global, s.logger)
	if inner.sourceClient == nil || inner.targetClient == nil {
		if isURIError(inner.connectErr) {
			return domain.Unrecoverable("connect to MongoDB: %v", inner.connectErr)
		}
		if inner.connectErr != nil {
			return fmt.Errorf("connect to the source and target: %w", inner.connectErr)
		}
		return fmt.Errorf("connect to the source and target")
	}

	// Nothing is read or written until the direction is agreed.
	inner.checkpoints = inner.checkpointStore(targetDBName)
	stopGuard, guardErr := inner.claimDirection(ctx, sourceDBName, targetDBName)
	if guardErr != nil {
		return domain.Unrecoverable("%v", guardErr)
	}
	defer stopGuard()

	metrics.SetTaskUp(labels, true)
	defer metrics.SetTaskUp(labels, false)

	// One position for the whole task, on the target, which is the side that
	// survives the outage this exists for.
	store := &checkpoint.MongoStore{
		Database: inner.targetClient.Database(targetDBName),
		TaskID:   s.cfg.ID,
	}

	reader := &Reader{
		Client: inner.sourceClient,
		Config: s.cfg,
		Logger: s.logger,
		Labels: labels,
	}
	defer reader.Close()

	runner := &pipeline.Runner{
		Reader: reader,
		Applier: &Applier{
			Client:         inner.targetClient,
			TargetDatabase: targetDBName,
			Mappings:       s.cfg.Mappings,
			Checkpoints:    store,
			Logger:         s.logger,
			NoTransaction:  noTransaction(),
		},
		Snapshotter: &Snapshotter{
			Syncer:   inner,
			Config:   s.cfg,
			Logger:   s.logger,
			sourceDB: sourceDBName,
			targetDB: targetDBName,
		},
		Checkpoints: store,
		Opts: pipeline.Options{
			Labels: labels,
			Logger: s.logger,
			Engine: "MongoDB",
		},
	}

	s.logger.Info("[MongoDB] Starting synchronization...")
	return runner.Run(ctx)
}
