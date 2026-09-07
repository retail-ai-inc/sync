package mongodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
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
	Labels metrics.Labels

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
	payload, err := encodeClusterTime(at)
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

	tables, err := s.collections(ctx, source)
	if err != nil {
		return err
	}

	// Debezium's snapshot context: how many objects the copy covers and how
	// many are left. MongoDB is the one engine here that knows both up front.
	started := time.Now()
	metrics.SnapshotStarted(s.Labels, len(tables))
	remaining := len(tables)

	var failures []string
	for _, table := range tables {
		targetName := table.TargetTable
		if targetName == "" {
			targetName = table.SourceTable
		}

		// The collection has to exist, be partitioned the way the source is, and
		// carry the source's indexes — all before a document is copied. Sharding an
		// empty collection is immediate; sharding one that already holds the copy is
		// a migration.
		if err := s.Syncer.ensureCollectionExists(ctx, target, targetName); err != nil {
			failures = append(failures,
				fmt.Sprintf("%s.%s: could not create the target collection: %v",
					s.targetDB, targetName, err))
			continue
		}
		if err := s.Syncer.matchSharding(ctx, s.sourceDB, table.SourceTable, s.targetDB, targetName); err != nil {
			// Two sides partitioned differently is not a gap a retry closes,
			// and not one the other collections make up for, so it stops the
			// task rather than joining the list of things that went wrong.
			return err
		}

		if err := s.Syncer.doInitialSync(ctx,
			source.Collection(table.SourceTable),
			target.Collection(targetName),
			s.sourceDB, s.targetDB); err != nil {
			failures = append(failures,
				fmt.Sprintf("%s.%s: %v", s.sourceDB, table.SourceTable, err))
		} else if table.AdvancedSettings.SyncIndexes {
			// After the documents rather than before them. An index that exists
			// during the load is maintained one document at a time, against every
			// index the collection has; built afterwards it is a single bulk pass.
			// Nothing reads the target until the copy finishes, so the collection
			// loses nothing by being unindexed while it fills -- and the copy
			// finishing sooner is also what keeps the changes buffered behind it
			// from piling up.
			if err := s.Syncer.copyIndexes(ctx,
				source.Collection(table.SourceTable),
				target.Collection(targetName)); err != nil {
				// An index the target lacks makes it answer correctly and too
				// slowly to serve, which at a failover is its own outage — but
				// it is not a reason to leave the data uncopied.
				s.Logger.Warnf("[MongoDB] Could not copy the indexes of %s.%s: %v",
					s.sourceDB, table.SourceTable, err)
			}
		}
		remaining--
		metrics.SnapshotProgress(s.Labels, 0, remaining, time.Since(started).Seconds())
	}

	if len(failures) > 0 {
		return fmt.Errorf("the initial copy is incomplete, so the stream must not start "+
			"from it: %s", strings.Join(failures, "; "))
	}
	return nil
}

// collections reports what to copy, discovering it from the source when the
// task names nothing. The two halves of this used to disagree.
func (s *Snapshotter) collections(ctx context.Context, source *mongo.Database) ([]config.TableMapping, error) {
	var listed []config.TableMapping
	for _, mapping := range s.Config.Mappings {
		listed = append(listed, mapping.Tables...)
	}
	if len(listed) > 0 {
		return listed, nil
	}

	names, err := discovery.MongoCollections(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("the task names no collections, so they have to be "+
			"discovered from %s, and that failed: %w", s.sourceDB, err)
	}
	if len(names) == 0 {
		s.Logger.Warnf("[MongoDB] %s holds no collections to copy", s.sourceDB)
		return nil, nil
	}

	discovered := make([]config.TableMapping, 0, len(names))
	for _, name := range names {
		discovered = append(discovered, config.TableMapping{
			SourceTable: name,
			TargetTable: name,
			// A task that names its collections carries this per collection, and
			// nothing here overrides it. One that names none is copying a database
			// whole, which is a standby to fail over to -- and a standby that
			// answers every query with a collection scan is an outage of its own.
			// There is no per-collection setting to read in that case, so the
			// default has to be the one that leaves the copy usable.
			AdvancedSettings: config.AdvancedSettings{SyncIndexes: true},
		})
	}
	s.Logger.Infof("[MongoDB] The task names no collections, so all %d in %s are "+
		"being copied", len(discovered), s.sourceDB)
	return discovered, nil
}

type Syncer struct {
	cfg    config.SyncConfig
	global *config.Config
	logger *logrus.Logger
}

func NewSyncer(cfg config.SyncConfig, global *config.Config, logger *logrus.Logger) *Syncer {
	return &Syncer{cfg: cfg, global: global, logger: logger}
}

func (s *Syncer) Start(ctx context.Context) error {
	if err := security.CheckKeyForMappings(s.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	labels := metrics.Labels{"task": fmt.Sprint(s.cfg.ID), "engine": "mongodb"}
	sourceDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	targetDBName := dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection)
	metrics.SetTaskInfo(labels,
		dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		dsn.Endpoint(s.cfg.Type, s.cfg.TargetConnection))

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
	stopGuard, guardErr := inner.claimDirection(ctx, sourceDBName, targetDBName)
	if guardErr != nil {
		if directionlock.IsBlocking(guardErr) {
			return domain.Unrecoverable("%v", guardErr)
		}
		// Anything else is transient and the task is restarted for it: another
		// process still finishing its shutdown, or an endpoint that is briefly
		// unreachable — the guard reads its claims from the databases, so an
		// outage on either side fails it while the outage lasts.
		return fmt.Errorf("%w", guardErr)
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

	// An environment variable can turn off the guarantee the rest of this
	// pipeline is built on, so it says so where somebody will see it. A batch
	// applied outside a transaction can be half applied, and the position is then
	// recorded past changes the target does not have: the copy is quietly wrong,
	// and the next consistency check is what finds it, if one is running.
	bare := noTransaction()
	if warning := describeNoTransaction(bare, s.cfg.ID); warning != "" {
		s.logger.Warn(warning)
	}

	runner := &pipeline.Runner{
		Reader: reader,
		Applier: &Applier{
			Client: inner.targetClient,
			// The source, for the changes that cannot be written as the fields
			// they touched and have to be read back whole.
			Source:         inner.sourceClient,
			Mask:           inner.maskValue,
			TargetDatabase: targetDBName,
			Mappings:       s.cfg.Mappings,
			Checkpoints:    store,
			Logger:         s.logger,
			Labels:         labels,
			NoTransaction:  bare,
		},
		Snapshotter: &Snapshotter{
			Syncer:   inner,
			Config:   s.cfg,
			Logger:   s.logger,
			Labels:   labels,
			sourceDB: sourceDBName,
			targetDB: targetDBName,
		},
		Checkpoints: store,
		Resyncs:     s.resyncs(inner, store, sourceDBName),
		Opts: pipeline.Options{
			// The changes of a batch go to the target in the order the stream held
			// them. The alternative splits a batch into runs that may be applied
			// independently, deciding independence by the _id: two changes to one
			// document keep their order, everything else may move.
			StreamOrder: true,
			Labels:      labels,
			Logger:      s.logger,
			Engine:      "MongoDB",
		},
	}

	// A task that names its collections means it, so nothing here widens the
	// scope. What it does is say which collections are not in the copy, because
	// the alternative is finding out during a failover.
	go s.watchSourceCollections(ctx, inner, sourceDBName, labels)

	s.logger.Info("[MongoDB] Starting synchronization...")
	return runner.Run(ctx)
}

// watchSourceCollections scans the source on an interval and publishes what it
// finds: the collections this task does not replicate, for a task that lists
// its collections, and the number it captures, for one that lists none.
//
// A task that lists its collections has a gap whenever the source grows
// another, and the gap is invisible. A task that lists none has no gap, but its
// captured count is knowable only here -- the reader counts the task's list,
// which is empty in exactly that case.
func (s *Syncer) watchSourceCollections(ctx context.Context, inner *MongoDBSyncer,
	sourceDB string, labels metrics.Labels) {

	listed := map[string]bool{}
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable != "" {
				listed[strings.ToLower(table.SourceTable)] = true
			}
		}
	}
	database := inner.sourceClient.Database(sourceDB)
	warned := map[string]bool{}

	discovery.Poll(ctx, unlistedScanEvery, func() {
		names, err := discovery.MongoCollections(ctx, database)
		if err != nil {
			s.logger.Debugf("[MongoDB] Could not list the collections in %s: %v",
				sourceDB, err)
			return
		}
		if len(listed) == 0 {
			// The task replicates the database as a whole, so it captures whatever
			// the source holds, and that moves as collections are created.
			metrics.SetCapturedTables(labels, len(names))
			return
		}
		missing := discovery.Unlisted(listed, warned, names)
		if len(missing) == 0 {
			return
		}
		s.logger.Warnf("[MongoDB] %s holds %d collections this task does not "+
			"replicate: %v. They are not in the disaster-recovery copy. Add them to "+
			"the task, or remove every collection from it to replicate the database "+
			"as a whole.", sourceDB, len(missing), missing)
		metrics.SetUnreplicated(labels, float64(len(warned)))
	})
}

// resyncs builds a re-copy for each object the task asks to have re-copied.
//
// Its progress is stored beside the position, on the target, so an interrupted
// repair resumes rather than starting the object again.
func (s *Syncer) resyncs(inner *MongoDBSyncer, store *checkpoint.MongoStore, sourceDB string) []*pipeline.Resync {
	if len(s.cfg.Resync) == 0 {
		return nil
	}
	chunks := &Chunks{Client: inner.sourceClient, Database: sourceDB, Masker: inner}

	var out []*pipeline.Resync
	for _, name := range s.cfg.Resync {
		ns := domain.Namespace{DB: sourceDB, Object: name}
		out = append(out, &pipeline.Resync{
			NS:          ns,
			Reader:      chunks,
			Progress:    store,
			ProgressKey: "resync:" + ns.String(),
		})
		s.logger.Infof("[MongoDB] %s is listed for a re-copy alongside the stream", ns)
	}
	return out
}

// PurgeCheckpoints removes what a task left on its target, for a task that is
// being deleted.
func PurgeCheckpoints(ctx context.Context, cfg config.SyncConfig) error {
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.TargetConnection))
	if err != nil {
		return fmt.Errorf("connect to the target: %w", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()

	store := &checkpoint.MongoStore{
		Database: client.Database(dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection)),
		TaskID:   cfg.ID,
	}
	return store.Purge(ctx)
}
