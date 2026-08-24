package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// Snapshotter makes the first copy of a MySQL source.
//
// Pin and Copy share one pinned connection on purpose: the coordinates and every
// SELECT that reads through them have to be the same session, inside one
// consistent snapshot. Reading the coordinates afterwards — which is what
// starting the stream with no stored position amounts to — loses every write
// made while the copy was running, and the copy of a payment table runs for as
// long as it runs.
type Snapshotter struct {
	Config config.SyncConfig
	Target *sql.DB
	Logger logrus.FieldLogger

	source *sql.DB
	conn   *sql.Conn
	syncer *MySQLSyncer
}

// Pin opens the consistent snapshot and reads the coordinates the stream will
// resume from.
func (s *Snapshotter) Pin(ctx context.Context) (domain.Position, error) {
	source, err := sql.Open("mysql", s.Config.SourceConnection)
	if err != nil {
		return domain.Position{}, fmt.Errorf("open the source: %w", err)
	}
	s.source = source

	conn, err := source.Conn(ctx)
	if err != nil {
		source.Close()
		return domain.Position{}, fmt.Errorf("pin a source connection: %w", err)
	}
	s.conn = conn

	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		s.release()
		return domain.Position{}, fmt.Errorf("open a consistent snapshot: %w", err)
	}

	s.syncer = &MySQLSyncer{cfg: s.Config, logger: s.Logger}
	pinned, err := s.syncer.sourceCheckpoint(ctx, conn)
	if err != nil {
		s.release()
		return domain.Position{}, fmt.Errorf("read the source's binlog coordinates: %w. "+
			"The copy cannot start without them, because every write made while it ran "+
			"would then belong to neither the copy nor the stream", err)
	}
	pinned.Source = dsn.Endpoint(s.Config.Type, s.Config.SourceConnection)

	payload, err := checkpoint.Encode(*pinned)
	if err != nil {
		s.release()
		return domain.Position{}, fmt.Errorf("encode the pinned position: %w", err)
	}
	s.Logger.Infof("[MySQL] Snapshot pinned at %+v", *pinned)
	return domain.Position{Payload: payload}, nil
}

// Copy fills the target, reading through the pinned connection.
func (s *Snapshotter) Copy(ctx context.Context) error {
	if s.conn == nil {
		return fmt.Errorf("the snapshot was not pinned")
	}
	defer s.release()
	return s.syncer.doInitialSync(ctx, s.conn, s.Target)
}

func (s *Snapshotter) release() {
	if s.conn != nil {
		_, _ = s.conn.ExecContext(context.Background(), "COMMIT")
		s.conn.Close()
		s.conn = nil
	}
	if s.source != nil {
		s.source.Close()
		s.source = nil
	}
}

// Syncer replicates one MySQL server through the shared pipeline.
//
// One of these covers every database the task maps: the binlog is a single log
// per server, so a syncer per database opened several dump connections to read
// the same bytes.
type Syncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger
}

// NewSyncer builds the syncer for one task.
func NewSyncer(cfg config.SyncConfig, logger *logrus.Logger) *Syncer {
	return &Syncer{cfg: cfg, logger: logger}
}

// Start replicates until the context is cancelled or the stream cannot carry on.
func (s *Syncer) Start(ctx context.Context) error {
	if err := security.CheckKeyForMappings(s.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	labels := metrics.Labels{"task": fmt.Sprint(s.cfg.ID), "engine": "mysql"}

	var targetDB *sql.DB
	err := resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		targetDB, connErr = sql.Open("mysql", s.cfg.TargetConnection)
		if connErr != nil {
			return connErr
		}
		return targetDB.PingContext(ctx)
	})
	if err != nil {
		return fmt.Errorf("connect to the target: %w", err)
	}
	defer targetDB.Close()

	// Nothing is read or written until the direction is agreed. A target that
	// has been promoted, or a source that is itself somebody's target, means the
	// pair has been reversed and carrying on would overwrite the newer side with
	// the older one.
	guard := &MySQLSyncer{cfg: s.cfg, logger: s.logger}
	releaseGuard, guardErr := guard.claimDirection(ctx, targetDB)
	if guardErr != nil {
		return domain.Unrecoverable("%v", guardErr)
	}
	defer releaseGuard()

	metrics.SetTaskUp(labels, true)
	defer metrics.SetTaskUp(labels, false)

	// The position lives on the target, which is the side that survives the
	// outage this setup exists for, and it is written in the transaction that
	// applies the data.
	store := &checkpoint.SQLStore{
		DB:     targetDB,
		Schema: dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection),
		TaskID: s.cfg.ID,
	}
	if err := store.Ensure(ctx); err != nil {
		return fmt.Errorf("prepare the position table: %w", err)
	}

	reader := &Reader{Config: s.cfg, Logger: s.logger, Labels: labels}
	defer reader.Close()

	runner := &pipeline.Runner{
		Reader: reader,
		Applier: &Applier{
			DB:          targetDB,
			Checkpoints: store,
			Logger:      s.logger,
		},
		Snapshotter: &Snapshotter{Config: s.cfg, Target: targetDB, Logger: s.logger},
		Checkpoints: store,
		Resyncs:     s.resyncs(store),
		Opts: pipeline.Options{
			Labels: labels,
			Logger: s.logger,
			Engine: "MySQL",
		},
	}

	s.logger.Info("[MySQL] Starting synchronization...")
	return runner.Run(ctx)
}

// resyncs builds a re-copy for each table the task asks to have re-copied.
func (s *Syncer) resyncs(store *checkpoint.SQLStore) []*pipeline.Resync {
	if len(s.cfg.Resync) == 0 {
		return nil
	}
	source, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Errorf("[MySQL] Cannot open a second source connection for the "+
			"re-copy, so nothing will be re-copied: %v", err)
		return nil
	}

	sourceDB := dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection)
	targetDB := dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection)
	handler := &MyEventHandler{mappings: s.cfg.Mappings}
	chunks := &Chunks{
		Source:         source,
		Database:       sourceDB,
		TargetDatabase: targetDB,
		TargetOf: func(table string) string {
			if targets := handler.targetsFor(sourceDB, table); len(targets) > 0 {
				return targets[0]
			}
			return table
		},
	}

	var out []*pipeline.Resync
	for _, name := range s.cfg.Resync {
		ns := domain.Namespace{DB: sourceDB, Object: name}
		out = append(out, &pipeline.Resync{
			NS:          ns,
			Reader:      chunks,
			Progress:    store,
			ProgressKey: "resync:" + ns.String(),
		})
		s.logger.Infof("[MySQL] %s is listed for a re-copy alongside the stream", ns)
	}
	return out
}
