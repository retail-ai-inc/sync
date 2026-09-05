package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/internal/replication/infra/discovery"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// Snapshotter makes the first copy of a MySQL source. Pin and Copy share one
// pinned connection on purpose: the coordinates and every SELECT that reads
// through them have to be the same session, inside one consistent snapshot.
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

func NewSyncer(cfg config.SyncConfig, logger *logrus.Logger) *Syncer {
	return &Syncer{cfg: cfg, logger: logger}
}

func (s *Syncer) Start(ctx context.Context) error {
	if err := security.CheckKeyForMappings(s.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	labels := metrics.Labels{"task": fmt.Sprint(s.cfg.ID), "engine": "mysql"}
	metrics.SetTaskInfo(labels,
		dsn.Endpoint(s.cfg.Type, s.cfg.SourceConnection),
		dsn.Endpoint(s.cfg.Type, s.cfg.TargetConnection))

	targetConnection, err := withoutForeignKeyChecks(s.cfg.TargetConnection)
	if err != nil {
		return fmt.Errorf("read the target connection: %w", err)
	}

	var targetDB *sql.DB
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		targetDB, connErr = sql.Open("mysql", targetConnection)
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
		if directionlock.IsBlocking(guardErr) {
			return domain.Unrecoverable("%v", guardErr)
		}
		// Anything else is transient and the task is restarted for it: another
		// process still finishing its shutdown, or an endpoint that is briefly
		// unreachable — the guard reads its claims from the databases, so an
		// outage on either side fails it while the outage lasts.
		return fmt.Errorf("%w", guardErr)
	}
	defer releaseGuard()

	// The target's own settings, now that there is a connection to it and the
	// direction is settled. Checked here rather than beside the source check,
	// which runs before either endpoint is open.
	if err := targetPreflight(ctx, targetDB,
		dsn.GetDatabaseName(s.cfg.Type, s.cfg.TargetConnection),
		s.targetTables(), s.logger); err != nil {
		return err
	}

	metrics.SetTaskUp(labels, true)
	defer metrics.SetTaskUp(labels, false)

	// What the source has to be set up to do, asked before the snapshot rather
	// than after it: a source that cannot be replicated correctly should cost
	// nothing to discover, and discovering it after the copy costs the copy.
	if err := s.checkSource(ctx); err != nil {
		return err
	}

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

	// A task that names its tables means it, so nothing here widens the scope.
	// What it does is say which tables are not in the copy, because the
	// alternative is finding out during a failover.
	go s.watchSourceTables(ctx, dsn.GetDatabaseName(s.cfg.Type, s.cfg.SourceConnection), labels)

	runner := &pipeline.Runner{
		Reader: reader,
		Applier: &Applier{
			DB:          targetDB,
			Checkpoints: store,
			Logger:      s.logger,
			Labels:      labels,
		},
		Snapshotter: &Snapshotter{Config: s.cfg, Target: targetDB, Logger: s.logger},
		Checkpoints: store,
		Resyncs:     s.resyncs(store),
		Opts: pipeline.Options{
			// The statements of a batch go to the target in the order the binlog held
			// them. Splitting a batch into runs exists to let an applier work on
			// several at once, and this one does not: it executes every run, and every
			// statement in it, one after another inside a single transaction.
			StreamOrder: true,
			Labels:      labels,
			Logger:      s.logger,
			Engine:      "MySQL",
		},
	}

	s.logger.Info("[MySQL] Starting synchronization...")
	return runner.Run(ctx)
}

// checkSource opens a short-lived connection to the source and runs the
// preflight checks over it.
//
// It is its own connection because the stream's is canal's and the snapshot's is
// pinned inside a consistent read; neither is available at this point, and both
// are made after the answer here decides whether to carry on at all.
func (s *Syncer) checkSource(ctx context.Context) error {
	source, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		return fmt.Errorf("open the source to check its settings: %w", err)
	}
	defer source.Close()

	return preflight(ctx, source, s.cfg.Type, s.logger)
}

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
		// The same field security the stream applies. A re-copy reads the source
		// rows directly, so without this it writes what the stream masks.
		Mappings: s.cfg.Mappings,
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

// unlistedScanEvery is how often a task that names its tables is compared
// against what the source actually holds.
// withoutForeignKeyChecks turns off foreign key enforcement for every
// connection the target pool hands out.
//
// A copy is not an application: it reproduces what the source holds, in the
// order the source's own log gives it, and a foreign key checked on the way in
// rejects both. It rejects a child row copied before its parent, because the
// copy walks tables by name and not by dependency. And it rejects a row the
// source itself holds in violation -- a source loaded with the checks off keeps
// orphans, and a standby that refuses them is not a copy of the source but an
// opinion about it, missing exactly the rows nobody knew were there.
//
// It is set on the connection rather than per statement because database/sql
// hands out whichever pooled connection is free.
func withoutForeignKeyChecks(connection string) (string, error) {
	cfg, err := mysqldriver.ParseDSN(connection)
	if err != nil {
		return "", err
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	cfg.Params["foreign_key_checks"] = "0"
	return cfg.FormatDSN(), nil
}

const unlistedScanEvery = 5 * time.Minute

// targetTables names the tables this task writes to, which is what makes the
// target's trigger check specific rather than a scan of the whole schema.
//
// A mapping that names no target table writes to one with the source's name.
func (s *Syncer) targetTables() []string {
	var tables []string
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			switch {
			case table.TargetTable != "":
				tables = append(tables, table.TargetTable)
			case table.SourceTable != "":
				tables = append(tables, table.SourceTable)
			}
		}
	}
	return tables
}

// watchSourceTables scans the source on an interval and publishes what it
// finds: the tables this task does not replicate, for a task that names its
// tables, and the number it captures, for one that names none.
//
// It never starts replicating an unlisted table: a task that names its tables
// means it, and quietly widening the scope would be worse than the gap.
func (s *Syncer) watchSourceTables(ctx context.Context, sourceDBName string,
	labels metrics.Labels) {

	listed := map[string]bool{}
	for _, mapping := range s.cfg.Mappings {
		for _, table := range mapping.Tables {
			if table.SourceTable != "" {
				listed[strings.ToLower(table.SourceTable)] = true
			}
		}
	}
	source, err := sql.Open("mysql", s.cfg.SourceConnection)
	if err != nil {
		s.logger.Debugf("[MySQL] Could not open the source to check which tables it "+
			"holds: %v", err)
		return
	}
	defer source.Close()

	warned := map[string]bool{}
	discovery.Poll(ctx, unlistedScanEvery, func() {
		tables, err := discovery.MySQLTables(ctx, source, sourceDBName)
		if err != nil {
			s.logger.Debugf("[MySQL] Could not list the tables in %s: %v", sourceDBName, err)
			return
		}
		if len(listed) == 0 {
			// The task replicates the database as a whole, so it captures whatever
			// the source holds, and that moves as tables are created.
			metrics.SetCapturedTables(labels, len(tables))
			return
		}
		missing := discovery.Unlisted(listed, warned, tables)
		if len(missing) == 0 {
			return
		}
		s.logger.Warnf("[MySQL] %s holds %d tables this task does not replicate: %v. "+
			"They are not in the disaster-recovery copy. Add them to the task, or "+
			"remove every table from it to replicate the database as a whole.",
			sourceDBName, len(missing), missing)
		metrics.SetUnreplicated(labels, float64(len(warned)))
	})
}

// PurgeCheckpoints removes what a task left on its target, for a task that is
// being deleted. Best effort by design: a target that cannot be reached must
// not make a task undeletable.
func PurgeCheckpoints(ctx context.Context, cfg config.SyncConfig) error {
	target, err := sql.Open("mysql", cfg.TargetConnection)
	if err != nil {
		return fmt.Errorf("connect to the target: %w", err)
	}
	defer target.Close()

	store := &checkpoint.SQLStore{
		DB:     target,
		Schema: dsn.GetDatabaseName(cfg.Type, cfg.TargetConnection),
		TaskID: cfg.ID,
	}
	return store.Purge(ctx)
}
