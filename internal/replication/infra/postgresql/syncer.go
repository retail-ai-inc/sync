package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/internal/replication/infra/security"
)

// PostgreSQL through the same pipeline as the other three engines.
//
// It used to have its own loop: decode a message, write the row, record the
// position per commit. That is why it was the one link with no batching, no
// byte budget, no queue depth, no retention window and no lag beyond a count of
// rows applied -- and the one whose reader and applier could not be tested
// without a PostgreSQL, because both were methods on a struct holding a
// *pgx.Conn.
//
// The decode, the write and the copy are now the three ports the pipeline
// takes, so the shared machinery applies and each half can be exercised on its
// own.

type Syncer struct {
	cfg    config.SyncConfig
	logger logrus.FieldLogger
}

func NewSyncer(cfg config.SyncConfig, logger *logrus.Logger) *Syncer {
	return &Syncer{cfg: cfg, logger: logger.WithField("sync_task_id", cfg.ID)}
}

// NewPostgreSQLSyncer is the name the supervisor knows.
func NewPostgreSQLSyncer(cfg config.SyncConfig, logger *logrus.Logger) *Syncer {
	return NewSyncer(cfg, logger)
}

func (s *Syncer) Start(ctx context.Context) error {
	if err := security.CheckKeyForMappings(s.cfg.Mappings); err != nil {
		return domain.Unrecoverable("%v", err)
	}

	slot := s.cfg.PGReplicationSlot()
	plugin := s.cfg.PGPlugin()
	if slot == "" || plugin == "" {
		return domain.Unrecoverable("this task names no pg_replication_slot or " +
			"pg_plugin, so there is nothing to read changes from")
	}

	labels := s.labels()
	s.logger.Info("[PostgreSQL] Starting synchronization...")

	source, err := s.openSource(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = source.Close(ctx) }()

	// The reader looks keys up while the copy streams a table on source, and a
	// pgx.Conn refuses a second query while one is open.
	keySource, err := s.openSource(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = keySource.Close(ctx) }()

	stream, err := s.openStream(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close(ctx) }()

	target, err := s.openTarget(ctx)
	if err != nil {
		return err
	}
	defer target.Close()

	// Nothing is read or written until the direction is agreed. A target that
	// has been promoted, or a source that is itself somebody's target, means the
	// pair has been reversed and carrying on would overwrite the newer side with
	// the older one.
	release, err := s.claimDirection(ctx, target)
	if err != nil {
		if directionlock.IsBlocking(err) {
			return domain.Unrecoverable("%v", err)
		}
		return fmt.Errorf("%w", err)
	}
	defer release()

	metrics.SetTaskInfo(labels,
		dsn.Endpoint("postgresql", s.cfg.SourceConnection),
		dsn.Endpoint("postgresql", s.cfg.TargetConnection))
	metrics.SetTaskUp(labels, true)
	defer metrics.SetTaskUp(labels, false)

	// No Schema: PostgreSQL qualifies a table by schema, and the database name
	// is not one -- the table lives in whatever the connection's search path
	// resolves to, which is what the connection string already chose.
	store := &checkpoint.SQLStore{
		DB:                   target,
		TaskID:               s.cfg.ID,
		NumberedPlaceholders: true,
	}
	if err := store.Ensure(ctx); err != nil {
		return fmt.Errorf("prepare the position table: %w", err)
	}

	from, slotSeen, err := s.startingPoint(ctx, store, source, slot)
	if err != nil {
		return err
	}

	consistent, snapshot, err := s.ensureSlot(ctx, stream, slot, plugin, slotSeen)
	if err != nil {
		return fmt.Errorf("prepare the replication slot: %w", err)
	}
	if from == 0 && consistent > 0 {
		from = consistent
	}

	work := &schemaWork{Source: source, Target: target, Config: s.cfg, Logger: s.logger}
	if err := work.prepare(ctx); err != nil {
		s.logger.Warnf("[PostgreSQL] Could not prepare the target's schema: %v", err)
	}

	// Before StartReplication: the stream's next command ends the slot's export.
	var copyRead pgx.Tx
	if consistent > 0 {
		if copyRead, err = readAt(ctx, source, snapshot); err != nil {
			return err
		}
	}

	if err := pglogrepl.StartReplication(ctx, stream, slot, from,
		pglogrepl.StartReplicationOptions{PluginArgs: s.pluginArgs()}); err != nil {
		return fmt.Errorf("start the replication stream at %s: %w", from, err)
	}

	reader := &Reader{
		Source: stream,
		Keys:   s.keyLookup(ctx, &schemaWork{Source: keySource, Logger: s.logger}),
		Config: s.cfg,
		Logger: s.logger,
		Labels: labels,
	}
	reader.Applied(from)
	reader.Confirm = func(ctx context.Context) error {
		received, applied := reader.Positions()
		return pglogrepl.SendStandbyStatusUpdate(ctx, stream, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: received,
			WALFlushPosition: applied,
			WALApplyPosition: applied,
		})
	}

	stop := s.confirmPeriodically(ctx, reader)
	defer stop()

	runner := &pipeline.Runner{
		Reader: reader,
		Applier: &Applier{
			DB:          target,
			Checkpoints: store,
			// The file an operator configured, written after the commit rather
			// than in it: only the target can take part in the transaction, and
			// the file is there to be looked at rather than resumed from first.
			Mirror:  s.positionFile(),
			Applied: reader.Applied,
			Logger:  s.logger,
			Labels:  labels,
		},
		Snapshotter: &Snapshotter{
			Schema:          work,
			Snapshot:        copyRead,
			ConsistentPoint: consistent,
			Source:          dsn.Endpoint("postgresql", s.cfg.SourceConnection),
			Config:          s.cfg,
			Logger:          s.logger,
			Labels:          labels,
		},
		Checkpoints: store,
		Opts: pipeline.Options{
			// The statements of a batch go to the target in the order the WAL held
			// them. The applier runs every run one after another in one transaction,
			// so splitting buys nothing and can move a key-changing UPDATE ahead of
			// an earlier change to its old key.
			StreamOrder: true,
			Labels:      labels,
			Logger:      s.logger,
			Engine:      "PostgreSQL",
		},
	}
	return runner.Run(ctx)
}

// confirmPeriodically tells the source how far this task has got, on a timer as
// well as when asked.
//
// The three positions are not the same thing, and reporting one number for all
// of them is what made them dangerous: the server discards WAL the standby says
// it has flushed, so confirming everything received let it recycle segments
// carrying changes the target had not been given yet.
func (s *Syncer) confirmPeriodically(ctx context.Context, reader *Reader) func() {
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(confirmEvery)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := reader.Confirm(ctx); err != nil && ctx.Err() == nil {
					s.logger.Warnf("[PostgreSQL] Could not report progress to the "+
						"source: %v", err)
				}
			}
		}
	}()
	return func() { close(done) }
}

// confirmEvery is how often the source is told where this task has got to. The
// server keeps WAL until it is told otherwise, so a long gap here is disk on
// the source rather than a risk to the copy.
const confirmEvery = 8 * time.Second

func (s *Syncer) pluginArgs() []string {
	publications := s.cfg.PGPublicationNames
	if publications == "" {
		publications = "mypub"
		s.logger.Warn("[PostgreSQL] This task names no publication, so 'mypub' is assumed")
	}
	return []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", publications),
	}
}

// keyLookup reads a table's primary key, once per table.
//
// Cached because it reads through the source connection, and one DELETE per row
// asking the source for the same answer is a round trip per row. Only answers
// are cached: a failure kept as "no key" addresses every later row by every
// column, which a key-only old row or an unchanged-key update never matches.
func (s *Syncer) keyLookup(ctx context.Context, work *schemaWork) func(string, string) ([]string, error) {
	known := map[string][]string{}
	return func(schema, table string) ([]string, error) {
		name := schema + "." + table
		if keys, seen := known[name]; seen {
			return keys, nil
		}
		keys, err := work.primaryKey(schema, table)
		if err != nil {
			return nil, fmt.Errorf("read the primary key of %s, which its rows are "+
				"addressed by: %w", name, err)
		}
		known[name] = keys
		return keys, nil
	}
}

// startingPoint's bool is true only when the slot was looked for and found.
func (s *Syncer) startingPoint(ctx context.Context, store *checkpoint.SQLStore,
	source *pgx.Conn, slot string) (pglogrepl.LSN, bool, error) {

	payload, err := store.Load(ctx, "")
	if err != nil {
		return 0, false, fmt.Errorf("read the stored position: %w", err)
	}
	here := dsn.Endpoint("postgresql", s.cfg.SourceConnection)
	lsn, elsewhere, err := decodeLSN(payload, here)
	if err != nil {
		return 0, false, fmt.Errorf("read the stored position: %w", err)
	}

	exists := false
	if lsn > 0 || elsewhere != "" {
		if exists, err = hasSlot(ctx, source, slot); err != nil {
			return 0, false, fmt.Errorf("look for the replication slot %s: %w", slot, err)
		}
	}
	from, err := resumePoint(lsn, elsewhere, here, slot, exists, s.cfg.ID)
	if err != nil {
		return 0, false, err
	}

	switch {
	case elsewhere != "":
		s.logger.Warnf("[PostgreSQL] The stored position %s belongs to %s, and this "+
			"task reads %s: a log position means nothing on another server, so "+
			"replication slot %s resumes from its own position on %s instead",
			lsn, elsewhere, here, slot, here)
	case from > 0:
		s.logger.Infof("[PostgreSQL] Resuming from %s", from)
	}
	return from, exists, nil
}

func hasSlot(ctx context.Context, source *pgx.Conn, slot string) (bool, error) {
	var exists bool
	err := source.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
		slotIdentifier(slot)).Scan(&exists)
	return exists, err
}

// maxIdentifierBytes is NAMEDATALEN - 1 in a default PostgreSQL build.
const maxIdentifierBytes = 63

// slotIdentifier is the name the server gives the slot pglogrepl names without
// quoting: the replication grammar folds an unquoted identifier to lower case,
// keeps a quoted one as written, and truncates either to maxIdentifierBytes.
func slotIdentifier(slot string) string {
	name := slot
	if len(name) >= 2 && name[0] == '"' && name[len(name)-1] == '"' {
		name = strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	} else {
		name = strings.Map(func(r rune) rune {
			if r >= 'A' && r <= 'Z' {
				return r + ('a' - 'A')
			}
			return r
		}, name)
	}
	if len(name) > maxIdentifierBytes {
		name = name[:maxIdentifierBytes]
	}
	return name
}

// ensureSlot creates the replication slot, or reports that it is already there.
//
// The consistent point, and the snapshot exported at it, are returned only for
// a slot this call created. For one that already existed the answer is
// deliberately not the server's current position: that is where it is writing
// now, and this is the restart case, so taking it would skip everything
// committed while the task was down and no later message would ever carry it.
//
// seen skips the create for a slot already found on the source: one dropped
// since must fail the stream, not be made again at the current end of the WAL.
// A slot found here without seen is refused: the copy it would need has no
// snapshot at the slot's position, so it would repeat changes the slot streams.
func (s *Syncer) ensureSlot(ctx context.Context, stream *pgconn.PgConn, slot, plugin string,
	seen bool) (pglogrepl.LSN, string, error) {

	system, err := pglogrepl.IdentifySystem(ctx, stream)
	if err != nil {
		return 0, "", fmt.Errorf("ask the source to identify itself: %w", err)
	}
	s.logger.Infof("[PostgreSQL] Source system %s, timeline %d, at %s",
		system.SystemID, system.Timeline, system.XLogPos)

	if seen {
		s.logger.Infof("[PostgreSQL] Replication slot %s is already there", slot)
		return 0, "", nil
	}

	created, err := pglogrepl.CreateReplicationSlot(ctx, stream, slot, plugin,
		pglogrepl.CreateReplicationSlotOptions{Temporary: false, SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return 0, "", fmt.Errorf("create the replication slot %s: %w", slot, err)
		}
		return 0, "", domain.Unrecoverable("replication slot %s already exists on %s, "+
			"and this task has no stored position, so it needs a first copy: that copy "+
			"cannot be read at the point the slot streams from, and the changes it "+
			"shares with the stream would be written twice. To copy again, drop the "+
			"slot on the source with SELECT pg_drop_replication_slot('%s') once "+
			"nothing else reads from it, empty the target tables, and restart sync",
			slot, endpointOf(s.cfg.SourceConnection), slotIdentifier(slot))
	}

	lsn, err := pglogrepl.ParseLSN(created.ConsistentPoint)
	if err != nil {
		return 0, "", fmt.Errorf("read the slot's consistent point %q: %w",
			created.ConsistentPoint, err)
	}
	s.logger.Infof("[PostgreSQL] Created replication slot %s at %s", slot, lsn)
	return lsn, created.SnapshotName, nil
}

func (s *Syncer) openSource(ctx context.Context) (*pgx.Conn, error) {
	var conn *pgx.Conn
	err := resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		conn, connErr = pgx.Connect(ctx, s.cfg.SourceConnection)
		return connErr
	})
	if err != nil {
		return nil, fmt.Errorf("connect to the source: %w", err)
	}
	return conn, nil
}

func (s *Syncer) openStream(ctx context.Context) (*pgconn.PgConn, error) {
	replicationDSN, err := replicationConnection(s.cfg.SourceConnection)
	if err != nil {
		return nil, domain.Unrecoverable("build the replication connection: %v", err)
	}

	var conn *pgconn.PgConn
	err = resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		var connErr error
		conn, connErr = pgconn.Connect(ctx, replicationDSN)
		return connErr
	})
	if err != nil {
		return nil, fmt.Errorf("open a replication connection to the source: %w", err)
	}
	return conn, nil
}

func (s *Syncer) openTarget(ctx context.Context) (*sql.DB, error) {
	target, err := sql.Open("postgres", s.cfg.TargetConnection)
	if err != nil {
		return nil, fmt.Errorf("open the target: %w", err)
	}
	if err := resilience.Retry(ctx, 5, 2*time.Second, 2.0, func() error {
		return target.PingContext(ctx)
	}); err != nil {
		target.Close()
		return nil, fmt.Errorf("connect to the target: %w", err)
	}
	return target, nil
}

func (s *Syncer) claimDirection(ctx context.Context, target *sql.DB) (func(), error) {
	// The claim is written on both endpoints, so the source needs an ordinary
	// connection as well as the replication one -- the replication connection
	// takes no ordinary statements.
	source, err := sql.Open("postgres", s.cfg.SourceConnection)
	if err != nil {
		return nil, fmt.Errorf("open the source to claim the direction: %w", err)
	}

	guard := &directionlock.Guard{
		TaskID: s.cfg.ID,
		Source: &directionlock.SQLStore{
			DB:                   source,
			Address:              dsn.Endpoint("postgresql", s.cfg.SourceConnection),
			NumberedPlaceholders: true,
		},
		Target: &directionlock.SQLStore{
			DB:                   target,
			Address:              dsn.Endpoint("postgresql", s.cfg.TargetConnection),
			NumberedPlaceholders: true,
		},
	}

	release, err := directionlock.Hold(ctx, guard, s.logger, "PostgreSQL")
	if err != nil {
		_ = source.Close()
		return nil, err
	}
	// The connection opened for the claim is this function's own, so it closes
	// with the claim rather than living as long as the syncer.
	return func() {
		release()
		_ = source.Close()
	}, nil
}

// positionFile is the copy of the position an operator asked to have on disk,
// or nil when the task names no path.
func (s *Syncer) positionFile() checkpoint.Store {
	if s.cfg.PGPositionPath == "" {
		return nil
	}
	return &checkpoint.FileStore{Path: s.cfg.PGPositionPath}
}

func (s *Syncer) labels() metrics.Labels {
	return metrics.Labels{"task": fmt.Sprint(s.cfg.ID), "engine": domain.EngineLabel(s.cfg.Type)}
}

// replicationConnection turns the task's connection string into one that opens
// a replication connection.
//
// libpq accepts two forms and only the URL one used to be handled: the keyword
// form ("host=x dbname=y") parses as a relative path rather than failing, so the
// replication parameter was appended as a query string onto something that has
// none, and the connection was refused with an error naming neither.
func replicationConnection(connection string) (string, error) {
	trimmed := strings.TrimSpace(connection)
	if trimmed == "" {
		return "", fmt.Errorf("the source connection string is empty")
	}

	if !strings.HasPrefix(trimmed, "postgres://") && !strings.HasPrefix(trimmed, "postgresql://") {
		if !strings.Contains(trimmed, "=") {
			return "", fmt.Errorf("%q is neither a postgres:// URL nor a keyword "+
				"connection string", connection)
		}
		return replicationKeywords(trimmed), nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("replication", "database")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// replicationKeywords sets replication=database in a keyword connection string,
// replacing any value already there.
func replicationKeywords(connection string) string {
	var kept []string
	for _, field := range strings.Fields(connection) {
		if key, _, found := strings.Cut(field, "="); found && key == "replication" {
			continue
		}
		kept = append(kept, field)
	}
	return strings.Join(append(kept, "replication=database"), " ")
}
