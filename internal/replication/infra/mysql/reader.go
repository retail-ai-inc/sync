package mysql

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/checkpoint"
)

// Reader turns one MySQL server's binlog into a stream of events, covering
// every database the task maps: the binlog is one log per server. It is a
// canal.EventHandler too — canal calls it back on its own goroutine.
type Reader struct {
	canal.DummyEventHandler

	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels
	// AllowKeyless replicates a table with no primary key best-effort rather than
	// refusing it.
	AllowKeyless bool

	canal *canal.Canal
	conv  *MyEventHandler

	out  chan *domain.Event
	fail chan error
	// done is closed by Close before the canal is, so an event handed over on the
	// way out has somewhere to go. Closing out instead crashed: canal.Close calls
	// OnPosSynced once more.
	done chan struct{}

	// tx holds the source transaction being read, handed over together at the
	// boundary so a batch is never cut inside one.
	tx []*domain.Event

	// source names the server the positions belong to, without credentials.
	source string
	flavor string

	// lastGTID is the newest GTID set reported, kept because canal omits it on
	// some positions: recording only the current call overwrote it with nothing,
	// and the restart fell back to file and offset for good.
	lastGTID string

	// lastSchemaChange is when a schema change last went through, so its age needs
	// no timer.
	lastSchemaChange time.Time

	// lastLogFile is the file the info series was last published for, so it is
	// written on change rather than per event.
	lastLogFile string

	// inTransaction says a GTID event opened a transaction whose end is unseen, so
	// no position until then is a boundary: canal reports one at BEGIN whose GTID
	// set already counts the transaction as done, and recording it steps over rows
	// nobody read.
	inTransaction bool

	// resumed says the stream started from a stored position, which is what puts
	// rows written before a schema change at risk of being decoded against the new
	// shape.
	resumed bool
	// appliedSince records, per table, when this run first handed rows over.
	// That is when canal asked the source for the table's shape, so it is what a
	// reordering statement's own timestamp has to be compared against.
	appliedSince map[string]time.Time

	closeOnce sync.Once
}

// heartbeatEvery is how often a silent stream says so, built from the position
// canal reports on its own timer: without it "replication has stopped" was
// invisible.
const heartbeatEvery = 10 * time.Second

// Open starts the binlog stream at a position, or at the current end when there
// is none.
//
// The context is not used: canal takes none, and the stream is stopped by
// Close. A context here that looked as though it controlled the stream is what
// this signature used to imply.
func (r *Reader) Open(_ context.Context, from domain.Position) error {
	cfg, err := r.canalConfig()
	if err != nil {
		return err
	}

	c, err := canal.NewCanal(cfg)
	if err != nil {
		return fmt.Errorf("connect to the source: %w", err)
	}
	r.canal = c
	r.flavor = cfg.Flavor
	r.source = dsn.Endpoint(r.Config.Type, r.Config.SourceConnection)

	r.conv = r.converter()
	r.resumed = !from.IsZero()
	r.appliedSince = map[string]time.Time{}
	r.out = make(chan *domain.Event, 1)
	r.fail = make(chan error, 1)
	r.done = make(chan struct{})
	c.SetEventHandler(r)

	stored := &binlogCheckpoint{}
	if _, err := checkpoint.Decode(from.Payload, stored); err != nil {
		return fmt.Errorf("read the stored position: %w", err)
	}
	if err := resumableHere(stored, r.source); err != nil {
		return err
	}

	// A starting stream begins outside any transaction, whatever the last one was
	// doing.
	r.inTransaction = false

	metrics.SetConnected(r.Labels, true)
	// Only when the task lists its tables; see the MongoDB reader's copy of this.
	if n := r.capturedTables(); n > 0 {
		metrics.SetCapturedTables(r.Labels, n)
	}

	go func() {
		// out is deliberately not closed here: the stream ends by way of fail, and
		// closing out raced with canal.Close's final OnPosSynced — a send on a closed
		// channel takes the process down.
		switch {
		case from.IsZero():
			// Nothing recorded: the caller has made its copy and has no coordinates, so
			// start where the log is now.
			r.fail <- c.Run()
		case stored.gtidSet() != nil:
			// Preferred: the transactions themselves, which survive a failover to
			// another server.
			r.lastGTID = stored.GTID
			r.fail <- c.StartFromGTID(stored.gtidSet())
		default:
			// File and offset name a place on one server and nowhere else, so a source
			// that keeps GTIDs and a position that does not is a task that will not
			// survive its next failover.
			r.Logger.Warnf("[MySQL] Resuming from a file and offset rather than a " +
				"GTID. That position names a place on this server and nowhere else, " +
				"so a failover leaves it pointing at nothing and this shard has to be " +
				"copied again. It cannot be repaired from here: a stream that did not " +
				"start by GTID reports none, so the position stays this way.")
			r.fail <- c.RunFrom(stored.position())
		}
	}()

	return nil
}

func (r *Reader) Next(ctx context.Context) (*domain.Event, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-r.fail:
		if err != nil {
			return nil, r.classify(err)
		}
		return nil, fmt.Errorf("the binlog stream ended without an error")
	case <-r.done:
		return nil, fmt.Errorf("the binlog stream was closed")
	case event := <-r.out:
		return event, nil
	}
}

// Close stops the stream. Calling it more than once is safe.
func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		// Before the canal, so the event it hands over as it closes is dropped rather
		// than pushed at a pipeline that has stopped reading.
		if r.done != nil {
			close(r.done)
		}
		if r.canal != nil {
			r.canal.Close()
		}
	})
	return nil
}

// capturedTables counts the objects this task watches (Debezium:
// CapturedTables) — the number moving on its own is how a mapping edit that
// dropped a table shows up.
func (r *Reader) capturedTables() int {
	n := 0
	for _, mapping := range r.Config.Mappings {
		n += len(mapping.Tables)
	}
	return n
}

// errReaderClosed says an event arrived after Close. Not a failure: its
// position was never recorded, so it is read again next time.
var errReaderClosed = errors.New("the reader has been closed")

func (r *Reader) classify(err error) error {
	if err == nil {
		return nil
	}
	metrics.CountDisconnect(r.Labels)
	metrics.SetConnected(r.Labels, false)
	// A purged binlog cannot be waited out: the position is gone, so a fresh copy
	// is the only way forward.
	if reason, purged := positionNoLongerAvailable(err); purged {
		return domain.Unrecoverable("%s", reason)
	}
	return err
}

// canalConfig builds the stream's configuration, covering every database the
// task maps.
func (r *Reader) canalConfig() (*canal.Config, error) {
	cfg := canal.NewDefaultConfig()
	if strings.EqualFold(r.Config.Type, "mariadb") {
		cfg.Flavor = mysql.MariaDBFlavor
	} else {
		cfg.Flavor = mysql.MySQLFlavor
	}

	parsed, err := mysqldriver.ParseDSN(r.Config.SourceConnection)
	if err != nil {
		return nil, fmt.Errorf("read the source connection string: %w", err)
	}
	cfg.Addr = parsed.Addr
	cfg.User = parsed.User
	cfg.Password = parsed.Passwd
	cfg.TLSConfig = tlsFor(parsed)
	cfg.Dump.ExecutionPath = r.Config.DumpExecutionPath

	// canal reports a synced position on this timer even when nothing changes,
	// which is what the heartbeat is built from.
	cfg.HeartbeatPeriod = heartbeatEvery

	includes, err := r.includeTables()
	if err != nil {
		return nil, err
	}
	cfg.IncludeTableRegex = includes
	return cfg, nil
}

// includeTables lists the tables the stream carries, across every mapped
// database.
func (r *Reader) includeTables() ([]string, error) {
	var includes []string
	seen := map[string]bool{}

	fallback := dsn.GetDatabaseName(r.Config.Type, r.Config.SourceConnection)
	for _, mapping := range r.Config.Mappings {
		db := mapping.SourceDatabase
		if db == "" {
			db = fallback
		}
		if db == "" {
			return nil, domain.Unrecoverable(
				"a mapping names no source database and the connection string names none " +
					"either, so there is nothing to read from")
		}
		if len(mapping.Tables) == 0 {
			// Nothing listed for this database, so replicate all of it, including tables
			// created after the task started.
			add(&includes, seen, fmt.Sprintf("%s\\..*", db))
			continue
		}
		for _, table := range mapping.Tables {
			add(&includes, seen, fmt.Sprintf("%s\\.%s", db, table.SourceTable))
		}
	}

	if len(includes) == 0 {
		if fallback == "" {
			return nil, domain.Unrecoverable(
				"the task maps nothing and its connection string names no database")
		}
		add(&includes, seen, fmt.Sprintf("%s\\..*", fallback))
	}
	return includes, nil
}

func add(list *[]string, seen map[string]bool, pattern string) {
	if seen[pattern] {
		return
	}
	seen[pattern] = true
	*list = append(*list, pattern)
}

// converter renders statements only. Its target connection is never set: the
// statements go to the sink instead of being applied here.
func (r *Reader) converter() *MyEventHandler {
	discovering := false
	for _, mapping := range r.Config.Mappings {
		if len(mapping.Tables) == 0 {
			discovering = true
		}
	}

	h := &MyEventHandler{
		mappings:         r.Config.Mappings,
		logger:           r.Logger,
		TargetConnection: r.Config.TargetConnection,
		labels:           r.Labels,
		discovering:      discovering,
		sourceDatabase:   dsn.GetDatabaseName(r.Config.Type, r.Config.SourceConnection),
		allowKeyless:     r.AllowKeyless,
	}
	h.sink = func(stmt *statement) error {
		if stmt == nil {
			return nil
		}
		r.tx = append(r.tx, &domain.Event{
			Op:      opOf(stmt.query),
			Key:     "",
			Payload: *stmt,
			Bytes:   len(stmt.query),
		})
		return nil
	}
	return h
}

func (r *Reader) OnRow(e *canal.RowsEvent) error {
	before := len(r.tx)
	if err := r.conv.OnRow(e); err != nil {
		return err
	}
	if len(r.tx) == before {
		// The converter produced nothing, so no mapping covers this table.
		metrics.CountFiltered(r.Labels, len(e.Rows))
		return nil
	}

	// Namespace, key and source time are the same for every statement the event
	// produced, and the converter does not know about events.
	ns := domain.Namespace{DB: e.Table.Schema, Object: e.Table.Name}
	at := time.Time{}
	if e.Header != nil && e.Header.Timestamp > 0 {
		at = time.Unix(int64(e.Header.Timestamp), 0)
	}
	for i := before; i < len(r.tx); i++ {
		r.tx[i].NS = ns
		r.tx[i].SourceTime = at
		r.tx[i].Key = rowKey(e, i-before)
	}
	if len(r.tx) > before {
		if _, seen := r.appliedSince[ns.String()]; !seen {
			r.appliedSince[ns.String()] = time.Now()
		}
	}
	return nil
}

// OnXID marks the end of a source transaction, the only place a batch may be
// cut. The whole transaction is handed over here.
func (r *Reader) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	r.inTransaction = false
	return r.handOver(pos, nil, header)
}

// OnGTID marks the start of a source transaction: it carries no rows, but
// everything until the matching XID belongs to one transaction and is no place
// to record a position.
func (r *Reader) OnGTID(_ *replication.EventHeader, _ mysql.BinlogGTIDEvent) error {
	r.inTransaction = true
	return nil
}

// OnDDL turns a schema change into its own event and its own barrier: a row
// using a new column cannot land before the column exists.
func (r *Reader) OnDDL(header *replication.EventHeader, pos mysql.Position, e *replication.QueryEvent) error {
	if e == nil {
		return nil
	}
	ddlAt := time.Time{}
	if header != nil && header.Timestamp > 0 {
		ddlAt = time.Unix(int64(header.Timestamp), 0)
	}
	if err := r.checkReordering(string(e.Schema), string(e.Query), ddlAt); err != nil {
		return err
	}

	decisions, err := r.conv.planDDL(string(e.Schema), string(e.Query))
	if err != nil {
		// BEGIN and COMMIT markers arrive as query events and do not parse; anything
		// else that fails to parse means the schemas drift, so it is said loudly.
		r.Logger.Warnf("[MySQL] Not propagating a statement that could not be parsed: %v", err)
		return nil
	}

	at := time.Time{}
	if header != nil && header.Timestamp > 0 {
		at = time.Unix(int64(header.Timestamp), 0)
	}

	for _, decision := range decisions {
		switch decision.action {
		case ddlNotSchema:
			// Dropped without being counted; see notASchemaChange.
			r.Logger.Debugf("[MySQL][DDL] Not a schema change, dropping %q", e.Query)
		case ddlSkip:
			// Not a refusal: the statement is none of this task's business, and
			// nothing stopped. Counting it as one buried the number that means
			// replication has halted under one that means nothing happened.
			metrics.CountDDLSkipped(r.Labels, decision.kind)
			r.Logger.Debugf("[MySQL][DDL] Skipping %q: %s", e.Query, decision.reason)
		case ddlBlock:
			// A refusal nobody can see is a decision nobody can audit. Debezium has no
			// equivalent because it propagates whatever it is told to.
			metrics.CountSchemaRefused(r.Labels, "blocked")
			return domain.Unrecoverable(
				"refusing to replicate %q: it %s. Replication has stopped so the change "+
					"can be made on the target deliberately", e.Query, decision.reason)
		case ddlApply:
			metrics.CountSchemaChange(r.Labels, 1)
			metrics.SetSchemaChangeAge(r.Labels, 0)
			r.lastSchemaChange = time.Now()
			r.tx = append(r.tx, &domain.Event{
				NS:         domain.Namespace{DB: string(e.Schema)},
				Op:         domain.OpSchema,
				Payload:    statement{query: decision.query},
				Bytes:      len(decision.query),
				SourceTime: at,
			})
		}
	}
	return r.handOver(pos, nil, header)
}

// schemaReadSkew is how much disagreement between the source's clock and this
// process's is absorbed before a statement counts as older than the shape read
// for it. The comparison is across two clocks because the binlog timestamp is
// the only source-side one there is; a few minutes of NTP drift must not turn
// into either verdict on its own, and erring towards refusing is the safe side.
const schemaReadSkew = 5 * time.Minute

// checkReordering stops the task when a column-moving statement that had
// already run on the source arrives after rows for that table were handed
// over: canal resolves column names from the source's current shape, so those
// rows were decoded against the shape this statement produced.
//
// A statement that runs while the stream is live is not that case. The shape
// was read before it, the rows already handed over were decoded under the one
// they were written with, and refusing there halts a healthy task -- which is
// what happened to the staging MySQL task on 2026-09-14, where an ALTER ran
// nine hours into a run and stopped replication over rows that were correct.
func (r *Reader) checkReordering(defaultSchema, query string, at time.Time) error {
	if !r.resumed || len(r.appliedSince) == 0 {
		// Nothing was read before this statement, so nothing was read against the
		// wrong shape.
		return nil
	}

	stmts, _, err := parser.New().Parse(query, "", "")
	if err != nil {
		// BEGIN and COMMIT markers arrive as query events and do not parse.
		return nil
	}
	for _, stmt := range stmts {
		if !reordersColumns(stmt) {
			continue
		}
		for _, ref := range tableRefs(stmt) {
			schemaName := ref.Schema.O
			if schemaName == "" {
				schemaName = defaultSchema
			}
			name := domain.Namespace{DB: schemaName, Object: ref.Name.O}.String()
			readAt, applied := r.appliedSince[name]
			if !applied {
				continue
			}
			if !at.IsZero() && !at.Before(readAt.Add(-schemaReadSkew)) {
				// The statement ran on the source after the shape was read for
				// this table, so the rows decoded against that shape are the
				// ones written under it. The change itself is propagated by the
				// ordinary path below.
				continue
			}
			return domain.Unrecoverable(
				"%s was reordered by %q, and rows written before that statement have "+
					"already been applied in this run. Their column names came from the "+
					"table's shape after the change rather than the one they were written "+
					"under, so they were written to the target in the wrong columns — "+
					"cleanly, with the row counts still agreeing. The correct values are "+
					"only at the source: copy %s again",
				name, query, name)
		}
	}
	return nil
}

// OnPosSynced is canal's periodic report of where the stream stands. With
// events waiting it closes a transaction that produced no XID; with none it is
// the heartbeat, carrying a position so a restart does not re-read empty log.
func (r *Reader) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, _ bool) error {
	if len(r.tx) > 0 {
		r.inTransaction = false
		return r.handOver(pos, set, header)
	}
	if r.inTransaction {
		// Between the BEGIN and the rows: the position offered already counts this
		// transaction as read, so recording it would step over the whole of it.
		return nil
	}
	return r.handOver(pos, set, header, heartbeatOnly)
}

type handOverOpt int

const heartbeatOnly handOverOpt = 1

// handOver pushes the accumulated transaction onto the channel, marking its
// last event as the boundary and attaching the position.
func (r *Reader) handOver(pos mysql.Position, set mysql.GTIDSet, header *replication.EventHeader, opts ...handOverOpt) error {
	payload, err := r.encode(pos, set)
	if err != nil {
		return err
	}

	// The offset is a number so it can be graphed; the file name goes on an info
	// series because it changes hourly. The GTID set would be one series per
	// transaction.
	metrics.SetSourcePosition(r.Labels, int64(pos.Pos))
	if !r.lastSchemaChange.IsZero() {
		// Refreshed on every hand-over, heartbeats included, so the age climbs while
		// nothing happens instead of freezing.
		metrics.SetSchemaChangeAge(r.Labels, time.Since(r.lastSchemaChange).Seconds())
	}
	if pos.Name != r.lastLogFile {
		r.lastLogFile = pos.Name
		metrics.SetSourceInfo(r.Labels, pos.Name, r.source)
	}

	events := r.tx
	r.tx = nil

	if len(events) == 0 {
		for _, o := range opts {
			if o == heartbeatOnly {
				at := time.Time{}
				if header != nil && header.Timestamp > 0 {
					at = time.Unix(int64(header.Timestamp), 0)
				}
				events = []*domain.Event{{
					Heartbeat:       true,
					Pos:             domain.Position{Payload: payload},
					EndsTransaction: true,
					SourceTime:      at,
				}}
			}
		}
		if len(events) == 0 {
			return nil
		}
	}

	last := events[len(events)-1]
	last.EndsTransaction = true
	last.Pos = domain.Position{Payload: payload}

	for _, event := range events {
		select {
		case r.out <- event:
		case <-r.done:
			// Close was called, so nothing is reading. The position was never recorded,
			// so the event is read again from the source.
			return errReaderClosed
		case <-time.After(time.Minute):
			// The applier has not taken an event for a minute, so the queue is full and
			// staying full. The backlog belongs in the source's log, not this process's
			// heap.
			return fmt.Errorf("the pipeline has been blocked for a minute, so the " +
				"binlog stream is being closed rather than read further ahead of what " +
				"can be applied")
		}
	}
	return nil
}

func (r *Reader) encode(pos mysql.Position, set mysql.GTIDSet) (string, error) {
	cp := binlogCheckpoint{Name: pos.Name, Pos: pos.Pos, Source: r.source}
	if set != nil {
		if text := set.String(); text != "" {
			r.lastGTID = text
		}
	}
	// Keeping the previous GTID is right even though it names an earlier point
	// than the file and offset beside it: replaying an upsert is free, and
	// dropping it turns a position that survives a failover into one that does
	// not.
	if r.lastGTID != "" {
		cp.GTID = r.lastGTID
		cp.Flavor = r.flavor
	}
	return checkpoint.Encode(cp)
}

// rowKey identifies the record a statement addresses, so two changes to one row
// are never reordered. The primary key columns are what the target is addressed
// by.
func rowKey(e *canal.RowsEvent, n int) string {
	if e.Table == nil || len(e.Table.PKColumns) == 0 {
		return ""
	}
	rows := e.Rows
	if e.Action == canal.UpdateAction {
		// Rows come in before/after pairs and the statement was built from the pair,
		// so the nth statement addresses the nth pair.
		if idx := n*2 + 1; idx < len(rows) {
			return keyOf(rows[idx], e.Table.PKColumns)
		}
		return ""
	}
	if n < len(rows) {
		return keyOf(rows[n], e.Table.PKColumns)
	}
	return ""
}

func keyOf(row []interface{}, pk []int) string {
	var b strings.Builder
	for _, i := range pk {
		if i >= len(row) {
			return ""
		}
		fmt.Fprintf(&b, "%v\x00", row[i])
	}
	return b.String()
}

// opOf reports what a rendered statement does, for the log and for the schema-
// change barrier.
func opOf(query string) domain.Op {
	switch {
	case strings.HasPrefix(query, "INSERT"), strings.HasPrefix(query, "REPLACE"):
		return domain.OpInsert
	case strings.HasPrefix(query, "UPDATE"):
		return domain.OpUpdate
	case strings.HasPrefix(query, "DELETE"):
		return domain.OpDelete
	}
	return domain.OpSchema
}

func tlsFor(cfg *mysqldriver.Config) *tls.Config {
	switch strings.ToLower(cfg.TLSConfig) {
	case "true":
		host := cfg.Addr
		if i := strings.LastIndex(host, ":"); i > 0 {
			host = host[:i]
		}
		return &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	case "skip-verify":
		return &tls.Config{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}
	}
	return nil
}

// resumableHere refuses a stored position that names a server this task is not
// reading.
//
// The old syncer checked this and the shared pipeline did not, so the position
// carried the endpoint it was written against and nothing ever compared it. A
// task repointed at a different server then resumed from a file and offset that
// mean nothing there — read successfully, against data they do not describe.
//
// A GTID set is exempt, and that is the whole point of one: it names the
// transactions rather than a place in one server's log, so it stays valid across
// the failover this deployment exists for. Discarding a position because the
// endpoint moved, which is what the old check did, would have forced a full
// re-copy after every failover.
func resumableHere(stored *binlogCheckpoint, source string) error {
	if stored.Name == "" || stored.Source == "" || stored.Source == source {
		return nil
	}
	if stored.GTID != "" {
		return nil
	}
	return domain.Unrecoverable("the stored position is a file and offset recorded "+
		"against %s, and this task reads %s. A binlog offset names a place on one "+
		"server and nowhere else, so resuming from it here would read bytes that "+
		"describe other data. Clear this task's position to copy the source again, "+
		"or point the task back at %s", stored.Source, source, stored.Source)
}
