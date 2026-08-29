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

// Reader turns one MySQL server's binlog into a stream of events.
//
// One stream covers every database the task maps, not one per database. The
// binlog is a single log per server, so a task per database meant several
// binlog dump connections to the same server all reading the same bytes: the
// source did the work once for each of them, and each held a connection slot.
//
// It is a canal.EventHandler as well as a domain.Reader. canal calls it back on
// its own goroutine; Next hands the events over a channel.
type Reader struct {
	canal.DummyEventHandler

	Config config.SyncConfig
	Logger logrus.FieldLogger
	Labels metrics.Labels
	// AllowKeyless replicates a table with no primary key on a best-effort
	// basis rather than refusing it.
	AllowKeyless bool

	canal *canal.Canal
	conv  *MyEventHandler

	out  chan *domain.Event
	fail chan error
	// done is closed by Close, before the canal is, so an event the canal hands
	// over on its way out has somewhere to go other than a channel nobody is
	// reading. Closing out instead was a crash: canal.Close calls OnPosSynced
	// one last time, and the goroutine below had usually closed out by then.
	done chan struct{}

	// tx holds the events of the source transaction being read. They are handed
	// over together, at the boundary, so a batch can never be cut inside one.
	tx []*domain.Event

	// source names the server the positions belong to, without credentials.
	source string
	flavor string

	// lastGTID is the newest GTID set the stream has reported, kept so a
	// checkpoint written without one does not throw away the one before it.
	//
	// canal does not carry a GTID set on every position it reports. Recording
	// only what the current call carried meant one such call overwrote the
	// stored GTID with nothing, and a restart then resumed from file and offset
	// instead — which canal in turn does not track GTIDs for, so every later
	// checkpoint lost it too. Measured on Cloud SQL: the first start logged
	// "start sync binlog at GTID set", and after a few restarts the stored
	// position was {"Name":"mysql-bin.000037","Pos":41017161} with no GTID at
	// all. File and offset are local to one server, so a failover would have
	// left that position pointing at nothing.
	lastGTID string

	// lastSchemaChange is when a schema change was last carried through, so its
	// age can be published without a timer of its own.
	lastSchemaChange time.Time

	// lastLogFile is the source log file the info series was last published
	// for, so that series is written when it changes rather than per event.
	lastLogFile string

	// inTransaction says a GTID event has opened a transaction whose end has not
	// been seen yet, so no position reported in the meantime is a boundary.
	//
	// canal reports a position at the BEGIN of every transaction, and the GTID
	// set it carries already counts that transaction as done: go-mysql adds the
	// GTID to the set when it reads the GTID event, which comes before the rows.
	// Recording that position hands a restart a checkpoint that points past rows
	// this process has not read yet, and they are then never read at all. It
	// needs a crash in the window to show, which is why it survived every clean
	// stop: measured under 1,000 tx/s with kills every 15s, a handful of rows per
	// run reached the source and never the target, with nothing logged.
	inTransaction bool

	// resumed says the stream started from a stored position rather than from
	// the end of the log, which is what puts rows written before a schema change
	// at risk of being decoded against the shape that change produced.
	resumed bool
	// appliedSince names the tables that have had rows handed over since this
	// reader opened. A reordering statement arriving after them means those rows
	// were read against the wrong shape.
	appliedSince map[string]bool

	closeOnce sync.Once
	stop      func()
}

// heartbeatEvery is how often a stream with nothing to say says so.
//
// A reader that delivers nothing looks exactly like one that is up to date, so
// "replication has stopped" was the one condition the monitoring could not see.
// canal reports a synced position on its own timer even when no rows are
// changing, which is a liveness signal already in hand: turned into an event it
// costs nothing and proves the link end to end.
const heartbeatEvery = 10 * time.Second

// Open starts the binlog stream at a position, or at the current end when there
// is none.
func (r *Reader) Open(ctx context.Context, from domain.Position) error {
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
	r.appliedSince = map[string]bool{}
	r.out = make(chan *domain.Event, 1)
	r.fail = make(chan error, 1)
	r.done = make(chan struct{})
	c.SetEventHandler(r)

	streamCtx, stop := context.WithCancel(ctx)
	r.stop = stop

	stored := &binlogCheckpoint{}
	if _, err := checkpoint.Decode(from.Payload, stored); err != nil {
		return fmt.Errorf("read the stored position: %w", err)
	}

	// A stream that is starting begins outside any transaction, whatever the
	// one before it was in the middle of when it stopped.
	r.inTransaction = false

	metrics.SetConnected(r.Labels, true)
	metrics.SetCapturedTables(r.Labels, r.capturedTables())

	go func() {
		// out is deliberately not closed here. The stream ends by way of fail,
		// which Next already waits on; closing out as well raced with the final
		// OnPosSynced that canal.Close makes, and lost — a send on a closed
		// channel takes the whole process down, every other task with it.
		switch {
		case from.IsZero():
			// Nothing recorded: the caller has already made its copy and has no
			// coordinates, so start where the log is now.
			r.fail <- c.Run()
		case stored.gtidSet() != nil:
			// Preferred: the transactions themselves, which stay meaningful
			// across a failover to a different server.
			r.lastGTID = stored.GTID
			r.fail <- c.StartFromGTID(stored.gtidSet())
		default:
			// File and offset name a place on one server and nowhere else. A
			// source that keeps GTIDs and a position that does not is a task
			// that will not survive its next failover, and nothing else says
			// so — the task looks healthy right up until the position is
			// meaningless. It cannot be repaired from here: canal reports no
			// GTID for a stream it did not start by GTID, so the position stays
			// this way until the shard is copied again.
			r.Logger.Warnf("[MySQL] Resuming from a file and offset rather than a " +
				"GTID. That position names a place on this server and nowhere else, " +
				"so a failover leaves it pointing at nothing and this shard has to be " +
				"copied again. It cannot be repaired from here: a stream that did not " +
				"start by GTID reports none, so the position stays this way.")
			r.fail <- c.RunFrom(stored.position())
		}
		_ = streamCtx
	}()

	return nil
}

// Next hands over the next event, or the reason the stream ended.
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
		// Before the canal, so that the event it hands over as it closes is
		// dropped rather than pushed at a pipeline that has stopped reading.
		if r.done != nil {
			close(r.done)
		}
		if r.stop != nil {
			r.stop()
		}
		if r.canal != nil {
			r.canal.Close()
		}
	})
	return nil
}

// capturedTables counts the objects this task watches.
//
// Debezium: CapturedTables. It is worth publishing because the number changing
// on its own is how a mapping edit that dropped a table shows up — the table
// stops being replicated, and nothing else says so.
func (r *Reader) capturedTables() int {
	n := 0
	for _, mapping := range r.Config.Mappings {
		n += len(mapping.Tables)
	}
	return n
}

// errReaderClosed says an event arrived after the reader was closed. It is not
// a failure: the position it belongs to was never recorded, so the event is
// read again next time.
var errReaderClosed = errors.New("the reader has been closed")

// classify decides whether a stream failure is worth retrying.
func (r *Reader) classify(err error) error {
	if err == nil {
		return nil
	}
	metrics.CountDisconnect(r.Labels)
	metrics.SetConnected(r.Labels, false)
	// A purged binlog cannot be waited out: the position no longer exists, so
	// every attempt fails the same way and a fresh copy is the only way forward.
	if reason, purged := positionNoLongerAvailable(err); purged {
		return domain.Unrecoverable("%s", reason)
	}
	return err
}

// canalConfig builds the stream's configuration, covering every database the
// task maps rather than one of them.
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

	// canal reports a synced position on this timer even when nothing is
	// changing, which is what the heartbeat is built from.
	cfg.HeartbeatPeriod = heartbeatEvery

	includes, err := r.includeTables()
	if err != nil {
		return nil, err
	}
	cfg.IncludeTableRegex = includes
	return cfg, nil
}

// includeTables lists the tables the stream should carry, across every mapped
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
			// Nothing listed for this database, so replicate all of it —
			// including tables created after the task started, which used simply
			// not to be replicated with no warning anywhere.
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

// converter is the handler whose only job is to render statements. Its target
// connection is never set: the statements go to the sink instead of being
// applied here.
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

// ------------------------------------------------------- canal callbacks

// OnRow converts the rows into statements, which the sink collects.
func (r *Reader) OnRow(e *canal.RowsEvent) error {
	before := len(r.tx)
	if err := r.conv.OnRow(e); err != nil {
		return err
	}
	if len(r.tx) == before {
		// The converter produced nothing, so no mapping covers this table.
		// Debezium: NumberOfEventsFiltered.
		metrics.CountFiltered(r.Labels, len(e.Rows))
		return nil
	}

	// The namespace, key and source time are the same for every statement the
	// event produced, and the converter does not know about events.
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
		r.appliedSince[ns.String()] = true
	}
	return nil
}

// OnXID marks the end of a source transaction, which is the only place a batch
// may be cut. The whole transaction is handed over here, together.
func (r *Reader) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	r.inTransaction = false
	return r.handOver(pos, nil, header)
}

// OnGTID marks the start of a source transaction.
//
// It carries no rows of its own; what it establishes is that everything until
// the matching XID belongs to one transaction, and so is not a place a position
// may be recorded.
func (r *Reader) OnGTID(_ *replication.EventHeader, _ mysql.BinlogGTIDEvent) error {
	r.inTransaction = true
	return nil
}

// OnDDL turns a schema change into an event of its own. A DDL is its own
// transaction boundary, and nothing may be reordered across it: a row using a
// new column cannot land before the column exists.
func (r *Reader) OnDDL(header *replication.EventHeader, pos mysql.Position, e *replication.QueryEvent) error {
	if e == nil {
		return nil
	}
	if err := r.checkReordering(string(e.Schema), string(e.Query)); err != nil {
		return err
	}

	decisions, err := r.conv.planDDL(string(e.Schema), string(e.Query))
	if err != nil {
		// BEGIN and COMMIT markers arrive as query events and do not parse.
		// Carrying on is right for those; for anything else the schemas drift,
		// so it is said loudly.
		r.Logger.Warnf("[MySQL] Not propagating a statement that could not be parsed: %v", err)
		return nil
	}

	at := time.Time{}
	if header != nil && header.Timestamp > 0 {
		at = time.Unix(int64(header.Timestamp), 0)
	}

	for _, decision := range decisions {
		switch decision.action {
		case ddlSkip:
			metrics.CountSchemaRefused(r.Labels, "skipped")
			r.Logger.Debugf("[MySQL][DDL] Skipping %q: %s", e.Query, decision.reason)
		case ddlBlock:
			// A refusal is a decision, and a decision nobody can see is a
			// decision nobody can audit. Debezium has no equivalent because it
			// propagates whatever it is told to.
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

// checkReordering stops the task when a statement that moves a column arrives
// after rows for that table have already been handed over in this run.
//
// Those rows were read against the shape this statement produced rather than the
// one they were written under, because the column names come from asking the
// source for its current schema and not from the binlog. They wrote cleanly and
// they are wrong, and the row counts agree — see reorder.go for why this is the
// one case that is neither loud nor harmless.
func (r *Reader) checkReordering(defaultSchema, query string) error {
	if !r.resumed || len(r.appliedSince) == 0 {
		// Nothing was read before this statement, so nothing was read against
		// the wrong shape.
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
			if !r.appliedSince[name] {
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

// OnPosSynced is canal's periodic report of where the stream stands.
//
// With events waiting it closes a transaction that produced no XID — a
// non-transactional engine, or a stream stopped mid-transaction. With nothing
// waiting it is the heartbeat: proof the link is alive, carrying a position, so
// a restart does not re-read a stretch of log that held nothing.
func (r *Reader) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, _ bool) error {
	if len(r.tx) > 0 {
		r.inTransaction = false
		return r.handOver(pos, set, header)
	}
	if r.inTransaction {
		// Between the BEGIN and the rows. The position offered here already
		// counts this transaction as read, and nothing of it has been handed
		// over, so recording it would step over the whole transaction.
		return nil
	}
	return r.handOver(pos, set, header, heartbeatOnly)
}

type handOverOpt int

const heartbeatOnly handOverOpt = 1

// handOver pushes the accumulated transaction onto the channel, marking its last
// event as the boundary and attaching the position.
func (r *Reader) handOver(pos mysql.Position, set mysql.GTIDSet, header *replication.EventHeader, opts ...handOverOpt) error {
	payload, err := r.encode(pos, set)
	if err != nil {
		return err
	}

	// Where the stream has got to. The offset is a number so it can be graphed;
	// the file name goes on an info series because it changes every few hours
	// rather than every transaction. Publishing the GTID set here instead would
	// make one series per transaction and take the scrape down with it.
	metrics.SetSourcePosition(r.Labels, int64(pos.Pos))
	if !r.lastSchemaChange.IsZero() {
		// Refreshed on every hand-over, heartbeats included, so the age climbs
		// while nothing is happening instead of freezing at whatever it was.
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
			// Close was called, so nothing is reading any more. The position
			// this event belongs to was never recorded, so it will be read
			// again from the source: dropping it here loses nothing.
			return errReaderClosed
		case <-time.After(time.Minute):
			// The applier has not taken an event for a minute, so the queue
			// ahead of it is full and staying full. Closing the stream is
			// right: the backlog belongs in the source's log, where it is
			// durable, not in this process's heap.
			return fmt.Errorf("the pipeline has been blocked for a minute, so the " +
				"binlog stream is being closed rather than read further ahead of what " +
				"can be applied")
		}
	}
	return nil
}

// encode renders the position the way the checkpoint store holds it.
func (r *Reader) encode(pos mysql.Position, set mysql.GTIDSet) (string, error) {
	cp := binlogCheckpoint{Name: pos.Name, Pos: pos.Pos, Source: r.source}
	if set != nil {
		if text := set.String(); text != "" {
			r.lastGTID = text
		}
	}
	// Keeping the previous GTID is right even though it names an earlier point
	// than the file and offset beside it: resuming from it replays a little,
	// and every write here is an upsert, so replaying is free. Dropping it is
	// not free — it turns a position that survives a failover into one that
	// does not.
	if r.lastGTID != "" {
		cp.GTID = r.lastGTID
		cp.Flavor = r.flavor
	}
	return checkpoint.Encode(cp)
}

// rowKey identifies the record a statement addresses, so two changes to one row
// are never reordered against each other.
//
// The primary key columns are what the target is addressed by, so they are what
// identifies the record. A table with no key produces no key here — such a table
// is refused before this point unless an operator has said otherwise, and a
// best-effort copy of one has nothing to order.
func rowKey(e *canal.RowsEvent, n int) string {
	if e.Table == nil || len(e.Table.PKColumns) == 0 {
		return ""
	}
	rows := e.Rows
	if e.Action == canal.UpdateAction {
		// The rows come in before/after pairs and the statement was built from
		// the pair, so the nth statement addresses the nth pair.
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

// opOf reports what a rendered statement does, for the log and for the ordering
// barrier a schema change needs.
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

// tlsFor reads the TLS setting out of a parsed DSN.
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
