package pipeline

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Checkpoints records how far the target has been written.
//
// It is the same shape as the checkpoint store the engines already use, named
// here so the runner does not depend on the infrastructure package.
type Checkpoints interface {
	Load(ctx context.Context, key string) (string, error)
	Save(ctx context.Context, key, payload string) error
}

// Options configure one run.
type Options struct {
	Limits Limits
	// FlushInterval is the longest a partly filled batch waits. It is an upper
	// bound rather than a cost paid on every batch: a batch with nothing behind
	// it in the queue is sent at once, so this only bounds the case where events
	// keep arriving but too slowly to fill one.
	FlushInterval time.Duration
	// QueueCapacity bounds how far the reader may run ahead of the applier.
	// Reaching it stops the reader, which is what back pressure is: the source
	// log holds the backlog, where it is durable, rather than this process's
	// heap, where it is not.
	QueueCapacity int
	// ReportInterval is how often the health gauges are refreshed while nothing
	// is happening, so a stalled task shows a rising age rather than a frozen
	// one.
	ReportInterval time.Duration

	Labels metrics.Labels
	Logger logrus.FieldLogger
	// Engine names the engine in log lines, as "[MySQL]" or "[MongoDB]".
	Engine string

	// StreamOrder hands the batch to the applier in the order it was read, as a
	// single run, instead of splitting it into runs that may be applied in
	// parallel.
	//
	// The split is safe for an engine whose events are idempotent writes of a
	// whole record: two upserts of different rows commute. It is not safe for an
	// engine whose log is a command stream. A command is not idempotent the way
	// an upsert is — replaying INCR adds again — and two commands on different
	// keys may still have been one atomic act at the source, so the order they
	// were read in is the only order known to be correct.
	StreamOrder bool
}

const (
	defaultFlushInterval  = 500 * time.Millisecond
	defaultQueueCapacity  = 2000
	defaultReportInterval = time.Second
)

func (o Options) flushInterval() time.Duration {
	if o.FlushInterval > 0 {
		return o.FlushInterval
	}
	return defaultFlushInterval
}

func (o Options) queueCapacity() int {
	if o.QueueCapacity > 0 {
		return o.QueueCapacity
	}
	return defaultQueueCapacity
}

func (o Options) reportInterval() time.Duration {
	if o.ReportInterval > 0 {
		return o.ReportInterval
	}
	return defaultReportInterval
}

// Runner is the replication loop. One per task.
type Runner struct {
	Reader      domain.Reader
	Applier     domain.Applier
	Snapshotter domain.Snapshotter
	Checkpoints Checkpoints
	// CheckpointKey names this task's position in the store. Empty is the
	// single position a single-stream task has, which is the normal case.
	CheckpointKey string

	// Resyncs re-copy one object each, alongside the stream, without stopping
	// it. Empty is the normal case.
	Resyncs []*Resync

	Opts Options

	// clock is time.Now, replaced in tests.
	clock func() time.Time

	mu sync.Mutex
	// oldestPending is when the source made the oldest change that has been
	// read and not yet applied. The applied lag is measured from it so that a
	// task which has stopped applying shows a lag that climbs.
	oldestPending time.Time
	// lastAppliedAt is when the source made the most recent change that did
	// reach the target.
	lastAppliedAt time.Time
	// lastHeardAt is when anything last arrived, heartbeats included.
	lastHeardAt time.Time
	// lastReadAt is the source's own time of the newest change read from the
	// stream. A re-copy holds each chunk until this has passed the moment the
	// chunk was read, which is what orders the two against each other.
	lastReadAt time.Time
	queueUsed  int

	// window is the source's retention, and windowAt when it was last asked
	// for. It is a server setting rather than a moving quantity, so it is read
	// occasionally and not once a second.
	window   time.Duration
	windowAt time.Time
	// windowFailed stops a source that cannot answer from being asked, and
	// logged about, on every refresh.
	windowFailed bool

	// events counts what the stream carried, split by operation. It is built
	// once because counting happens per event.
	events *metrics.EventCounters
	// queueBytes is how much unapplied change data the batch is holding.
	queueBytes int64
}

// counters prepares the per-operation counters on first use.
func (r *Runner) counters() *metrics.EventCounters {
	if r.events == nil {
		r.events = metrics.NewEventCounters(r.Opts.Labels)
	}
	return r.events
}

// retentionRefresh is how often the source is asked how far its log reaches.
//
// The answer is a configuration setting, not a measurement, and on a sharded
// deployment reaching it costs a round trip per shard.
const retentionRefresh = 5 * time.Minute

func (r *Runner) now() time.Time {
	if r.clock != nil {
		return r.clock()
	}
	return time.Now()
}

func (r *Runner) log() logrus.FieldLogger {
	if r.Opts.Logger != nil {
		return r.Opts.Logger
	}
	return logrus.StandardLogger()
}

func (r *Runner) tag(format string) string {
	if r.Opts.Engine == "" {
		return format
	}
	return "[" + r.Opts.Engine + "] " + format
}

// Run replicates until the context is cancelled or the stream cannot continue.
//
// The returned error is what the supervisor decides on: nil or a transient
// failure means try again, an ErrUnrecoverable means stop and tell somebody.
func (r *Runner) Run(ctx context.Context) error {
	start, err := r.startingPoint(ctx)
	if err != nil {
		return err
	}

	if err := r.Reader.Open(ctx, start); err != nil {
		return fmt.Errorf("open the source stream: %w", err)
	}
	defer func() { _ = r.Reader.Close() }()

	now := r.now()
	r.mu.Lock()
	r.lastHeardAt = now
	r.lastAppliedAt = now
	r.mu.Unlock()

	queue := make(chan *domain.Event, r.Opts.queueCapacity())
	readCtx, stopReading := context.WithCancel(ctx)
	defer stopReading()

	// The queue has more than one producer once a re-copy is running, so it is
	// closed after all of them have finished rather than by whichever finishes
	// first. Closing it from the reader alone was a data race, and worse than
	// that: a re-copy still handing over a chunk would send on a closed channel
	// and take the process down.
	var producers sync.WaitGroup

	readErr := make(chan error, 1)
	producers.Add(1)
	go func() {
		defer producers.Done()
		readErr <- r.read(readCtx, queue)
	}()

	stopReporting := r.report(readCtx)
	defer stopReporting()

	resyncErr := r.startResyncs(readCtx, queue, &producers)

	go func() {
		producers.Wait()
		close(queue)
	}()

	applyErr := r.apply(ctx, queue)

	// A re-copy that failed is worth reporting even when the stream ended
	// cleanly: the object it was repairing is still wrong.
	select {
	case err := <-resyncErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log().Errorf(r.tag("A re-copy stopped: %v"), err)
		}
	default:
	}

	// The reader is the one that knows why the stream ended, so its error is
	// preferred: an applier that stops because its queue closed says nothing
	// useful about the source going away.
	stopReading()
	select {
	case err := <-readErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	case <-time.After(5 * time.Second):
		r.log().Warn(r.tag("The source reader did not stop within five seconds"))
	}
	return applyErr
}

// startResyncs runs each re-copy alongside the stream, pushing its chunks onto
// the same queue so the applier orders them against the stream's changes.
func (r *Runner) startResyncs(ctx context.Context, queue chan<- *domain.Event, producers *sync.WaitGroup) <-chan error {
	failed := make(chan error, len(r.Resyncs)+1)
	if len(r.Resyncs) == 0 {
		return failed
	}

	read := func() time.Time {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.lastReadAt
	}

	for _, resync := range r.Resyncs {
		resync := resync
		producers.Add(1)
		go func() {
			defer producers.Done()
			r.log().Infof(r.tag("Re-copying %s alongside the stream"), resync.NS)
			err := resync.run(ctx, read, func(events []*domain.Event) error {
				if len(events) == 0 {
					return nil
				}
				// A chunk is its own batch: it carries no position, so applying
				// it never moves the stream's offset, and its last event closes
				// the batch so it is not held waiting for a boundary that will
				// not come.
				events[len(events)-1].EndsTransaction = true
				for _, event := range events {
					event.Pos = domain.Position{}
					select {
					case queue <- event:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
			if err == nil {
				r.log().Infof(r.tag("Finished re-copying %s"), resync.NS)
			}
			failed <- err
		}()
	}
	return failed
}

// startingPoint reads the stored position, taking a snapshot when there is none.
func (r *Runner) startingPoint(ctx context.Context) (domain.Position, error) {
	payload, err := r.Checkpoints.Load(ctx, r.CheckpointKey)
	if err != nil {
		return domain.Position{}, fmt.Errorf("read the stored position: %w", err)
	}
	if payload != "" {
		r.log().Infof(r.tag("Resuming from the stored position"))
		return domain.Position{Payload: payload}, nil
	}

	if r.Snapshotter == nil {
		return domain.Position{}, nil
	}

	// snapshotDone separates "finished" from "gave up" for the deferred report
	// below: a copy that returns an error must not be recorded as completed.
	snapshotDone := false

	// The position is pinned before a row is copied and stored only once the
	// copy has finished. Pinning afterwards loses every write made while the
	// copy ran; storing before it finishes means an interrupted copy resumes
	// from a point it never reached.
	// The snapshot context, in Debezium's terms. Until now an initial copy was
	// invisible from outside: it either finished or the task looked stuck, with
	// no way to tell how far it had got or whether it had given up. That
	// mattered the first time a Redis shard had to be re-copied because the
	// source's backlog had rolled past its position.
	started := r.now()
	metrics.SnapshotStarted(r.Opts.Labels, 0)
	defer func() {
		if !snapshotDone {
			metrics.SnapshotFinished(r.Opts.Labels, false, r.now().Sub(started).Seconds())
		}
	}()

	pinned, err := r.Snapshotter.Pin(ctx)
	if err != nil {
		return domain.Position{}, fmt.Errorf("pin the snapshot's starting point: %w", err)
	}
	r.log().Infof(r.tag("Snapshot pinned; copying"))

	if err := r.Snapshotter.Copy(ctx); err != nil {
		return domain.Position{}, fmt.Errorf("copy the source: %w", err)
	}
	if err := r.Checkpoints.Save(ctx, r.CheckpointKey, pinned.Payload); err != nil {
		return domain.Position{}, fmt.Errorf("record the snapshot's starting point: %w", err)
	}
	snapshotDone = true
	metrics.SnapshotFinished(r.Opts.Labels, true, r.now().Sub(started).Seconds())
	r.log().Infof(r.tag("Copy finished; streaming from the pinned point"))
	return pinned, nil
}

// read moves events from the reader onto the queue until it cannot.
func (r *Runner) read(ctx context.Context, queue chan<- *domain.Event) error {
	for {
		event, err := r.Reader.Next(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return nil
			}
			return err
		}
		if event == nil {
			continue
		}

		now := r.now()
		r.mu.Lock()
		r.lastHeardAt = now
		if !event.Heartbeat && r.oldestPending.IsZero() && !event.SourceTime.IsZero() {
			r.oldestPending = event.SourceTime
		}
		if !event.SourceTime.IsZero() && event.SourceTime.After(r.lastReadAt) {
			r.lastReadAt = event.SourceTime
		}
		r.mu.Unlock()

		if !event.SourceTime.IsZero() {
			metrics.SetReadLag(r.Opts.Labels, now.Sub(event.SourceTime).Seconds())
		}

		select {
		case queue <- event:
		case <-ctx.Done():
			return nil
		}
	}
}

// apply drains the queue into batches and writes each one.
func (r *Runner) apply(ctx context.Context, queue <-chan *domain.Event) error {
	var b batch
	timer := time.NewTimer(r.Opts.flushInterval())
	defer timer.Stop()

	// pending is the position the batch would advance to, held until the batch
	// is applied. It is never recorded ahead of the data it points past.
	var pending domain.Position

	flush := func() error {
		if b.len() == 0 {
			return nil
		}
		if !b.cuttable() {
			// The batch ends inside a source transaction, so it is not a legal
			// cut point. Waiting is right: the alternative is showing the target
			// half a transaction.
			return nil
		}
		events := b.take()
		return r.applyBatch(ctx, events, pending)
	}

	for {
		select {
		case <-ctx.Done():
			// A clean stop applies what is already whole, so restarting does not
			// replay it.
			if err := flush(); err != nil {
				return err
			}
			return nil

		case event, ok := <-queue:
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				return nil
			}

			// A schema change gets a batch to itself: it cannot share a
			// transaction with rows, so a batch holding both could not be
			// applied atomically.
			if standsAlone(event) && b.len() > 0 {
				if err := flush(); err != nil {
					return err
				}
			}

			b.add(event)
			if !event.Pos.IsZero() {
				pending = event.Pos
			}

			if standsAlone(event) {
				if err := flush(); err != nil {
					return err
				}
				resetTimer(timer, r.Opts.flushInterval())
				continue
			}
			r.mu.Lock()
			r.queueUsed = len(queue)
			r.queueBytes = int64(b.bytes)
			r.mu.Unlock()

			// Nothing else is waiting, so holding this batch back can only add
			// latency. A batch exists to spread the cost of a round trip over
			// several events; with an empty queue there are no further events to
			// spread it over, and the wait is paid for nothing.
			//
			// This is what every mature pipeline does by default — Kafka's
			// linger.ms is 0, a change stream's getMore returns as soon as it has
			// anything, MySQL's group commit delay is 0. Batching is meant to be
			// what a busy pipeline falls into, not a toll an idle one pays. It
			// needs no configuration to get right: under load the queue is rarely
			// empty and batches fill as before, while an idle task now sends at
			// once. flush() still refuses to cut inside a source transaction, so
			// the atomicity this pipeline is built on is untouched.
			if len(queue) == 0 {
				if err := flush(); err != nil {
					return err
				}
				resetTimer(timer, r.Opts.flushInterval())
				continue
			}

			if b.overrunning(r.Opts.Limits) {
				return domain.Unrecoverable(
					"a single source transaction has produced more than %d events, which is "+
						"more than this task may hold. A batch cannot be cut inside a "+
						"transaction without showing the target a state the source was never "+
						"in, so replication has stopped. Raise the limit if the transaction "+
						"is legitimate", r.Opts.Limits.maxTransactionEvents())
			}

			if b.full(r.Opts.Limits) {
				if err := flush(); err != nil {
					return err
				}
				resetTimer(timer, r.Opts.flushInterval())
			}

		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
			resetTimer(timer, r.Opts.flushInterval())
		}
	}
}

// refreshApplied asks the checkpoint store to re-read what the target holds, for
// stores that can. One that cannot is left alone: it is then no worse off than
// it was before retrying existed.
func (r *Runner) refreshApplied(ctx context.Context) error {
	type refreshable interface {
		Refresh(ctx context.Context) error
	}
	if store, ok := r.Checkpoints.(refreshable); ok {
		return store.Refresh(ctx)
	}
	return nil
}

// applyWithRetry writes one batch, waiting for a target that is not ready
// rather than ending the run.
//
// Returning the error used to end Run, which stopped the reader with it. That
// is the wrong shape for this pipeline: the reader is what keeps the source's
// replication log from rolling past the position, and on Memorystore that log
// is a fixed ring of tens of kilobytes. Ending the run over a target that was
// briefly unavailable therefore cost a full re-copy — measured at 30 KB of
// source writes, which at 12,000 ops/s is twenty milliseconds of downtime.
//
// A reader that keeps reading turns that into what the on-disk buffer was built
// for: the outage costs buffer space, and the buffer filling is its own loud
// failure. This is the shape MySQL replication has always had, where the I/O
// thread keeps filling the relay log while the SQL thread is stuck, and the one
// Debezium gets from writing into Kafka rather than into the target itself.
//
// An error that retrying cannot fix is still returned, and wrapped as
// unrecoverable so the supervisor stops the task and says so rather than
// restarting it forever. A poisoned event must not be quietly stepped over: the
// position does not move, because this returns before the caller records it.
func (r *Runner) applyWithRetry(ctx context.Context, runs [][]*domain.Event,
	pos domain.Position, count int) (bool, error) {

	const (
		firstWait = 500 * time.Millisecond
		maxWait   = 15 * time.Second
	)
	wait := firstWait
	for attempt := 1; ; attempt++ {
		committed, err := r.Applier.Apply(ctx, runs, pos)
		if err == nil {
			if attempt > 1 {
				r.log().Infof(r.tag("The target accepted the batch again after %d attempts"), attempt)
			}
			return committed, nil
		}
		metrics.Failed(r.Opts.Labels, count)

		if ctx.Err() != nil {
			return false, fmt.Errorf("apply %d changes: %w", count, err)
		}
		if domain.IsUnrecoverable(err) {
			return false, fmt.Errorf("apply %d changes: %w", count, err)
		}
		if permanentApplyFailure(err) {
			// Retrying will fail identically for as long as anybody lets it, and
			// stepping over it would leave the target permanently different from
			// the source with nothing blocked and nothing alarming.
			return false, domain.Unrecoverable(
				"applying %d changes failed in a way retrying cannot fix: %v. "+
					"The position has not moved, so nothing has been skipped; "+
					"this needs somebody to look at the event and the target",
				count, err)
		}

		if attempt == 1 {
			r.log().Warnf(r.tag("The target would not take a batch (%v); holding the "+
				"batch and reading on, so the source's log is not left to roll past us"), err)
		}

		// Before trying the same batch again, ask the target what it actually
		// holds. A failure is not proof the write did not happen: a timeout can
		// arrive after the transaction landed, and re-applying it then repeats a
		// command that is not idempotent — measured as an RPUSH landing three
		// times too often under packet loss. Re-reading is what a task restart
		// always did, and is what makes retrying in place as safe as restarting.
		if err := r.refreshApplied(ctx); err != nil {
			return false, fmt.Errorf("apply %d changes: %w", count, err)
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("apply %d changes: %w", count, err)
		case <-time.After(wait):
		}
		if wait *= 2; wait > maxWait {
			wait = maxWait
		}
	}
}

// permanentApplyFailure reports whether the target refused a write for a reason
// that will refuse it again — a poisoned event rather than a target that is
// merely unavailable.
//
// The MySQL side is decided by SQLSTATE rather than by a list of error numbers.
// Two classes never become applicable by being retried:
//
//	23xxx  an integrity constraint the target holds — a foreign key, a unique
//	       index, a NOT NULL, a CHECK. The row is refused every time it is
//	       offered.
//	42xxx  the target does not have the table, the column or the privilege the
//	       event needs. None of those appear by asking again.
//
// Deciding this one error number at a time did not hold up: a missing table was
// added after the pipeline sat retrying "Table 'bench.nopk' doesn't exist" with
// task_up at 1, and a foreign key was added after it did the same with "Cannot
// add or update a child row". Both are the same shape, and so is every other
// member of those two classes.
func permanentApplyFailure(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToUpper(err.Error())

	// MySQL reports SQLSTATE in parentheses after the error number, as in
	// "Error 1452 (23000): ...".
	if m := sqlStatePattern.FindStringSubmatch(text); m != nil {
		switch m[1][:2] {
		case "23", "42":
			return true
		}
	}

	// MongoDB reports its own codes in the message rather than as SQLSTATE.
	// The same reasoning applies: an event the target refuses on its merits will
	// be refused every time it is offered, and retrying it for ever leaves a
	// task that looks alive while nothing moves. Measured on a sharded pair: a
	// duplicate key against a unique index on the target held one batch and
	// retried it indefinitely with task_blocked at 0, so the only sign was the
	// lag climbing — the exact shape the SQLSTATE classification was added to
	// stop on the MySQL side.
	for _, permanent := range []string{
		"E11000",                    // duplicate key against an index the target holds
		"E11001",                    // the older spelling of the same thing
		"DOCUMENTVALIDATIONFAILURE", // a validator the target has and the source does not
		"BSONOBJECTTOOLARGE",        // the document cannot be written at any time
	} {
		if strings.Contains(text, permanent) {
			return true
		}
	}

	for _, permanent := range []string{
		"WRONGTYPE",        // the key holds another type on the target
		"ERR VALUE IS NOT", // an increment against something that is not a number
		"ERR SYNTAX",
		"ERR UNKNOWN COMMAND",
		"BUSYGROUP",
	} {
		if strings.Contains(text, permanent) {
			return true
		}
	}
	return false
}

// sqlStatePattern finds the SQLSTATE a MySQL driver puts after the error number.
var sqlStatePattern = regexp.MustCompile(`\(([0-9A-Z]{5})\)`)

// applyBatch writes one batch and records where it got to.
//
// The whole batch goes to the applier at once, already split into runs, so it
// can be committed as a single atomic unit. Handing the runs over one at a time
// would let a failure land between two of them, which is the torn batch this
// design exists to prevent.
func (r *Runner) applyBatch(ctx context.Context, events []*domain.Event, pos domain.Position) error {
	writable := applicable(events)

	// What the stream carried, in Debezium's terms: events by operation, and
	// source transactions carried through. The pair is what catches a whole
	// transaction going missing — the defect this pipeline shipped with, where
	// a checkpoint moved past a transaction whose rows were never read. Neither
	// number alone would have shown it.
	counters := r.counters()
	transactions := 0
	filtered := 0
	for _, e := range events {
		if e.Heartbeat {
			continue
		}
		if e.EndsTransaction {
			transactions++
		}
	}
	for _, e := range writable {
		counters.Count(e.Op.String(), 1)
	}
	if n := len(events) - len(writable); n > 0 {
		// Read but not applicable: heartbeats and anything no mapping covers.
		for _, e := range events {
			if e.Heartbeat {
				n--
			}
		}
		filtered = n
	}
	metrics.CountFiltered(r.Opts.Labels, filtered)
	metrics.CountTransaction(r.Opts.Labels, transactions)

	newest := newestSourceTime(events)
	committedByApplier := false

	if len(writable) > 0 {
		runs := [][]*domain.Event{writable}
		if !r.Opts.StreamOrder {
			runs = orderedRuns(writable)
		}
		committed, err := r.applyWithRetry(ctx, runs, pos, len(writable))
		if err != nil {
			// The whole batch was rolled back, so every source transaction in
			// it is a transaction the target does not have.
			metrics.CountRolledBack(r.Opts.Labels, transactions)
			return err
		}
		committedByApplier = committed
		metrics.Applied(r.Opts.Labels, len(writable))
	}

	// The position moves only now, after everything it points past is on the
	// target. Recording it earlier would let a restart resume beyond changes
	// this process still held. An applier that committed it with the data has
	// already done this, and doing it again would be a second round trip for a
	// value that is already correct.
	if !pos.IsZero() && !committedByApplier {
		if err := r.Checkpoints.Save(ctx, r.CheckpointKey, pos.Payload); err != nil {
			return fmt.Errorf("record the position: %w", err)
		}
	}

	now := r.now()
	r.mu.Lock()
	if !newest.IsZero() {
		r.lastAppliedAt = newest
	}
	r.oldestPending = time.Time{}
	r.mu.Unlock()

	if !newest.IsZero() {
		metrics.SetLag(r.Opts.Labels, now.Sub(newest).Seconds())
	}
	return nil
}

// report refreshes the health gauges on a timer.
//
// Without it the applied lag only moves when a batch lands, so a task that has
// stopped applying leaves the gauge frozen at its last healthy value — and an
// alert on a frozen gauge never fires. Here the lag is measured from the oldest
// change still waiting, so it climbs for exactly as long as the task is stuck.
func (r *Runner) report(ctx context.Context) (stop func()) {
	ticker := time.NewTicker(r.Opts.reportInterval())
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				now := r.now()
				r.mu.Lock()
				oldest := r.oldestPending
				applied := r.lastAppliedAt
				read := r.lastReadAt
				heard := r.lastHeardAt
				used := r.queueUsed
				held := r.queueBytes
				r.mu.Unlock()

				// Behind: measured from the oldest change still waiting, so the
				// number climbs for exactly as long as the task is stuck.
				//
				// Caught up: measured from the newest thing the stream has
				// reported, heartbeats included. Measuring from the last change
				// applied instead made the lag climb whenever the source was
				// merely quiet — a database nobody had written to for an hour
				// reported an hour of lag while being perfectly up to date, which
				// is how a quiet Sunday pages somebody.
				lag, known := lagSeconds(now, oldest, read, applied)
				if known {
					metrics.SetLag(r.Opts.Labels, lag)
				}
				r.reportRetention(ctx, now, lag, known)
				if !heard.IsZero() {
					metrics.SetLastEventAge(r.Opts.Labels, now.Sub(heard).Seconds())
				}
				metrics.SetQueue(r.Opts.Labels, used, r.Opts.queueCapacity())
				metrics.SetQueueBytes(r.Opts.Labels, held)
			}
		}
	}()

	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// lagSeconds is how far behind the source the task is.
//
// Behind: measured from the oldest change still waiting, so the number climbs
// for exactly as long as the task is stuck.
//
// Caught up: measured from the newest thing the stream has reported, heartbeats
// included. Measuring from the last change applied instead made the lag climb
// whenever the source was merely quiet — a database nobody had written to for an
// hour reported an hour of lag while being perfectly up to date, which is how a
// quiet Sunday pages somebody.
func lagSeconds(now, oldest, read, applied time.Time) (float64, bool) {
	switch {
	case !oldest.IsZero():
		return now.Sub(oldest).Seconds(), true
	case !read.IsZero():
		return now.Sub(read).Seconds(), true
	case !applied.IsZero():
		return now.Sub(applied).Seconds(), true
	}
	return 0, false
}

// reportRetention publishes how long the task could afford to be stopped.
//
// The window is asked for rarely; the headroom is recomputed every tick from
// it, because the lag underneath it moves.
func (r *Runner) reportRetention(ctx context.Context, now time.Time, lag float64, lagKnown bool) {
	source, ok := r.Reader.(domain.Retention)
	if !ok || r.windowFailed {
		return
	}

	if r.windowAt.IsZero() || now.Sub(r.windowAt) >= retentionRefresh {
		window, err := source.Window(ctx)
		switch {
		case errors.Is(err, domain.ErrWindowNotYet):
			// Not an answer, but not a refusal either: some sources have to be
			// measured twice before they can say. Asking again next time is the
			// difference between the metric appearing a few minutes late and it
			// never appearing at all.
			return
		case err != nil:
			// Asked once, told no. Publishing a guess here would be worse than
			// publishing nothing: the number is only read when somebody is
			// deciding whether there is still time to restart rather than
			// re-copy.
			r.windowFailed = true
			r.log().WithError(err).Warn(r.tag(
				"the source did not say how far its log reaches, so no retention " +
					"headroom is published for this task"))
			return
		case window <= 0:
			r.windowFailed = true
			r.log().Warn(r.tag(
				"the source reported a retention window of zero, so no headroom is published"))
			return
		}
		r.window, r.windowAt = window, now
	}

	if !lagKnown {
		// Nothing has been read yet, so there is no position whose age the
		// headroom could be measured from.
		return
	}
	metrics.SetRetention(r.Opts.Labels, r.window.Seconds(), r.window.Seconds()-lag)
}

// newestSourceTime reports when the source made the most recent change in a
// batch, ignoring the syncer's own heartbeats.
func newestSourceTime(events []*domain.Event) time.Time {
	var newest time.Time
	for _, e := range events {
		if e.Heartbeat || e.SourceTime.IsZero() {
			continue
		}
		if e.SourceTime.After(newest) {
			newest = e.SourceTime
		}
	}
	return newest
}

// resetTimer restarts a timer that may or may not have fired.
func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
