package pipeline

import (
	"context"
	"errors"
	"fmt"
	"github.com/retail-ai-inc/sync/internal/platform/resilience"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Checkpoints records how far the target has been written, named here so the
// runner does not depend on the infrastructure package.
type Checkpoints interface {
	Load(ctx context.Context, key string) (string, error)
	Save(ctx context.Context, key, payload string) error
}

type Options struct {
	Limits Limits
	// FlushInterval is the longest a partly filled batch waits; a batch with an
	// empty queue behind it is sent at once.
	FlushInterval time.Duration
	// QueueCapacity bounds how far the reader may run ahead. Reaching it stops the
	// reader, so the backlog sits in the source log where it is durable.
	QueueCapacity int
	// QueueBytes bounds how much change data may be held between the reader and
	// the applier. The capacities above bound the event count, which is not a
	// bound on memory: a MongoDB document may be 16MB and a Redis value 512MB.
	// Zero means the default.
	QueueBytes int64
	// SnapshotQueueCapacity bounds it while the initial copy runs, when nothing
	// drains the queue yet. Reaching this one is not durable the way reaching
	// QueueCapacity is: the reader stops, and the pinned point then ages out of
	// the source's log while the copy still has hours left, which costs the whole
	// copy.
	SnapshotQueueCapacity int
	// ReportInterval is how often the health gauges refresh while nothing happens,
	// so a stalled task shows a rising age rather than a frozen one.
	ReportInterval time.Duration
	// ShutdownGrace bounds the last batch once the run is asked to stop: being
	// killed past the pod's grace period is a harder stop than the one this
	// avoids.
	ShutdownGrace time.Duration

	Labels metrics.Labels
	Logger logrus.FieldLogger
	// Engine names the engine in log lines, as "[MySQL]" or "[MongoDB]".
	Engine string

	// StreamOrder hands the batch over as one run in read order. Splitting is only
	// safe when every event is an idempotent write of a whole record.
	StreamOrder bool
}

const (
	defaultFlushInterval = 500 * time.Millisecond
	defaultQueueCapacity = 2000
	// Enough to hold a batch of the largest events several times over, and far
	// short of what two hundred thousand of them would be.
	defaultQueueBytes = 512 << 20
	// Slots are a pointer each, so a deep queue costs almost nothing until the
	// changes actually arrive; what it buys is a copy that survives a source
	// still being written to.
	defaultSnapshotQueueCapacity = 200000
	defaultReportInterval        = time.Second
	// Well inside Kubernetes' default thirty-second termination grace period.
	defaultShutdownGrace = 5 * time.Second
)

func (o Options) flushInterval() time.Duration {
	if o.FlushInterval > 0 {
		return o.FlushInterval
	}
	return defaultFlushInterval
}

func (o Options) queueBytes() int64 {
	if o.QueueBytes > 0 {
		return o.QueueBytes
	}
	return defaultQueueBytes
}

func (o Options) queueCapacity() int {
	if o.QueueCapacity > 0 {
		return o.QueueCapacity
	}
	return defaultQueueCapacity
}

func (o Options) snapshotQueueCapacity() int {
	if o.SnapshotQueueCapacity > 0 {
		return o.SnapshotQueueCapacity
	}
	return defaultSnapshotQueueCapacity
}

func (o Options) reportInterval() time.Duration {
	if o.ReportInterval > 0 {
		return o.ReportInterval
	}
	return defaultReportInterval
}

func (o Options) shutdownGrace() time.Duration {
	if o.ShutdownGrace > 0 {
		return o.ShutdownGrace
	}
	return defaultShutdownGrace
}

// Runner is the replication loop. One per task.
type Runner struct {
	Reader      domain.Reader
	Applier     domain.Applier
	Snapshotter domain.Snapshotter
	Checkpoints Checkpoints
	// CheckpointKey names this task's position; empty is the single-stream case.
	CheckpointKey string

	// Resyncs re-copy one object each alongside the stream. Empty is the normal
	// case.
	Resyncs []*Resync

	Opts Options

	// clock is time.Now, replaced in tests.
	clock func() time.Time

	mu sync.Mutex
	// oldestPending is when the source made the oldest change read and not yet
	// applied, so a task that stops applying shows a lag that climbs.
	oldestPending time.Time
	// lastAppliedAt is when the source made the most recent change that reached
	// the target.
	lastAppliedAt time.Time
	// lastHeardAt is when anything last arrived, heartbeats included.
	lastHeardAt time.Time
	// lastReadAt is the source's time of the newest change read. A re-copy holds
	// each chunk until this passes it, which is what orders the two.
	lastReadAt time.Time
	queueUsed  int
	// queueCap is the capacity the queue was made with, which differs between the
	// initial copy and the stream that follows it.
	queueCap int

	// window is the source's retention and windowAt when it was asked for: a
	// server setting, so it is read occasionally rather than every second.
	window   time.Duration
	windowAt time.Time
	// windowFailed stops a source that cannot answer from being asked on every
	// refresh.
	windowFailed bool

	// events counts what the stream carried by operation, built once because
	// counting happens per event.
	events *metrics.EventCounters
	// held bounds the change data between the reader and the applier, which the
	// queue's own capacity does not: it counts events, and an event has no fixed
	// size.
	held *budget
}

func (r *Runner) counters() *metrics.EventCounters {
	if r.events == nil {
		r.events = metrics.NewEventCounters(r.Opts.Labels)
	}
	return r.events
}

// retentionRefresh is how often the source is asked how far its log reaches — a
// setting, not a measurement, and a round trip per shard.
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
// nil or a transient error means try again; Unrecoverable means stop.
func (r *Runner) Run(ctx context.Context) error {
	start, owed, err := r.startingPoint(ctx)
	if err != nil {
		return err
	}

	// The stream opens before a record is copied, and the reader below drains it
	// from the moment it does. Copying first and opening afterwards is what this
	// replaces: the copy's own writes push the source's log past the pinned
	// point, so a source whose smallest shard holds an hour of log loses that
	// point long before a copy of any size finishes -- and it is only found out
	// at the end, with the position already recorded, which leaves the task
	// unable to resume and unable to copy again. Opening first costs nothing,
	// since the pinned point is where the stream starts either way, and it keeps
	// the cursor ahead of the truncation for as long as the copy runs.
	if err := r.Reader.Open(ctx, start); err != nil {
		return fmt.Errorf("open the source stream: %w", err)
	}
	defer func() { _ = r.Reader.Close() }()

	now := r.now()
	r.mu.Lock()
	r.lastHeardAt = now
	r.lastAppliedAt = now
	r.mu.Unlock()

	r.queueCap = r.Opts.queueCapacity()
	if owed {
		r.queueCap = r.Opts.snapshotQueueCapacity()
	}
	r.held = newBudget(r.Opts.queueBytes())
	queue := make(chan *domain.Event, r.queueCap)
	readCtx, stopReading := context.WithCancel(ctx)
	defer stopReading()

	// The queue has several producers once a re-copy runs, so it is closed after
	// all of them finish: closing it from the reader raced, and a re-copy still
	// handing over a chunk took the process down.
	var producers sync.WaitGroup

	readErr := make(chan error, 1)
	producers.Add(1)
	go func() {
		defer producers.Done()
		readErr <- resilience.Guard(func() error { return r.read(readCtx, queue) })
	}()

	stopReporting := r.report(readCtx)
	defer stopReporting()

	// Nothing drains the queue yet, so the changes made while the copy runs
	// collect in it and are applied in stream order once the copy is done. That
	// converges even though the copy reads each record at whatever state it had
	// when the copy reached it: every change that took a record to that state is
	// itself in the queue, and the last one applied is the newest.
	if owed {
		if err := r.copy(ctx, start, queue); err != nil {
			return err
		}
	}

	resyncErr := r.startResyncs(readCtx, queue, &producers)

	go func() {
		producers.Wait()
		close(queue)
	}()

	applyErr := r.apply(ctx, queue)

	// A re-copy that failed is worth reporting even when the stream ended cleanly.
	select {
	case err := <-resyncErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			r.log().Errorf(r.tag("A re-copy stopped: %v"), err)
		}
	default:
	}

	// The reader knows why the stream ended, so its error wins: an applier
	// stopping because its queue closed says nothing about the source.
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

// startResyncs runs each re-copy alongside the stream, on the same queue so the
// applier orders them against the stream's changes.
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
			defer func() {
				if recovered := recover(); recovered != nil {
					failed <- resilience.Recovered(recovered)
				}
			}()
			r.log().Infof(r.tag("Re-copying %s alongside the stream"), resync.NS)
			err := resync.run(ctx, read, func(events []*domain.Event) error {
				if len(events) == 0 {
					return nil
				}
				// A chunk is its own batch: it carries no position, and its last event
				// closes the batch so it is not held for a boundary that will not come.
				events[len(events)-1].EndsTransaction = true
				for _, event := range events {
					event.Pos = domain.Position{}
					r.held.acquire(ctx, int64(event.Bytes))
					select {
					case queue <- event:
					case <-ctx.Done():
						r.held.release(int64(event.Bytes))
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

// startingPoint reports where the stream starts and whether the target still
// owes an initial copy. It pins but does not copy: the copy runs once the
// stream is open, which is the whole point of the split.
func (r *Runner) startingPoint(ctx context.Context) (domain.Position, bool, error) {
	payload, err := r.Checkpoints.Load(ctx, r.CheckpointKey)
	if err != nil {
		return domain.Position{}, false, fmt.Errorf("read the stored position: %w", err)
	}
	if payload != "" {
		r.log().Infof(r.tag("Resuming from the stored position"))
		return domain.Position{Payload: payload}, false, nil
	}

	if r.Snapshotter == nil {
		return domain.Position{}, false, nil
	}

	pinned, err := r.Snapshotter.Pin(ctx)
	if err != nil {
		return domain.Position{}, false, fmt.Errorf("pin the snapshot's starting point: %w", err)
	}
	return pinned, true, nil
}

// copy fills the target while the stream is open and being read.
//
// The position is recorded only once the copy has finished: a copy that gave up
// halfway must not leave behind a position that claims a complete base, because
// the stream would then carry on from it and the gap would stay for good.
func (r *Runner) copy(ctx context.Context, pinned domain.Position, queue chan *domain.Event) error {
	started := r.now()
	metrics.SnapshotStarted(r.Opts.Labels, 0)
	r.log().Infof(r.tag("Snapshot pinned; copying with the stream already open"))

	failed := func(err error) error {
		metrics.SnapshotFinished(r.Opts.Labels, false, r.now().Sub(started).Seconds())
		return err
	}

	stopWatching := r.watchQueuePressure(ctx, queue)
	err := r.Snapshotter.Copy(ctx)
	stopWatching()
	if err != nil {
		return failed(fmt.Errorf("copy the source: %w", err))
	}

	if err := r.Checkpoints.Save(ctx, r.CheckpointKey, pinned.Payload); err != nil {
		return failed(fmt.Errorf("record the snapshot's starting point: %w", err))
	}
	metrics.SnapshotFinished(r.Opts.Labels, true, r.now().Sub(started).Seconds())
	r.log().Infof(r.tag("Copy finished; applying the changes made while it ran"))
	return nil
}

// watchQueuePressure reports a queue running out of room while the copy holds
// the applier. A full queue stops the reader, and a stopped reader is the one
// thing opening the stream early was meant to prevent: the source's log carries
// on past the pinned point with nothing consuming it.
func (r *Runner) watchQueuePressure(ctx context.Context, queue chan *domain.Event) func() {
	done := make(chan struct{})
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				r.log().Errorf(r.tag("The queue-pressure watch stopped: %v"),
					resilience.Recovered(recovered))
			}
		}()
		ticker := time.NewTicker(r.Opts.reportInterval())
		defer ticker.Stop()
		warned := false
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				held, room := len(queue), cap(queue)
				if warned || room == 0 || held*10 < room*8 {
					continue
				}
				warned = true
				r.log().Warnf(r.tag("The changes made during the copy hold %d of the "+
					"queue's %d places. If it fills, the stream stops being read and the "+
					"pinned point can age out of the source's log, which costs the whole "+
					"copy: raise SnapshotQueueCapacity, or copy when the source is quieter"),
					held, room)
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

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

		// Charged before the hand-over and released when the batch holding it has
		// landed, so the reader waits on memory as well as on the event count.
		r.held.acquire(ctx, int64(event.Bytes))

		select {
		case queue <- event:
		case <-ctx.Done():
			r.held.release(int64(event.Bytes))
			return nil
		}
	}
}

func (r *Runner) apply(ctx context.Context, queue <-chan *domain.Event) error {
	var b batch
	timer := time.NewTimer(r.Opts.flushInterval())
	defer timer.Stop()

	// pending is the position the batch would advance to, never recorded ahead of
	// the data it points past.
	var pending domain.Position

	// The run's context while the run lives, an independent bounded one once it is
	// asked to stop, because a driver refuses a cancelled context before it
	// reaches the target. Chosen here, not in the stop branch: a cancelled context
	// and a non-empty queue are ready at once and select picks at random.
	flush := func() error {
		if b.len() == 0 {
			return nil
		}
		if !b.cuttable() {
			// The batch ends inside a source transaction, so it is not a legal cut
			// point; the alternative is showing the target half a transaction.
			return nil
		}

		writeCtx, giveUp := ctx, func() {}
		if ctx.Err() != nil {
			writeCtx, giveUp = context.WithTimeout(
				context.WithoutCancel(ctx), r.Opts.shutdownGrace())
		}
		defer giveUp()

		// The room these events took is given back once they have landed: until
		// then they are still held, and the reader must not run further ahead on
		// the strength of a batch that has not been written.
		held := int64(b.bytes)
		events := b.take()
		err := r.applyBatch(writeCtx, events, pending)
		if err == nil {
			r.held.release(held)
		}
		return err
	}

	for {
		select {
		case <-ctx.Done():
			// A clean stop applies what is already whole. flush knows the run was asked
			// to stop and writes through a context of its own.
			if err := flush(); err != nil {
				// Reported, not returned: the position did not move either, so the next
				// start replays it. Returning would call a stop a failure.
				r.log().Warnf(r.tag("The last batch did not reach the target before the "+
					"stop, and will be replayed on the next start: %v"), err)
			}
			return nil

		case event, ok := <-queue:
			if !ok {
				if err := flush(); err != nil {
					return err
				}
				return nil
			}

			// A schema change gets a batch to itself: it cannot share a transaction with
			// rows.
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
			r.mu.Unlock()

			// Nothing else waiting, so holding the batch back only costs latency —
			// Kafka's linger.ms is 0 for the same reason.
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

// refreshApplied asks the store to re-read what the target holds, for stores
// that can; one that cannot is no worse off than before retrying existed.
func (r *Runner) refreshApplied(ctx context.Context) error {
	type refreshable interface {
		Refresh(ctx context.Context) error
	}
	if store, ok := r.Checkpoints.(refreshable); ok {
		return store.Refresh(ctx)
	}
	return nil
}

// applyWithRetry waits for a target that is not ready rather than ending the
// run: the reader is what keeps the source's log from rolling past the
// position, and on Memorystore that log is a ring of tens of kilobytes.
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
			// Retrying would fail identically for ever, and stepping over it would leave
			// the target permanently different with nothing alarming.
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

		// A failure is not proof the write did not happen: a timeout can arrive after
		// the transaction landed, and replaying then repeats a non-idempotent
		// command.
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
// Decided by SQLSTATE class rather than error number, because deciding one
// number at a time kept missing members of the same class:
//
//	23xxx  an integrity constraint the target holds. Refused every time.
//	42xxx  the target lacks the table, column or privilege. Asking again does
//	       not create it.
func permanentApplyFailure(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToUpper(err.Error())

	// MySQL reports SQLSTATE in parentheses after the error number: "Error 1452
	// (23000)".
	if m := sqlStatePattern.FindStringSubmatch(text); m != nil {
		switch m[1][:2] {
		case "23", "42":
			return true
		}
	}

	// MongoDB reports its codes in the message, not as SQLSTATE. Measured on a
	// sharded pair, a duplicate key retried for ever with task_blocked at 0.
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

var sqlStatePattern = regexp.MustCompile(`\(([0-9A-Z]{5})\)`)

// applyBatch writes one batch and records where it got to. The whole batch goes
// over at once so a failure cannot land between two runs.
func (r *Runner) applyBatch(ctx context.Context, events []*domain.Event, pos domain.Position) error {
	writable := applicable(events)

	// Events by operation and source transactions carried through. The pair is
	// what catches a whole transaction going missing; neither number alone shows
	// it.
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
			// The whole batch rolled back, so every source transaction in it is one the
			// target does not have.
			metrics.CountRolledBack(r.Opts.Labels, transactions)
			return err
		}
		committedByApplier = committed
		metrics.Applied(r.Opts.Labels, len(writable))
	}

	// The position moves only now, after everything it points past is on the
	// target. An applier that committed it with the data has already done this.
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

// report refreshes the health gauges on a timer: without it a stuck task leaves
// the lag frozen at its last healthy value, and an alert never fires.
func (r *Runner) report(ctx context.Context) (stop func()) {
	ticker := time.NewTicker(r.Opts.reportInterval())
	done := make(chan struct{})

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				r.log().Errorf(r.tag("The health reporter stopped, so its gauges are "+
					"now stale: %v"), resilience.Recovered(recovered))
			}
		}()
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
				r.mu.Unlock()
				// What is held between the reader and the applier, not what the
				// current batch happens to hold: the events still in the queue are
				// held too, and they are the ones the count cannot bound.
				held := r.held.heldBytes()

				// See lagSeconds for which clock each case is measured from.
				lag, known := lagSeconds(now, oldest, read, applied)
				if known {
					metrics.SetLag(r.Opts.Labels, lag)
				}
				r.reportRetention(ctx, now, lag, known)
				if !heard.IsZero() {
					metrics.SetLastEventAge(r.Opts.Labels, now.Sub(heard).Seconds())
				}
				metrics.SetQueue(r.Opts.Labels, used, r.queueCap)
				metrics.SetQueueBytes(r.Opts.Labels, held)
			}
		}
	}()

	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

// lagSeconds reports how far behind the target is: the age of the oldest change
// that has been read and not yet applied.
//
// Nothing waiting means nothing is behind, so the answer is zero. It used to be
// the age of the newest thing the stream had reported, which is a different
// question -- and on a quiet source the newest thing reported is the last
// heartbeat, so the figure walked from zero up to the heartbeat interval and
// dropped back. That read as three to five seconds of steady lag on links whose
// measured end-to-end delay was about a tenth of a second, and it was not a
// small number being reported imprecisely: it was the wrong quantity. The
// earlier spelling, measuring from the last change applied, was wrong the same
// way in the other direction -- an hour since the last write reported an hour
// of lag.
//
// Whether the stream is alive is a separate question with its own metric.
// sync_source_last_event_age_seconds measures the age of the newest thing heard
// from the source, heartbeats included, and rising there is what says a link has
// gone quiet. Folding the two together left neither answerable.
//
// Unknown, rather than zero, until something has been read or applied: a task
// that has not reached its source yet is not caught up.
func lagSeconds(now, oldest, read, applied time.Time) (float64, bool) {
	if !oldest.IsZero() {
		return now.Sub(oldest).Seconds(), true
	}
	if read.IsZero() && applied.IsZero() {
		return 0, false
	}
	return 0, true
}

// reportRetention publishes how long the task could afford to be stopped. The
// window is asked for rarely; the headroom is recomputed each tick.
func (r *Runner) reportRetention(ctx context.Context, now time.Time, lag float64, lagKnown bool) {
	source, ok := r.Reader.(domain.Retention)
	if !ok || r.windowFailed {
		return
	}

	if r.windowAt.IsZero() || now.Sub(r.windowAt) >= retentionRefresh {
		window, err := source.Window(ctx)
		switch {
		case errors.Is(err, domain.ErrWindowNotYet):
			// Not an answer but not a refusal: some sources must be measured twice
			// before they can say, so asking again beats never publishing at all.
			return
		case err != nil:
			// Asked once, told no. A guess would be worse than nothing: this is only
			// read when somebody is deciding restart against re-copy.
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
		// Nothing read yet, so there is no position whose age the headroom could use.
		return
	}
	metrics.SetRetention(r.Opts.Labels, r.window.Seconds(), r.window.Seconds()-lag)
}

// newestSourceTime reports when the source made the most recent change in a
// batch, ignoring heartbeats.
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

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}
