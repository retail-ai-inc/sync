package pipeline

import (
	"context"
	"errors"
	"fmt"
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
	// FlushInterval is how long a partly filled batch waits. It is the syncer's
	// own contribution to the recovery point, paid on every change that arrives
	// more slowly than a batch fills.
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

	// The position is pinned before a row is copied and stored only once the
	// copy has finished. Pinning afterwards loses every write made while the
	// copy ran; storing before it finishes means an interrupted copy resumes
	// from a point it never reached.
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
			r.mu.Unlock()

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

// applyBatch writes one batch and records where it got to.
//
// The whole batch goes to the applier at once, already split into runs, so it
// can be committed as a single atomic unit. Handing the runs over one at a time
// would let a failure land between two of them, which is the torn batch this
// design exists to prevent.
func (r *Runner) applyBatch(ctx context.Context, events []*domain.Event, pos domain.Position) error {
	writable := applicable(events)
	newest := newestSourceTime(events)
	committedByApplier := false

	if len(writable) > 0 {
		runs := orderedRuns(writable)
		committed, err := r.Applier.Apply(ctx, runs, pos)
		if err != nil {
			metrics.Failed(r.Opts.Labels, len(writable))
			return fmt.Errorf("apply %d changes: %w", len(writable), err)
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
