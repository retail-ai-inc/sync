package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// Writing a batch, one transaction per slot.
//
// A Redis cluster has no atomic unit spanning slots — MULTI requires one hash
// slot — so the position cannot be committed with a whole batch the way it can
// for MySQL or MongoDB, and a crash between the data and the position replays
// commands: a wrong counter, or a payment queue entry handled twice.
//
// The way out is to use the transactions that do exist. Every command in a
// cluster's replication stream touches a single slot, so each slot's commands
// commit together with a marker saying how far that slot has been applied.

type Applier struct {
	Target goredis.UniversalClient
	// Source is used to re-read a key's value, for the phase where commands
	// cannot be replayed safely and for repairing divergence.
	Source goredis.UniversalClient

	Positions *Checkpoints
	Commands  *commandTable
	// Link is the connection this shard reads through. The applied offset is
	// recorded on it so that it and the received offset are published together;
	// see link.applied.
	Link   *link
	Logger logrus.FieldLogger
	Labels metrics.Labels

	// Concurrency bounds how many slot transactions are in flight at once. Zero
	// means the default.
	Concurrency int
	// SourceHomeDB is the database the source client is on, so a read from
	// another one knows where to put the connection back.
	SourceHomeDB int
	// RestoreState writes back what this task keeps on the target, into a
	// transaction that has just replicated a flush.
	//
	// A flush empties whole databases, and this task's own position and
	// direction claim live in one of them, so replicating the source's flush
	// destroys them. Doing it inside the same transaction leaves no moment where
	// they are missing: a lost position costs a full re-copy, and a lost claim
	// leaves the target unclaimed for anything else to take as a source.
	//
	// It is a function rather than the values because what has to be written
	// belongs to two other packages, and the applier has no business knowing
	// either. Nil means there is nothing to restore.
	RestoreState func(ctx context.Context, pipe goredis.Pipeliner, position string)
	// BookkeepingDB is the database the slot markers and the stored position
	// live in. It is the database the target connection was opened on, and it
	// is not one of the databases being replicated into -- the markers belong
	// to this task rather than to any database the source has.
	BookkeepingDB int

	mu sync.Mutex
	// skipped counts commands dropped because their slot had already applied
	// them. It is the evidence that the replay path is being exercised, which a
	// test cannot otherwise tell from the replay path never being reached.
	skipped int
}

const defaultConcurrency = 16

func (a *Applier) concurrency() int {
	if a.Concurrency > 0 {
		return a.Concurrency
	}
	return defaultConcurrency
}

// repairKey names a key to re-read, in the database it lives in.
type repairKey struct {
	db  int
	key []byte
}

type work struct {
	slot int
	// commands are the stream commands for this slot, in the order they were
	// read.
	commands []*command
	// repairs are keys whose value is to be re-read from the source and written
	// whole, used where replaying a command would not be safe. Each carries the
	// database it belongs to: the value has to be read from that one.
	//
	// A key appears once however many times the batch changed it: the repair
	// writes whatever the source holds now, so reading it twice in one batch
	// would cost a round trip to arrive at the same answer.
	repairs []repairKey
	seen    map[string]bool
}

// Apply writes one batch and records how far each slot it touched has got.
//
// It reports having committed the position itself, because it has: every slot's
// marker went in with that slot's data. A batch where some slots commit and
// others fail is safe to retry — the ones that landed skip their commands the
// second time round, because their marker is already past them.
func (a *Applier) Apply(ctx context.Context, runs [][]*domain.Event, pos domain.Position) (bool, error) {
	events := flatten(runs)
	if len(events) == 0 {
		return true, nil
	}

	position, err := decodePosition(pos.Payload)
	if err != nil {
		return false, err
	}
	markers := a.Positions.markersFor(position.Offset)

	batchEnd := endOf(events)
	if batchEnd == 0 {
		return false, fmt.Errorf("a batch of %d events carries no stream offset", len(events))
	}

	started := time.Now()

	// A flush empties whole databases, so nothing in this batch may be applied
	// beside it: the slot transactions run concurrently, and a write that landed
	// after a concurrent flush would be erased by it. The whole batch goes in
	// one transaction, in the order the stream had it.
	// Planned per segment in the flush path, so planning the batch here as well
	// would count every skipped command twice -- and Skipped() is the evidence
	// that the replay path is being exercised at all.
	var jobs []work
	if containsFlush(events) {
		if err := a.applyAroundFlush(ctx, events, batchEnd, markers, pos.Payload); err != nil {
			return false, err
		}
	} else {
		planned, err := a.plan(events, markers)
		if err != nil {
			return false, err
		}
		jobs = planned
		if err := a.run(ctx, jobs, batchEnd, markers); err != nil {
			return false, err
		}
	}

	// Every slot this batch touched has landed, so the resume floor may move to
	// the end of it. This is the only place it advances, and only from here: a
	// batch that failed part way leaves it where it was, so the slots that did
	// not land are read again rather than assumed.
	if err := a.Positions.Save(ctx, "", pos.Payload); err != nil && ctx.Err() == nil {
		// The floor is a hint. Losing it costs a longer replay next time, which
		// the markers make harmless, so this is not worth failing a batch over —
		// and a task being stopped is not worth mentioning at all.
		a.logger().Warnf("[Redis] Could not record the resume floor: %v", err)
	}

	metrics.ObserveBatch(a.Labels, time.Since(started), 0, len(jobs), len(jobs), len(events))
	if a.Link != nil {
		a.Link.applied.Store(batchEnd)
	}
	metrics.CountValueRepairs(a.Labels, repairsIn(jobs))
	return true, nil
}

func repairsIn(jobs []work) int {
	total := 0
	for _, job := range jobs {
		total += len(job.repairs)
	}
	return total
}

func (a *Applier) logger() logrus.FieldLogger { return orDefault(a.Logger) }

func (a *Applier) Skipped() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.skipped
}

func (a *Applier) plan(events []*domain.Event, markers []int64) ([]work, error) {
	bySlot := make(map[int]*work)
	order := make([]int, 0, 16)

	for _, event := range events {
		switch payload := event.Payload.(type) {
		case *command:
			// A command at or before the slot's marker is already on the target.
			// This is the skip that makes replaying the stream safe.
			if payload.offset <= markers[payload.slot] {
				a.mu.Lock()
				a.skipped++
				a.mu.Unlock()
				continue
			}
			job, ok := bySlot[payload.slot]
			if !ok {
				job = &work{slot: payload.slot}
				bySlot[payload.slot] = job
				order = append(order, payload.slot)
			}
			job.commands = append(job.commands, payload)

		case *valueRepair:
			if payload.offset != 0 && payload.offset <= markers[payload.slot] {
				continue
			}
			job, ok := bySlot[payload.slot]
			if !ok {
				job = &work{slot: payload.slot}
				bySlot[payload.slot] = job
				order = append(order, payload.slot)
			}
			if job.seen == nil {
				job.seen = make(map[string]bool)
			}
			if !job.seen[string(payload.key)] {
				job.seen[string(payload.key)] = true
				job.repairs = append(job.repairs, repairKey{db: payload.db, key: payload.key})
			}

		case *flush:
			// Ordered separately; see applyAroundFlush.

		default:
			return nil, fmt.Errorf("a batch carried a %T, which this applier cannot write",
				event.Payload)
		}
	}

	jobs := make([]work, 0, len(order))
	for _, slot := range order {
		jobs = append(jobs, *bySlot[slot])
	}
	return jobs, nil
}

func (a *Applier) run(ctx context.Context, jobs []work, batchEnd int64, markers []int64) error {
	if len(jobs) == 0 {
		return nil
	}

	limit := a.concurrency()
	if limit > len(jobs) {
		limit = len(jobs)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
		done     = make([]bool, len(jobs))
	)
	queue := make(chan int)

	for worker := 0; worker < limit; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range queue {
				err := a.applySlot(ctx, jobs[index], batchEnd)
				mu.Lock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
				} else {
					done[index] = true
				}
				mu.Unlock()
			}
		}()
	}
	for index := range jobs {
		queue <- index
	}
	close(queue)
	wg.Wait()

	// Every slot that committed has moved on, whether or not its neighbours did.
	// Recording that here is what makes retrying the batch cheap and correct.
	for index, ok := range done {
		if ok {
			markers[jobs[index].slot] = batchEnd
		}
	}
	return firstErr
}

func (a *Applier) applySlot(ctx context.Context, job work, batchEnd int64) error {
	repairs, err := a.readRepairs(ctx, job)
	if err != nil {
		return err
	}

	tx := a.Target.TxPipeline()

	// The stream interleaves the source's databases, so the transaction moves
	// between them as its commands do. SELECT inside MULTI is what keeps this
	// one transaction: splitting it per database would write the slot's marker
	// more than once, and a marker written by a transaction that committed
	// while its neighbour failed says applied about work that is not.
	at := a.BookkeepingDB
	selectDB := func(db int) {
		if db != at {
			tx.Do(ctx, "select", db)
			at = db
		}
	}
	for _, cmd := range job.commands {
		selectDB(cmd.db)
		tx.Do(ctx, cmd.arguments()...)
	}
	for _, repair := range repairs {
		selectDB(repair.db)
		repair.queue(ctx, tx)
	}

	// Back to where the bookkeeping lives, both for the marker and so the
	// connection is returned to the pool on the database its owner expects.
	selectDB(a.BookkeepingDB)
	// Stamped with the source history it belongs to. An offset means nothing
	// outside one: after a reshard the slot is served by another master, which
	// numbers its stream independently, and a bare number left by the old one
	// could read as "already applied" about a command the new one has never
	// sent.
	marker := tx.Set(ctx, OffsetKey(job.slot, a.Positions.TaskID),
		markerValue(a.Positions.ReplID(), batchEnd), 0)

	results, err := tx.Exec(ctx)

	// The per-command results are read before Exec's own error, not after it.
	//
	// Redis does not roll a transaction back when one of its commands fails at
	// run time — the rest still apply, and so does the marker. go-redis reports
	// the first failing command as Exec's error, so returning on that error sent
	// a run-time failure back as something a retry could fix, while the marker in
	// the same transaction had already moved the position past it. The task
	// restarted, the marker skipped the command that had failed, and the
	// difference was permanent and invisible. The check below existed for exactly
	// this and could not be reached.
	for index, result := range results {
		if failed := result.Err(); serverRefused(failed) {
			return domain.Unrecoverable(
				"%v failed on the target in slot %d: %v. The target's copy of that "+
					"key has diverged from the source; a transaction that fails part way "+
					"is not rolled back, so the position has already moved past it. "+
					"Re-copy the key or the task", describeResult(results, index), job.slot, failed)
		}
	}

	// No command reported a failure, so this is the transport rather than the
	// data: the transaction did not run, nothing was applied, and nothing moved.
	if err != nil && err != goredis.Nil {
		return fmt.Errorf("write slot %d: %w", job.slot, err)
	}
	if err := marker.Err(); err != nil && err != goredis.Nil {
		return fmt.Errorf("record the marker for slot %d: %w", job.slot, err)
	}
	return nil
}

func (a *Applier) readRepairs(ctx context.Context, job work) ([]*repairedValue, error) {
	if len(job.repairs) == 0 {
		return nil, nil
	}
	if a.Source == nil {
		return nil, fmt.Errorf("a repair needs the source to read from")
	}

	// One read per database. A repair is the exception rather than the rule, so
	// the extra round trip costs less than holding a client open per database.
	byDB := map[int][][]byte{}
	order := make([]int, 0, 2)
	for _, repair := range job.repairs {
		if _, seen := byDB[repair.db]; !seen {
			order = append(order, repair.db)
		}
		byDB[repair.db] = append(byDB[repair.db], repair.key)
	}

	var values []*repairedValue
	for _, db := range order {
		read, err := readValuesIn(ctx, a.Source, a.SourceHomeDB, db, byDB[db])
		if err != nil {
			return nil, err
		}
		values = append(values, read...)
	}
	return values, nil
}

func flatten(runs [][]*domain.Event) []*domain.Event {
	if len(runs) == 1 {
		return runs[0]
	}
	var events []*domain.Event
	for _, run := range runs {
		events = append(events, run...)
	}
	return events
}

func endOf(events []*domain.Event) int64 {
	var end int64
	for _, event := range events {
		switch payload := event.Payload.(type) {
		case *command:
			if payload.offset > end {
				end = payload.offset
			}
		case *valueRepair:
			if payload.offset > end {
				end = payload.offset
			}
		case *flush:
			if payload.offset > end {
				end = payload.offset
			}
		}
	}
	return end
}

func describeResult(results []goredis.Cmder, index int) string {
	if index < 0 || index >= len(results) {
		return "a command"
	}
	return results[index].Name()
}

func containsFlush(events []*domain.Event) bool {
	for _, event := range events {
		if _, ok := event.Payload.(*flush); ok {
			return true
		}
	}
	return false
}

// applyAroundFlush writes a batch that contains a flush.
//
// A flush empties whole databases, so nothing may be applied beside it: the
// ordinary path commits slots concurrently, and a write that landed beside a
// flush rather than before or after it would be erased or spared by a race. The
// batch is split at each flush and the pieces go through the ordinary per-slot
// path, with the flush executed between them.
//
// The pieces keep that path rather than becoming one transaction of their own,
// because a cluster has no transaction spanning slots -- and the client does
// not say so. It splits a cross-slot MULTI into one per slot and reports
// success, so a batch written that way looks atomic and is not.
func (a *Applier) applyAroundFlush(ctx context.Context, events []*domain.Event,
	batchEnd int64, markers []int64, position string) error {

	segment := make([]*domain.Event, 0, len(events))

	// Each segment records how far it reached, not how far the batch reaches. A
	// marker is what makes a replayed command skippable, so writing the batch's
	// end against a segment claims the rest of the batch has landed when it has
	// not -- and the events after a flush in the same batch would then be
	// skipped, silently, on the way to being applied at all.

	writeSegment := func() error {
		if len(segment) == 0 {
			return nil
		}
		jobs, err := a.plan(segment, markers)
		if err != nil {
			return err
		}
		end := endOf(segment)
		segment = segment[:0]
		if end == 0 {
			end = batchEnd
		}
		return a.run(ctx, jobs, end, markers)
	}

	for _, event := range events {
		flushed, isFlush := event.Payload.(*flush)
		if !isFlush {
			segment = append(segment, event)
			continue
		}
		if err := writeSegment(); err != nil {
			return err
		}
		// Only SWAPDB is skipped once landed: applied twice it swaps the databases
		// back, while a flush is idempotent and replaying it resets the markers.
		if isSwap(flushed) && everySlotPassed(markers, flushed.offset) {
			continue
		}
		if err := a.flushTarget(ctx, flushed, markers, position); err != nil {
			return err
		}
	}
	return writeSegment()
}

func isSwap(f *flush) bool { return strings.EqualFold(f.name(), "swapdb") }

// everySlotPassed reports whether every slot has applied up to offset. Past a
// flush that holds only once the flush, and the position restored with it,
// have landed.
func everySlotPassed(markers []int64, offset int64) bool {
	for _, at := range markers {
		if at < offset {
			return false
		}
	}
	return true
}

// flushTarget empties the target the way the source was emptied, and puts back
// what this task keeps there.
//
// On one server that is a single transaction, so there is no moment where the
// position and the direction claim are missing. A cluster has no such
// transaction: the flush has to reach every master, and no MULTI spans them. So
// the order carries the safety instead -- the markers go first, because a crash
// between any two steps then costs a replay rather than a skip.
func (a *Applier) flushTarget(ctx context.Context, f *flush,
	markers []int64, position string) error {

	// Everything restored below names the flush, not the end of the batch it
	// arrived in: the rest of the batch has not been applied yet.
	here, err := positionAt(position, f.offset)
	if err != nil {
		return err
	}

	cluster, isCluster := a.Target.(*goredis.ClusterClient)
	if !isCluster {
		if err := a.flushOneServer(ctx, f, here); err != nil {
			return err
		}
		a.forgetMarkers(markers, f)
		return nil
	}

	// A flush reaches one node of a cluster and empties what that node holds,
	// which is its slots and no more. Sending it on to every master of the
	// target empties the whole key space instead -- the other shards' data
	// included, which the source still has. Only the keys this shard is
	// responsible for go.
	spans, ranged := slotRanges(a.Positions.Shard)
	if !ranged {
		return fmt.Errorf("shard %q does not name a slot range, so the reach of %s "+
			"on the target cannot be worked out", a.Positions.Shard, f.name())
	}
	for _, span := range spans {
		if err := a.dropMarkersIn(ctx, span.start, span.end); err != nil {
			return err
		}
	}
	if err := deleteSlotRanges(ctx, cluster, spans); err != nil {
		return fmt.Errorf("carry %s across slots %s: %w", f.name(), a.Positions.Shard, err)
	}
	if a.RestoreState != nil {
		pipe := a.Target.Pipeline()
		a.RestoreState(ctx, pipe, here)
		if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
			return fmt.Errorf("restore this task's state after %s: %w", f.name(), err)
		}
	}
	a.forgetMarkers(markers, f)
	return nil
}

// flushOneServer is the whole operation as one transaction, which is available
// on a single server and is what keeps the state from being missing for an
// instant.
func (a *Applier) flushOneServer(ctx context.Context, f *flush, position string) error {
	tx := a.Target.TxPipeline()

	if f.db != a.BookkeepingDB {
		tx.Do(ctx, "select", f.db)
	}
	tx.Do(ctx, f.arguments()...)
	if f.db != a.BookkeepingDB {
		tx.Do(ctx, "select", a.BookkeepingDB)
	}

	keys := make([]string, 0, SlotCount)
	for slot := 0; slot < SlotCount; slot++ {
		keys = append(keys, OffsetKey(slot, a.Positions.TaskID))
	}
	tx.Del(ctx, keys...)

	// The flush may have emptied the database these live in -- FLUSHALL always
	// does, and FLUSHDB does when the source flushes the one this task keeps its
	// state in. Writing them back inside the same transaction is what leaves no
	// moment where they are gone.
	if a.RestoreState != nil {
		a.RestoreState(ctx, tx, position)
	}

	results, err := tx.Exec(ctx)
	if err != nil && err != goredis.Nil {
		return fmt.Errorf("carry %s to the target: %w", f.name(), err)
	}
	for _, result := range results {
		if err := result.Err(); err != nil && err != goredis.Nil {
			return domain.Unrecoverable(
				"%v failed on the target: %v. The target's copy has diverged from the "+
					"source; a transaction that fails part way is not something a retry "+
					"can put right", result.Args(), err)
		}
	}
	return nil
}

// serverRefused reports whether the server executed a command and refused it,
// as opposed to the answer never arriving.
//
// Only the first has moved the position: Redis does not roll a transaction back
// when a command fails at run time, so the marker beside it applied. A context
// that was cancelled, or a connection that dropped, leaves the outcome unknown
// and the batch is replayed -- which is what a shutdown looks like, and
// treating that as divergence would block a task every time it stopped.
func serverRefused(err error) bool {
	if err == nil || err == goredis.Nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// go-redis gives a server's own error this type; everything raised on the
	// client side -- dial failures, timeouts, cancellation -- has another.
	var fromServer goredis.Error
	return errors.As(err, &fromServer)
}

// slotRange reads one slot range out of a shard's name. A single server is
// named "0" and owns no range, which is what tells the caller its flush covers
// the whole database rather than a slice of one.
func slotRange(shard string) (start, end int, ok bool) {
	first, last, found := strings.Cut(shard, "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.Atoi(first)
	if err != nil {
		return 0, 0, false
	}
	end, err = strconv.Atoi(last)
	if err != nil || start > end || start < 0 || end >= SlotCount {
		return 0, 0, false
	}
	return start, end, true
}

type slotSpan struct{ start, end int }

type slotSpans []slotSpan

func (spans slotSpans) has(slot int) bool {
	for _, span := range spans {
		if slot >= span.start && slot <= span.end {
			return true
		}
	}
	return false
}

// slotRanges reads every range a shard owns out of its name, which joins them
// with commas when its master owns more than one. Every one of them has to
// parse, or the shard's reach is unknown.
func slotRanges(shard string) (slotSpans, bool) {
	var spans slotSpans
	for _, part := range strings.Split(shard, ",") {
		start, end, ok := slotRange(part)
		if !ok {
			return nil, false
		}
		spans = append(spans, slotSpan{start, end})
	}
	return spans, true
}

// deleteSlotRanges empties the target of the keys one shard is responsible for.
//
// There is no command for "flush these slots", so the target is walked and the
// keys whose slot falls in the ranges are removed. A flush is rare enough to
// afford the walk, and the alternative -- sending the flush to every master --
// deletes the other shards' data, which the source still has.
func deleteSlotRanges(ctx context.Context, cluster *goredis.ClusterClient, spans slotSpans) error {
	return cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
		var cursor uint64
		for {
			keys, next, err := node.Scan(ctx, cursor, "*", 500).Result()
			if err != nil {
				return err
			}
			var doomed []string
			for _, key := range keys {
				if spans.has(SlotOf([]byte(key))) {
					doomed = append(doomed, key)
				}
			}
			if len(doomed) > 0 {
				// Through the cluster client: this node holds them now, but the
				// delete is addressed by key so a slot that has moved still lands.
				pipe := cluster.Pipeline()
				for _, key := range doomed {
					pipe.Del(ctx, key)
				}
				if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
					return err
				}
			}
			if next == 0 {
				return nil
			}
			cursor = next
		}
	})
}

// dropMarkersIn removes the markers of the slots a flush covers. The others
// belong to shards this flush says nothing about.
func (a *Applier) dropMarkersIn(ctx context.Context, start, end int) error {
	pipe := a.Target.Pipeline()
	for slot := start; slot <= end; slot++ {
		pipe.Del(ctx, OffsetKey(slot, a.Positions.TaskID))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return fmt.Errorf("drop the slot markers before a flush: %w", err)
	}
	return nil
}

// forgetMarkers matches in memory what the flush did on the target: the markers
// are gone, and every slot now stands where the flush did.
//
// It takes the flush rather than an offset on purpose. A marker is what makes a
// command skippable, so passing the end of the batch here skips everything the
// batch still has to apply after the flush -- a silent loss, and one that reads
// as correct until somebody traces a missing write. Taking the flush leaves no
// offset for a caller to choose.
func (a *Applier) forgetMarkers(markers []int64, f *flush) {
	for slot := range markers {
		markers[slot] = f.offset
	}
}

// positionAt rewrites a position so it names the flush rather than the end of
// the batch the flush arrived in.
//
// Restoring the batch's end would put the resume floor past writes that have
// not been applied: a slot with no marker is taken to have applied up to the
// floor, and dropMarkers has just removed every marker.
func positionAt(payload string, offset int64) (string, error) {
	position, err := decodePosition(payload)
	if err != nil {
		return "", err
	}
	position.Offset = offset
	return position.encode()
}
