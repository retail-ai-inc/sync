package redis

import (
	"context"
	"fmt"
	"strconv"
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
	jobs, err := a.plan(events, markers)
	if err != nil {
		return false, err
	}
	if containsFlush(events) {
		if err := a.applyInOrder(ctx, events, batchEnd, markers); err != nil {
			return false, err
		}
	} else if err := a.run(ctx, jobs, batchEnd, markers); err != nil {
		return false, err
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
			// Ordered separately; see applyInOrder.

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
	marker := tx.Set(ctx, OffsetKey(job.slot, a.Positions.TaskID),
		strconv.FormatInt(batchEnd, 10), 0)

	results, err := tx.Exec(ctx)
	if err != nil && err != goredis.Nil {
		return fmt.Errorf("write slot %d: %w", job.slot, err)
	}
	if err := marker.Err(); err != nil && err != goredis.Nil {
		return fmt.Errorf("record the marker for slot %d: %w", job.slot, err)
	}

	// Redis does not roll a transaction back when one of its commands fails at
	// run time — the rest still apply, and so does the marker. So a failure here
	// is not something a retry can fix: the position has moved past it. It means
	// the target's copy of that key has diverged from the source, and the honest
	// answer is to stop and say which key, rather than to carry on with a
	// difference nobody can see.
	for index, result := range results {
		if err := result.Err(); err != nil && err != goredis.Nil {
			return domain.Unrecoverable(
				"%v failed on the target in slot %d: %v. The target's copy of that "+
					"key has diverged from the source; a transaction that fails part way "+
					"is not rolled back, so the position has already moved past it. "+
					"Re-copy the key or the task", describeResult(results, index), job.slot, err)
		}
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

// applyInOrder writes a batch that contains a flush.
//
// One transaction, in stream order, because a flush empties whole databases and
// the ordinary path applies slots concurrently: a write that landed beside a
// flush rather than before or after it would be erased or spared by a race.
//
// The slot markers are dropped rather than advanced. A marker says a slot has
// been applied to some offset, and the flush has just destroyed what it was
// attesting to; a marker left standing above the resume floor would skip, on the
// next replay, exactly the writes that have to be made again.
func (a *Applier) applyInOrder(ctx context.Context, events []*domain.Event,
	batchEnd int64, markers []int64) error {

	// Repairs read from the source, which cannot be done inside the transaction.
	repairs := map[string]*repairedValue{}
	for _, event := range events {
		payload, ok := event.Payload.(*valueRepair)
		if !ok {
			continue
		}
		read, err := readValuesIn(ctx, a.Source, a.SourceHomeDB, payload.db, [][]byte{payload.key})
		if err != nil {
			return err
		}
		for _, value := range read {
			repairs[strconv.Itoa(value.db)+":"+string(value.key)] = value
		}
	}

	tx := a.Target.TxPipeline()
	at := a.BookkeepingDB
	selectDB := func(db int) {
		if db != at {
			tx.Do(ctx, "select", db)
			at = db
		}
	}

	for _, event := range events {
		switch payload := event.Payload.(type) {
		case *command:
			selectDB(payload.db)
			tx.Do(ctx, payload.arguments()...)
		case *valueRepair:
			value, ok := repairs[strconv.Itoa(payload.db)+":"+string(payload.key)]
			if !ok {
				continue
			}
			selectDB(payload.db)
			value.queue(ctx, tx)
		case *flush:
			selectDB(payload.db)
			tx.Do(ctx, payload.arguments()...)
		}
	}

	selectDB(a.BookkeepingDB)
	keys := make([]string, 0, SlotCount)
	for slot := 0; slot < SlotCount; slot++ {
		keys = append(keys, OffsetKey(slot, a.Positions.TaskID))
	}
	tx.Del(ctx, keys...)

	results, err := tx.Exec(ctx)
	if err != nil && err != goredis.Nil {
		return fmt.Errorf("write a batch containing a flush: %w", err)
	}
	for _, result := range results {
		if err := result.Err(); err != nil && err != goredis.Nil {
			return domain.Unrecoverable(
				"%v failed on the target: %v. The target's copy has diverged from the "+
					"source; a transaction that fails part way is not something a retry "+
					"can put right", result.Args(), err)
		}
	}

	// The markers are gone from the target, so nothing may be skipped against
	// them until they are written again.
	for slot := range markers {
		markers[slot] = batchEnd
	}
	return nil
}
