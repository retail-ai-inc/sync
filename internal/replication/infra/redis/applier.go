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
// A Redis cluster has no atomic unit that spans slots: MULTI requires every key
// in it to hash to the same one. So the position cannot be committed alongside a
// whole batch the way it can in MySQL or MongoDB — and without that, a crash
// between writing the data and recording the position replays commands, which
// for INCR or LPUSH means a counter that is wrong or a queue entry handled
// twice. Silently, and for a payment queue, expensively.
//
// The way out is to stop looking for one transaction and use the ones that do
// exist. Every command in a cluster's replication stream touches a single slot,
// and a hash tag places a key in a chosen slot, so each slot's commands can be
// committed together with a marker saying how far that slot has been applied.
// The batch is then a set of per-slot transactions, each exactly once, and the
// replay problem is gone rather than mitigated.

// Applier writes batches of stream commands to the target cluster.
type Applier struct {
	Target goredis.UniversalClient
	// Source is used to re-read a key's value, for the phase where commands
	// cannot be replayed safely and for repairing divergence.
	Source goredis.UniversalClient

	Positions *Checkpoints
	Commands  *commandTable
	Logger    logrus.FieldLogger
	Labels    metrics.Labels

	// Concurrency bounds how many slot transactions are in flight at once. Zero
	// means the default.
	Concurrency int

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

// work is what one slot has to have done to it.
type work struct {
	slot int
	// commands are the stream commands for this slot, in the order they were
	// read.
	commands []*command
	// repairs are keys whose value is to be re-read from the source and written
	// whole, used where replaying a command would not be safe.
	//
	// A key appears once however many times the batch changed it: the repair
	// writes whatever the source holds now, so reading it twice in one batch
	// would cost a round trip to arrive at the same answer.
	repairs [][]byte
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

	jobs, err := a.plan(events, markers)
	if err != nil {
		return false, err
	}
	started := time.Now()
	if err := a.run(ctx, jobs, batchEnd, markers); err != nil {
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
	metrics.SetAppliedOffset(a.Labels, batchEnd)
	metrics.CountValueRepairs(a.Labels, repairsIn(jobs))
	return true, nil
}

// repairsIn counts the keys a batch copied whole rather than by replaying.
func repairsIn(jobs []work) int {
	total := 0
	for _, job := range jobs {
		total += len(job.repairs)
	}
	return total
}

func (a *Applier) logger() logrus.FieldLogger { return orDefault(a.Logger) }

// Skipped is how many commands were dropped as already applied.
func (a *Applier) Skipped() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.skipped
}

// plan groups the batch by slot, dropping what the target has already applied.
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
				job.repairs = append(job.repairs, payload.key)
			}

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

// run executes the slot transactions, several at a time.
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

// applySlot writes one slot's commands and its marker in a single transaction.
func (a *Applier) applySlot(ctx context.Context, job work, batchEnd int64) error {
	repairs, err := a.readRepairs(ctx, job)
	if err != nil {
		return err
	}

	tx := a.Target.TxPipeline()
	for _, cmd := range job.commands {
		tx.Do(ctx, cmd.arguments()...)
	}
	for _, repair := range repairs {
		repair.queue(ctx, tx)
	}
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

// readRepairs fetches the current value of every key this slot has to repair.
func (a *Applier) readRepairs(ctx context.Context, job work) ([]*repairedValue, error) {
	if len(job.repairs) == 0 {
		return nil, nil
	}
	if a.Source == nil {
		return nil, fmt.Errorf("a repair needs the source to read from")
	}
	return readValues(ctx, a.Source, job.repairs)
}

// ------------------------------------------------------------------- helpers

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

// endOf is the stream offset the batch reaches.
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
