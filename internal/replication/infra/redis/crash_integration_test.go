//go:build integration

package redis

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
	"github.com/retail-ai-inc/sync/internal/replication/app/pipeline"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// The test that decides whether the design works.
//
// Everything else in this package is in service of one claim: that a command
// stream can be replayed into a Redis cluster exactly once, without the atomic
// unit that MySQL and MongoDB have. If that claim is false the replay is silent
// and the damage is a counter that is quietly wrong or a payment queued twice, so
// it is not enough to test the happy path — the crash has to be provoked, over
// and over, while non-idempotent commands are in flight.

func targetAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("SYNC_REDIS_TARGET")
	if addr == "" {
		t.Skip("set SYNC_REDIS_TARGET to a Redis to replicate into")
	}
	return addr
}

// emptyBoth clears the two instances, which is why it insists on being told to.
func emptyBoth(t *testing.T, source, target *goredis.Client) {
	t.Helper()
	if os.Getenv("SYNC_REDIS_ALLOW_FLUSH") != "1" {
		t.Skip("set SYNC_REDIS_ALLOW_FLUSH=1 to let this test empty the two instances")
	}
	ctx := context.Background()
	for _, client := range []*goredis.Client{source, target} {
		if err := client.FlushAll(ctx).Err(); err != nil {
			t.Fatalf("empty %s: %v", client.Options().Addr, err)
		}
	}
}

// rig is one assembly of the pipeline, built and thrown away per crash.
type rig struct {
	runner  *pipeline.Runner
	link    *link
	applier *Applier
}

func buildRig(t *testing.T, source, target *goredis.Client, bufferDir string,
	commands *commandTable) *rig {
	t.Helper()

	buffer, err := OpenBuffer(BufferOptions{Dir: bufferDir, SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenBuffer: %v", err)
	}
	quiet := logrus.New()
	quiet.SetLevel(logrus.WarnLevel)

	labels := metrics.Labels{"task": "crash", "shard": "0"}
	connection := &link{
		opts:   StreamOptions{Addr: source.Options().Addr, IdleTimeout: 20 * time.Second},
		buffer: buffer,
		shard:  "0",
		logger: quiet,
		labels: labels,
	}
	positions := &Checkpoints{Target: target, TaskID: 1, Shard: "0"}

	applier := &Applier{
		Target:    target,
		Source:    source,
		Positions: positions,
		Commands:  commands,
		Logger:    quiet,
		Labels:    labels,
	}

	return &rig{
		link:    connection,
		applier: applier,
		runner: &pipeline.Runner{
			Reader: &Reader{
				Shard:    "0",
				Link:     connection,
				Target:   target,
				Commands: commands,
				Logger:   quiet,
				Labels:   labels,
			},
			Applier: applier,
			Snapshotter: &Snapshotter{
				Link:   connection,
				Source: source,
				Target: target,
				Logger: quiet,
				Labels: labels,
			},
			Checkpoints: positions,
			Opts: pipeline.Options{
				Limits:        pipeline.Limits{MaxEvents: 200},
				FlushInterval: 20 * time.Millisecond,
				StreamOrder:   true,
				Engine:        "Redis",
				Logger:        quiet,
				Labels:        labels,
			},
		},
	}
}

func (r *rig) stop() {
	r.link.close()
	_ = r.link.buffer.Close()
}

// workload writes commands that cannot be replayed safely.
//
// INCR, LPUSH, ZINCRBY and APPEND are the whole point: each one applied twice
// leaves a different result from each one applied once, and none of them says so.
// A test built on SET would pass whether the position logic worked or not.
func workload(ctx context.Context, t *testing.T, client *goredis.Client) (stop func() int) {
	var (
		wg      sync.WaitGroup
		written int
		mu      sync.Mutex
	)
	inner, cancel := context.WithCancel(ctx)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; inner.Err() == nil; i++ {
			pipe := client.Pipeline()
			// Spread across slots so more than one transaction is involved.
			for k := 0; k < 8; k++ {
				n := (i*8 + k) % 40
				pipe.Incr(inner, fmt.Sprintf("count:%d", n))
				pipe.RPush(inner, fmt.Sprintf("queue:%d", n%10), fmt.Sprintf("job-%d-%d", i, k))
				pipe.ZIncrBy(inner, fmt.Sprintf("score:%d", n%5), 1, fmt.Sprintf("m%d", n))
				pipe.Append(inner, fmt.Sprintf("log:%d", n%7), "x")
				pipe.Set(inner, fmt.Sprintf("plain:%d", n), i, 0)
			}
			if _, err := pipe.Exec(inner); err != nil && inner.Err() == nil {
				t.Logf("workload: %v", err)
			}
			mu.Lock()
			written += 40
			mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	return func() int {
		cancel()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		return written
	}
}

// TestCommandsAreAppliedExactlyOnceAcrossCrashes kills the pipeline repeatedly
// while non-idempotent commands are in flight, and insists the two sides end up
// identical.
//
// A crash here is the context being cancelled part way through applying a batch,
// followed by every piece of in-memory state being thrown away and rebuilt from
// what survived: the markers in the target and the segments on disk. That is what
// a killed process leaves behind.
func TestCommandsAreAppliedExactlyOnceAcrossCrashes(t *testing.T) {
	source := sourceClient(t, sourceAddr(t))
	target := sourceClient(t, targetAddr(t))
	emptyBoth(t, source, target)

	ctx := context.Background()
	if err := source.ConfigSet(ctx, "repl-backlog-size", "67108864").Err(); err != nil {
		t.Fatalf("widen the backlog: %v", err)
	}
	// Deliberately left at its default. Setting it to zero looks like an
	// optimisation — start the fork at once instead of waiting to batch replicas
	// — and on Redis 8.10.1 it makes the master accept the replica, report it
	// online, and then send nothing at all. See the warning in syncer.go.
	if err := source.ConfigSet(ctx, "repl-diskless-sync-delay", "5").Err(); err != nil {
		t.Fatalf("set the fork delay: %v", err)
	}
	commands, err := loadCommandTable(ctx, target)
	if err != nil {
		t.Fatalf("loadCommandTable: %v", err)
	}

	bufferDir := t.TempDir()

	// Seed a little data so the first copy has something to copy, then let the
	// pipeline reach the command phase before anything is disturbed.
	//
	// The crashes have to land in steady state to mean anything. While the stream
	// is still inside the window the first copy was taken over, changes are
	// applied by re-reading values — which is idempotent whatever the position
	// logic does, so a crash there proves nothing about it.
	for i := 0; i < 50; i++ {
		if err := source.Set(ctx, fmt.Sprintf("seed:%d", i), i, 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	reachCommandPhase(t, source, target, bufferDir, commands)

	stopWorkload := workload(ctx, t, source)

	const crashes = 20
	random := rand.New(rand.NewSource(7))
	skipped := 0
	for attempt := 0; attempt < crashes; attempt++ {
		runCtx, kill := context.WithCancel(ctx)
		current := buildRig(t, source, target, bufferDir, commands)

		done := make(chan error, 1)
		go func() { done <- current.runner.Run(runCtx) }()

		// Let it work for a while, then pull the plug mid-flight.
		time.Sleep(time.Duration(80+random.Intn(220)) * time.Millisecond)
		kill()

		select {
		case err := <-done:
			if domain.IsUnrecoverable(err) {
				t.Fatalf("attempt %d stopped for good: %v", attempt, err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("attempt %d did not stop when killed", attempt)
		}
		skipped += current.applier.Skipped()
		current.stop()
	}

	written := stopWorkload()
	t.Logf("%d commands written across %d crashes", written, crashes)

	// Now let it catch up undisturbed.
	runCtx, stopRun := context.WithCancel(ctx)
	final := buildRig(t, source, target, bufferDir, commands)
	done := make(chan error, 1)
	go func() { done <- final.runner.Run(runCtx) }()

	deadline := time.Now().Add(60 * time.Second)
	var same bool
	var difference string
	var stopped error
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		select {
		case stopped = <-done:
			// A run that has stopped will never catch up, so waiting out the
			// deadline would report a difference and hide the reason for it.
			deadline = time.Now()
		default:
		}
		same, difference = compare(t, source, target)
		if same {
			break
		}
	}
	stopRun()
	if stopped == nil {
		stopped = <-done
	}
	final.stop()
	if stopped != nil {
		t.Logf("the catch-up run returned: %v", stopped)
	}

	skipped += final.applier.Skipped()

	if !same {
		t.Fatalf("the two sides differ after %d crashes:\n%s", crashes, difference)
	}

	// Two checks that the test tested anything, both of which it silently failed
	// before they were here.
	//
	// The value phase applies changes by re-reading the key, which is idempotent
	// however often it happens — so a test that never leaves it would pass with
	// the skip removed entirely, and did. And a run that never replays an applied
	// command never exercises the skip at all.
	phase := recordedPhase(t, target)
	if phase != phaseCommand {
		t.Errorf("the position is still in the %q phase, so the run never replayed "+
			"a command: everything was applied by re-reading values, which is "+
			"idempotent whatever the position logic does", phase)
	}
	if skipped == 0 {
		t.Error("no command was ever skipped as already applied, so the crashes " +
			"never produced a replay and the exactly-once logic was not exercised")
	}
	t.Logf("phase %s, %d commands skipped as already applied", phase, skipped)
}

// reachCommandPhase runs the pipeline undisturbed until it has passed the end of
// the first copy.
func reachCommandPhase(t *testing.T, source, target *goredis.Client, bufferDir string,
	commands *commandTable) {
	t.Helper()

	ctx, stop := context.WithCancel(context.Background())
	warmup := buildRig(t, source, target, bufferDir, commands)
	done := make(chan error, 1)
	go func() { done <- warmup.runner.Run(ctx) }()

	select {
	case err := <-done:
		t.Fatalf("the warmup run stopped straight away: %v", err)
	case <-time.After(1500 * time.Millisecond):
	}

	deadline := time.Now().Add(30 * time.Second)
	for round := 0; time.Now().Before(deadline); round++ {
		// Something has to be written for the stream to carry anything at all: an
		// idle master does not advance its offset, so nothing would be applied and
		// the phase would never be recorded.
		if err := source.Set(context.Background(),
			fmt.Sprintf("warmup:%d", round), round, 0).Err(); err != nil {
			t.Fatalf("warmup write: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// A run that has stopped will never record a phase, so waiting for one
		// would just burn the timeout and report the wrong thing.
		select {
		case err := <-done:
			warmup.stop()
			t.Fatalf("the warmup run stopped after %d rounds: %v", round, err)
		default:
		}

		payload, err := target.Get(context.Background(), metaKey(1, "0")).Result()
		if err != nil {
			continue
		}
		position, err := decodePosition(payload)
		if err == nil && position.Phase == phaseCommand {
			stop()
			<-done
			warmup.stop()
			return
		}
	}
	stop()
	<-done
	warmup.stop()
	t.Fatal("the pipeline never reached the command phase, so the crash loop would " +
		"only exercise the idempotent path")
}

// recordedPhase reads which phase the stored position is in.
func recordedPhase(t *testing.T, target *goredis.Client) string {
	t.Helper()
	payload, err := target.Get(context.Background(), metaKey(1, "0")).Result()
	if err != nil {
		t.Fatalf("read the stored position: %v", err)
	}
	position, err := decodePosition(payload)
	if err != nil {
		t.Fatalf("decode the stored position: %v", err)
	}
	return position.Phase
}

// compare checks every key of the source against the target, by value rather
// than by serialisation: the two may legitimately encode the same list or hash
// differently.
func compare(t *testing.T, source, target *goredis.Client) (bool, string) {
	t.Helper()
	ctx := context.Background()

	keys, err := allKeys(ctx, source)
	if err != nil {
		return false, fmt.Sprintf("read the source's keys: %v", err)
	}
	targetKeys, err := allKeys(ctx, target)
	if err != nil {
		return false, fmt.Sprintf("read the target's keys: %v", err)
	}

	var problems []string
	for _, key := range keys {
		want, err := digest(ctx, source, key)
		if err != nil {
			return false, fmt.Sprintf("read %s from the source: %v", key, err)
		}
		got, err := digest(ctx, target, key)
		if err != nil {
			return false, fmt.Sprintf("read %s from the target: %v", key, err)
		}
		if want != got {
			problems = append(problems, fmt.Sprintf("  %s\n    source: %s\n    target: %s",
				key, truncate(want), truncate(got)))
		}
	}
	// Anything on the target the source does not have is a ghost, and a ghost
	// after a failover is a record nobody can explain.
	present := make(map[string]bool, len(keys))
	for _, key := range keys {
		present[key] = true
	}
	for _, key := range targetKeys {
		if IsOffsetKey(key) || isMetaKey(key) {
			continue
		}
		if !present[key] {
			problems = append(problems, "  "+key+" exists only on the target")
		}
	}

	if len(problems) == 0 {
		return true, ""
	}
	sort.Strings(problems)
	if len(problems) > 12 {
		problems = append(problems[:12], fmt.Sprintf("  ... and %d more", len(problems)-12))
	}
	return false, strings.Join(problems, "\n")
}

func allKeys(ctx context.Context, client *goredis.Client) ([]string, error) {
	var keys []string
	err := scanOne(ctx, client, 500, func(page []string) error {
		keys = append(keys, page...)
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

// digest renders a key's value in a form that can be compared across servers.
func digest(ctx context.Context, client *goredis.Client, key string) (string, error) {
	kind, err := client.Type(ctx, key).Result()
	if err == goredis.Nil {
		return "<missing>", nil
	}
	if err != nil {
		return "", err
	}

	switch kind {
	case "none":
		return "<missing>", nil
	case "string":
		value, err := client.Get(ctx, key).Result()
		if err == goredis.Nil {
			return "<missing>", nil
		}
		return "string:" + value, err
	case "list":
		items, err := client.LRange(ctx, key, 0, -1).Result()
		return "list:" + strings.Join(items, ","), err
	case "set":
		items, err := client.SMembers(ctx, key).Result()
		sort.Strings(items)
		return "set:" + strings.Join(items, ","), err
	case "zset":
		items, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return "", err
		}
		var parts []string
		for _, item := range items {
			parts = append(parts, fmt.Sprintf("%v=%g", item.Member, item.Score))
		}
		return "zset:" + strings.Join(parts, ","), nil
	case "hash":
		fields, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			return "", err
		}
		names := make([]string, 0, len(fields))
		for name := range fields {
			names = append(names, name)
		}
		sort.Strings(names)
		var parts []string
		for _, name := range names {
			parts = append(parts, name+"="+fields[name])
		}
		return "hash:" + strings.Join(parts, ","), nil
	}
	return kind + ":<not compared>", nil
}

func truncate(text string) string {
	if len(text) <= 120 {
		return text
	}
	return text[:120] + fmt.Sprintf("… (%d bytes)", len(text))
}
