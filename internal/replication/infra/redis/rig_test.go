//go:build integration

package redis

import (
	"context"
	"fmt"
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
)

// What the integration tests are built out of.

// ------------------------------------------------------------------ clients

// redisAt opens a client for one or more addresses.
func redisAt(t *testing.T, addrs []string) goredis.UniversalClient {
	t.Helper()
	var client goredis.UniversalClient
	if len(addrs) == 1 {
		client = goredis.NewClient(&goredis.Options{Addr: addrs[0]})
	} else {
		client = goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs})
	}
	t.Cleanup(func() { client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping %v: %v", addrs, err)
	}
	return client
}

func addrsFrom(t *testing.T, variable string) []string {
	t.Helper()
	value := os.Getenv(variable)
	if value == "" {
		t.Skipf("set %s to a Redis address, or several separated by commas", variable)
	}
	return strings.Split(value, ",")
}

func emptyBoth(t *testing.T, source, target goredis.UniversalClient) {
	t.Helper()
	if os.Getenv("SYNC_REDIS_ALLOW_FLUSH") != "1" {
		t.Skip("set SYNC_REDIS_ALLOW_FLUSH=1 to let this test empty the two ends")
	}
	ctx := context.Background()
	for _, client := range []goredis.UniversalClient{source, target} {
		if cluster, ok := client.(*goredis.ClusterClient); ok {
			err := cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
				return node.FlushAll(ctx).Err()
			})
			if err != nil {
				t.Fatalf("empty a cluster: %v", err)
			}
			continue
		}
		if err := client.FlushAll(ctx).Err(); err != nil {
			t.Fatalf("empty a server: %v", err)
		}
	}
}

// widenBacklogs gives the source room to hold history across a disconnect, and
// leaves the fork delay alone.
func widenBacklogs(t *testing.T, source goredis.UniversalClient) {
	t.Helper()
	ctx := context.Background()
	set := func(client goredis.UniversalClient) error {
		return client.ConfigSet(ctx, "repl-backlog-size", "67108864").Err()
	}
	var err error
	if cluster, ok := source.(*goredis.ClusterClient); ok {
		err = cluster.ForEachMaster(ctx, func(ctx context.Context, node *goredis.Client) error {
			return set(node)
		})
	} else {
		err = set(source)
	}
	if err != nil {
		t.Fatalf("widen the backlog: %v", err)
	}
}

// ------------------------------------------------------------------- the rig

// rig is one assembly of the pipeline — one runner per source shard — built and
// thrown away per crash.
type rig struct {
	runners  []*pipeline.Runner
	links    []*link
	appliers []*Applier
	nodes    []*goredis.Client
	shards   []string
}

func newRig(t *testing.T, source, target goredis.UniversalClient, root string,
	commands *commandTable, taskID int, seed string) *rig {
	t.Helper()

	shards, err := shardsOf(context.Background(), source, seed)
	if err != nil {
		t.Fatalf("find the source's shards: %v", err)
	}
	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)

	built := &rig{}
	for _, sh := range shards {
		buffer, err := OpenBuffer(BufferOptions{
			Dir:          fmt.Sprintf("%s/%s", root, sanitise(sh.id)),
			SegmentBytes: 1 << 20,
		})
		if err != nil {
			t.Fatalf("OpenBuffer: %v", err)
		}
		labels := metrics.Labels{"task": fmt.Sprint(taskID), "shard": sh.id}
		connection := &link{
			opts:   StreamOptions{Addr: sh.addr, IdleTimeout: 20 * time.Second},
			buffer: buffer,
			shard:  sh.id,
			logger: quiet,
			labels: labels,
		}
		node := goredis.NewClient(&goredis.Options{Addr: sh.addr})
		positions := &Checkpoints{Target: target, TaskID: taskID, Shard: sh.id}
		applier := &Applier{
			Target: target, Source: source, Link: connection, Positions: positions,
			Commands: commands, Logger: quiet, Labels: labels,
		}

		built.links = append(built.links, connection)
		built.appliers = append(built.appliers, applier)
		built.nodes = append(built.nodes, node)
		built.shards = append(built.shards, sh.id)
		built.runners = append(built.runners, &pipeline.Runner{
			Reader: &Reader{
				Shard: sh.id, Link: connection, Target: target,
				Commands: commands, Logger: quiet, Labels: labels,
			},
			Applier: applier,
			Snapshotter: &Snapshotter{
				Link: connection, Node: node, Source: source, Target: target,
				Logger: quiet, Labels: labels,
			},
			Checkpoints:   positions,
			CheckpointKey: sh.id,
			Opts: pipeline.Options{
				Limits:        pipeline.Limits{MaxEvents: 200},
				FlushInterval: 20 * time.Millisecond,
				StreamOrder:   true,
				Engine:        "Redis",
				Logger:        quiet,
				Labels:        labels,
			},
		})
	}
	return built
}

func (r *rig) run(ctx context.Context) chan error {
	done := make(chan error, len(r.runners))
	for _, runner := range r.runners {
		runner := runner
		go func() { done <- runner.Run(ctx) }()
	}
	return done
}

func (r *rig) wait(t *testing.T, done chan error, within time.Duration) []error {
	t.Helper()
	var errs []error
	for range r.runners {
		select {
		case err := <-done:
			errs = append(errs, err)
		case <-time.After(within):
			t.Fatal("a shard did not stop when it was told to")
		}
	}
	return errs
}

func (r *rig) stop() {
	for i, connection := range r.links {
		connection.close()
		_ = connection.buffer.Close()
		_ = r.nodes[i].Close()
	}
}

func (r *rig) skipped() int {
	total := 0
	for _, applier := range r.appliers {
		total += applier.Skipped()
	}
	return total
}

// inCommandPhase reports whether every shard has passed the end of its first
// copy, which is when replaying commands starts.
func (r *rig) inCommandPhase(target goredis.UniversalClient, taskID int) bool {
	ctx := context.Background()
	for _, shard := range r.shards {
		payload, err := target.Get(ctx, metaKey(taskID, shard)).Result()
		if err != nil {
			return false
		}
		position, err := decodePosition(payload)
		if err != nil || position.Phase != phaseCommand {
			return false
		}
	}
	return true
}

func (r *rig) reachCommandPhase(t *testing.T, source, target goredis.UniversalClient,
	taskID int, within time.Duration) {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(within)
	for round := 0; ; round++ {
		if time.Now().After(deadline) {
			t.Fatal("not every shard reached the command phase, so the crashes would " +
				"only exercise the idempotent path")
		}
		// Something has to be written for the stream to carry anything: an idle
		// master does not advance its offset, so nothing would be applied and the
		// phase would never be recorded.
		if err := source.Set(ctx, fmt.Sprintf("warm:%d", round), round, 0).Err(); err != nil {
			t.Fatalf("warm-up write: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		if r.inCommandPhase(target, taskID) {
			return
		}
	}
}

// ------------------------------------------------------------------ workload

// workload writes commands that cannot be replayed safely.
func workload(ctx context.Context, client goredis.UniversalClient, spread int) func() int {
	inner, cancel := context.WithCancel(ctx)
	written := make(chan int, 1)

	go func() {
		count := 0
		for i := 0; inner.Err() == nil; i++ {
			for k := 0; k < 6; k++ {
				n := (i*6 + k) % spread
				client.Incr(inner, fmt.Sprintf("count:%d", n))
				client.RPush(inner, fmt.Sprintf("queue:%d", n%12), fmt.Sprintf("job-%d-%d", i, k))
				client.ZIncrBy(inner, fmt.Sprintf("score:%d", n%8), 1, fmt.Sprintf("m%d", n))
				client.Append(inner, fmt.Sprintf("log:%d", n%9), "x")
				count += 4
			}
			time.Sleep(6 * time.Millisecond)
		}
		written <- count
	}()

	var once sync.Once
	return func() int {
		once.Do(cancel)
		return <-written
	}
}

// ---------------------------------------------------------------- comparison

// compare checks every key of the source against the target, by value rather
// than by serialisation: two servers may legitimately encode the same list or
// hash differently.
func compare(t *testing.T, source, target goredis.UniversalClient) (bool, string) {
	t.Helper()
	ctx := context.Background()

	keys, err := everyKey(ctx, source)
	if err != nil {
		return false, fmt.Sprintf("read the source's keys: %v", err)
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
	onTarget, err := everyKey(ctx, target)
	if err != nil {
		return false, fmt.Sprintf("read the target's keys: %v", err)
	}
	for _, key := range onTarget {
		if IsOffsetKey(key) || isMetaKey(key) || present[key] {
			continue
		}
		problems = append(problems, "  "+key+" exists only on the target")
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

func everyKey(ctx context.Context, client goredis.UniversalClient) ([]string, error) {
	var keys []string
	err := scanAll(ctx, client, 500, func(page []string) error {
		keys = append(keys, page...)
		return nil
	})
	sort.Strings(keys)
	return keys, err
}

func digest(ctx context.Context, client goredis.UniversalClient, key string) (string, error) {
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

// converge waits for the two sides to agree, and reports what still differs if
// they do not.
func converge(t *testing.T, source, target goredis.UniversalClient,
	within time.Duration, stopped func() bool) (bool, string) {
	t.Helper()

	deadline := time.Now().Add(within)
	var (
		same       bool
		difference string
	)
	for time.Now().Before(deadline) {
		time.Sleep(400 * time.Millisecond)
		same, difference = compare(t, source, target)
		if same {
			return true, ""
		}
		if stopped != nil && stopped() {
			// A run that has stopped will never catch up, so waiting out the
			// deadline would report a difference and hide the reason for it.
			break
		}
	}
	return same, difference
}

func truncate(text string) string {
	if len(text) <= 120 {
		return text
	}
	return text[:120] + fmt.Sprintf("… (%d bytes)", len(text))
}
