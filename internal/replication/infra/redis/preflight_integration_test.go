//go:build integration

package redis

import (
	"context"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

func preflightSyncer(t *testing.T) *Syncer {
	t.Helper()
	quiet := logrus.New()
	quiet.SetLevel(logrus.ErrorLevel)
	return &Syncer{cfg: config.SyncConfig{ID: 77}, logger: quiet}
}

// gaugeFor reads one sample back, and reports whether it was set at all. A
// check that could not read the server sets nothing, which is a different
// outcome from setting zero.
func gaugeFor(t *testing.T, name, task string) (float64, bool) {
	t.Helper()
	for _, sample := range metrics.Default.Snapshot(name) {
		if sample.Labels["task"] == task {
			return sample.Value, true
		}
	}
	return 0, false
}

func TestThePreflightReadsARealTarget(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer source.Close()
	defer target.Close()

	labels := metrics.Labels{"task": "preflight-real"}
	preflightSyncer(t).targetPreflight(context.Background(), source, target, labels)

	// The eviction policy and the module list are answered by every server, so
	// both gauges have to come back. The size check is the one that legitimately
	// sets nothing: a target with no limit set is unlimited, not too small.
	if _, ok := gaugeFor(t, metrics.TargetEvictsKeys, "preflight-real"); !ok {
		t.Error("the eviction policy gauge was not set, so the check could not " +
			"read a server that does answer INFO memory")
	}
	missing, ok := gaugeFor(t, metrics.TargetMissingModules, "preflight-real")
	if !ok {
		t.Error("the module gauge was not set against a server that answers MODULE LIST")
	}
	if missing != 0 {
		t.Errorf("the two test servers are the same image, so the source should have "+
			"no module the target lacks; got %v", missing)
	}
}

// A server that cannot be reached has to leave the gauges alone rather than
// report a healthy target, because "nothing said otherwise" is what a green
// dashboard would be read as.
func TestThePreflightSaysNothingAboutAnUnreachableTarget(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	defer source.Close()
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()

	labels := metrics.Labels{"task": "preflight-dead"}
	preflightSyncer(t).targetPreflight(context.Background(), source, dead, labels)

	for _, name := range []string{
		metrics.TargetEvictsKeys, metrics.TargetTooSmall, metrics.TargetMissingModules,
	} {
		if value, ok := gaugeFor(t, name, "preflight-dead"); ok {
			t.Errorf("%s was set to %v for a target that never answered", name, value)
		}
	}
}

func TestTheSizeCheckReadsBothEnds(t *testing.T) {
	source := redisAt(t, addrsFrom(t, "SYNC_REDIS_SOURCE"))
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer source.Close()
	defer target.Close()
	ctx := context.Background()

	used, ok := memoryNumber(ctx, source, "used_memory")
	if !ok || used <= 0 {
		t.Fatalf("used_memory read back as %d (ok=%v), which no running server reports", used, ok)
	}
	if _, ok := memoryNumber(ctx, target, "maxmemory"); !ok {
		t.Error("maxmemory was not readable; an unset limit reports 0 rather than nothing")
	}
	if got := memoryField(ctx, target, "no_such_field"); got != "" {
		t.Errorf("a field INFO does not carry read back as %q", got)
	}
}

func TestModulesCannotBeComparedWhenOneEndWillNotSay(t *testing.T) {
	target := redisAt(t, addrsFrom(t, "SYNC_REDIS_TARGET"))
	defer target.Close()
	dead := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer dead.Close()
	ctx := context.Background()

	if _, ok := moduleNames(ctx, dead); ok {
		t.Error("a server that cannot be reached reported a module list")
	}

	labels := metrics.Labels{"task": "preflight-halfblind"}
	// The source answers and the target does not, which is the order the check
	// reads them in; neither way round may set the gauge.
	preflightSyncer(t).checkModules(ctx, target, dead, labels)
	if value, ok := gaugeFor(t, metrics.TargetMissingModules, "preflight-halfblind"); ok {
		t.Errorf("the module gauge was set to %v when the target would not say", value)
	}
}
