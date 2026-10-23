package redis

import (
	"context"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// What the target has to be set up to do. Nothing checked it: every one of
// these leaves the replica quietly incomplete while the task reports that it
// applied everything -- because it did, and the target threw part of it away
// afterwards.
//
// Each is reported as a gauge as well as a warning. A warning at start-up is
// read once, if at all, and "the DR copy has been silently dropping keys for a
// month" is not something to find out during a failover.

// targetPreflight checks what the target will do with what is sent to it.
//
// Nothing here refuses the task. Every one is a property of a target somebody
// else administers, and stopping replication over it leaves Osaka further
// behind than running with the gauge raised does. Deciding what to do about it
// is what the gauge is for.
func (s *Syncer) targetPreflight(ctx context.Context, source, target goredis.UniversalClient,
	labels metrics.Labels) {

	s.checkEvictionPolicy(ctx, target, labels)
	s.checkTargetSize(ctx, source, target, labels)
	s.checkModules(ctx, source, target, labels)
}

// checkEvictionPolicy is the one that matters most.
//
// Every policy but noeviction deletes keys to stay under the memory limit, and
// deletes them on the target only -- so the replica is missing keys the source
// still holds, no command said to remove them, and the reconciler is the only
// thing that would ever notice. Between 63 and 92 percent of the keys on this
// source carry a TTL, which is exactly the population volatile-lru reaches for
// first.
func (s *Syncer) checkEvictionPolicy(ctx context.Context, target goredis.UniversalClient,
	labels metrics.Labels) {

	policy := memoryField(ctx, target, "maxmemory_policy")
	if policy == "" {
		// Managed Redis refuses CONFIG GET, so INFO is the way in; a target
		// that answers neither still replicates.
		s.logger.Debug("[Redis] Could not read the target's eviction policy")
		return
	}

	evicts := !strings.EqualFold(policy, "noeviction")
	metrics.SetTargetEvictsKeys(labels, evicts)
	if evicts {
		s.logger.Warnf("[Redis] The target's maxmemory-policy is %s, so it deletes "+
			"keys of its own accord when it runs short of memory. Those keys are "+
			"gone from the replica while the source still has them, no command said "+
			"to remove them, and nothing but the next full comparison would find "+
			"out. Set it to noeviction on the target and give it at least as much "+
			"memory as the source.", policy)
	}
}

// checkTargetSize compares the target's limit against what the source holds.
//
// A target with no limit set answers 0, which is unlimited rather than empty.
func (s *Syncer) checkTargetSize(ctx context.Context, source, target goredis.UniversalClient,
	labels metrics.Labels) {

	limit, limitOK := memoryNumber(ctx, target, "maxmemory")
	used, usedOK := memoryNumber(ctx, source, "used_memory")
	if !limitOK || !usedOK || limit == 0 {
		return
	}

	tooSmall := limit < used
	metrics.SetTargetTooSmall(labels, tooSmall)
	if tooSmall {
		s.logger.Warnf("[Redis] The target's memory limit is %d bytes and the source "+
			"is holding %d. The copy does not fit, so the target either evicts or "+
			"starts refusing writes part way through it.", limit, used)
	}
}

// checkModules compares the two servers' modules.
//
// A value whose type comes from a module is serialised by DUMP with that type,
// and RESTORE on a server without the module refuses it. The keys are copied
// one batch at a time, so this surfaces as a failure part way through rather
// than at the start.
func (s *Syncer) checkModules(ctx context.Context, source, target goredis.UniversalClient,
	labels metrics.Labels) {

	sourceModules, ok := moduleNames(ctx, source)
	if !ok {
		return
	}
	targetModules, ok := moduleNames(ctx, target)
	if !ok {
		return
	}

	var missing []string
	for name := range sourceModules {
		if !targetModules[name] {
			missing = append(missing, name)
		}
	}

	metrics.SetTargetMissingModules(labels, len(missing))
	if len(missing) > 0 {
		s.logger.Warnf("[Redis] The source has %d module(s) the target does not "+
			"(%s). RESTORE refuses a value whose type comes from a module the "+
			"server has not loaded, so the copy fails on the first such key rather "+
			"than at the start. Load them on the target.",
			len(missing), strings.Join(missing, ", "))
	}
}

// moduleNames lists a server's loaded modules. The second result is false when
// the server will not say, which managed Redis often will not.
func moduleNames(ctx context.Context, client goredis.UniversalClient) (map[string]bool, bool) {
	raw, err := client.Do(ctx, "MODULE", "LIST").Result()
	if err != nil {
		return nil, false
	}
	entries, ok := raw.([]interface{})
	if !ok {
		return nil, false
	}

	names := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if name := moduleName(entry); name != "" {
			names[name] = true
		}
	}
	return names, true
}

// moduleName reads the name out of one MODULE LIST entry, which is a flat
// name/value array under RESP2 and a map under RESP3.
func moduleName(entry interface{}) string {
	switch shaped := entry.(type) {
	case map[interface{}]interface{}:
		if name, ok := shaped["name"].(string); ok {
			return name
		}
	case map[string]interface{}:
		if name, ok := shaped["name"].(string); ok {
			return name
		}
	case []interface{}:
		for i := 0; i+1 < len(shaped); i += 2 {
			if field, ok := shaped[i].(string); ok && field == "name" {
				if name, ok := shaped[i+1].(string); ok {
					return name
				}
			}
		}
	}
	return ""
}

// memoryField reads one field out of INFO memory.
//
// INFO rather than CONFIG GET because managed Redis refuses CONFIG: the
// retention measurement had to be written the same way.
func memoryField(ctx context.Context, client goredis.UniversalClient, field string) string {
	info, err := client.Info(ctx, "memory").Result()
	if err != nil {
		return ""
	}
	return infoField(info, field)
}

func memoryNumber(ctx context.Context, client goredis.UniversalClient, field string) (int64, bool) {
	raw := memoryField(ctx, client, field)
	if raw == "" {
		return 0, false
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return value, true
}

// infoField finds a field in an INFO reply, which is name:value a line at a
// time with comment lines starting '#'.
func infoField(info, field string) string {
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, value, found := strings.Cut(line, ":")
		if found && name == field {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// infoNumber reads a numeric field out of an INFO reply, zero when it is absent
// or not a number.
func infoNumber(info, field string) int64 {
	value, err := strconv.ParseInt(infoField(info, field), 10, 64)
	if err != nil {
		return 0
	}
	return value
}
