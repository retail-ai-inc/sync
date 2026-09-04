package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// How long this shard could stay stopped. The answer is the source's backlog
// divided by how fast it is being written: a stopped task is bridged on
// restart by the history the source still holds, and once that history has
// rolled past the recorded position the only way back is copying the shard
// again.

func (r *Reader) Window(ctx context.Context) (time.Duration, error) {
	if r.Configured > 0 {
		return r.Configured, nil
	}
	if r.Node == nil {
		return 0, fmt.Errorf("no connection to shard %s to ask how much history it "+
			"keeps; set the task's retention window instead", r.Shard)
	}

	backlog, err := r.backlog(ctx)
	if err != nil {
		return 0, err
	}
	rate, err := r.rate()
	if err != nil {
		return 0, err
	}
	return time.Duration(float64(backlog) / rate * float64(time.Second)), nil
}

// backlog is how much history the source keeps, asked for occasionally.
//
// While the rate is still unknown this is called once a second, and the size of
// a backlog is a setting rather than a measurement — so the answer is kept for a
// while rather than fetched on every tick.
func (r *Reader) backlog(ctx context.Context) (int64, error) {
	if r.backlogBytes > 0 && time.Since(r.backlogAt) < backlogFreshFor {
		return r.backlogBytes, nil
	}
	size, err := backlogBytes(ctx, r.Node)
	if err != nil {
		return 0, err
	}
	r.backlogBytes, r.backlogAt = size, time.Now()
	return size, nil
}

const backlogFreshFor = 5 * time.Minute

// rate is how many stream bytes a second the source is producing, measured
// from how far the relay has read between two calls. The first call cannot
// answer, and says so rather than guessing: a headroom figure is read when
// somebody is deciding whether there is still time to restart rather than re-
// copy, and a number invented from one sample is worse than no number.
func (r *Reader) rate() (float64, error) {
	now := time.Now()
	offset := r.Link.head()

	previousAt, previousOffset := r.sampledAt, r.sampledOffset
	r.sampledAt, r.sampledOffset = now, offset

	if previousAt.IsZero() {
		return 0, fmt.Errorf("shard %s has only been measured once, so how fast its "+
			"stream is written is not known yet: %w", r.Shard, domain.ErrWindowNotYet)
	}
	elapsed := now.Sub(previousAt).Seconds()
	if elapsed <= 0 {
		return 0, fmt.Errorf("no time has passed since the last measurement: %w",
			domain.ErrWindowNotYet)
	}
	bytes := float64(offset - previousOffset)
	if bytes <= 0 {
		// A source nobody is writing to has no rate, and dividing by it would
		// report an unbounded window. Reporting nothing is the honest answer —
		// and it is worth asking again, because the source may simply be quiet
		// for a while rather than for ever.
		return 0, fmt.Errorf("shard %s has had nothing written to it since the last "+
			"measurement, so its backlog covers an unknown length of time: %w",
			r.Shard, domain.ErrWindowNotYet)
	}
	return bytes / elapsed, nil
}

// backlogBytes reads how much history the source keeps.
//
// INFO first, CONFIG second. They report the same number, but a managed Redis
// commonly refuses CONFIG altogether -- Memorystore answers "unknown command"
// -- while INFO is left alone. Asking CONFIG first meant no window was ever
// published for a managed source, which is the kind of source whose backlog
// cannot be enlarged and therefore the kind whose window most needs watching.
func backlogBytes(ctx context.Context, node goredis.UniversalClient) (int64, error) {
	size, infoErr := backlogFromInfo(ctx, node)
	if infoErr == nil {
		return size, nil
	}

	values, err := node.ConfigGet(ctx, "repl-backlog-size").Result()
	if err != nil {
		return 0, fmt.Errorf("read the backlog size: %w; and CONFIG: %w", infoErr, err)
	}
	raw, ok := values["repl-backlog-size"]
	if !ok {
		return 0, fmt.Errorf("the source did not report repl-backlog-size")
	}
	size, err = strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("repl-backlog-size reads %q", raw)
	}
	if size <= 0 {
		return 0, fmt.Errorf("the source's repl-backlog-size is %d", size)
	}
	return size, nil
}

// backlogFromInfo reads the backlog out of INFO replication.
//
// repl_backlog_histlen is what the backlog actually holds, which is what a
// resume has to land inside; repl_backlog_size is what it may grow to. The
// smaller of the two is the honest answer while the backlog is still filling.
func backlogFromInfo(ctx context.Context, node goredis.UniversalClient) (int64, error) {
	info, err := node.Info(ctx, "replication").Result()
	if err != nil {
		return 0, fmt.Errorf("read INFO replication: %w", err)
	}
	return backlogFromInfoText(info)
}

// backlogFromInfoText is the parsing, apart from the round trip, so the shapes
// a real server answers with can be pinned without one.
func backlogFromInfoText(info string) (int64, error) {
	fields := map[string]int64{}
	for _, line := range strings.Split(info, "\n") {
		name, value, found := strings.Cut(strings.TrimSpace(line), ":")
		if !found {
			continue
		}
		if name != "repl_backlog_size" && name != "repl_backlog_histlen" {
			continue
		}
		n, convErr := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
		if convErr == nil {
			fields[name] = n
		}
	}

	size, held := fields["repl_backlog_size"], fields["repl_backlog_histlen"]
	switch {
	case size <= 0 && held <= 0:
		return 0, fmt.Errorf("INFO replication reported no backlog, so the source " +
			"is keeping no history a resume could land in")
	case held <= 0:
		// Allocated but not yet filled: it will hold this much once it has run
		// for long enough.
		return size, nil
	case size <= 0 || held < size:
		return held, nil
	default:
		return size, nil
	}
}
