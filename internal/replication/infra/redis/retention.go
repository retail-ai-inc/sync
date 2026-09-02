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

// How long this shard could stay stopped.
//
// The answer is the source's backlog divided by how fast it is being written:
// a stopped task is bridged on restart by the history the source still holds,
// and once that history has rolled past the recorded position the only way back
// is copying the shard again.
//
// Note which window this is. The relay's own disk buffer is much larger, and it
// is what covers a target that is briefly unavailable while the relay keeps
// running. It covers nothing at all while the relay is stopped, because then
// nothing is being written to it. Those are two different failures and the one
// this metric answers for is the second.

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

// rate is how many stream bytes a second the source is producing, measured from
// how far the relay has read between two calls.
//
// The first call cannot answer, and says so rather than guessing: a headroom
// figure is read when somebody is deciding whether there is still time to
// restart rather than re-copy, and a number invented from one sample is worse
// than no number.
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

func backlogBytes(ctx context.Context, node goredis.UniversalClient) (int64, error) {
	values, err := node.ConfigGet(ctx, "repl-backlog-size").Result()
	if err != nil {
		return 0, fmt.Errorf("read repl-backlog-size: %w", err)
	}
	raw, ok := values["repl-backlog-size"]
	if !ok {
		return 0, fmt.Errorf("the source did not report repl-backlog-size")
	}
	size, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("repl-backlog-size reads %q", raw)
	}
	if size <= 0 {
		return 0, fmt.Errorf("the source's repl-backlog-size is %d", size)
	}
	return size, nil
}
