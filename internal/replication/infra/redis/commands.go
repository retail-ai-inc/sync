package redis

import (
	"context"
	"fmt"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/retail-ai-inc/sync/internal/replication/domain"
)

// What each command in the stream means, and which key it touches. The slot a
// command belongs to decides which transaction its position marker goes in, so
// getting the key wrong would put the marker in a different slot from the data
// and lose the atomicity the design rests on.
//
// Key positions come from COMMAND INFO rather than a table written down here: a
// table of command shapes drifts, and a stale entry misplaces a key silently.
// One key is enough — every command a cluster puts in its replication stream
// touches a single slot, so the first key names the slot for all of them.

type classification uint8

const (
	classWrite classification = iota + 1
	// classHeartbeat proves the link is alive and changes nothing.
	classHeartbeat
	// classTransaction opens or closes a transaction in the stream.
	classTransactionBegin
	classTransactionEnd
	// classIgnored is something with no effect on the target's data.
	classIgnored
	// classFlush empties or rearranges whole databases. It belongs to no key,
	// so it belongs to no slot, and it cannot be applied beside the batch's
	// other work.
	classFlush
	// classSelect changes which database the commands after it belong to.
	classSelect
	// classRefused is something that cannot be replicated safely.
	classRefused
)

type commandTable struct {
	// keyAt is the one-based position of the first key, per command name.
	keyAt map[string]int
	// movable names commands whose keys are not at a fixed position.
	movable map[string]bool
}

func loadCommandTable(ctx context.Context, client goredis.UniversalClient) (*commandTable, error) {
	infos, err := client.Command(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("read the server's command specifications: %w", err)
	}
	table := &commandTable{
		keyAt:   make(map[string]int, len(infos)),
		movable: make(map[string]bool),
	}
	for name, info := range infos {
		lower := strings.ToLower(name)
		table.keyAt[lower] = int(info.FirstKeyPos)
		// A command with no fixed first key either takes none or works them out
		// at runtime. The ones that matter here are told apart by asking the
		// server for the keys of the actual command, which classify does.
		if info.FirstKeyPos == 0 {
			table.movable[lower] = true
		}
	}
	if len(table.keyAt) == 0 {
		return nil, fmt.Errorf("the server listed no commands")
	}
	return table, nil
}

// keyless names the commands that legitimately carry no key, and what to do
// with each. Everything here is decided by what the command means for the
// target's data, which is not something COMMAND INFO can say.
var keyless = map[string]classification{
	"ping":     classHeartbeat,
	"multi":    classTransactionBegin,
	"exec":     classTransactionEnd,
	"replconf": classIgnored,
	// Pub/sub is not state. A subscriber on the target is not a copy of one on
	// the source, and forwarding the message would deliver it twice to anything
	// listening in both regions.
	"publish":  classIgnored,
	"spublish": classIgnored,
	// SELECT says which database the commands after it belong to. A cluster has
	// only database zero, so it is only ever "select 0" there; a standalone
	// server with several databases interleaves all of them into one stream, and
	// dropping this left every database's writes landing in whichever one the
	// target connection happened to be on.
	"select": classSelect,
	"ping\n": classHeartbeat,

	// These empty or rearrange whole databases. They were refused, on the
	// reasoning that emptying the target destroys the disaster recovery copy and
	// that an accident on the source must not take the copy with it. That holds
	// for a standby somebody fails over to; it does not hold for a copy that is
	// meant to be what the source is, where refusing them means the two diverge
	// the first time the source clears a cache -- and sources do that as a matter
	// of routine. They are carried, and the applier makes them a barrier.
	"flushall": classFlush,
	"flushdb":  classFlush,
	"swapdb":   classFlush,
}

func (t *commandTable) classify(ctx context.Context, client goredis.UniversalClient,
	command *Command) (classification, []byte, error) {

	name := strings.ToLower(command.Name())
	if name == "" {
		return classIgnored, nil, nil
	}
	if class, ok := keyless[name]; ok {
		return class, nil, nil
	}

	at, known := t.keyAt[name]
	if !known {
		// A command this target has never heard of cannot be applied to it, and
		// guessing which key it touches would put its marker in the wrong slot.
		return classRefused, nil, domain.Unrecoverable(
			"the source sent %q, which the target does not know. The two are "+
				"running different versions or different modules; replicating it "+
				"would either fail or land in the wrong slot", command.Name())
	}

	if at > 0 && at < len(command.Args) {
		return classWrite, command.Args[at], nil
	}

	if t.movable[name] {
		// Commands that work their keys out at runtime. Redis 7 and later
		// replicate the effects of a script rather than the script itself, so
		// these should not reach the stream at all; asking the server is both
		// correct and rare enough to afford.
		key, err := t.askForKey(ctx, client, command)
		if err != nil {
			return classRefused, nil, err
		}
		if key == nil {
			return classIgnored, nil, nil
		}
		return classWrite, key, nil
	}
	return classRefused, nil, domain.Unrecoverable(
		"the source sent %q with %d arguments, but its first key should be at "+
			"position %d", command.Name(), len(command.Args)-1, at)
}

func (t *commandTable) askForKey(ctx context.Context, client goredis.UniversalClient,
	command *Command) ([]byte, error) {

	args := make([]interface{}, 0, len(command.Args)+2)
	args = append(args, "command", "getkeys")
	for _, arg := range command.Args {
		args = append(args, arg)
	}
	keys, err := client.Do(ctx, args...).StringSlice()
	if err != nil {
		if strings.Contains(err.Error(), "no keys") ||
			strings.Contains(err.Error(), "The command has no key arguments") {
			return nil, nil
		}
		return nil, domain.Unrecoverable(
			"could not work out which key %q touches: %v. Applying it would risk "+
				"recording its position against the wrong slot", command.Name(), err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return []byte(keys[0]), nil
}
