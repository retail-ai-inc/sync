package mongodb

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// nudgeInterval is how often an idle sharded source is asked to advance its
// oplog. It is the floor on replication latency for a cluster whose shards are
// not all busy, so it wants to be short; every tick costs one no-op oplog
// entry per shard, so it does not want to be shorter than the latency anyone
// will notice.
//
// A second was measured against a real sharded source and turned out to be the
// whole of the delay: events arrived between 0.6 and 2.2 seconds old, which is
// one nudge window plus the merge. A quarter of a second costs four no-op
// entries a second per shard instead of one -- each is around a hundred bytes,
// so about forty megabytes of oplog a day against a source whose window holds
// thirty-one hours of real traffic.
const nudgeInterval = 250 * time.Millisecond

// nudger keeps a sharded source's shards from going quiet.  # Why this exists
// On a sharded cluster mongos merges one change stream per shard and must
// return events in cluster-time order. It can only release an event stamped T
// once *every* shard has reported oplog progress at or past T — otherwise a
// later write on a quiet shard could turn out to belong before it.
type nudger struct {
	client   *mongo.Client
	logger   logrus.FieldLogger
	interval time.Duration

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// startNudging begins nudging when the source is a sharded cluster.
//
// It returns nil for a replica set: there is no merge to hold anything up there,
// so the no-op writes would buy nothing and still cost oplog.
func startNudging(ctx context.Context, client *mongo.Client, logger logrus.FieldLogger) *nudger {
	sharded, err := isMongos(ctx, client)
	if err != nil {
		logger.Warnf("[MongoDB] Could not tell whether the source is a sharded "+
			"cluster, so idle shards will not be nudged and replication latency may "+
			"be up to periodicNoopIntervalSecs on a quiet cluster: %v", err)
		return nil
	}
	if !sharded {
		return nil
	}

	n := &nudger{
		client:   client,
		logger:   logger,
		interval: nudgeInterval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	go n.run()
	return n
}

func (n *nudger) Stop() {
	if n == nil {
		return
	}
	n.stopOnce.Do(func() { close(n.stop) })
	<-n.done
}

func (n *nudger) run() {
	defer close(n.done)

	ticker := time.NewTicker(n.interval)
	defer ticker.Stop()

	for {
		select {
		case <-n.stop:
			return
		case <-ticker.C:
			if !n.nudge() {
				return
			}
		}
	}
}

// nudgeTimeout bounds one nudge. It is not the interval: the interval is short
// on purpose, and a timeout that short would abandon every nudge that took
// longer than one tick to reach every shard -- which is the case this exists
// for. A nudge that runs long delays the next tick instead, because the loop
// is sequential and a Go ticker drops what it cannot deliver.
const nudgeTimeout = 2 * time.Second

// nudge advances every shard's oplog once, and reports whether to keep going.
// A failure that is the credentials refusing the command will fail identically
// for ever, so it stops rather than logging the same line every second.
func (n *nudger) nudge() bool {
	ctx, cancel := context.WithTimeout(context.Background(), nudgeTimeout)
	defer cancel()

	err := n.client.Database("admin").RunCommand(ctx, bson.D{
		{Key: "appendOplogNote", Value: 1},
		{Key: "data", Value: bson.D{{Key: "sync", Value: "advance idle shards"}}},
	}).Err()
	if err == nil {
		return true
	}

	if unauthorized(err) {
		n.logger.Warnf("[MongoDB] This task's credentials may not run "+
			"appendOplogNote, so idle shards are left to the server's own no-op "+
			"writer. On a cluster where some shards are quiet that puts a floor "+
			"under replication latency of periodicNoopIntervalSecs, 10 seconds by "+
			"default. Grant the role that allows appendOplogNote, or set "+
			"periodicNoopIntervalSecs lower when the cluster is built: %v", err)
		return false
	}

	n.logger.Debugf("[MongoDB] Could not advance the idle shards this time: %v", err)
	return true
}

// unauthorized reports whether the server refused the command outright, as
// opposed to failing to answer it.
func unauthorized(err error) bool {
	var cmd mongo.CommandError
	if !errors.As(err, &cmd) {
		return false
	}
	switch cmd.Code {
	case 13, // Unauthorized
		18,  // AuthenticationFailed
		59,  // CommandNotFound, which an older server answers with
		303: // CommandNotSupported
		return true
	}
	return false
}
