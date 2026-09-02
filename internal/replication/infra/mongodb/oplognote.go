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
// oplog.
//
// It is the floor on replication latency for a cluster whose shards are not all
// busy, so it wants to be short; every tick costs one no-op oplog entry per
// shard, so it does not want to be shorter than the latency anyone will notice.
// One second measured 999 ms end to end against 9,618 ms with no nudging.
const nudgeInterval = time.Second

// nudger keeps a sharded source's shards from going quiet.
//
// # Why this exists
//
// On a sharded cluster mongos merges one change stream per shard and must
// return events in cluster-time order. It can only release an event stamped T
// once *every* shard has reported oplog progress at or past T — otherwise a
// later write on a quiet shard could turn out to belong before it. A shard with
// no writes advances only through the server's periodic no-op writer, which
// runs every periodicNoopIntervalSecs, 10 by default. So one quiet shard holds
// every other shard's events for up to ten seconds.
//
// This is not a throughput problem and no amount of tuning inside the syncer
// touches it: the events have not been handed out yet. Measured on a three-shard
// cluster with the collection otherwise idle, a single document took 7.0–9.7
// seconds to arrive; with the same cluster taking 200 writes/s spread over all
// three shards it took 1.6 seconds.
//
// MongoDB's own guidance for change streams names two remedies: lower
// periodicNoopIntervalSecs, or write no-op entries with appendOplogNote. The
// first can only be set at startup, so it is a decision made when a cluster is
// built and useless to a syncer pointed at one that already exists. The second
// is a command any client can send, and mongos fans it out to every shard —
// verified by watching all three shards' oplogs advance to the same timestamp
// from one call.
//
// # What it costs
//
// One no-op oplog entry per shard per tick. That is a write to the source, which
// a replication tool otherwise never makes, and it needs a role that allows
// appendOplogNote. Where the credentials do not allow it the nudger says so once
// and stops, leaving replication working at the latency the cluster gives it.
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

// nudge advances every shard's oplog once, and reports whether to keep going.
//
// A failure that is the credentials refusing the command will fail identically
// for ever, so it stops rather than logging the same line every second. Anything
// else — the cluster restarting, a network blip — is left to the next tick,
// because the nudger is an optimisation and must never be the reason
// replication stops.
func (n *nudger) nudge() bool {
	ctx, cancel := context.WithTimeout(context.Background(), n.interval)
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
