// Package redis replicates one Redis deployment to another by reading the
// replication stream a replica would read.
//
// The shape follows infra/mysql and infra/mongodb — a Reader, an Applier and a
// Snapshotter driven by app/pipeline — with one structural difference. Those
// engines have a single log for the whole server, so one reader covers
// everything. A Redis cluster has a replication stream per shard and no way to
// join them, so a task runs a pipeline for each.
//
// The hard part is committing a position with the data. A cluster has no
// transaction spanning slots, so there is no single unit a batch can be
// committed in; the way out is per-slot transactions, each carrying its own
// marker. See applier.go.
package redis

import "github.com/sirupsen/logrus"

// orDefault falls back to the standard logger.
//
// Every component here takes a logger and every one of them may be built
// without it, mostly in tests. One place to say so beats five identical methods
// that could drift apart.
func orDefault(l logrus.FieldLogger) logrus.FieldLogger {
	if l != nil {
		return l
	}
	return logrus.StandardLogger()
}
