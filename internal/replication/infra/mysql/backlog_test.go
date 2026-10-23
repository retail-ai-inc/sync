package mysql

import (
	"context"
	"testing"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/retail-ai-inc/sync/internal/platform/metrics"
)

// The count the source cannot be asked for: how many transactions a GTID set
// holds, which the performance tests read as "how far behind".

func TestCountingTheTransactionsInAGTIDSet(t *testing.T) {
	cases := []struct {
		set  string
		want int64
	}{
		{"", 0},
		{"4a4eec89-4e4a-11ec-bafd-42010a76c007:7", 1},
		{"4a4eec89-4e4a-11ec-bafd-42010a76c007:1-10", 10},
		{"4a4eec89-4e4a-11ec-bafd-42010a76c007:1-10:21-25", 15},
		// Two servers in one set, as after a failover.
		{"4a4eec89-4e4a-11ec-bafd-42010a76c007:1-10,8f0c0f6a-1d9e-11ee-9d3e-42010a76c008:1-3", 13},
	}
	for _, c := range cases {
		got, err := countGTIDs(c.set)
		if err != nil {
			t.Fatalf("%q: %v", c.set, err)
		}
		if got != c.want {
			t.Errorf("%q: counted %d, want %d", c.set, got, c.want)
		}
	}
}

func TestAGTIDSetThatWillNotParseIsReported(t *testing.T) {
	if _, err := countGTIDs("not a gtid set"); err == nil {
		t.Fatal("a set that cannot be parsed was counted as something")
	}
}

// Without GTIDs on both sides, only a byte offset remains, and an offset is a
// distance only within one binlog file.

func TestTheByteDistanceIsOnlyMeasuredWithinOneFile(t *testing.T) {
	ctx := context.Background()

	same, err := distance(ctx, nil,
		binlogHead{File: "mysql-bin.001110", Position: 5000},
		&binlogCheckpoint{Name: "mysql-bin.001110", Pos: 1200})
	if err != nil {
		t.Fatalf("distance: %v", err)
	}
	if same.bytes != 3800 || same.appliedOffset != 1200 {
		t.Errorf("same file: bytes=%d applied=%d, want 3800 and 1200", same.bytes, same.appliedOffset)
	}
	if same.transactions != -1 {
		t.Errorf("transactions=%d without any GTID, want -1 (not measured)", same.transactions)
	}

	// The source has rotated: the head's offset is small, the applied offset
	// large, and subtracting them would report the target AHEAD of the source.
	rotated, err := distance(ctx, nil,
		binlogHead{File: "mysql-bin.001111", Position: 400},
		&binlogCheckpoint{Name: "mysql-bin.001110", Pos: 100_699_313})
	if err != nil {
		t.Fatalf("distance: %v", err)
	}
	if rotated.bytes != -1 {
		t.Errorf("across a rotation bytes=%d, want -1 (not a distance)", rotated.bytes)
	}
}

// MariaDB's GTIDs are domain-server-sequence, and its server has no
// GTID_SUBTRACT; asking would be an error on every poll.
func TestMariaDBGTIDsAreNotSubtracted(t *testing.T) {
	got, err := distance(context.Background(), nil,
		binlogHead{File: "b.000002", Position: 900, GTIDSet: "0-1-500"},
		&binlogCheckpoint{Name: "b.000002", Pos: 300, GTID: "0-1-480", Flavor: gomysql.MariaDBFlavor})
	if err != nil {
		t.Fatalf("distance: %v", err)
	}
	if got.transactions != -1 {
		t.Errorf("transactions=%d, want the GTID comparison skipped for MariaDB", got.transactions)
	}
	if got.bytes != 600 {
		t.Errorf("bytes=%d, want the byte distance still measured", got.bytes)
	}
}

// Nothing measured means nothing published: a -1 that reached Prometheus
// would read as "ahead by one".
func TestAnUnmeasuredTransactionCountIsNotPublished(t *testing.T) {
	labels := testLabels()
	forget(labels)

	backlog{transactions: -1, appliedOffset: 42, bytes: -1}.publish(labels)

	if _, ok := gaugeValue("sync_source_transactions_behind", labels); ok {
		t.Error("an unmeasured transaction count was published")
	}
	if v, ok := gaugeValue("sync_applied_position_bytes", labels); !ok || v != 42 {
		t.Errorf("applied position = %v (published=%v), want 42", v, ok)
	}

	backlog{transactions: 7, appliedOffset: 43, bytes: 10}.publish(labels)
	if v, ok := gaugeValue("sync_source_transactions_behind", labels); !ok || v != 7 {
		t.Errorf("transactions behind = %v (published=%v), want 7", v, ok)
	}
}

func testLabels() metrics.Labels {
	return metrics.Labels{"task": "backlog-test", "engine": "mysql"}
}

func forget(labels metrics.Labels) { metrics.Default.Forget(labels) }

func gaugeValue(name string, labels metrics.Labels) (float64, bool) {
	for _, s := range metrics.Default.Snapshot(name) {
		if s.Labels.Key() == labels.Key() {
			return s.Value, true
		}
	}
	return 0, false
}
