//go:build integration

package app

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/retail-ai-inc/sync/test/harness"
)

// The promotion marker has to survive on the databases themselves, because it
// is what outlives the processes: the pod is replaced daily, and the marker has
// to still be there when the task starts again.

func TestAPromotionIsWrittenToARealRedisTarget(t *testing.T) {
	db := useTempTaskDB(t)
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.RedisSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.RedisTarget)
	cfg := fmt.Sprintf(`{"type":"redis",
		"sourceConn":{"host":%q,"port":%q,"database":"0"},
		"targetConn":{"host":%q,"port":%q,"database":"0"}}`,
		sourceHost, sourcePort, targetHost, targetPort)
	id := itoa(insertTask(t, db, 1, cfg))
	ctx := context.Background()
	t.Cleanup(func() { _ = DemoteTarget(ctx, id) })

	if _, promoted, err := TargetPromotion(ctx, id); err != nil || promoted {
		t.Fatalf("a target reports promoted=%v before anything was written (%v)", promoted, err)
	}

	if _, err := PromoteTarget(ctx, id, "operator"); err != nil {
		t.Fatalf("PromoteTarget: %v", err)
	}
	claim, promoted, err := TargetPromotion(ctx, id)
	if err != nil || !promoted {
		t.Fatalf("TargetPromotion after promoting = %v (%v)", promoted, err)
	}
	if claim.Owner != "operator" {
		t.Errorf("owner = %q", claim.Owner)
	}

	if err := DemoteTarget(ctx, id); err != nil {
		t.Fatalf("DemoteTarget: %v", err)
	}
	if _, promoted, _ := TargetPromotion(ctx, id); promoted {
		t.Error("the promotion survived being cleared")
	}
}

func TestAPromotionIsWrittenToARealMySQLTarget(t *testing.T) {
	db := useTempTaskDB(t)
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.MySQLSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.MySQLTarget)
	cfg := fmt.Sprintf(`{"type":"mysql",
		"sourceConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"source_db"},
		"targetConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"target_db"}}`,
		sourceHost, sourcePort, targetHost, targetPort)
	id := itoa(insertTask(t, db, 1, cfg))
	// A second task into the same target, and one into a different database on
	// the same server, which must be left running.
	sibling := insertTask(t, db, 1, cfg)
	elsewhere := insertTask(t, db, 1, strings.Replace(cfg, `"database":"target_db"`, `"database":"other_db"`, 1))
	ctx := context.Background()
	t.Cleanup(func() { _ = DemoteTarget(ctx, id) })

	stopped, err := PromoteTarget(ctx, id, "operator")
	if err != nil {
		t.Fatalf("PromoteTarget: %v", err)
	}
	if want := []int{int(mustAtoi(t, id)), int(sibling)}; !reflect.DeepEqual(stopped, want) {
		t.Errorf("stopped = %v, want %v", stopped, want)
	}
	for _, task := range []struct {
		id   int64
		want int
	}{{mustAtoi(t, id), 0}, {sibling, 0}, {elsewhere, 1}} {
		var enable int
		if err := db.QueryRow(`SELECT enable FROM sync_tasks WHERE id=?`, task.id).Scan(&enable); err != nil {
			t.Fatalf("read enable: %v", err)
		}
		if enable != task.want {
			t.Errorf("task %d enable = %d after the promotion, want %d", task.id, enable, task.want)
		}
	}
	if _, promoted, err := TargetPromotion(ctx, id); err != nil || !promoted {
		t.Fatalf("TargetPromotion = %v (%v)", promoted, err)
	}
	if err := DemoteTarget(ctx, id); err != nil {
		t.Fatalf("DemoteTarget: %v", err)
	}
	if _, promoted, _ := TargetPromotion(ctx, id); promoted {
		t.Error("the promotion survived being cleared")
	}
}

func mustAtoi(t *testing.T, s string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAPromotionIsWrittenToARealMongoDBTarget(t *testing.T) {
	db := useTempTaskDB(t)
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.MongoSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.MongoTarget)
	// directConnection, because the fixture's replica set advertises an address
	// this process cannot reach.
	cfg := fmt.Sprintf(`{"type":"mongodb",
		"sourceConn":{"host":%q,"port":%q,"database":"source_db","directConnection":"true"},
		"targetConn":{"host":%q,"port":%q,"database":"target_db","directConnection":"true"}}`,
		sourceHost, sourcePort, targetHost, targetPort)
	id := itoa(insertTask(t, db, 1, cfg))
	ctx := context.Background()
	t.Cleanup(func() { _ = DemoteTarget(ctx, id) })

	if _, err := PromoteTarget(ctx, id, "operator"); err != nil {
		t.Fatalf("PromoteTarget: %v", err)
	}
	if _, promoted, err := TargetPromotion(ctx, id); err != nil || !promoted {
		t.Fatalf("TargetPromotion = %v (%v)", promoted, err)
	}
	if err := DemoteTarget(ctx, id); err != nil {
		t.Fatalf("DemoteTarget: %v", err)
	}
	if _, promoted, _ := TargetPromotion(ctx, id); promoted {
		t.Error("the promotion survived being cleared")
	}
}

func TestAPromotionIsWrittenToARealPostgresTarget(t *testing.T) {
	db := useTempTaskDB(t)
	sourceHost, sourcePort := harness.SplitHostPort(t, harness.PostgresSource)
	targetHost, targetPort := harness.SplitHostPort(t, harness.PostgresTarget)
	// sslmode is explicit: the default is "require" now, and the stack's
	// postgres speaks plaintext.
	cfg := fmt.Sprintf(`{"type":"postgresql",
		"sourceConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"source_db","sslmode":"disable"},
		"targetConn":{"host":%q,"port":%q,"user":"root","password":"root","database":"target_db","sslmode":"disable"}}`,
		sourceHost, sourcePort, targetHost, targetPort)
	id := itoa(insertTask(t, db, 1, cfg))
	ctx := context.Background()
	t.Cleanup(func() { _ = DemoteTarget(ctx, id) })

	if _, err := PromoteTarget(ctx, id, "operator"); err != nil {
		t.Fatalf("PromoteTarget: %v", err)
	}
	if _, promoted, err := TargetPromotion(ctx, id); err != nil || !promoted {
		t.Fatalf("TargetPromotion = %v (%v)", promoted, err)
	}
}
