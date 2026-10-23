//go:build integration

package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/retail-ai-inc/sync/internal/platform/config"
	"github.com/retail-ai-inc/sync/internal/platform/dsn"
	"github.com/retail-ai-inc/sync/internal/replication/domain"
	"github.com/retail-ai-inc/sync/internal/replication/infra/directionlock"
	"github.com/retail-ai-inc/sync/test/harness"
)

// plantTargetClaim writes a claim on the task's target the way another process
// would, and removes it afterwards.
func plantTargetClaim(t *testing.T, cfg config.SyncConfig, target *sql.DB, claim directionlock.Claim) *directionlock.SQLStore {
	t.Helper()

	store := &directionlock.SQLStore{
		DB:      target,
		Schema:  targetDB,
		Address: dsn.Endpoint(cfg.Type, cfg.TargetConnection),
	}
	ctx := context.Background()
	if err := store.Put(ctx, claim); err != nil {
		t.Fatalf("plant the claim: %v", err)
	}
	t.Cleanup(func() { _ = store.Remove(context.Background(), claim.TaskID) })
	return store
}

// startRefused starts the task and returns what Start returned, failing if it
// ran until the deadline instead.
func startRefused(t *testing.T, cfg config.SyncConfig) error {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.ErrorLevel)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := NewSyncer(cfg, logger).Start(ctx)
	if ctx.Err() != nil {
		t.Fatalf("the task ran until it was cancelled (%v); it must refuse on its own", err)
	}
	if err == nil {
		t.Fatal("the task started; it must refuse")
	}
	return err
}

func assertTargetTableAbsent(t *testing.T, target *sql.DB, table string) {
	t.Helper()

	var n int
	if err := target.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?`, targetDB, table).Scan(&n); err != nil {
		t.Fatalf("look for the table on the target: %v", err)
	}
	if n != 0 {
		t.Errorf("the refused task created %s on the target", table)
	}
}

func claimsOf(t *testing.T, store *directionlock.SQLStore, taskID int) []directionlock.Claim {
	t.Helper()

	all, err := store.Claims(context.Background())
	if err != nil {
		t.Fatalf("read the claims: %v", err)
	}
	var mine []directionlock.Claim
	for _, c := range all {
		if c.TaskID == taskID {
			mine = append(mine, c)
		}
	}
	return mine
}

func TestAReversedPairStopsTheTaskBeforeItTouchesTheTarget(t *testing.T) {
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)
	table := harness.UniqueName("reversed")
	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'a')", table))

	cfg := syncTask(t, table)
	store := plantTargetClaim(t, cfg, tgt, directionlock.Claim{
		TaskID:    harness.UniqueTaskID(),
		Role:      directionlock.RoleSource,
		Peer:      dsn.Endpoint(cfg.Type, cfg.SourceConnection),
		Owner:     "osaka-after-failover",
		UpdatedAt: time.Now().UTC(),
	})

	err := startRefused(t, cfg)

	// A failure means the supervisor restarts the task forever instead of stopping it for the operator.
	if !domain.IsUnrecoverable(err) || !strings.Contains(err.Error(), "being replicated out of") {
		t.Fatalf("Start into a target that is now a source returned %v, want the unrecoverable direction refusal", err)
	}
	assertTargetTableAbsent(t, tgt, table)
	if mine := claimsOf(t, store, cfg.ID); len(mine) != 0 {
		t.Errorf("the refused task left its claim on the target: %+v", mine)
	}
}

func TestAnotherProcessRunningTheTaskIsRetriedRatherThanStopped(t *testing.T) {
	src, tgt := open(t, harness.MySQLSource, sourceDB), open(t, harness.MySQLTarget, targetDB)
	table := harness.UniqueName("concurrent")
	createSourceTable(t, src, tgt, table)
	mustExec(t, src, fmt.Sprintf("INSERT INTO %s (id, name) VALUES (1, 'a')", table))

	cfg := syncTask(t, table)
	store := plantTargetClaim(t, cfg, tgt, directionlock.Claim{
		TaskID:    cfg.ID,
		Role:      directionlock.RoleTarget,
		Peer:      dsn.Endpoint(cfg.Type, cfg.SourceConnection),
		Owner:     "the-pod-still-shutting-down",
		UpdatedAt: time.Now().UTC(),
	})

	err := startRefused(t, cfg)

	// A failure means a rolling restart stops the task for good instead of waiting for the old pod to exit.
	if domain.IsUnrecoverable(err) || !directionlock.IsConcurrent(err) {
		t.Fatalf("Start beside another live process of the task returned %v, want a retryable concurrent refusal", err)
	}
	assertTargetTableAbsent(t, tgt, table)
	mine := claimsOf(t, store, cfg.ID)
	if len(mine) != 1 || mine[0].Owner != "the-pod-still-shutting-down" {
		t.Errorf("the other process's claim was not left as it was: %+v", mine)
	}
}
