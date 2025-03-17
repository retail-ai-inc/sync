package test

import (
	// "database/sql"
	// "os"
	"testing"
	// "github.com/retail-ai-inc/sync/pkg/config"
	"github.com/retail-ai-inc/sync/pkg/db"
	"time"
)

func testTC10LogHookWriting(t *testing.T) {
	db, err := db.OpenSQLiteDB()
	if err != nil {
		t.Fatalf("open sqlite fail: %v", err)
	}
	defer db.Close()

	t.Log("[TC10] Triggering some ops to produce logs...")
	time.Sleep(2 * time.Second)

	var c int
	err = db.QueryRow("SELECT COUNT(*) FROM sync_log").Scan(&c)
	if err != nil {
		t.Fatalf("[TC10] scan fail: %v", err)
	}
	t.Logf("[TC10] sync_log => %d logs", c)
	if c < 1 {
		t.Errorf("[TC10] expected logs in sync_log, found 0")
	}
}
