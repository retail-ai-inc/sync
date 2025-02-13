package test

import (
	"sync"
	"testing"
	// "time"
	"strings"
	// "github.com/retail-ai-inc/sync/pkg/config"
)

func testTC11MultiTaskConcurrency(t *testing.T, tasks []SyncTaskRow) {
	t.Log("[TC11] concurrency test across tasks, we do parallel inserts example...")

	var wg sync.WaitGroup
	for _, tk := range tasks {
		if tk.Enable == 0 {
			continue
		}
		switch strings.ToLower(tk.Parsed.Type) {
		case "mysql":
			wg.Add(1)
			go func(task SyncTaskRow) {
				defer wg.Done()
				dsn := buildMySQLDSN(task.Parsed.SourceConn)
				doRandomMySQLInserts(t, dsn, task)
			}(tk)
		case "mongodb":
			wg.Add(1)
			go func(task SyncTaskRow) {
				defer wg.Done()
				dsn := buildMongoDSN(task.Parsed.SourceConn)
				doRandomMongoInserts(t, dsn, task)
			}(tk)
		default:
			t.Logf("[TC11] skip concurrency for type=%s", tk.Parsed.Type)
		}
	}
	wg.Wait()
	t.Log("[TC11] concurrency test done. Check final data in each target + logs.")
}
