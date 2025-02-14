package test

import (
	"github.com/retail-ai-inc/sync/pkg/state"
	"testing"
)

func testTC12StateStore_SaveLoad(t *testing.T) {
	// Initialize
	stateDir := t.TempDir()
	stateStore := state.NewFileStateStore(stateDir)
	testKey := "sync_state_test"
	testVal := []byte("hello_coverage")

	// Save state
	if err := stateStore.Save(testKey, testVal); err != nil {
		t.Fatalf("Failed to save state key=%s: %v", testKey, err)
	}

	// Load state
	loadedVal, err := stateStore.Load(testKey)
	if err != nil {
		t.Fatalf("Failed to load state key=%s: %v", testKey, err)
	}

	// Verify state
	if string(loadedVal) != string(testVal) {
		t.Fatalf("Unexpected state load => got=%s, want=%s", loadedVal, testVal)
	}
}
