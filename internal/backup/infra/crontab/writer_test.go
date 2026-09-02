package crontab

import (
	"testing"
)

func TestNewCronManagerKeepsItsArguments(t *testing.T) {
	cm := NewCronManager(nil, "http://127.0.0.1:8080/api")

	if cm == nil {
		t.Fatal("NewCronManager returned nil")
	}
	if cm.apiServer != "http://127.0.0.1:8080/api" {
		t.Errorf("apiServer = %q", cm.apiServer)
	}
}

// executeCommand and executeCommandStreaming are deliberately untested: they
// have no callers anywhere in the repository (executeCommand only calls
// executeCommandStreaming, and nothing calls executeCommand), and the comment
// on executeCommand marks it deprecated.
