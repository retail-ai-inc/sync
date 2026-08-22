package redis

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
)

func newSyncer(t *testing.T, positionPath string) *RedisSyncer {
	t.Helper()

	logger := logrus.New()
	logger.SetLevel(logrus.PanicLevel)
	return &RedisSyncer{logger: logger, positionPath: positionPath}
}

func TestStreamPositionRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "redis.pos")
	s := newSyncer(t, path)

	s.saveStreamPosition("events", "1692600000000-0")

	if got := s.loadStreamPosition("events"); got != "1692600000000-0" {
		t.Errorf("loadStreamPosition = %q, want the saved id", got)
	}
}

func TestSaveStreamPositionCreatesDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "redis.pos")
	s := newSyncer(t, path)

	s.saveStreamPosition("events", "1-1")

	if _, err := os.Stat(path + ".events"); err != nil {
		t.Errorf("the position file was not written: %v", err)
	}
}

// TestEachStreamKeepsItsOwnPosition matters because a task may replicate more
// than one stream. One shared file meant the second stream overwrote the
// first's position and both resumed from the wrong place.
func TestEachStreamKeepsItsOwnPosition(t *testing.T) {
	s := newSyncer(t, filepath.Join(t.TempDir(), "redis.pos"))

	s.saveStreamPosition("events", "1-1")
	s.saveStreamPosition("audit", "2-2")

	if got := s.loadStreamPosition("events"); got != "1-1" {
		t.Errorf("events position = %q", got)
	}
	if got := s.loadStreamPosition("audit"); got != "2-2" {
		t.Errorf("audit position = %q", got)
	}
}

func TestLoadStreamPositionTrimsWhitespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "redis.pos")
	if err := os.WriteFile(path+".events", []byte("  5-5\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	s := newSyncer(t, path)

	if got := s.loadStreamPosition("events"); got != "5-5" {
		t.Errorf("loadStreamPosition = %q, want %q", got, "5-5")
	}
}

func TestStreamPositionWithoutPath(t *testing.T) {
	s := newSyncer(t, "")

	// With no configured path the store is a no-op in both directions, so a
	// task that omits redis_position_path silently keeps no stream offset.
	s.saveStreamPosition("events", "1-1")
	if got := s.loadStreamPosition("events"); got != "" {
		t.Errorf("loadStreamPosition = %q, want empty", got)
	}
}

func TestLoadStreamPositionMissingFile(t *testing.T) {
	s := newSyncer(t, filepath.Join(t.TempDir(), "absent.pos"))

	if got := s.loadStreamPosition("events"); got != "" {
		t.Errorf("loadStreamPosition = %q, want empty", got)
	}
}
