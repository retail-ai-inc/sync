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

	s.saveStreamPosition("1692600000000-0")

	if got := s.loadStreamPosition(); got != "1692600000000-0" {
		t.Errorf("loadStreamPosition = %q, want the saved id", got)
	}
}

func TestSaveStreamPositionCreatesDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a", "b", "redis.pos")
	s := newSyncer(t, path)

	s.saveStreamPosition("1-1")

	if _, err := os.Stat(path); err != nil {
		t.Errorf("the position file was not written: %v", err)
	}
}

func TestLoadStreamPositionTrimsWhitespace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "redis.pos")
	if err := os.WriteFile(path, []byte("  5-5\n"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	s := newSyncer(t, path)

	if got := s.loadStreamPosition(); got != "5-5" {
		t.Errorf("loadStreamPosition = %q, want %q", got, "5-5")
	}
}

func TestStreamPositionWithoutPath(t *testing.T) {
	s := newSyncer(t, "")

	// With no configured path the store is a no-op in both directions, so a
	// task that omits redis_position_path silently keeps no stream offset.
	s.saveStreamPosition("1-1")
	if got := s.loadStreamPosition(); got != "" {
		t.Errorf("loadStreamPosition = %q, want empty", got)
	}
}

func TestLoadStreamPositionMissingFile(t *testing.T) {
	s := newSyncer(t, filepath.Join(t.TempDir(), "absent.pos"))

	if got := s.loadStreamPosition(); got != "" {
		t.Errorf("loadStreamPosition = %q, want empty", got)
	}
}
