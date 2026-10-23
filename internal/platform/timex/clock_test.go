package timex

import (
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func TestGetCurrentTime(t *testing.T) {
	before := time.Now()
	got := GetCurrentTime()
	after := time.Now()

	if got.Before(before) || got.After(after) {
		t.Errorf("GetCurrentTime returned %v, outside [%v, %v]", got, before, after)
	}
}
