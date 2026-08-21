//go:build racedemo

package domain

import (
	"sync"
	"testing"
)

// TestTheSessionIsUnsynchronised records the other half of T-070. The Session
// has no mutex, so concurrent handlers race on its two fields. This test is
// gated because it exists to be run under -race, where it reports the race that
// production has.
func TestTheSessionIsUnsynchronised(t *testing.T) {
	s := freshSession()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Authenticate("alice", "admin")
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.IsAdmin()
		}()
	}
	wg.Wait()
}
