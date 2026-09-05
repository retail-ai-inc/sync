package dbinspect

import (
	"os"
	"testing"
	"time"
)

// TestMain shortens the network timeouts for the whole package.
//
// Every case that points at a host which is not there pays them in full, and
// there are several: at five and ten seconds they were about thirty-five of
// this package's thirty-six seconds. The servers these tests do reach are on
// this machine, so a shorter wait says the same thing sooner.
func TestMain(m *testing.M) {
	probeTimeout = 500 * time.Millisecond
	probeReadTimeout = 2 * time.Second
	schemaTimeout = 2 * time.Second
	schemaDialTimeout = 500 * time.Millisecond
	os.Exit(m.Run())
}
