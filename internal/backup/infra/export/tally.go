package export

import (
	"os"
	"sync"

	"github.com/sirupsen/logrus"
)

// What a run actually backed up.
//
// A job's outcome said "completed" and nothing else, so a backup of two
// hundred and ninety thousand records and a backup of nothing looked the same
// -- and one of the seven jobs in staging really was uploading an empty file
// every night. The counts are gathered as the export goes and read once it is
// over.

// Tally is what one run wrote out.
type Tally struct {
	Files   int
	Bytes   int64
	Records int64
}

// Empty reports a run that backed nothing up. Expected when nothing was
// written in the window, and also what a query against the wrong column looks
// like.
func (t Tally) Empty() bool { return t.Records == 0 && t.Bytes == 0 }

type tally struct {
	mu sync.Mutex
	Tally
}

// countRecords adds what one object's export held.
func (e *BackupExecutor) countRecords(records int64) {
	e.tally.mu.Lock()
	defer e.tally.mu.Unlock()
	e.tally.Records += records
}

// countUpload adds a file that reached the destination, by its size on disk.
// A file that cannot be measured is still a file: the count of them is what
// says a job produced nothing at all.
func (e *BackupExecutor) countUpload(path string) {
	e.tally.mu.Lock()
	defer e.tally.mu.Unlock()

	e.tally.Files++
	stat, err := os.Stat(path)
	if err != nil {
		logrus.Warnf("[BackupExecutor] Could not measure %s to report how much was "+
			"backed up: %v", path, err)
		return
	}
	e.tally.Bytes += stat.Size()
}

// Uploaded reports what the run wrote out, once it is over.
func (e *BackupExecutor) Uploaded() Tally {
	e.tally.mu.Lock()
	defer e.tally.mu.Unlock()
	return e.tally.Tally
}
