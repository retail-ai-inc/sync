package export

import (
	"bufio"
	"os"

	"github.com/sirupsen/logrus"
)

// reportIfEmpty says so when an export produced nothing.
//
// An empty export is not an error and never was: the incremental jobs select
// the rows created in one day, and a day with no rows is an ordinary outcome.
// The run is recorded as a success, the upload happens, and GCS ends up holding
// a 222-byte zip around an empty file.
//
// Which is indistinguishable, from the outside, from the query having gone
// wrong — a renamed timestamp column, a changed time zone, a mapping pointed at
// the wrong collection. Measured on staging: jobs 10 and 14 have been uploading
// empty files since 8-31 and every run reported success.
//
// So it is said once, loudly, naming the object. It stays a success because
// failing a legitimately empty day would be worse, but nobody has to infer it
// from a file size in a bucket.
func reportIfEmpty(engine, object string, records int64, query interface{}) {
	if records > 0 {
		return
	}
	logrus.Warnf("[BackupExecutor] ⚠️  %s export of %s produced 0 records, so the "+
		"backup for this run is empty. That is expected when nothing was written in "+
		"the window, and is also what a query against the wrong column or time zone "+
		"looks like. Query: %v", engine, object, query)
}

// countCSVDataRows counts the data rows of a CSV, which is its lines less the
// header. It exists so an empty MySQL export is as visible as an empty MongoDB
// one: the fix that taught only one engine something is the shape of half the
// defects this codebase has had.
func countCSVDataRows(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// A CSV cell can be long; the same allowance the JSONL counter makes.
	scanner.Buffer(make([]byte, 0, 1024*1024), 64*1024*1024)

	var lines int64
	for scanner.Scan() {
		if len(scanner.Bytes()) > 0 {
			lines++
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if lines == 0 {
		return 0, nil
	}
	return lines - 1, nil // the header is not a row
}
