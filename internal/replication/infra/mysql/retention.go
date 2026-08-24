package mysql

import (
	"context"
	"fmt"
	"time"
)

// Window is how far back the source's binlog reaches.
//
// It comes from binlog_expire_logs_seconds, which is the promise the server
// makes rather than a measurement of what is on disk: a file is only removed
// once it has been rotated and is older than the setting, so the real history
// is at least this long and usually a little longer. Under-reporting is the
// right direction for a number somebody uses to decide whether there is still
// time to restart the task rather than copy the database again.
//
// A configured window overrides it, for a managed source whose real retention
// is governed by something the server does not know about — Cloud SQL keeps
// binlogs for its transaction-log retention period, which can be longer than
// the variable says.
func (r *Reader) Window(ctx context.Context) (time.Duration, error) {
	if r.Config.RetentionWindow > 0 {
		return r.Config.RetentionWindow, nil
	}
	if r.canal == nil {
		return 0, fmt.Errorf("the stream is not open, so the source cannot be asked")
	}

	result, err := r.canal.Execute(`SELECT @@GLOBAL.binlog_expire_logs_seconds`)
	if err != nil {
		return 0, fmt.Errorf("read binlog_expire_logs_seconds: %w", err)
	}
	if result.RowNumber() == 0 {
		return 0, fmt.Errorf("the source reported no binlog_expire_logs_seconds")
	}
	seconds, err := result.GetInt(0, 0)
	if err != nil {
		return 0, fmt.Errorf("read binlog_expire_logs_seconds: %w", err)
	}
	if seconds <= 0 {
		// Zero turns automatic purging off entirely. That is not an unlimited
		// window — an operator still runs PURGE BINARY LOGS, and the disk still
		// fills — so it is reported as unknown rather than as forever.
		return 0, fmt.Errorf("binlog_expire_logs_seconds is 0, so the server makes no " +
			"retention promise; set the task's retention window to say what the " +
			"real one is")
	}
	return time.Duration(seconds) * time.Second, nil
}
