package mysql

import (
	"context"
	"fmt"
	"time"
)

// Window is how far back the source's binlog reaches. It comes from
// binlog_expire_logs_seconds, which is the promise the server makes rather
// than a measurement of what is on disk: a file is only removed once it has
// been rotated and is older than the setting, so the real history is at least
// this long and usually a little longer.
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
