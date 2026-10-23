package export

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// The control database's own backup.
//
// Every task's configuration, credentials and schedule live in one SQLite file,
// and it had no backup path of its own: the exporters covered the databases
// being replicated and not the one that says what to replicate. Losing it costs
// a full re-copy of every link, because the stored positions go with it.
//
// It is not copied. A SQLite file that is being written to is not a database
// when read back byte for byte -- a copy can land mid-transaction, with a
// journal that describes a state the file no longer has. VACUUM INTO asks the
// engine for a consistent file instead, taking the same locks a reader takes,
// so a live database can be backed up without stopping it.

// executeSQLiteBackup writes a consistent copy of the control database and
// hands it to the same compression and upload path the other exporters use.
func (e *BackupExecutor) executeSQLiteBackup(
	ctx context.Context, path, tempDir string, config ExecutorBackupConfig,
) error {
	logrus.Infof("[BackupExecutor] Starting SQLite backup of %s", path)

	if path == "" {
		return fmt.Errorf("no database file to back up: set the source database to the " +
			"path of sync.db")
	}
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	name := strings.TrimSuffix(baseName(path), ".db")
	dateStr := time.Now().Format("2006-01-02")
	outputPath := fmt.Sprintf("%s/%s%s%s.db", tempDir, name, JSONFilenameSeparator, dateStr)
	zipPath := fmt.Sprintf("%s/%s%s%s.zip", tempDir, name, ZIPFilenameSeparator, dateStr)

	// Both are removed however this returns, so a destination that is
	// unreachable for a while does not fill the disk one snapshot at a time.
	defer func() {
		for _, leftover := range []string{outputPath, zipPath} {
			if err := os.Remove(leftover); err != nil && !os.IsNotExist(err) {
				logrus.Warnf("[BackupExecutor] Failed to remove %s: %v", leftover, err)
			}
		}
	}()

	if err := vacuumInto(ctx, path, outputPath); err != nil {
		return err
	}

	if _, err := e.compressAndUpload(ctx, tempDir, outputPath, zipPath,
		!isCompressionDisabled(config.CompressionType), config); err != nil {
		return err
	}

	logrus.Infof("[BackupExecutor] SQLite backup workflow completed for %s", path)
	return nil
}

// vacuumInto asks SQLite for a consistent copy at destination.
//
// The destination must not exist: VACUUM INTO refuses to overwrite, which is
// the behaviour to keep -- a half-written snapshot silently replaced by another
// is worse than a failed backup.
func vacuumInto(ctx context.Context, source, destination string) error {
	if err := os.Remove(destination); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("clear %s before the snapshot: %w", destination, err)
	}

	// Read-only: a backup must not be able to change what it is backing up, and
	// the mode is what stops a stray write from a future edit here.
	db, err := sql.Open("sqlite3", "file:"+source+"?mode=ro")
	if err != nil {
		return fmt.Errorf("open %s: %w", source, err)
	}
	defer db.Close()

	// The destination is a literal because SQLite takes no parameter there.
	// source and destination are operator-configured paths, not request input.
	if _, err := db.ExecContext(ctx,
		fmt.Sprintf("VACUUM INTO %s", quoteSQLiteString(destination))); err != nil {
		return fmt.Errorf("snapshot %s into %s: %w", source, destination, err)
	}

	info, err := os.Stat(destination)
	if err != nil {
		return fmt.Errorf("the snapshot of %s was not written: %w", source, err)
	}
	if info.Size() == 0 {
		return fmt.Errorf("the snapshot of %s is empty", source)
	}
	logrus.Infof("[BackupExecutor] Snapshot of %s is %d bytes", source, info.Size())
	return nil
}

// quoteSQLiteString renders a path as a SQLite string literal, doubling any
// quote it contains.
func quoteSQLiteString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func baseName(path string) string {
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		return path[i+1:]
	}
	return path
}
