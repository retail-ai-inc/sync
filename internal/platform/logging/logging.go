package logging

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3" // for sqlite
	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"
)

var (
	Log      = logrus.New()
	logMutex sync.RWMutex
)

type CustomTextFormatter struct {
}

func (f *CustomTextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := entry.Time.Format("2006/01/02 15:04:05")
	level := strings.ToUpper(entry.Level.String())

	dataKeys := make([]string, 0, len(entry.Data))
	for k := range entry.Data {
		dataKeys = append(dataKeys, k)
	}
	sort.Strings(dataKeys)

	var dataStr string
	if len(dataKeys) > 0 {
		var parts []string
		for _, k := range dataKeys {
			v := entry.Data[k]
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		// Joined with a space: the separator used to be the empty string, so two
		// fields came out as "sync_task_id=7table=users", which neither reads
		// nor parses.
		dataStr = " " + strings.Join(parts, " ")
	}

	logLine := fmt.Sprintf("[%s] [%s] %s%s\n", timestamp, level, entry.Message, dataStr)
	return []byte(logLine), nil
}

func InitLogger(logLevel string) *logrus.Logger {
	logger := logrus.New()
	logger.Out = os.Stdout
	logger.SetLevel(getLogLevel(logLevel))
	logger.SetFormatter(&CustomTextFormatter{})

	// Check if SQLite logging is enabled via environment variable
	// Default behavior: SQLite logging is DISABLED (not writing to SQLite)
	enableSQLiteLogging := os.Getenv("ENABLE_SQLITE_LOGGING") == "true"

	if enableSQLiteLogging {
		logger.AddHook(NewSQLiteHook())
		logger.Info("SQLite logging is enabled via ENABLE_SQLITE_LOGGING environment variable")
	} else {
		logger.Debug("SQLite logging is disabled (default behavior)")
	}

	// Thread-safe update of global logger
	logMutex.Lock()
	Log = logger
	logMutex.Unlock()

	return logger
}

func getLogLevel(level string) logrus.Level {
	switch strings.ToLower(level) {
	case "debug":
		return logrus.DebugLevel
	case "info":
		return logrus.InfoLevel
	case "warn", "warning":
		return logrus.WarnLevel
	case "error":
		return logrus.ErrorLevel
	case "fatal":
		return logrus.FatalLevel
	case "panic":
		return logrus.PanicLevel
	default:
		return logrus.InfoLevel
	}
}

// SQLiteHook writes log lines into the control database.
//
// It used to open and close a database connection for every line, which at the
// info level is one SQLite open per log entry, and it answered every failure
// with nil — so the rows could stop being written for good with nothing to show
// it. The connection is now opened once and kept, and a failure is reported to
// logrus once per outage rather than never or once per line.
type SQLiteHook struct {
	formatter *CustomTextFormatter

	mu sync.Mutex
	db *sql.DB
	// reported says the current failure has already been passed to logrus, so a
	// database that stays unreachable does not produce one complaint per line.
	reported bool
}

func NewSQLiteHook() *SQLiteHook {
	return &SQLiteHook{
		formatter: &CustomTextFormatter{},
	}
}

// Close releases the connection the hook holds.
func (h *SQLiteHook) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closeLocked()
}

func (h *SQLiteHook) closeLocked() error {
	if h.db == nil {
		return nil
	}
	db := h.db
	h.db = nil
	return db.Close()
}

// fail drops the connection so the next line reconnects, and reports the
// failure the first time it happens.
func (h *SQLiteHook) fail(err error) error {
	_ = h.closeLocked()
	if h.reported {
		return nil
	}
	h.reported = true
	return err
}

func (h *SQLiteHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.db == nil {
		db, err := sqlite.OpenSQLiteDB()
		if err != nil {
			return h.fail(fmt.Errorf("open the log database: %w", err))
		}
		h.db = db
	}

	// Use the hook's own formatter instead of accessing global Log.Formatter
	formatted, _ := h.formatter.Format(entry)
	level := entry.Level.String()
	message := strings.TrimSuffix(string(formatted), "\n")

	syncTaskID := 0
	if val, ok := entry.Data["sync_task_id"]; ok {
		switch v := val.(type) {
		case int:
			syncTaskID = v
		case int64:
			syncTaskID = int(v)
		case string:
			parsed, e := strconv.Atoi(v)
			if e == nil {
				syncTaskID = parsed
			}
		}
	}

	const insSQL = `
INSERT INTO sync_log(level, message, sync_task_id)
VALUES(?, ?, ?);
`
	if _, err := h.db.Exec(insSQL, level, message, syncTaskID); err != nil {
		return h.fail(fmt.Errorf("write a log row: %w", err))
	}
	h.reported = false
	return nil
}

func (h *SQLiteHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
