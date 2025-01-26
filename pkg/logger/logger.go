package logger

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	_ "github.com/mattn/go-sqlite3" // for sqlite
)

var Log = logrus.New()

type CustomTextFormatter struct {
}

func (f *CustomTextFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	timestamp := entry.Time.Format("2006/01/02 15:04:05")
	level := strings.ToUpper(entry.Level.String())

	// First, concatenate all fields data (sorted by key for readability)
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
		dataStr = " " + strings.Join(parts, " ")
	}

	// [2025/01/26 10:16:52] [INFO] row_count_minutely db_type=MONGODB src_db=test ...
	logLine := fmt.Sprintf("[%s] [%s] %s%s\n", timestamp, level, entry.Message, dataStr)
	return []byte(logLine), nil
}

func InitLogger(logLevel string) *logrus.Logger {
	logger := logrus.New()
	logger.Out = os.Stdout
	logger.SetLevel(getLogLevel(logLevel))

	logger.SetFormatter(&CustomTextFormatter{})

	// Add Hook to write to SQLite
	logger.AddHook(NewSQLiteHook())

	Log = logger
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


type SQLiteHook struct {
	// TODO: Add any configuration options here
	// Simplified handling, open the DB each time a log is written
}

func NewSQLiteHook() *SQLiteHook {
	return &SQLiteHook{}
}

func (h *SQLiteHook) Fire(entry *logrus.Entry) error {
	dbPath := os.Getenv("SYNC_DB_PATH")
	if dbPath == "" {
		dbPath = "sync.db"
	}
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil
	}
	defer db.Close()

	// Use the same format as the console to get the complete log line (including fields)
	formatted, _ := Log.Formatter.Format(entry)

	level := entry.Level.String()
	// Put the complete formatted log line in the message field, including time, level, and all fields
	message := strings.TrimSuffix(string(formatted), "\n") // Remove the trailing newline

	// Insert into the sync_log table
	_, _ = db.Exec(`
		INSERT INTO sync_log(level, message) 
		VALUES(?, ?)
	`, level, message)

	return nil
}

func (h *SQLiteHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
