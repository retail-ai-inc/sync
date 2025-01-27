package logger

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strconv"
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

	logLine := fmt.Sprintf("[%s] [%s] %s%s\n", timestamp, level, entry.Message, dataStr)
	return []byte(logLine), nil
}

func InitLogger(logLevel string) *logrus.Logger {
	logger := logrus.New()
	logger.Out = os.Stdout
	logger.SetLevel(getLogLevel(logLevel))
	logger.SetFormatter(&CustomTextFormatter{})

	// 在这里添加写入SQLite的Hook
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

	formatted, _ := Log.Formatter.Format(entry)
	level := entry.Level.String()
	message := strings.TrimSuffix(string(formatted), "\n")

	// 若 entry.Data["sync_task_id"] 存在，就取其值；否则用 0
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
	_, _ = db.Exec(insSQL, level, message, syncTaskID)
	return nil
}

func (h *SQLiteHook) Levels() []logrus.Level {
	return logrus.AllLevels
}
