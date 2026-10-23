package logging

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3" // for sqlite
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
