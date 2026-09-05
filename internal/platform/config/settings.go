package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/sqlite"
	"github.com/sirupsen/logrus"
)

// The settings an operator changes without a rebuild.
//
// Every one of these was a constant in the code or an environment variable, so
// turning the consistency check on meant a redeploy and finding out what the
// batch limit was meant reading the source. They are stored in the control
// database, which is the thing the UI already edits and the thing a backup
// carries.
//
// Zero means "use the built-in default" throughout. That is what a database
// from before these columns holds, so adding them changed nothing.

// Settings is the whole of it. The zero value is every default.
type Settings struct {
	// VerifyInterval is how often the consistency check compares the two ends.
	// Zero leaves it off, which is what it has always been.
	VerifyInterval time.Duration `json:"verifyIntervalSeconds"`
	// VerifyRepair lets that check write to the target. Off unless asked for:
	// a comparison that repairs is a second writer, and the review of the
	// payment path asks for it to stay off until the repair protocol has a
	// watermark.
	VerifyRepair bool `json:"verifyRepair"`
	// LagAlertSeconds is how far behind a task may be before it is reported.
	// Zero leaves the alert off.
	LagAlertSeconds float64 `json:"lagAlertSeconds"`
	// MonitoringRetentionDays is how long monitoring_log rows are kept.
	MonitoringRetentionDays int `json:"monitoringRetentionDays"`
	// BatchMaxEvents and BatchMaxBytes bound one batch. The byte bound is what
	// keeps a batch of large rows from being unbounded memory.
	BatchMaxEvents int `json:"batchMaxEvents"`
	BatchMaxBytes  int `json:"batchMaxBytes"`
	// MongoNoTransaction applies a MongoDB batch without a transaction. It
	// gives up the atomicity of a batch and is here to be visible, not to be
	// used: a half-applied batch and its position are no longer one thing.
	MongoNoTransaction bool `json:"mongoNoTransaction"`
}

// settingColumns is the order the columns are read and written in.
const settingColumns = `verify_interval_seconds, verify_repair, lag_alert_seconds,
	monitoring_retention_days, batch_max_events, batch_max_bytes, mongo_no_transaction`

// LoadSettings reads the settings from the control database.
//
// A database that cannot be read is reported rather than silently defaulted:
// the difference between "nobody set an interval" and "the settings could not
// be read" is the difference between the check being off on purpose and being
// off by accident.
func LoadSettings() (Settings, error) {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return Settings{}, err
	}
	defer db.Close()

	var (
		s             Settings
		interval, lag int64
		repair, noTx  int
		retention     int
		events, bytes int
	)
	err = db.QueryRow(`SELECT `+settingColumns+` FROM config_global WHERE id = 1`).
		Scan(&interval, &repair, &lag, &retention, &events, &bytes, &noTx)
	if err != nil {
		return Settings{}, fmt.Errorf("read the settings: %w", err)
	}

	s.VerifyInterval = time.Duration(interval) * time.Second
	s.VerifyRepair = repair != 0
	s.LagAlertSeconds = float64(lag)
	s.MonitoringRetentionDays = retention
	s.BatchMaxEvents = events
	s.BatchMaxBytes = bytes
	s.MongoNoTransaction = noTx != 0
	return s, nil
}

// SaveSettings writes them back.
func SaveSettings(s Settings) error {
	db, err := sqlite.OpenSQLiteDB()
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Exec(`UPDATE config_global SET
		verify_interval_seconds = ?, verify_repair = ?, lag_alert_seconds = ?,
		monitoring_retention_days = ?, batch_max_events = ?, batch_max_bytes = ?,
		mongo_no_transaction = ?
		WHERE id = 1`,
		int64(s.VerifyInterval/time.Second), boolToInt(s.VerifyRepair),
		int64(s.LagAlertSeconds), s.MonitoringRetentionDays,
		s.BatchMaxEvents, s.BatchMaxBytes, boolToInt(s.MongoNoTransaction))
	if err != nil {
		return fmt.Errorf("write the settings: %w", err)
	}
	return nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// Overridden reports the settings an environment variable is currently taking
// precedence over, so the settings page can say why a value it shows is not
// the value in force.
//
// The variables are kept working on purpose: they are how this is deployed
// today, and an upgrade that silently ignored them would change behaviour
// without anyone asking for it. A setting is the value when no variable says
// otherwise.
func Overridden() map[string]string {
	over := map[string]string{}
	for field, name := range map[string]string{
		"verifyIntervalSeconds":   "SYNC_VERIFY_INTERVAL",
		"verifyRepair":            "SYNC_VERIFY_REPAIR",
		"lagAlertSeconds":         "SYNC_LAG_ALERT_SECONDS",
		"monitoringRetentionDays": "SYNC_MONITORING_RETENTION_DAYS",
		"mongoNoTransaction":      "SYNC_MONGO_NO_TRANSACTION",
	} {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			over[field] = name + "=" + value
		}
	}
	return over
}

// DurationSetting resolves one duration: the environment variable when it is
// set, otherwise the stored setting.
func DurationSetting(envName string, stored time.Duration, log logrus.FieldLogger) time.Duration {
	raw := strings.TrimSpace(os.Getenv(envName))
	if raw == "" {
		return stored
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds < 0 {
		if log != nil {
			log.Warnf("%s is %q, which is not a number of seconds; using the stored "+
				"setting instead", envName, raw)
		}
		return stored
	}
	if log != nil && time.Duration(seconds)*time.Second != stored {
		log.Infof("%s is set, so it takes precedence over the stored setting", envName)
	}
	return time.Duration(seconds) * time.Second
}
