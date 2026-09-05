package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/retail-ai-inc/sync/internal/platform/config"
)

// The settings endpoint.
//
// Read by anyone signed in, written by an administrator: these change what
// every task does, and the audit trail the administrative group carries is
// what says who changed them.

type settingsBody struct {
	VerifyIntervalSeconds   int64   `json:"verifyIntervalSeconds"`
	VerifyRepair            bool    `json:"verifyRepair"`
	LagAlertSeconds         float64 `json:"lagAlertSeconds"`
	MonitoringRetentionDays int     `json:"monitoringRetentionDays"`
	BatchMaxEvents          int     `json:"batchMaxEvents"`
	BatchMaxBytes           int     `json:"batchMaxBytes"`
	MongoNoTransaction      bool    `json:"mongoNoTransaction"`
}

func bodyOf(s config.Settings) settingsBody {
	return settingsBody{
		VerifyIntervalSeconds:   int64(s.VerifyInterval / time.Second),
		VerifyRepair:            s.VerifyRepair,
		LagAlertSeconds:         s.LagAlertSeconds,
		MonitoringRetentionDays: s.MonitoringRetentionDays,
		BatchMaxEvents:          s.BatchMaxEvents,
		BatchMaxBytes:           s.BatchMaxBytes,
		MongoNoTransaction:      s.MongoNoTransaction,
	}
}

func settingsOf(b settingsBody) config.Settings {
	return config.Settings{
		VerifyInterval:          time.Duration(b.VerifyIntervalSeconds) * time.Second,
		VerifyRepair:            b.VerifyRepair,
		LagAlertSeconds:         b.LagAlertSeconds,
		MonitoringRetentionDays: b.MonitoringRetentionDays,
		BatchMaxEvents:          b.BatchMaxEvents,
		BatchMaxBytes:           b.BatchMaxBytes,
		MongoNoTransaction:      b.MongoNoTransaction,
	}
}

// SettingsHandler GET /api/settings
//
// It reports what is stored and, separately, the environment variables in
// force over them. A page that showed only the stored value would be telling
// somebody their change had taken effect when a variable was overruling it.
func SettingsHandler(w http.ResponseWriter, r *http.Request) {
	stored, err := config.LoadSettings()
	if err != nil {
		writeStatus(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "message": err.Error(),
		})
		return
	}
	writeStatus(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"data":       bodyOf(stored),
		"overridden": config.Overridden(),
	})
}

// UpdateSettingsHandler PUT /api/settings
func UpdateSettingsHandler(w http.ResponseWriter, r *http.Request) {
	var body settingsBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeStatus(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "message": "the body is not a settings object",
		})
		return
	}
	if reason := refuse(body); reason != "" {
		writeStatus(w, http.StatusBadRequest, map[string]interface{}{
			"success": false, "message": reason,
		})
		return
	}

	if err := config.SaveSettings(settingsOf(body)); err != nil {
		writeStatus(w, http.StatusInternalServerError, map[string]interface{}{
			"success": false, "message": err.Error(),
		})
		return
	}
	writeStatus(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"data":       body,
		"overridden": config.Overridden(),
	})
}

// refuse reports why a settings object cannot be stored, or "" when it can.
//
// Negative values are refused rather than clamped: zero already means "the
// built-in default", so a negative number is somebody meaning something else,
// and guessing which is how a setting ends up doing the opposite of what its
// author intended.
func refuse(b settingsBody) string {
	switch {
	case b.VerifyIntervalSeconds < 0:
		return "verifyIntervalSeconds cannot be negative; 0 turns the check off"
	case b.LagAlertSeconds < 0:
		return "lagAlertSeconds cannot be negative; 0 turns the alert off"
	case b.MonitoringRetentionDays < 0:
		return "monitoringRetentionDays cannot be negative; 0 keeps the default"
	case b.BatchMaxEvents < 0:
		return "batchMaxEvents cannot be negative; 0 keeps the default"
	case b.BatchMaxBytes < 0:
		return "batchMaxBytes cannot be negative; 0 keeps the default"
	}
	return ""
}
