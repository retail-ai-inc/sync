package httpx

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

func ErrorJSON(w http.ResponseWriter, msg string, err error) {
	logrus.Errorf("%s => %v", msg, err)
	resp := map[string]interface{}{
		"success": false,
		"error":   msg,
		"detail":  err.Error(),
	}
	WriteJSON(w, resp)
}

func WriteJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

// TimeNowStr returns the current time formatted as a string in UTC timezone
// for database storage purposes
func TimeNowStr() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05")
}

// ConvertTimeToJST converts a time string from UTC to JST timezone for SQL time format
// This handles the specific format used in the database "2006-01-02 15:04:05"
func ConvertTimeToJST(input string) string {
	if input == "" {
		return ""
	}

	// First try parsing with standard SQL format
	layout := "2006-01-02 15:04:05"
	t, err := time.Parse(layout, input)
	if err == nil {
		jst := time.FixedZone("JST", 9*60*60)
		return t.In(jst).Format(layout)
	}

	// If that fails, try RFC3339 format
	t, err = time.Parse(time.RFC3339, input)
	if err == nil {
		jst := time.FixedZone("JST", 9*60*60)
		return t.In(jst).Format(layout)
	}

	// Return original if we can't parse
	return input
}
