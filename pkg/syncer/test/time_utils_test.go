package test

import (
	"testing"
	"time"

	"github.com/retail-ai-inc/sync/pkg/utils"
)

// Test ReplaceDatePlaceholders function
func TestReplaceDatePlaceholders(t *testing.T) {
	// Get current time for expected values
	now := time.Now()
	expectedYear := now.Format("2006")
	expectedMonth := now.Format("01")
	expectedDay := now.Format("02")

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Replace YYYY with year",
			input:    "backup_YYYY.tar.gz",
			expected: "backup_" + expectedYear + ".tar.gz",
		},
		{
			name:     "Replace MM with month",
			input:    "backup_MM.tar.gz",
			expected: "backup_" + expectedMonth + ".tar.gz",
		},
		{
			name:     "Replace DD with day",
			input:    "backup_DD.tar.gz",
			expected: "backup_" + expectedDay + ".tar.gz",
		},
		{
			name:     "Replace multiple placeholders",
			input:    "backup_YYYY_MM_DD.tar.gz",
			expected: "backup_" + expectedYear + "_" + expectedMonth + "_" + expectedDay + ".tar.gz",
		},
		{
			name:     "Replace lowercase placeholders",
			input:    "backup_yyyy_mm_dd.tar.gz",
			expected: "backup_" + expectedYear + "_" + expectedMonth + "_" + expectedDay + ".tar.gz",
		},
		{
			name:     "Mixed case placeholders",
			input:    "backup_YYYY-mm-DD.tar.gz",
			expected: "backup_" + expectedYear + "-" + expectedMonth + "-" + expectedDay + ".tar.gz",
		},
		{
			name:     "No placeholders",
			input:    "backup.tar.gz",
			expected: "backup.tar.gz",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.ReplaceDatePlaceholders(tt.input)
			if result != tt.expected {
				t.Errorf("ReplaceDatePlaceholders(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Test GetTodayDateString function
func TestGetTodayDateString(t *testing.T) {
	t.Run("Returns today's date in YYYY-MM-DD format", func(t *testing.T) {
		result := utils.GetTodayDateString()
		expected := time.Now().Format("2006-01-02")
		if result != expected {
			t.Errorf("GetTodayDateString() = %q, want %q", result, expected)
		}
	})

	t.Run("Returns valid date format", func(t *testing.T) {
		result := utils.GetTodayDateString()
		if len(result) != 10 {
			t.Errorf("GetTodayDateString() length = %d, want 10", len(result))
		}

		// Parse to ensure it's valid date
		_, err := time.Parse("2006-01-02", result)
		if err != nil {
			t.Errorf("GetTodayDateString() returned invalid date format: %v", err)
		}
	})
}

// Test ParseDatabaseTimestamp function
func TestParseDatabaseTimestamp(t *testing.T) {
	tests := []struct {
		name        string
		timestamp   string
		expectError bool
	}{
		{
			name:        "Valid timestamp",
			timestamp:   "2023-01-01 12:00:00",
			expectError: false,
		},
		{
			name:        "Valid timestamp with different time",
			timestamp:   "2023-12-25 10:30:45",
			expectError: false,
		},
		{
			name:        "Invalid format - missing time",
			timestamp:   "2023-01-01",
			expectError: true,
		},
		{
			name:        "Invalid format - wrong separator",
			timestamp:   "2023-01-01T12:00:00",
			expectError: true,
		},
		{
			name:        "Invalid date",
			timestamp:   "2023-13-01 12:00:00",
			expectError: true,
		},
		{
			name:        "Empty string",
			timestamp:   "",
			expectError: true,
		},
		{
			name:        "Completely invalid",
			timestamp:   "invalid-date",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := utils.ParseDatabaseTimestamp(tt.timestamp)
			if tt.expectError {
				if err == nil {
					t.Errorf("ParseDatabaseTimestamp(%q) expected error but got none", tt.timestamp)
				}
			} else {
				if err != nil {
					t.Errorf("ParseDatabaseTimestamp(%q) unexpected error: %v", tt.timestamp, err)
				}
				if result.IsZero() {
					t.Errorf("ParseDatabaseTimestamp(%q) returned zero time", tt.timestamp)
				}
			}
		})
	}
}

// Test ProcessTimeRangeQuery function
func TestProcessTimeRangeQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    map[string]interface{}
		expected int // number of fields in output
	}{
		{
			name: "Daily time range query",
			query: map[string]interface{}{
				"createdAt": map[string]interface{}{
					"type":        "daily",
					"startOffset": float64(-1),
					"endOffset":   float64(0),
				},
			},
			expected: 1,
		},
		{
			name: "Non-time range query",
			query: map[string]interface{}{
				"status": "active",
				"count":  100,
			},
			expected: 2,
		},
		{
			name: "Mixed query",
			query: map[string]interface{}{
				"status": "active",
				"createdAt": map[string]interface{}{
					"type":        "daily",
					"startOffset": float64(-7),
					"endOffset":   float64(0),
				},
			},
			expected: 2,
		},
		{
			name:     "Empty query",
			query:    map[string]interface{}{},
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := utils.ProcessTimeRangeQuery(tt.query)
			if err != nil {
				t.Errorf("ProcessTimeRangeQuery(%v) unexpected error: %v", tt.query, err)
			}
			if len(result) != tt.expected {
				t.Errorf("ProcessTimeRangeQuery(%v) result length = %d, want %d", tt.query, len(result), tt.expected)
			}
		})
	}
}

// Test GetJSTTimeRange function
func TestGetJSTTimeRange(t *testing.T) {
	tests := []struct {
		name        string
		startOffset int
		endOffset   int
		expectError bool
	}{
		{
			name:        "Valid range - yesterday to today",
			startOffset: -1,
			endOffset:   0,
			expectError: false,
		},
		{
			name:        "Valid range - last 7 days",
			startOffset: -7,
			endOffset:   0,
			expectError: false,
		},
		{
			name:        "Valid range - same day",
			startOffset: 0,
			endOffset:   0,
			expectError: false,
		},
		{
			name:        "Valid range - future days",
			startOffset: 1,
			endOffset:   7,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := utils.GetJSTTimeRange(tt.startOffset, tt.endOffset)

			if tt.expectError {
				if err == nil {
					t.Errorf("GetJSTTimeRange(%d, %d) expected error but got none", tt.startOffset, tt.endOffset)
				}
			} else {
				if err != nil {
					t.Errorf("GetJSTTimeRange(%d, %d) unexpected error: %v", tt.startOffset, tt.endOffset, err)
				}

				// Check that times are not zero
				if start.IsZero() || end.IsZero() {
					t.Errorf("GetJSTTimeRange(%d, %d) returned zero time", tt.startOffset, tt.endOffset)
				}

				// Check that times are start of day
				if start.Hour() != 0 || start.Minute() != 0 || start.Second() != 0 {
					t.Errorf("GetJSTTimeRange(%d, %d) start time should be start of day", tt.startOffset, tt.endOffset)
				}
				if end.Hour() != 0 || end.Minute() != 0 || end.Second() != 0 {
					t.Errorf("GetJSTTimeRange(%d, %d) end time should be start of day", tt.startOffset, tt.endOffset)
				}
			}
		})
	}
}

// Test GetUTCTimeRange function
func TestGetUTCTimeRange(t *testing.T) {
	tests := []struct {
		name        string
		startOffset int
		endOffset   int
		expectError bool
	}{
		{
			name:        "Valid range - yesterday to today",
			startOffset: -1,
			endOffset:   0,
			expectError: false,
		},
		{
			name:        "Valid range - last 7 days",
			startOffset: -7,
			endOffset:   0,
			expectError: false,
		},
		{
			name:        "Valid range - same day",
			startOffset: 0,
			endOffset:   0,
			expectError: false,
		},
		{
			name:        "Valid range - future days",
			startOffset: 1,
			endOffset:   7,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := utils.GetUTCTimeRange(tt.startOffset, tt.endOffset)

			if tt.expectError {
				if err == nil {
					t.Errorf("GetUTCTimeRange(%d, %d) expected error but got none", tt.startOffset, tt.endOffset)
				}
			} else {
				if err != nil {
					t.Errorf("GetUTCTimeRange(%d, %d) unexpected error: %v", tt.startOffset, tt.endOffset, err)
				}

				// Check that times are not zero
				if start.IsZero() || end.IsZero() {
					t.Errorf("GetUTCTimeRange(%d, %d) returned zero time", tt.startOffset, tt.endOffset)
				}

				// Check that times are in UTC
				if start.Location() != time.UTC {
					t.Errorf("GetUTCTimeRange(%d, %d) start time is not in UTC", tt.startOffset, tt.endOffset)
				}
				if end.Location() != time.UTC {
					t.Errorf("GetUTCTimeRange(%d, %d) end time is not in UTC", tt.startOffset, tt.endOffset)
				}
			}
		})
	}
}

// Test edge cases and error conditions
func TestTimeUtilsEdgeCases(t *testing.T) {
	t.Run("ReplaceDatePlaceholders with special characters", func(t *testing.T) {
		now := time.Now()
		expectedYear := now.Format("2006")
		expectedMonth := now.Format("01")
		expectedDay := now.Format("02")

		input := "backup_YYYY_MM_DD_special'chars.tar.gz"
		result := utils.ReplaceDatePlaceholders(input)
		expected := "backup_" + expectedYear + "_" + expectedMonth + "_" + expectedDay + "_special'chars.tar.gz"
		if result != expected {
			t.Errorf("ReplaceDatePlaceholders with special chars failed")
		}
	})

	t.Run("ProcessTimeRangeQuery with invalid offsets", func(t *testing.T) {
		query := map[string]interface{}{
			"createdAt": map[string]interface{}{
				"type":        "daily",
				"startOffset": "invalid",
				"endOffset":   float64(0),
			},
		}

		result, err := utils.ProcessTimeRangeQuery(query)
		if err != nil {
			t.Errorf("ProcessTimeRangeQuery with invalid offset failed: %v", err)
		}
		// Should preserve original input on error
		if len(result) != 1 {
			t.Errorf("ProcessTimeRangeQuery should preserve original input on error")
		}
	})

	t.Run("Time range functions with extreme values", func(t *testing.T) {
		start, end, err := utils.GetJSTTimeRange(-365, 365)
		if err != nil {
			t.Errorf("GetJSTTimeRange with extreme values failed: %v", err)
		}
		if start.IsZero() || end.IsZero() {
			t.Errorf("GetJSTTimeRange with extreme values returned zero time")
		}

		start, end, err = utils.GetUTCTimeRange(-365, 365)
		if err != nil {
			t.Errorf("GetUTCTimeRange with extreme values failed: %v", err)
		}
		if start.IsZero() || end.IsZero() {
			t.Errorf("GetUTCTimeRange with extreme values returned zero time")
		}
	})
}

// Test TestTimeUtilsIntegration - main integration test for the entire test suite
func TestTimeUtilsIntegration(t *testing.T) {
	t.Run("ReplaceDatePlaceholders", func(t *testing.T) {
		TestReplaceDatePlaceholders(t)
	})

	t.Run("GetTodayDateString", func(t *testing.T) {
		TestGetTodayDateString(t)
	})

	t.Run("ParseDatabaseTimestamp", func(t *testing.T) {
		TestParseDatabaseTimestamp(t)
	})

	t.Run("ProcessTimeRangeQuery", func(t *testing.T) {
		TestProcessTimeRangeQuery(t)
	})

	t.Run("GetJSTTimeRange", func(t *testing.T) {
		TestGetJSTTimeRange(t)
	})

	t.Run("GetUTCTimeRange", func(t *testing.T) {
		TestGetUTCTimeRange(t)
	})

	t.Run("EdgeCases", func(t *testing.T) {
		TestTimeUtilsEdgeCases(t)
	})
}
