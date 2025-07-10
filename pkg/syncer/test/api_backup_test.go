package test

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/retail-ai-inc/sync/pkg/api"
	"github.com/retail-ai-inc/sync/pkg/backup"
)

// TestBackupExecutorBasics tests creating a new backup executor
func TestBackupExecutorBasics(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	defer db.Close()

	executor := backup.NewBackupExecutor(db)
	if executor == nil {
		t.Fatal("NewBackupExecutor() returned nil")
	}
}

// TestGetTableSchemaHandler tests the schema handler
func TestGetTableSchemaHandler(t *testing.T) {
	t.Run("invalid JSON body", func(t *testing.T) {
		req, err := http.NewRequest("POST", "/schema", strings.NewReader("invalid json"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.GetTableSchemaHandler)
		handler.ServeHTTP(rr, req)

		// Should return bad request
		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rr.Code)
		}
	})

	t.Run("empty body", func(t *testing.T) {
		req, err := http.NewRequest("POST", "/schema", strings.NewReader(""))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.GetTableSchemaHandler)
		handler.ServeHTTP(rr, req)

		// Should return bad request
		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400, got %d", rr.Code)
		}
	})

	t.Run("missing required fields", func(t *testing.T) {
		reqBody := `{"sourceType": "mysql"}`
		req, err := http.NewRequest("POST", "/schema", strings.NewReader(reqBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")

		rr := httptest.NewRecorder()
		handler := http.HandlerFunc(api.GetTableSchemaHandler)
		handler.ServeHTTP(rr, req)

		// Should return bad request or internal server error
		if rr.Code != http.StatusBadRequest && rr.Code != http.StatusInternalServerError {
			t.Errorf("Expected status 400 or 500, got %d", rr.Code)
		}
	})
}

// TestBackupExecutorCreation tests basic backup executor functionality
func TestBackupExecutorCreation(t *testing.T) {
	t.Run("valid database connection", func(t *testing.T) {
		db, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatalf("Failed to create test database: %v", err)
		}
		defer db.Close()

		executor := backup.NewBackupExecutor(db)
		if executor == nil {
			t.Error("NewBackupExecutor() returned nil")
		}
	})

	t.Run("nil database connection", func(t *testing.T) {
		executor := backup.NewBackupExecutor(nil)
		if executor == nil {
			t.Error("NewBackupExecutor() should handle nil database")
		}
	})
}

// TestAPIBackupIntegration - main integration test for API and backup functionality
func TestAPIBackupIntegration(t *testing.T) {
	t.Run("BackupExecutorBasics", func(t *testing.T) {
		TestBackupExecutorBasics(t)
	})

	t.Run("SchemaHandler", func(t *testing.T) {
		TestGetTableSchemaHandler(t)
	})

	t.Run("BackupExecutorCreation", func(t *testing.T) {
		TestBackupExecutorCreation(t)
	})
}
