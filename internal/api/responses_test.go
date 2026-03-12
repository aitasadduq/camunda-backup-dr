package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func TestWriteJSON_Success(t *testing.T) {
	w := httptest.NewRecorder()

	data := map[string]string{"key": "value"}
	writeJSON(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", ct)
	}

	var result map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("expected key=value, got key=%s", result["key"])
	}
}

func TestWriteJSON_NilData(t *testing.T) {
	w := httptest.NewRecorder()

	writeJSON(w, http.StatusNoContent, nil)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status 204, got %d", w.Code)
	}
	if w.Body.Len() != 0 {
		t.Errorf("expected empty body for nil data, got %d bytes", w.Body.Len())
	}
}

func TestWriteJSON_MarshalFailure(t *testing.T) {
	w := httptest.NewRecorder()

	// Channels cannot be marshaled to JSON
	writeJSON(w, http.StatusOK, make(chan int))

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500 on marshal failure, got %d", w.Code)
	}

	var errResp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to unmarshal fallback error: %v", err)
	}
	if errResp.Error != "internal_error" {
		t.Errorf("expected error type 'internal_error', got '%s'", errResp.Error)
	}
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()

	writeError(w, http.StatusBadRequest, "validation_error", "Missing field")

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}

	var errResp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if errResp.Error != "validation_error" {
		t.Errorf("expected error 'validation_error', got '%s'", errResp.Error)
	}
	if errResp.Message != "Missing field" {
		t.Errorf("expected message 'Missing field', got '%s'", errResp.Message)
	}
	if errResp.Code != http.StatusBadRequest {
		t.Errorf("expected code 400, got %d", errResp.Code)
	}
}

func TestWriteSuccess(t *testing.T) {
	w := httptest.NewRecorder()

	writeSuccess(w, http.StatusOK, "Operation successful", map[string]string{"id": "123"})

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}

	var resp SuccessResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Message != "Operation successful" {
		t.Errorf("expected message 'Operation successful', got '%s'", resp.Message)
	}
}

func TestWriteAppError_WithComponentAndInstance(t *testing.T) {
	w := httptest.NewRecorder()

	err := utils.NewAppError("backup_failed", "Backup failed", http.StatusInternalServerError).
		WithComponent("zeebe").
		WithInstance("test-instance")
	writeAppError(w, err)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.Code)
	}

	var errResp ErrorResponse
	if e := json.Unmarshal(w.Body.Bytes(), &errResp); e != nil {
		t.Fatalf("failed to unmarshal: %v", e)
	}
	if errResp.Component != "zeebe" {
		t.Errorf("expected component 'zeebe', got '%s'", errResp.Component)
	}
	if errResp.InstanceID != "test-instance" {
		t.Errorf("expected instance_id 'test-instance', got '%s'", errResp.InstanceID)
	}
}

func TestWriteAppError_GenericError(t *testing.T) {
	w := httptest.NewRecorder()

	// A plain error (not AppError) should fall back to 500
	writeAppError(w, fmt.Errorf("something went wrong"))

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500 for generic error, got %d", w.Code)
	}

	var errResp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if errResp.Code != http.StatusInternalServerError {
		t.Errorf("expected code 500, got %d", errResp.Code)
	}
}

func TestWriteAppError_ValidationError(t *testing.T) {
	w := httptest.NewRecorder()

	err := utils.NewValidationError("Name is required")
	writeAppError(w, err)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", w.Code)
	}

	var errResp ErrorResponse
	if e := json.Unmarshal(w.Body.Bytes(), &errResp); e != nil {
		t.Fatalf("failed to unmarshal: %v", e)
	}
	if errResp.Error != "validation_error" {
		t.Errorf("expected error 'validation_error', got '%s'", errResp.Error)
	}
}
