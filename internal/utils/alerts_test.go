package utils

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAlerter_IsEnabled(t *testing.T) {
	logger := NewLogger("info")

	t.Run("enabled with URL", func(t *testing.T) {
		a := NewAlerter("http://example.com/hook", logger)
		if !a.IsEnabled() {
			t.Error("should be enabled with non-empty URL")
		}
	})

	t.Run("disabled without URL", func(t *testing.T) {
		a := NewAlerter("", logger)
		if a.IsEnabled() {
			t.Error("should be disabled with empty URL")
		}
	})
}

func TestAlerter_SendAlert_Disabled(t *testing.T) {
	logger := NewLogger("info")
	a := NewAlerter("", logger)

	// Should be a no-op, not panic
	a.SendAlert(AlertCritical, "test", "msg", nil)
}

func TestAlerter_SendAlert_Success(t *testing.T) {
	var mu sync.Mutex
	var received *Alert

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var alert Alert
		if err := json.NewDecoder(r.Body).Decode(&alert); err != nil {
			t.Errorf("failed to decode alert: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = &alert
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := NewLogger("info")
	a := NewAlerter(server.URL, logger)

	a.SendAlert(AlertCritical, "Test Alert", "something happened", map[string]string{"key": "val"})

	// Wait for async send
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if received == nil {
		t.Fatal("alert was not received by webhook")
	}
	if received.Level != AlertCritical {
		t.Errorf("Level = %q, want %q", received.Level, AlertCritical)
	}
	if received.Title != "Test Alert" {
		t.Errorf("Title = %q, want %q", received.Title, "Test Alert")
	}
	if received.Metadata["key"] != "val" {
		t.Errorf("Metadata[key] = %q, want %q", received.Metadata["key"], "val")
	}
	if received.Timestamp == "" {
		t.Error("Timestamp should be set")
	}
}

func TestAlerter_SendAlert_ServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	logger := NewLogger("info")
	a := NewAlerter(server.URL, logger)

	// Should not panic even on server error
	a.SendAlert(AlertWarning, "Test", "msg", nil)
	time.Sleep(100 * time.Millisecond)
}

func TestAlerter_ConvenienceMethods(t *testing.T) {
	var mu sync.Mutex
	var alerts []Alert

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var alert Alert
		json.NewDecoder(r.Body).Decode(&alert)
		mu.Lock()
		alerts = append(alerts, alert)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := NewLogger("info")
	a := NewAlerter(server.URL, logger)

	a.AlertBackupFailed("prod", "backup-123", "timeout")
	a.AlertCircuitOpen("elasticsearch")
	a.AlertCleanupFailed("prod", "backup-123", "S3 error")
	a.AlertStuckBackup("prod", "job-1", 2*time.Hour)
	a.AlertSchedulerError("scheduler crashed")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(alerts) != 5 {
		t.Fatalf("expected 5 alerts, got %d", len(alerts))
	}
}
