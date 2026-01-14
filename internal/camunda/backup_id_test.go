package camunda

import (
	"testing"
	"time"
)

func TestGenerateBackupID(t *testing.T) {
	backupID := GenerateBackupID()
	if backupID == "" {
		t.Error("Generated backup ID should not be empty")
	}

	// Verify format: YYYYMMDD-HHMMSS
	if len(backupID) != 15 {
		t.Errorf("Expected backup ID length 15, got %d", len(backupID))
	}

	// Verify it can be parsed back
	_, err := ParseBackupIDTimestamp(backupID)
	if err != nil {
		t.Errorf("Failed to parse generated backup ID: %v", err)
	}
}

func TestGenerateBackupIDWithTimestamp(t *testing.T) {
	timestamp := time.Date(2024, 1, 15, 14, 30, 45, 0, time.UTC)
	backupID := GenerateBackupIDWithTimestamp(timestamp)
	expected := "20240115-143045"
	if backupID != expected {
		t.Errorf("Expected backup ID %s, got %s", expected, backupID)
	}
}

func TestParseBackupIDTimestamp(t *testing.T) {
	backupID := "20240115-143045"
	timestamp, err := ParseBackupIDTimestamp(backupID)
	if err != nil {
		t.Fatalf("Failed to parse backup ID: %v", err)
	}

	expected := time.Date(2024, 1, 15, 14, 30, 45, 0, time.UTC)
	if !timestamp.Equal(expected) {
		t.Errorf("Expected timestamp %v, got %v", expected, timestamp)
	}
}

func TestParseBackupIDTimestamp_InvalidFormat(t *testing.T) {
	invalidIDs := []string{
		"invalid",
		"2024-01-15",
		"20240115",
		"20240115-14:30:45",
		"",
	}

	for _, invalidID := range invalidIDs {
		_, err := ParseBackupIDTimestamp(invalidID)
		if err == nil {
			t.Errorf("Expected error for invalid backup ID: %s", invalidID)
		}
	}
}

func TestValidateBackupID(t *testing.T) {
	validID := "20240115-143045"
	if err := ValidateBackupID(validID); err != nil {
		t.Errorf("Valid backup ID should not return error: %v", err)
	}

	invalidID := "invalid"
	if err := ValidateBackupID(invalidID); err == nil {
		t.Error("Invalid backup ID should return error")
	}
}
