package storage

import (
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func setupTestS3Storage(t *testing.T) *S3StorageImpl {
	logger := utils.NewLogger("debug")
	
	s3, err := NewS3Storage(
		"http://localhost:9000",
		"test-access-key",
		"test-secret-key",
		"test-bucket",
		"camunda-backups",
		logger,
	)
	
	if err != nil {
		t.Fatalf("Failed to create S3 storage: %v", err)
	}
	
	return s3
}

func createTestBackupHistory(camundaInstanceID, backupID string, status types.BackupStatus) *models.BackupHistory {
	history := models.NewBackupHistory(
		camundaInstanceID,
		"Test Camunda",
		backupID,
		types.TriggerTypeScheduled,
		"sequential",
		"/data/logs/test.log",
		"Test backup",
		"1.0.0",
		"1.0.0",
	)
	history.Status = status
	return history
}

func TestS3Storage_StoreAndGetLatestBackupID(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Store latest backup ID
	err := s3.StoreLatestBackupID("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to store latest backup ID: %v", err)
	}
	
	// Get latest backup ID
	backupID, err := s3.GetLatestBackupID("camunda1")
	if err != nil {
		t.Fatalf("Failed to get latest backup ID: %v", err)
	}
	
	if backupID != "20240101-120000" {
		t.Errorf("Expected backup ID '20240101-120000', got '%s'", backupID)
	}
}

func TestS3Storage_StoreAndGetBackupHistory(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Create test backup history
	history := createTestBackupHistory("camunda1", "20240101-120000", types.BackupStatusCompleted)
	
	// Store backup history
	err := s3.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}
	
	// Get backup history
	retrievedHistory, err := s3.GetBackupHistory("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to get backup history: %v", err)
	}
	
	// Verify backup history
	if retrievedHistory.BackupID != history.BackupID {
		t.Errorf("Expected backup ID '%s', got '%s'", history.BackupID, retrievedHistory.BackupID)
	}
	
	if retrievedHistory.Status != history.Status {
		t.Errorf("Expected status '%s', got '%s'", history.Status, retrievedHistory.Status)
	}
}

func TestS3Storage_ListBackupHistory(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Create multiple backup histories
	for i := 0; i < 3; i++ {
		backupID := time.Now().Add(time.Duration(i) * time.Hour).Format("20060102-150405")
		history := createTestBackupHistory("camunda1", backupID, types.BackupStatusCompleted)
		err := s3.StoreBackupHistory(history)
		if err != nil {
			t.Fatalf("Failed to store backup history %d: %v", i, err)
		}
	}
	
	// List backup histories
	histories, err := s3.ListBackupHistory("camunda1", types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to list backup histories: %v", err)
	}
	
	// Verify histories
	if len(histories) != 3 {
		t.Errorf("Expected 3 backup histories, got %d", len(histories))
	}
	
	// Verify they are sorted by start time (newest first)
	for i := 1; i < len(histories); i++ {
		if histories[i].StartTime.After(histories[i-1].StartTime) {
			t.Errorf("Histories should be sorted by start time (newest first)")
		}
	}
}

func TestS3Storage_UpdateBackupStatus(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Create test backup history
	history := createTestBackupHistory("camunda1", "20240101-120000", types.BackupStatusRunning)
	err := s3.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}
	
	// Update backup status
	err = s3.UpdateBackupStatus("camunda1", "20240101-120000", types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to update backup status: %v", err)
	}
	
	// Get backup history and verify status
	retrievedHistory, err := s3.GetBackupHistory("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to get backup history: %v", err)
	}
	
	if retrievedHistory.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status '%s', got '%s'", types.BackupStatusCompleted, retrievedHistory.Status)
	}
	
	if retrievedHistory.EndTime == nil {
		t.Error("Expected EndTime to be set")
	}
}

func TestS3Storage_MoveToOrphaned(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Create test backup history
	history := createTestBackupHistory("camunda1", "20240101-120000", types.BackupStatusCompleted)
	err := s3.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}
	
	// Move to orphaned
	err = s3.MoveToOrphaned("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to move to orphaned: %v", err)
	}
	
	// Verify it's in orphaned
	orphanedBackups, err := s3.ListOrphanedBackups("camunda1")
	if err != nil {
		t.Fatalf("Failed to list orphaned backups: %v", err)
	}
	
	if len(orphanedBackups) != 1 {
		t.Errorf("Expected 1 orphaned backup, got %d", len(orphanedBackups))
	}
	
	// Verify it's still retrievable (from orphaned)
	_, err = s3.GetBackupHistory("camunda1", "20240101-120000")
}

func TestS3Storage_MoveToIncomplete(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Create test backup history
	history := createTestBackupHistory("camunda1", "20240101-120000", types.BackupStatusCompleted)
	err := s3.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}
	
	// Move to incomplete
	err = s3.MoveToIncomplete("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to move to incomplete: %v", err)
	}
	
	// Verify it's in incomplete
	incompleteBackups, err := s3.ListIncompleteBackups("camunda1")
	if err != nil {
		t.Fatalf("Failed to list incomplete backups: %v", err)
	}
	
	if len(incompleteBackups) != 1 {
		t.Errorf("Expected 1 incomplete backup, got %d", len(incompleteBackups))
	}
	
	// Verify status is updated
	if incompleteBackups[0].Status != types.BackupStatusIncomplete {
		t.Errorf("Expected status '%s', got '%s'", types.BackupStatusIncomplete, incompleteBackups[0].Status)
	}
}

func TestS3Storage_ListIncompleteBackups(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Create incomplete backup
	incompleteHistory := createTestBackupHistory("camunda1", "20240101-120000", types.BackupStatusIncomplete)
	err := s3.StoreBackupHistory(incompleteHistory)
	if err != nil {
		t.Fatalf("Failed to store incomplete backup history: %v", err)
	}
	
	// Create completed backup
	completedHistory := createTestBackupHistory("camunda1", "20240101-130000", types.BackupStatusCompleted)
	err = s3.StoreBackupHistory(completedHistory)
	if err != nil {
		t.Fatalf("Failed to store completed backup history: %v", err)
	}
	
	// List incomplete backups
	incompleteBackups, err := s3.ListIncompleteBackups("camunda1")
	if err != nil {
		t.Fatalf("Failed to list incomplete backups: %v", err)
	}
	
	// Verify only incomplete backups are returned
	if len(incompleteBackups) != 1 {
		t.Errorf("Expected 1 incomplete backup, got %d", len(incompleteBackups))
	}
	
	if incompleteBackups[0].BackupID != "20240101-120000" {
		t.Errorf("Expected backup ID '20240101-120000', got '%s'", incompleteBackups[0].BackupID)
	}
}

func TestS3Storage_DeleteBackupHistory(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Create test backup history
	history := createTestBackupHistory("camunda1", "20240101-120000", types.BackupStatusCompleted)
	err := s3.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}
	
	// Delete backup history
	err = s3.DeleteBackupHistory("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to delete backup history: %v", err)
	}
	
	// Verify it's deleted
	_, err = s3.GetBackupHistory("camunda1", "20240101-120000")
}

func TestS3Storage_ConcurrentAccess(t *testing.T) {
	s3 := setupTestS3Storage(t)
	
	// Test concurrent store operations
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			backupID := time.Now().Add(time.Duration(id) * time.Second).Format("20060102-150405")
			history := createTestBackupHistory("camunda1", backupID, types.BackupStatusCompleted)
			err := s3.StoreBackupHistory(history)
			if err != nil {
				t.Errorf("Failed to store backup history %d: %v", id, err)
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Verify all backups were stored
	histories, err := s3.ListBackupHistory("camunda1", types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to list backup histories: %v", err)
	}
	
	if len(histories) != 10 {
		t.Errorf("Expected 10 backup histories, got %d", len(histories))
	}
}