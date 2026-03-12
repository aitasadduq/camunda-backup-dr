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

// ============================================================
// Error path & helper tests for s3.go coverage improvement
// ============================================================

// --- NewS3Storage validation paths ---

func TestNewS3Storage_EmptyEndpoint(t *testing.T) {
	logger := utils.NewLogger("debug")
	_, err := NewS3Storage("", "key", "secret", "bucket", "prefix", logger)
	if err == nil {
		t.Fatal("Expected error for empty endpoint")
	}
	if err.Error() != "S3 endpoint is required" {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestNewS3Storage_EmptyAccessKey(t *testing.T) {
	logger := utils.NewLogger("debug")
	_, err := NewS3Storage("http://localhost:9000", "", "secret", "bucket", "prefix", logger)
	if err == nil {
		t.Fatal("Expected error for empty access key")
	}
	if err.Error() != "S3 access key is required" {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestNewS3Storage_EmptySecretKey(t *testing.T) {
	logger := utils.NewLogger("debug")
	_, err := NewS3Storage("http://localhost:9000", "key", "", "bucket", "prefix", logger)
	if err == nil {
		t.Fatal("Expected error for empty secret key")
	}
	if err.Error() != "S3 secret key is required" {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestNewS3Storage_EmptyBucket(t *testing.T) {
	logger := utils.NewLogger("debug")
	_, err := NewS3Storage("http://localhost:9000", "key", "secret", "", "prefix", logger)
	if err == nil {
		t.Fatal("Expected error for empty bucket")
	}
	if err.Error() != "S3 bucket is required" {
		t.Errorf("Unexpected error: %s", err.Error())
	}
}

func TestNewS3Storage_EmptyPrefixAllowed(t *testing.T) {
	logger := utils.NewLogger("debug")
	s3, err := NewS3Storage("http://localhost:9000", "key", "secret", "bucket", "", logger)
	if err != nil {
		t.Fatalf("Empty prefix should be allowed: %v", err)
	}
	if s3.prefix != "" {
		t.Errorf("Expected empty prefix, got '%s'", s3.prefix)
	}
}

// --- GetLatestBackupID error path ---

func TestS3Storage_GetLatestBackupID_NotFound(t *testing.T) {
	s3 := setupTestS3Storage(t)

	_, err := s3.GetLatestBackupID("nonexistent-instance")
	if err == nil {
		t.Fatal("Expected error for non-existent instance")
	}
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound, got %v", err)
	}
}

// --- GetBackupHistory: lookup in incomplete and orphaned maps ---

func TestS3Storage_GetBackupHistory_FromIncomplete(t *testing.T) {
	s3 := setupTestS3Storage(t)

	history := createTestBackupHistory("camunda1", "backup-inc", types.BackupStatusIncomplete)
	if err := s3.StoreBackupHistory(history); err != nil {
		t.Fatal(err)
	}

	retrieved, err := s3.GetBackupHistory("camunda1", "backup-inc")
	if err != nil {
		t.Fatalf("Should find backup in incomplete map: %v", err)
	}
	if retrieved.BackupID != "backup-inc" {
		t.Errorf("Expected 'backup-inc', got '%s'", retrieved.BackupID)
	}
}

func TestS3Storage_GetBackupHistory_FromOrphaned(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store in main, then move to orphaned
	history := createTestBackupHistory("camunda1", "backup-orph", types.BackupStatusCompleted)
	if err := s3.StoreBackupHistory(history); err != nil {
		t.Fatal(err)
	}
	if err := s3.MoveToOrphaned("camunda1", "backup-orph"); err != nil {
		t.Fatal(err)
	}

	retrieved, err := s3.GetBackupHistory("camunda1", "backup-orph")
	if err != nil {
		t.Fatalf("Should find backup in orphaned map: %v", err)
	}
	if retrieved.BackupID != "backup-orph" {
		t.Errorf("Expected 'backup-orph', got '%s'", retrieved.BackupID)
	}
}

func TestS3Storage_GetBackupHistory_NotFoundAnywhere(t *testing.T) {
	s3 := setupTestS3Storage(t)

	_, err := s3.GetBackupHistory("camunda1", "nonexistent")
	if err == nil {
		t.Fatal("Expected error")
	}
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound, got %v", err)
	}
}

// --- UpdateBackupStatus error paths ---

func TestS3Storage_UpdateBackupStatus_NotFound(t *testing.T) {
	s3 := setupTestS3Storage(t)

	err := s3.UpdateBackupStatus("camunda1", "nonexistent", types.BackupStatusCompleted)
	if err == nil {
		t.Fatal("Expected error for non-existent backup")
	}
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound, got %v", err)
	}
}

func TestS3Storage_UpdateBackupStatus_FromIncomplete(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store as incomplete
	history := createTestBackupHistory("camunda1", "backup-upd-inc", types.BackupStatusIncomplete)
	if err := s3.StoreBackupHistory(history); err != nil {
		t.Fatal(err)
	}

	// Update to completed — should move out of incomplete
	err := s3.UpdateBackupStatus("camunda1", "backup-upd-inc", types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	retrieved, err := s3.GetBackupHistory("camunda1", "backup-upd-inc")
	if err != nil {
		t.Fatalf("Should find backup after status update: %v", err)
	}
	if retrieved.Status != types.BackupStatusCompleted {
		t.Errorf("Expected COMPLETED, got %s", retrieved.Status)
	}
	if retrieved.EndTime == nil {
		t.Error("Expected EndTime to be set")
	}
	if retrieved.DurationSeconds == nil {
		t.Error("Expected DurationSeconds to be set")
	}
}

func TestS3Storage_UpdateBackupStatus_ToIncomplete(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store as running in main history
	history := createTestBackupHistory("camunda1", "backup-to-inc", types.BackupStatusRunning)
	if err := s3.StoreBackupHistory(history); err != nil {
		t.Fatal(err)
	}

	// Update to incomplete — should move into incomplete map
	err := s3.UpdateBackupStatus("camunda1", "backup-to-inc", types.BackupStatusIncomplete)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	retrieved, err := s3.GetBackupHistory("camunda1", "backup-to-inc")
	if err != nil {
		t.Fatalf("Should find backup in incomplete map: %v", err)
	}
	if retrieved.Status != types.BackupStatusIncomplete {
		t.Errorf("Expected INCOMPLETE, got %s", retrieved.Status)
	}
}

// --- DeleteBackupHistory error paths ---

func TestS3Storage_DeleteBackupHistory_NotFound(t *testing.T) {
	s3 := setupTestS3Storage(t)

	err := s3.DeleteBackupHistory("camunda1", "nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent backup")
	}
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound, got %v", err)
	}
}

func TestS3Storage_DeleteBackupHistory_FromIncomplete(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store as incomplete
	history := createTestBackupHistory("camunda1", "backup-del-inc", types.BackupStatusIncomplete)
	if err := s3.StoreBackupHistory(history); err != nil {
		t.Fatal(err)
	}

	// Delete from incomplete
	err := s3.DeleteBackupHistory("camunda1", "backup-del-inc")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify deleted
	_, err = s3.GetBackupHistory("camunda1", "backup-del-inc")
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound after deletion, got %v", err)
	}
}

func TestS3Storage_DeleteBackupHistory_FromOrphaned(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store, then move to orphaned
	history := createTestBackupHistory("camunda1", "backup-del-orph", types.BackupStatusCompleted)
	if err := s3.StoreBackupHistory(history); err != nil {
		t.Fatal(err)
	}
	if err := s3.MoveToOrphaned("camunda1", "backup-del-orph"); err != nil {
		t.Fatal(err)
	}

	// Delete from orphaned
	err := s3.DeleteBackupHistory("camunda1", "backup-del-orph")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify deleted
	_, err = s3.GetBackupHistory("camunda1", "backup-del-orph")
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound after deletion, got %v", err)
	}
}

// --- getS3Key ---

func TestS3Storage_getS3Key_IncompleteStatus(t *testing.T) {
	s3 := setupTestS3Storage(t)

	key := s3.getS3Key("camunda1", "20240101-120000", types.BackupStatusIncomplete)
	if key == "" {
		t.Fatal("Expected non-empty key")
	}
	if !contains(key, "incomplete") {
		t.Errorf("Expected key to contain 'incomplete', got '%s'", key)
	}
	if !contains(key, "camunda1") {
		t.Errorf("Expected key to contain 'camunda1', got '%s'", key)
	}
	if !contains(key, "20240101-120000.json") {
		t.Errorf("Expected key to end with '20240101-120000.json', got '%s'", key)
	}
}

func TestS3Storage_getS3Key_CompletedStatus(t *testing.T) {
	s3 := setupTestS3Storage(t)

	key := s3.getS3Key("camunda1", "20240101-120000", types.BackupStatusCompleted)
	if !contains(key, "history") {
		t.Errorf("Expected key to contain 'history', got '%s'", key)
	}
}

func TestS3Storage_getS3Key_FailedStatus(t *testing.T) {
	s3 := setupTestS3Storage(t)

	key := s3.getS3Key("camunda1", "backup1", types.BackupStatusFailed)
	if !contains(key, "history") {
		t.Errorf("Expected key to contain 'history', got '%s'", key)
	}
}

func TestS3Storage_getS3Key_RunningStatus(t *testing.T) {
	s3 := setupTestS3Storage(t)

	key := s3.getS3Key("camunda1", "backup1", types.BackupStatusRunning)
	if !contains(key, "history") {
		t.Errorf("Expected key to contain 'history', got '%s'", key)
	}
}

func TestS3Storage_getS3Key_DefaultStatus(t *testing.T) {
	s3 := setupTestS3Storage(t)

	key := s3.getS3Key("camunda1", "backup1", types.BackupStatus("UNKNOWN"))
	if !contains(key, "history") {
		t.Errorf("Expected default status to use 'history', got '%s'", key)
	}
}

func TestS3Storage_getS3Key_IncludesPrefix(t *testing.T) {
	s3 := setupTestS3Storage(t)

	key := s3.getS3Key("inst1", "bak1", types.BackupStatusCompleted)
	if !contains(key, s3.prefix) {
		t.Errorf("Expected key to contain prefix '%s', got '%s'", s3.prefix, key)
	}
}

func TestS3Storage_getS3Key_IncludesDatePath(t *testing.T) {
	s3 := setupTestS3Storage(t)

	key := s3.getS3Key("inst1", "bak1", types.BackupStatusCompleted)
	today := time.Now().Format("2006/01/02")
	if !contains(key, today) {
		t.Errorf("Expected key to contain today's date '%s', got '%s'", today, key)
	}
}

// --- serializeBackupHistory ---

func TestS3Storage_serializeBackupHistory_Valid(t *testing.T) {
	s3 := setupTestS3Storage(t)

	history := createTestBackupHistory("camunda1", "backup1", types.BackupStatusCompleted)
	data, err := s3.serializeBackupHistory(history)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("Expected non-empty serialized data")
	}
	if !contains(string(data), "backup1") {
		t.Errorf("Serialized data should contain backup ID")
	}
	if !contains(string(data), "camunda1") {
		t.Errorf("Serialized data should contain instance ID")
	}
}

func TestS3Storage_serializeBackupHistory_RoundTrip(t *testing.T) {
	s3 := setupTestS3Storage(t)

	original := createTestBackupHistory("camunda1", "backup-rt", types.BackupStatusFailed)
	original.ErrorMessage = "something went wrong"

	data, err := s3.serializeBackupHistory(original)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	restored, err := s3.deserializeBackupHistory(data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	if restored.BackupID != original.BackupID {
		t.Errorf("BackupID mismatch: %s vs %s", original.BackupID, restored.BackupID)
	}
	if restored.CamundaInstanceID != original.CamundaInstanceID {
		t.Errorf("CamundaInstanceID mismatch")
	}
	if restored.Status != original.Status {
		t.Errorf("Status mismatch: %s vs %s", original.Status, restored.Status)
	}
	if restored.ErrorMessage != original.ErrorMessage {
		t.Errorf("ErrorMessage mismatch: %s vs %s", original.ErrorMessage, restored.ErrorMessage)
	}
}

// --- deserializeBackupHistory ---

func TestS3Storage_deserializeBackupHistory_Valid(t *testing.T) {
	s3 := setupTestS3Storage(t)

	jsonData := []byte(`{
		"backup_id": "20240101-120000",
		"camunda_instance_id": "camunda1",
		"status": "COMPLETED"
	}`)

	history, err := s3.deserializeBackupHistory(jsonData)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if history.BackupID != "20240101-120000" {
		t.Errorf("Expected backup ID '20240101-120000', got '%s'", history.BackupID)
	}
	if history.CamundaInstanceID != "camunda1" {
		t.Errorf("Expected instance ID 'camunda1', got '%s'", history.CamundaInstanceID)
	}
}

func TestS3Storage_deserializeBackupHistory_InvalidJSON(t *testing.T) {
	s3 := setupTestS3Storage(t)

	_, err := s3.deserializeBackupHistory([]byte("{invalid json"))
	if err == nil {
		t.Fatal("Expected error for invalid JSON")
	}
	if !contains(err.Error(), "failed to deserialize backup history") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestS3Storage_deserializeBackupHistory_EmptyJSON(t *testing.T) {
	s3 := setupTestS3Storage(t)

	_, err := s3.deserializeBackupHistory([]byte(""))
	if err == nil {
		t.Fatal("Expected error for empty input")
	}
}

// --- parseBackupIDFromKey ---

func TestS3Storage_parseBackupIDFromKey_Standard(t *testing.T) {
	s3 := setupTestS3Storage(t)

	id := s3.parseBackupIDFromKey("camunda-backups/camunda1/history/2024/01/01/20240101-120000.json")
	if id != "20240101-120000" {
		t.Errorf("Expected '20240101-120000', got '%s'", id)
	}
}

func TestS3Storage_parseBackupIDFromKey_NoExtension(t *testing.T) {
	s3 := setupTestS3Storage(t)

	id := s3.parseBackupIDFromKey("prefix/instance/history/2024/01/01/backup-no-ext")
	if id != "backup-no-ext" {
		t.Errorf("Expected 'backup-no-ext', got '%s'", id)
	}
}

func TestS3Storage_parseBackupIDFromKey_SingleSegment(t *testing.T) {
	s3 := setupTestS3Storage(t)

	id := s3.parseBackupIDFromKey("mybackup.json")
	if id != "mybackup" {
		t.Errorf("Expected 'mybackup', got '%s'", id)
	}
}

func TestS3Storage_parseBackupIDFromKey_EmptyString(t *testing.T) {
	s3 := setupTestS3Storage(t)

	id := s3.parseBackupIDFromKey("")
	if id != "" {
		t.Errorf("Expected empty string, got '%s'", id)
	}
}

func TestS3Storage_parseBackupIDFromKey_TrailingSlash(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Edge case: key ends with "/" → last part is ""
	id := s3.parseBackupIDFromKey("prefix/instance/")
	if id != "" {
		t.Errorf("Expected empty string for trailing slash, got '%s'", id)
	}
}

// --- MoveToOrphaned / MoveToIncomplete not-found paths ---

func TestS3Storage_MoveToOrphaned_NotFound(t *testing.T) {
	s3 := setupTestS3Storage(t)

	err := s3.MoveToOrphaned("camunda1", "nonexistent")
	if err == nil {
		t.Fatal("Expected error")
	}
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound, got %v", err)
	}
}

func TestS3Storage_MoveToIncomplete_NotFound(t *testing.T) {
	s3 := setupTestS3Storage(t)

	err := s3.MoveToIncomplete("camunda1", "nonexistent")
	if err == nil {
		t.Fatal("Expected error")
	}
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound, got %v", err)
	}
}

// --- ListOrphanedBackups / ListIncompleteBackups empty paths ---

func TestS3Storage_ListOrphanedBackups_Empty(t *testing.T) {
	s3 := setupTestS3Storage(t)

	backups, err := s3.ListOrphanedBackups("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(backups) != 0 {
		t.Errorf("Expected 0, got %d", len(backups))
	}
}

func TestS3Storage_ListIncompleteBackups_Empty(t *testing.T) {
	s3 := setupTestS3Storage(t)

	backups, err := s3.ListIncompleteBackups("nonexistent")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(backups) != 0 {
		t.Errorf("Expected 0, got %d", len(backups))
	}
}

// --- ListBackupHistory: incomplete status filter ---

func TestS3Storage_ListBackupHistory_IncompleteFilter(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store one incomplete and one completed
	inc := createTestBackupHistory("camunda1", "inc1", types.BackupStatusIncomplete)
	if err := s3.StoreBackupHistory(inc); err != nil {
		t.Fatal(err)
	}
	comp := createTestBackupHistory("camunda1", "comp1", types.BackupStatusCompleted)
	if err := s3.StoreBackupHistory(comp); err != nil {
		t.Fatal(err)
	}

	histories, err := s3.ListBackupHistory("camunda1", types.BackupStatusIncomplete)
	if err != nil {
		t.Fatal(err)
	}
	if len(histories) != 1 {
		t.Errorf("Expected 1 incomplete, got %d", len(histories))
	}
	if len(histories) > 0 && histories[0].BackupID != "inc1" {
		t.Errorf("Expected 'inc1', got '%s'", histories[0].BackupID)
	}
}

func TestS3Storage_ListBackupHistory_EmptyFilter(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store a completed backup
	comp := createTestBackupHistory("camunda1", "comp2", types.BackupStatusCompleted)
	if err := s3.StoreBackupHistory(comp); err != nil {
		t.Fatal(err)
	}

	// Empty status filter returns all from main history
	histories, err := s3.ListBackupHistory("camunda1", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(histories) != 1 {
		t.Errorf("Expected 1 backup with empty filter, got %d", len(histories))
	}
}

func TestS3Storage_ListBackupHistory_NonexistentInstance(t *testing.T) {
	s3 := setupTestS3Storage(t)

	histories, err := s3.ListBackupHistory("nonexistent", types.BackupStatusCompleted)
	if err != nil {
		t.Fatal(err)
	}
	if len(histories) != 0 {
		t.Errorf("Expected 0, got %d", len(histories))
	}
}

// --- StoreBackupHistory: default status branch ---

func TestS3Storage_StoreBackupHistory_DefaultStatus(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Use an unknown/custom status to exercise the default branch
	history := createTestBackupHistory("camunda1", "backup-def", types.BackupStatus("CUSTOM"))
	err := s3.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	retrieved, err := s3.GetBackupHistory("camunda1", "backup-def")
	if err != nil {
		t.Fatalf("Should find backup: %v", err)
	}
	if retrieved.Status != types.BackupStatus("CUSTOM") {
		t.Errorf("Expected CUSTOM status, got %s", retrieved.Status)
	}
}

// helper
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// --- ListOrphanedBackups: with multiple entries triggers sort ---

func TestS3Storage_ListOrphanedBackups_MultipleSorted(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Store and move multiple backups to orphaned with different start times
	for i := 0; i < 3; i++ {
		backupID := time.Now().Add(time.Duration(i) * time.Hour).Format("20060102-150405")
		history := createTestBackupHistory("camunda1", backupID, types.BackupStatusCompleted)
		history.StartTime = time.Now().Add(time.Duration(i) * time.Hour)
		if err := s3.StoreBackupHistory(history); err != nil {
			t.Fatal(err)
		}
		if err := s3.MoveToOrphaned("camunda1", backupID); err != nil {
			t.Fatal(err)
		}
	}

	backups, err := s3.ListOrphanedBackups("camunda1")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 3 {
		t.Fatalf("Expected 3 orphaned backups, got %d", len(backups))
	}
	// Verify sorted newest first
	for i := 1; i < len(backups); i++ {
		if backups[i].StartTime.After(backups[i-1].StartTime) {
			t.Error("Orphaned backups should be sorted newest first")
		}
	}
}

// --- ListIncompleteBackups: with multiple entries triggers sort ---

func TestS3Storage_ListIncompleteBackups_MultipleSorted(t *testing.T) {
	s3 := setupTestS3Storage(t)

	for i := 0; i < 3; i++ {
		backupID := time.Now().Add(time.Duration(i) * time.Hour).Format("20060102-150405")
		history := createTestBackupHistory("camunda1", backupID, types.BackupStatusCompleted)
		history.StartTime = time.Now().Add(time.Duration(i) * time.Hour)
		if err := s3.StoreBackupHistory(history); err != nil {
			t.Fatal(err)
		}
		if err := s3.MoveToIncomplete("camunda1", backupID); err != nil {
			t.Fatal(err)
		}
	}

	backups, err := s3.ListIncompleteBackups("camunda1")
	if err != nil {
		t.Fatal(err)
	}
	if len(backups) != 3 {
		t.Fatalf("Expected 3 incomplete backups, got %d", len(backups))
	}
	// Verify sorted newest first
	for i := 1; i < len(backups); i++ {
		if backups[i].StartTime.After(backups[i-1].StartTime) {
			t.Error("Incomplete backups should be sorted newest first")
		}
	}
}

// --- UpdateBackupStatus: edge case with new instance map creation ---

func TestS3Storage_UpdateBackupStatus_NewInstanceMapCreated(t *testing.T) {
	s3 := setupTestS3Storage(t)

	// Directly populate incompleteBackups without going through StoreBackupHistory
	// so that backupHistory["direct-inst"] does NOT exist.
	history := createTestBackupHistory("direct-inst", "bak1", types.BackupStatusIncomplete)
	s3.incompleteBackups["direct-inst"] = map[string]*models.BackupHistory{
		"bak1": history,
	}

	// Update to completed — backupHistory["direct-inst"] doesn't exist,
	// so the default branch must create it.
	err := s3.UpdateBackupStatus("direct-inst", "bak1", types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	retrieved, err := s3.GetBackupHistory("direct-inst", "bak1")
	if err != nil {
		t.Fatal(err)
	}
	if retrieved.Status != types.BackupStatusCompleted {
		t.Errorf("Expected COMPLETED, got %s", retrieved.Status)
	}
}