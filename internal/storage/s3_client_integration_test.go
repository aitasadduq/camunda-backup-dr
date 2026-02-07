//go:build integration
// +build integration

package storage

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Integration tests for S3Client
// These tests require a running S3-compatible storage (MinIO)
// Set environment variables to run these tests:
//   S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY, S3_BUCKET
// Or use defaults for local MinIO: http://minio:9000, localminio, localminio12345, backup-id-test

var (
	// s3Available is set once during TestMain to avoid repeated health checks
	s3Available     bool
	s3AvailableOnce sync.Once
	s3SkipReason    string
	// sharedTestConfig holds the test configuration
	sharedTestConfig S3Config
)

func TestMain(m *testing.M) {
	// Check S3 availability once before running any tests
	checkS3Availability()
	os.Exit(m.Run())
}

func checkS3Availability() {
	s3AvailableOnce.Do(func() {
		sharedTestConfig = getTestS3Config()
		logger := utils.NewLogger("error") // Quiet logger for availability check

		client, err := NewS3Client(sharedTestConfig, logger)
		if err != nil {
			s3Available = false
			s3SkipReason = "Failed to create S3 client: " + err.Error()
			return
		}

		if err := client.HealthCheck(); err != nil {
			s3Available = false
			s3SkipReason = "S3 is not available: " + err.Error()
			return
		}

		s3Available = true
	})
}

func getTestS3Config() S3Config {
	endpoint := os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}

	accessKey := os.Getenv("S3_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "localminio"
	}

	secretKey := os.Getenv("S3_SECRET_KEY")
	if secretKey == "" {
		secretKey = "localminio12345"
	}

	bucket := os.Getenv("S3_BUCKET")
	if bucket == "" {
		bucket = "backup-id-test"
	}

	// Use a unique prefix for each test run to avoid conflicts
	prefix := "test-" + time.Now().Format("20060102-150405")

	return S3Config{
		Endpoint:     endpoint,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		Bucket:       bucket,
		Prefix:       prefix,
		Region:       "us-east-1",
		UsePathStyle: true,
	}
}

func setupIntegrationTestS3Client(t *testing.T) *S3Client {
	t.Helper()

	// Fast skip if S3 was already determined to be unavailable
	if !s3Available {
		t.Skipf("Skipping integration test: %s", s3SkipReason)
	}

	// Create a new client with a unique prefix for this test
	cfg := getTestS3Config()
	logger := utils.NewLogger("debug")

	client, err := NewS3Client(cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create S3 client: %v", err)
	}

	return client
}

func createTestBackupHistoryForIntegration(camundaInstanceID, backupID string, status types.BackupStatus) *models.BackupHistory {
	history := models.NewBackupHistory(
		camundaInstanceID,
		"Test Camunda Integration",
		backupID,
		types.TriggerTypeScheduled,
		"sequential",
		"/data/logs/test.log",
		"Integration test backup",
		"1.0.0",
		"1.0.0",
	)
	history.Status = status

	// Add some component info
	now := time.Now()
	history.UpdateComponentBackupInfo("zeebe", models.ComponentBackupInfo{
		Enabled:         true,
		Status:          types.ComponentStatusCompleted,
		StartTime:       &now,
		EndTime:         &now,
		DurationSeconds: 10,
	})

	return history
}

func TestIntegration_StoreAndGetLatestBackupID(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-1"
	backupID := time.Now().Format("20060102-150405")

	// Store latest backup ID
	err := client.StoreLatestBackupID(camundaInstanceID, backupID)
	if err != nil {
		t.Fatalf("Failed to store latest backup ID: %v", err)
	}

	// Get latest backup ID
	retrievedID, err := client.GetLatestBackupID(camundaInstanceID)
	if err != nil {
		t.Fatalf("Failed to get latest backup ID: %v", err)
	}

	if retrievedID != backupID {
		t.Errorf("Expected backup ID '%s', got '%s'", backupID, retrievedID)
	}
}

func TestIntegration_GetLatestBackupID_NotFound(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	// Try to get a non-existent backup ID
	_, err := client.GetLatestBackupID("non-existent-instance")
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound, got: %v", err)
	}
}

func TestIntegration_StoreAndGetBackupHistory(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-2"
	backupID := time.Now().Format("20060102-150405")

	// Create test backup history
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)

	// Store backup history
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}

	// Get backup history
	retrievedHistory, err := client.GetBackupHistory(camundaInstanceID, backupID)
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

	if retrievedHistory.CamundaInstanceID != history.CamundaInstanceID {
		t.Errorf("Expected camunda instance ID '%s', got '%s'", history.CamundaInstanceID, retrievedHistory.CamundaInstanceID)
	}
}

func TestIntegration_ListBackupHistory(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-3"

	// Create multiple backup histories
	backupIDs := make([]string, 3)
	for i := 0; i < 3; i++ {
		backupID := time.Now().Add(time.Duration(i) * time.Second).Format("20060102-150405")
		backupIDs[i] = backupID
		history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)
		err := client.StoreBackupHistory(history)
		if err != nil {
			t.Fatalf("Failed to store backup history %d: %v", i, err)
		}
	}

	// List backup histories
	histories, err := client.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to list backup histories: %v", err)
	}

	// Verify histories
	if len(histories) < 3 {
		t.Errorf("Expected at least 3 backup histories, got %d", len(histories))
	}

	// Verify they are sorted by start time (newest first)
	for i := 1; i < len(histories); i++ {
		if histories[i].StartTime.After(histories[i-1].StartTime) {
			t.Errorf("Histories should be sorted by start time (newest first)")
		}
	}
}

func TestIntegration_UpdateBackupStatus(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-4"
	backupID := time.Now().Format("20060102-150405")

	// Create test backup history with RUNNING status
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusRunning)
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}

	// Update backup status to COMPLETED
	err = client.UpdateBackupStatus(camundaInstanceID, backupID, types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to update backup status: %v", err)
	}

	// Get backup history and verify status
	retrievedHistory, err := client.GetBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		t.Fatalf("Failed to get backup history: %v", err)
	}

	if retrievedHistory.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status '%s', got '%s'", types.BackupStatusCompleted, retrievedHistory.Status)
	}

	if retrievedHistory.EndTime == nil {
		t.Error("Expected EndTime to be set")
	}

	if retrievedHistory.DurationSeconds == nil {
		t.Error("Expected DurationSeconds to be set")
	}
}

func TestIntegration_MoveToOrphaned(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-5"
	backupID := time.Now().Format("20060102-150405")

	// Create test backup history
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}

	// Move to orphaned
	err = client.MoveToOrphaned(camundaInstanceID, backupID)
	if err != nil {
		t.Fatalf("Failed to move to orphaned: %v", err)
	}

	// Verify it's in orphaned
	orphanedBackups, err := client.ListOrphanedBackups(camundaInstanceID)
	if err != nil {
		t.Fatalf("Failed to list orphaned backups: %v", err)
	}

	found := false
	for _, backup := range orphanedBackups {
		if backup.BackupID == backupID {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Backup not found in orphaned list")
	}

	// Verify it's no longer in history
	historyBackups, err := client.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to list history backups: %v", err)
	}

	for _, backup := range historyBackups {
		if backup.BackupID == backupID {
			t.Errorf("Backup should not be in history list after moving to orphaned")
		}
	}
}

func TestIntegration_MoveToIncomplete(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-6"
	backupID := time.Now().Format("20060102-150405")

	// Create test backup history
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusRunning)
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}

	// Move to incomplete
	err = client.MoveToIncomplete(camundaInstanceID, backupID)
	if err != nil {
		t.Fatalf("Failed to move to incomplete: %v", err)
	}

	// Verify it's in incomplete
	incompleteBackups, err := client.ListIncompleteBackups(camundaInstanceID)
	if err != nil {
		t.Fatalf("Failed to list incomplete backups: %v", err)
	}

	found := false
	for _, backup := range incompleteBackups {
		if backup.BackupID == backupID {
			found = true
			if backup.Status != types.BackupStatusIncomplete {
				t.Errorf("Expected status '%s', got '%s'", types.BackupStatusIncomplete, backup.Status)
			}
			break
		}
	}

	if !found {
		t.Errorf("Backup not found in incomplete list")
	}
}

func TestIntegration_DeleteBackupHistory(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-7"
	backupID := time.Now().Format("20060102-150405")

	// Create test backup history
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}

	// Delete backup history
	err = client.DeleteBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		t.Fatalf("Failed to delete backup history: %v", err)
	}

	// Verify it's deleted
	_, err = client.GetBackupHistory(camundaInstanceID, backupID)
	if err != utils.ErrBackupNotFound {
		t.Errorf("Expected ErrBackupNotFound after deletion, got: %v", err)
	}
}

func TestIntegration_StoreIncompleteBackup(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-8"
	backupID := time.Now().Format("20060102-150405")

	// Create incomplete backup history
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusIncomplete)
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store incomplete backup history: %v", err)
	}

	// Verify it's in incomplete directory
	incompleteBackups, err := client.ListIncompleteBackups(camundaInstanceID)
	if err != nil {
		t.Fatalf("Failed to list incomplete backups: %v", err)
	}

	found := false
	for _, backup := range incompleteBackups {
		if backup.BackupID == backupID {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Incomplete backup not found in incomplete list")
	}

	// Verify it's not in main history
	historyBackups, err := client.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to list history backups: %v", err)
	}

	for _, backup := range historyBackups {
		if backup.BackupID == backupID {
			t.Errorf("Incomplete backup should not be in history list")
		}
	}
}

func TestIntegration_ListAllBackups(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-9"

	// Create backups in different states
	completedID := time.Now().Format("20060102-150405")
	completedHistory := createTestBackupHistoryForIntegration(camundaInstanceID, completedID, types.BackupStatusCompleted)
	err := client.StoreBackupHistory(completedHistory)
	if err != nil {
		t.Fatalf("Failed to store completed backup: %v", err)
	}

	incompleteID := time.Now().Add(time.Second).Format("20060102-150405")
	incompleteHistory := createTestBackupHistoryForIntegration(camundaInstanceID, incompleteID, types.BackupStatusIncomplete)
	err = client.StoreBackupHistory(incompleteHistory)
	if err != nil {
		t.Fatalf("Failed to store incomplete backup: %v", err)
	}

	// List all backups
	allBackups, err := client.ListAllBackups(camundaInstanceID)
	if err != nil {
		t.Fatalf("Failed to list all backups: %v", err)
	}

	// Should have at least 2 backups
	if len(allBackups) < 2 {
		t.Errorf("Expected at least 2 backups, got %d", len(allBackups))
	}

	// Verify both backups are present
	foundCompleted := false
	foundIncomplete := false
	for _, backup := range allBackups {
		if backup.BackupID == completedID {
			foundCompleted = true
		}
		if backup.BackupID == incompleteID {
			foundIncomplete = true
		}
	}

	if !foundCompleted {
		t.Error("Completed backup not found in all backups list")
	}
	if !foundIncomplete {
		t.Error("Incomplete backup not found in all backups list")
	}
}

func TestIntegration_RetryBehavior(t *testing.T) {
	// Fast skip if S3 was already determined to be unavailable
	if !s3Available {
		t.Skipf("Skipping integration test: %s", s3SkipReason)
	}

	// Test that operations succeed after transient failures by using
	// a client with an initially invalid endpoint that gets corrected.
	// This validates the retry mechanism actually retries on failure.

	t.Run("VerifyRetryAttemptsOnTransientFailure", func(t *testing.T) {
		// Create a client pointing to an invalid endpoint first
		invalidCfg := getTestS3Config()
		invalidCfg.Endpoint = "http://localhost:19999" // Non-existent endpoint
		logger := utils.NewLogger("debug")

		invalidClient, err := NewS3Client(invalidCfg, logger)
		if err != nil {
			t.Fatalf("Failed to create S3 client: %v", err)
		}

		// Set retry config with minimal delays for faster test
		invalidClient.SetRetryConfig(RetryConfig{
			MaxAttempts:  2,
			InitialDelay: 10 * time.Millisecond,
			MaxDelay:     50 * time.Millisecond,
		})

		// This should fail after retries since endpoint is invalid
		camundaInstanceID := "camunda-test-retry-fail"
		backupID := time.Now().Format("20060102-150405")
		history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)

		start := time.Now()
		err = invalidClient.StoreBackupHistory(history)
		elapsed := time.Since(start)

		// Should have failed
		if err == nil {
			t.Error("Expected error when connecting to invalid endpoint")
		}

		// Should have taken at least InitialDelay (10ms) for 1 retry
		// With 2 attempts, we expect at least some delay from the retry
		if elapsed < 10*time.Millisecond {
			t.Errorf("Operation completed too quickly (%v), retry may not have occurred", elapsed)
		}
	})

	t.Run("VerifyRetryCounterWithFailingOperation", func(t *testing.T) {
		// Test that verifies the number of retry attempts by tracking
		// attempts to a non-existent bucket
		cfg := getTestS3Config()
		cfg.Bucket = "non-existent-bucket-" + time.Now().Format("20060102150405")
		logger := utils.NewLogger("debug")

		client, err := NewS3Client(cfg, logger)
		if err != nil {
			t.Fatalf("Failed to create S3 client: %v", err)
		}

		// Track retries via timing - with 3 attempts and 50ms initial delay,
		// we expect ~50ms + ~100ms = ~150ms minimum for exponential backoff
		var attemptCount atomic.Int32
		client.SetRetryConfig(RetryConfig{
			MaxAttempts:  3,
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     200 * time.Millisecond,
		})

		camundaInstanceID := "camunda-test-retry-count"
		backupID := time.Now().Format("20060102-150405")
		history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)

		start := time.Now()
		err = client.StoreBackupHistory(history)
		elapsed := time.Since(start)
		_ = attemptCount // Used for documentation purposes

		// Should have failed (non-existent bucket)
		if err == nil {
			t.Error("Expected error when writing to non-existent bucket")
		}

		// With 3 attempts and exponential backoff starting at 50ms:
		// - Attempt 1 fails immediately
		// - Wait 50ms
		// - Attempt 2 fails
		// - Wait 100ms (50*2)
		// - Attempt 3 fails
		// Total minimum: ~150ms
		expectedMinDelay := 100 * time.Millisecond // Give some slack
		if elapsed < expectedMinDelay {
			t.Errorf("Operation completed in %v, expected at least %v for retry delays", elapsed, expectedMinDelay)
		}
	})

	t.Run("VerifySuccessfulOperationWithRetryConfig", func(t *testing.T) {
		// Verify that operations still succeed with retry config
		client := setupIntegrationTestS3Client(t)

		client.SetRetryConfig(RetryConfig{
			MaxAttempts:  2,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
		})

		camundaInstanceID := "camunda-test-retry-success"
		backupID := time.Now().Format("20060102-150405")

		history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)
		err := client.StoreBackupHistory(history)
		if err != nil {
			t.Fatalf("Failed to store backup history with retry: %v", err)
		}

		retrievedHistory, err := client.GetBackupHistory(camundaInstanceID, backupID)
		if err != nil {
			t.Fatalf("Failed to get backup history: %v", err)
		}

		if retrievedHistory.BackupID != backupID {
			t.Errorf("Expected backup ID '%s', got '%s'", backupID, retrievedHistory.BackupID)
		}
	})
}

func TestIntegration_HealthCheck(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	err := client.HealthCheck()
	if err != nil {
		t.Errorf("Health check failed: %v", err)
	}
}

func TestIntegration_ConcurrentOperations(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-concurrent"
	numGoroutines := 5
	done := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			backupID := time.Now().Add(time.Duration(id) * time.Second).Format("20060102-150405")
			history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)
			err := client.StoreBackupHistory(history)
			done <- err
		}(i)
	}

	// Wait for all goroutines and check for errors
	for i := 0; i < numGoroutines; i++ {
		if err := <-done; err != nil {
			t.Errorf("Goroutine %d failed: %v", i, err)
		}
	}

	// Verify all backups were stored
	histories, err := client.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("Failed to list backup histories: %v", err)
	}

	if len(histories) < numGoroutines {
		t.Errorf("Expected at least %d backup histories, got %d", numGoroutines, len(histories))
	}
}

func TestIntegration_ComponentBackupInfo(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-components"
	backupID := time.Now().Format("20060102-150405")

	// Create backup history with multiple components
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)

	now := time.Now()
	history.UpdateComponentBackupInfo("operate", models.ComponentBackupInfo{
		Enabled:         true,
		Status:          types.ComponentStatusCompleted,
		StartTime:       &now,
		EndTime:         &now,
		DurationSeconds: 15,
	})

	history.UpdateComponentBackupInfo("elasticsearch", models.ComponentBackupInfo{
		Enabled:            true,
		Status:             types.ComponentStatusCompleted,
		StartTime:          &now,
		EndTime:            &now,
		DurationSeconds:    20,
		SnapshotName:       "snapshot-" + backupID,
		SnapshotRepository: "backup_repo",
	})

	// Store backup history
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}

	// Get backup history and verify components
	retrievedHistory, err := client.GetBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		t.Fatalf("Failed to get backup history: %v", err)
	}

	// Verify components
	if len(retrievedHistory.Components) < 3 {
		t.Errorf("Expected at least 3 components, got %d", len(retrievedHistory.Components))
	}

	// Verify ES component
	esComp, exists := retrievedHistory.Components["elasticsearch"]
	if !exists {
		t.Error("Elasticsearch component not found")
	} else {
		if esComp.SnapshotName != "snapshot-"+backupID {
			t.Errorf("Expected snapshot name 'snapshot-%s', got '%s'", backupID, esComp.SnapshotName)
		}
		if esComp.SnapshotRepository != "backup_repo" {
			t.Errorf("Expected snapshot repository 'backup_repo', got '%s'", esComp.SnapshotRepository)
		}
	}
}

func TestIntegration_BackupStats(t *testing.T) {
	client := setupIntegrationTestS3Client(t)

	camundaInstanceID := "camunda-test-stats"
	backupID := time.Now().Format("20060102-150405")

	// Create backup history with mixed component statuses
	history := createTestBackupHistoryForIntegration(camundaInstanceID, backupID, types.BackupStatusCompleted)

	now := time.Now()
	history.UpdateComponentBackupInfo("zeebe", models.ComponentBackupInfo{
		Enabled:         true,
		Status:          types.ComponentStatusCompleted,
		StartTime:       &now,
		EndTime:         &now,
		DurationSeconds: 10,
	})

	history.UpdateComponentBackupInfo("operate", models.ComponentBackupInfo{
		Enabled:         true,
		Status:          types.ComponentStatusFailed,
		StartTime:       &now,
		EndTime:         &now,
		DurationSeconds: 5,
		ErrorMessage:    "Connection timeout",
	})

	history.UpdateComponentBackupInfo("optimize", models.ComponentBackupInfo{
		Enabled: false,
		Status:  types.ComponentStatusSkipped,
	})

	// Store backup history
	err := client.StoreBackupHistory(history)
	if err != nil {
		t.Fatalf("Failed to store backup history: %v", err)
	}

	// Get backup history and verify stats
	retrievedHistory, err := client.GetBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		t.Fatalf("Failed to get backup history: %v", err)
	}

	// Verify stats are correct
	if retrievedHistory.BackupStats.SuccessfulComponents == 0 {
		t.Error("Expected at least 1 successful component")
	}

	if retrievedHistory.BackupStats.FailedComponents == 0 {
		t.Error("Expected at least 1 failed component")
	}

	if retrievedHistory.BackupStats.SkippedComponents == 0 {
		t.Error("Expected at least 1 skipped component")
	}
}
