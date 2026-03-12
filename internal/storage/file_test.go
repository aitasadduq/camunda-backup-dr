package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func setupTestFileStorage(t *testing.T) (*FileStorageImpl, string, func()) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "backup-controller-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	
	// Create config
	cfg := &config.Config{
		DataDir: tempDir,
	}
	
	logger := utils.NewLogger("debug")
	
	// Create file storage
	fs, err := NewFileStorage(tempDir, cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}
	
	// Cleanup function
	cleanup := func() {
		os.RemoveAll(tempDir)
	}
	
	return fs, tempDir, cleanup
}

func TestFileStorage_SaveAndLoadConfiguration(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Create test configuration
	config := &models.Configuration{
		Version: "1.0.0",
		CamundaInstances: []models.CamundaInstance{
			*models.NewCamundaInstance("camunda1", "Test Camunda 1", "https://test1.example.com"),
		},
	}
	
	// Save configuration
	err := fs.SaveConfiguration(config)
	if err != nil {
		t.Fatalf("Failed to save configuration: %v", err)
	}
	
	// Load configuration
	loadedConfig, err := fs.LoadConfiguration()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}
	
	// Verify configuration
	if loadedConfig.Version != config.Version {
		t.Errorf("Expected version %s, got %s", config.Version, loadedConfig.Version)
	}
	
	if len(loadedConfig.CamundaInstances) != len(config.CamundaInstances) {
		t.Errorf("Expected %d instances, got %d", len(config.CamundaInstances), len(loadedConfig.CamundaInstances))
	}
	
	if loadedConfig.CamundaInstances[0].ID != config.CamundaInstances[0].ID {
		t.Errorf("Expected instance ID %s, got %s", config.CamundaInstances[0].ID, loadedConfig.CamundaInstances[0].ID)
	}
}

func TestFileStorage_LoadConfiguration_Empty(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Remove config file if it exists
	configPath := filepath.Join(tempDir, "config.json")
	os.Remove(configPath)
	
	// Load configuration
	config, err := fs.LoadConfiguration()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}
	
	// Verify empty configuration
	if config.Version != "1.0.0" {
		t.Errorf("Expected version 1.0.0, got %s", config.Version)
	}
	
	if len(config.CamundaInstances) != 0 {
		t.Errorf("Expected 0 instances, got %d", len(config.CamundaInstances))
	}
}

func TestFileStorage_CreateLogFile(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Create log file
	err := fs.CreateLogFile("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	
	// Verify log file exists
	logDir := filepath.Join(tempDir, "logs", "camunda1")
	entries, err := os.ReadDir(logDir)
	if err != nil {
		t.Fatalf("Failed to read log directory: %v", err)
	}
	
	if len(entries) != 1 {
		t.Errorf("Expected 1 log file, got %d", len(entries))
	}
	
	if !entries[0].IsDir() {
		t.Logf("Log file created: %s", entries[0].Name())
	}
}

func TestFileStorage_WriteToLogFile(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Create log file
	err := fs.CreateLogFile("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	
	// Write to log file
	message := "Test log message"
	err = fs.WriteToLogFile("camunda1", "20240101-120000", message)
	if err != nil {
		t.Fatalf("Failed to write to log file: %v", err)
	}
	
	// Read log file
	content, err := fs.ReadLogFile("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	// Verify message
	if !containsString(content, message) {
		t.Errorf("Expected log to contain message '%s', got '%s'", message, content)
	}
}

func TestFileStorage_ListLogFiles(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Create multiple log files
	for i := 0; i < 3; i++ {
		backupID := time.Now().Add(time.Duration(i) * time.Hour).Format("20060102-150405")
		err := fs.CreateLogFile("camunda1", backupID)
		if err != nil {
			t.Fatalf("Failed to create log file %d: %v", i, err)
		}
	}
	
	// List log files
	logFiles, err := fs.ListLogFiles("camunda1")
	if err != nil {
		t.Fatalf("Failed to list log files: %v", err)
	}
	
	// Verify log files
	if len(logFiles) != 3 {
		t.Errorf("Expected 3 log files, got %d", len(logFiles))
	}
}

func TestFileStorage_CleanupOldLogFiles(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Create multiple log files
	for i := 0; i < 5; i++ {
		backupID := time.Now().Add(time.Duration(i) * time.Hour).Format("20060102-150405")
		err := fs.CreateLogFile("camunda1", backupID)
		if err != nil {
			t.Fatalf("Failed to create log file %d: %v", i, err)
		}
	}
	
	// Cleanup old log files (keep 2)
	err := fs.CleanupOldLogFiles("camunda1", 2)
	if err != nil {
		t.Fatalf("Failed to cleanup old log files: %v", err)
	}
	
	// Verify remaining log files
	logFiles, err := fs.ListLogFiles("camunda1")
	if err != nil {
		t.Fatalf("Failed to list log files: %v", err)
	}
	
	if len(logFiles) != 2 {
		t.Errorf("Expected 2 log files after cleanup, got %d", len(logFiles))
	}
}

func TestFileStorage_ConcurrentAccess(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Create log file
	err := fs.CreateLogFile("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	
	// Test concurrent writes
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			message := fmt.Sprintf("Concurrent message %d", id)
			err := fs.WriteToLogFile("camunda1", "20240101-120000", message)
			if err != nil {
				t.Errorf("Failed to write message %d: %v", id, err)
			}
			done <- true
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Read log file
	content, err := fs.ReadLogFile("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	// Verify all messages were written
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("Concurrent message %d", i)
		if !containsString(content, message) {
			t.Errorf("Expected log to contain message '%s'", message)
		}
	}
}

func TestFileStorage_DeleteLogFile(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()
	
	// Create log file
	err := fs.CreateLogFile("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}
	
	// Delete log file
	err = fs.DeleteLogFile("camunda1", "20240101-120000")
	if err != nil {
		t.Fatalf("Failed to delete log file: %v", err)
	}
	
	// Verify log file is deleted
	_, err = fs.ReadLogFile("camunda1", "20240101-120000")
	if err == nil {
		t.Error("Expected error when reading deleted log file")
	}
}

// Helper function

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && s[len(s)-len(substr):] == substr || 
	       len(s) >= len(substr) && s[:len(substr)] == substr ||
	       len(s) >= len(substr) && containsSubstring(s, substr)
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============================================================
// Error path tests for file.go coverage improvement
// ============================================================

// --- NewFileStorage error paths ---

func TestNewFileStorage_DataDirCreationFails(t *testing.T) {
	// Use a path nested under a file (not a directory) to make MkdirAll fail
	tempDir, err := os.MkdirTemp("", "fs-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a regular file that will block MkdirAll
	blockingFile := filepath.Join(tempDir, "blocker")
	if err := os.WriteFile(blockingFile, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{DataDir: tempDir}
	logger := utils.NewLogger("debug")

	// Attempt to create storage under a file path → MkdirAll should fail
	_, err = NewFileStorage(filepath.Join(blockingFile, "subdir"), cfg, logger)
	if err == nil {
		t.Fatal("Expected error when data directory creation fails")
	}
	if !containsSubstring(err.Error(), "failed to create data directory") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestNewFileStorage_LogsDirCreationFails(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "fs-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Pre-create a file named "logs" so that MkdirAll for the logs subdir fails
	logsBlocker := filepath.Join(tempDir, "logs")
	if err := os.WriteFile(logsBlocker, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{DataDir: tempDir}
	logger := utils.NewLogger("debug")

	_, err = NewFileStorage(tempDir, cfg, logger)
	if err == nil {
		t.Fatal("Expected error when logs directory creation fails")
	}
	if !containsSubstring(err.Error(), "failed to create logs directory") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- SaveConfiguration error paths ---

func TestFileStorage_SaveConfiguration_WriteError(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Make the data directory read-only so the temp file write fails
	if err := os.Chmod(tempDir, 0555); err != nil {
		t.Fatal(err)
	}
	// Restore permissions for cleanup
	defer os.Chmod(tempDir, 0755)

	cfg := &models.Configuration{
		Version:          "1.0.0",
		CamundaInstances: []models.CamundaInstance{},
	}

	err := fs.SaveConfiguration(cfg)
	if err == nil {
		t.Fatal("Expected error when writing to read-only directory")
	}
	if !containsSubstring(err.Error(), "failed to write configuration") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- LoadConfiguration error paths ---

func TestFileStorage_LoadConfiguration_CorruptedJSON(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Write invalid JSON to config file
	configPath := filepath.Join(tempDir, "config.json")
	if err := os.WriteFile(configPath, []byte("{invalid json!!!"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := fs.LoadConfiguration()
	if err == nil {
		t.Fatal("Expected error when loading corrupted JSON")
	}
	if !containsSubstring(err.Error(), "failed to unmarshal configuration") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

func TestFileStorage_LoadConfiguration_UnreadableFile(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create config file with no read permissions
	configPath := filepath.Join(tempDir, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"version":"1.0.0"}`), 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(configPath, 0644) // restore for cleanup

	_, err := fs.LoadConfiguration()
	if err == nil {
		t.Fatal("Expected error when config file is unreadable")
	}
	if !containsSubstring(err.Error(), "failed to read configuration file") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- CreateLogFile error paths ---

func TestFileStorage_CreateLogFile_MkdirAllFails(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Place a regular file where the instance logs dir would be created
	blocker := filepath.Join(tempDir, "logs", "camunda-blocked")
	if err := os.WriteFile(blocker, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}

	// Attempt to create a log file under the blocked path
	err := fs.CreateLogFile("camunda-blocked/sub", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error when instance logs directory creation fails")
	}
}

func TestFileStorage_CreateLogFile_ReadOnlyDir(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create the instance logs dir, then make it read-only
	instanceDir := filepath.Join(tempDir, "logs", "camunda-ro")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(instanceDir, 0555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(instanceDir, 0755)

	err := fs.CreateLogFile("camunda-ro", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error when log file creation fails in read-only directory")
	}
	if !containsSubstring(err.Error(), "failed to create log file") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- WriteToLogFile error paths ---

func TestFileStorage_WriteToLogFile_NoLogDir(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Write without creating a log file first — no instance dir exists
	err := fs.WriteToLogFile("nonexistent-instance", "20240101-120000", "msg")
	if err == nil {
		t.Fatal("Expected error when writing to log for non-existent instance")
	}
}

func TestFileStorage_WriteToLogFile_EmptyLogDir(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create instance dir but no log files
	instanceDir := filepath.Join(tempDir, "logs", "camunda-empty")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}

	err := fs.WriteToLogFile("camunda-empty", "20240101-120000", "msg")
	if err == nil {
		t.Fatal("Expected error when no log files exist")
	}
}

// --- ReadLogFile error paths ---

func TestFileStorage_ReadLogFile_NoDir(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	_, err := fs.ReadLogFile("nonexistent-instance", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error when reading log for non-existent instance")
	}
}

func TestFileStorage_ReadLogFile_BackupNotFound(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create a log file with one backup ID
	if err := fs.CreateLogFile("camunda1", "20240101-120000"); err != nil {
		t.Fatal(err)
	}

	// Try to read with a different backup ID
	_, err := fs.ReadLogFile("camunda1", "99999999-000000")
	if err == nil {
		t.Fatal("Expected error when backup ID not found in log files")
	}
}

// --- DeleteLogFile error paths ---

func TestFileStorage_DeleteLogFile_NoDir(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	err := fs.DeleteLogFile("nonexistent-instance", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error when deleting log for non-existent instance")
	}
}

func TestFileStorage_DeleteLogFile_BackupNotFound(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create a log file with one backup ID
	if err := fs.CreateLogFile("camunda1", "20240101-120000"); err != nil {
		t.Fatal(err)
	}

	// Try to delete with a different backup ID
	err := fs.DeleteLogFile("camunda1", "99999999-000000")
	if err == nil {
		t.Fatal("Expected error when backup ID not found")
	}
}

// --- listLogFilesHelper error paths ---

func TestFileStorage_ListLogFiles_NoDir(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// List for a non-existent instance — should return empty, not error
	files, err := fs.ListLogFiles("nonexistent-instance")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("Expected 0 files, got %d", len(files))
	}
}

func TestFileStorage_ListLogFiles_FiltersDirsAndNonLogFiles(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	instanceDir := filepath.Join(tempDir, "logs", "camunda-filter")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create a .log file, a .txt file, and a subdirectory
	os.WriteFile(filepath.Join(instanceDir, "backup.log"), []byte("log"), 0644)
	os.WriteFile(filepath.Join(instanceDir, "notes.txt"), []byte("txt"), 0644)
	os.MkdirAll(filepath.Join(instanceDir, "subdir"), 0755)

	files, err := fs.ListLogFiles("camunda-filter")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(files) != 1 {
		t.Errorf("Expected 1 .log file, got %d", len(files))
	}
	if len(files) > 0 && files[0] != "backup.log" {
		t.Errorf("Expected 'backup.log', got '%s'", files[0])
	}
}

// --- CleanupOldLogFiles error paths ---

func TestFileStorage_CleanupOldLogFiles_FewerThanKeepCount(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create 2 log files, ask to keep 5
	if err := fs.CreateLogFile("camunda1", "20240101-100000"); err != nil {
		t.Fatal(err)
	}
	if err := fs.CreateLogFile("camunda1", "20240101-110000"); err != nil {
		t.Fatal(err)
	}

	err := fs.CleanupOldLogFiles("camunda1", 5)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	files, _ := fs.ListLogFiles("camunda1")
	if len(files) != 2 {
		t.Errorf("Expected 2 files to remain, got %d", len(files))
	}
}

func TestFileStorage_CleanupOldLogFiles_NonExistentInstance(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Cleanup for a non-existent instance — listLogFilesHelper returns empty, no error
	err := fs.CleanupOldLogFiles("nonexistent-instance", 2)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestFileStorage_CleanupOldLogFiles_KeepZero(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create 3 files, keep 0 → all should be deleted
	for i := 0; i < 3; i++ {
		backupID := fmt.Sprintf("20240101-%02d0000", i+10)
		if err := fs.CreateLogFile("camunda1", backupID); err != nil {
			t.Fatal(err)
		}
	}

	err := fs.CleanupOldLogFiles("camunda1", 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	files, _ := fs.ListLogFiles("camunda1")
	if len(files) != 0 {
		t.Errorf("Expected 0 files after cleanup with keep=0, got %d", len(files))
	}
}

// --- findLatestLogFile error paths ---

func TestFileStorage_findLatestLogFile_NoDirReturnsError(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Directly call the unexported method via WriteToLogFile which uses it
	err := fs.WriteToLogFile("no-such-instance", "backup1", "msg")
	if err == nil {
		t.Fatal("Expected error from findLatestLogFile for missing dir")
	}
}

func TestFileStorage_findLatestLogFile_EmptyDirReturnsError(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create empty instance dir
	instanceDir := filepath.Join(tempDir, "logs", "camunda-empty2")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}

	// findLatestLogFile is called via WriteToLogFile
	err := fs.WriteToLogFile("camunda-empty2", "backup1", "msg")
	if err == nil {
		t.Fatal("Expected error from findLatestLogFile for empty dir")
	}
}

func TestFileStorage_findLatestLogFile_OnlyNonLogFiles(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create dir with only non-log files
	instanceDir := filepath.Join(tempDir, "logs", "camunda-nolog")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(instanceDir, "readme.txt"), []byte("hi"), 0644)
	os.MkdirAll(filepath.Join(instanceDir, "subdir"), 0755)

	err := fs.WriteToLogFile("camunda-nolog", "backup1", "msg")
	if err == nil {
		t.Fatal("Expected error from findLatestLogFile when only non-log files exist")
	}
}

func TestFileStorage_findLatestLogFile_SelectsMostRecent(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create two log files with different mod times
	instanceDir := filepath.Join(tempDir, "logs", "camunda-latest")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Write older file
	olderPath := filepath.Join(instanceDir, "camunda-latest-old.log")
	os.WriteFile(olderPath, []byte("old"), 0644)
	// Set its mod time to the past
	oldTime := time.Now().Add(-1 * time.Hour)
	os.Chtimes(olderPath, oldTime, oldTime)

	// Write newer file
	newerPath := filepath.Join(instanceDir, "camunda-latest-new.log")
	os.WriteFile(newerPath, []byte("new"), 0644)

	// WriteToLogFile should pick the newer file
	err := fs.WriteToLogFile("camunda-latest", "whatever", "appended message")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Read the newer file to confirm the message was appended there
	data, err := os.ReadFile(newerPath)
	if err != nil {
		t.Fatal(err)
	}
	if !containsSubstring(string(data), "appended message") {
		t.Error("Expected message to be appended to the newer log file")
	}
}

// --- findLogFileByBackupID error paths ---

func TestFileStorage_findLogFileByBackupID_NoDirReturnsError(t *testing.T) {
	fs, _, cleanup := setupTestFileStorage(t)
	defer cleanup()

	_, err := fs.ReadLogFile("no-such-instance", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error from findLogFileByBackupID for missing dir")
	}
}

func TestFileStorage_findLogFileByBackupID_NotFound(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create instance dir with a log file that does NOT match the backup ID
	instanceDir := filepath.Join(tempDir, "logs", "camunda-miss")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(instanceDir, "camunda-miss-20240101-120000.log"), []byte("log"), 0644)

	_, err := fs.ReadLogFile("camunda-miss", "99999999-000000")
	if err == nil {
		t.Fatal("Expected error when backup ID not found in any log file name")
	}
}

// --- WriteToLogFile: OpenFile error when log file is read-only ---

func TestFileStorage_WriteToLogFile_ReadOnlyLogFile(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create a log file normally
	if err := fs.CreateLogFile("camunda-ro-write", "20240101-120000"); err != nil {
		t.Fatal(err)
	}

	// Make the log file read-only so OpenFile with O_WRONLY fails
	instanceDir := filepath.Join(tempDir, "logs", "camunda-ro-write")
	entries, _ := os.ReadDir(instanceDir)
	for _, e := range entries {
		path := filepath.Join(instanceDir, e.Name())
		os.Chmod(path, 0444)
		defer os.Chmod(path, 0644)
	}

	err := fs.WriteToLogFile("camunda-ro-write", "20240101-120000", "should fail")
	if err == nil {
		t.Fatal("Expected error when log file is read-only")
	}
	if !containsSubstring(err.Error(), "failed to open log file") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- ReadLogFile: unreadable log file ---

func TestFileStorage_ReadLogFile_UnreadableFile(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create a log file
	if err := fs.CreateLogFile("camunda-unread", "20240101-120000"); err != nil {
		t.Fatal(err)
	}

	// Make the log file unreadable
	instanceDir := filepath.Join(tempDir, "logs", "camunda-unread")
	entries, _ := os.ReadDir(instanceDir)
	for _, e := range entries {
		path := filepath.Join(instanceDir, e.Name())
		os.Chmod(path, 0000)
		defer os.Chmod(path, 0644)
	}

	_, err := fs.ReadLogFile("camunda-unread", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error when log file is unreadable")
	}
	if !containsSubstring(err.Error(), "failed to read log file") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- DeleteLogFile: Remove fails (file already deleted) ---

func TestFileStorage_DeleteLogFile_AlreadyRemoved(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create a log file
	if err := fs.CreateLogFile("camunda-del-fail", "20240101-120000"); err != nil {
		t.Fatal(err)
	}

	// Manually remove the file so that findLogFileByBackupID finds the dir entry
	// but os.Remove fails. We need a different approach: make dir read-only so Remove fails.
	instanceDir := filepath.Join(tempDir, "logs", "camunda-del-fail")
	os.Chmod(instanceDir, 0555)
	defer os.Chmod(instanceDir, 0755)

	err := fs.DeleteLogFile("camunda-del-fail", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error when file cannot be removed")
	}
	if !containsSubstring(err.Error(), "failed to delete log file") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- CleanupOldLogFiles: Remove error during cleanup loop ---

func TestFileStorage_CleanupOldLogFiles_RemoveError(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create 4 log files
	for i := 0; i < 4; i++ {
		backupID := fmt.Sprintf("20240101-%02d0000", i+10)
		if err := fs.CreateLogFile("camunda-cleanup-err", backupID); err != nil {
			t.Fatal(err)
		}
	}

	// Make the instance dir read-only so Remove calls fail
	instanceDir := filepath.Join(tempDir, "logs", "camunda-cleanup-err")
	os.Chmod(instanceDir, 0555)
	defer os.Chmod(instanceDir, 0755)

	// Cleanup should not return error (it logs and continues), but files remain
	err := fs.CleanupOldLogFiles("camunda-cleanup-err", 1)
	if err != nil {
		t.Fatalf("CleanupOldLogFiles should not return error on Remove failure: %v", err)
	}
}

// --- findLatestLogFile: exercises the ReadDir error path ---

func TestFileStorage_findLatestLogFile_ReadDirError(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create the instance dir, add a log, then make it unreadable
	instanceDir := filepath.Join(tempDir, "logs", "camunda-readdir-fail")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(instanceDir, "test.log"), []byte("data"), 0644)

	// Make dir non-readable (but it still exists, so Stat won't fail with NotExist)
	os.Chmod(instanceDir, 0000)
	defer os.Chmod(instanceDir, 0755)

	err := fs.WriteToLogFile("camunda-readdir-fail", "backup1", "msg")
	if err == nil {
		t.Fatal("Expected error when ReadDir fails")
	}
}

// --- findLogFileByBackupID: exercises the ReadDir error path ---

func TestFileStorage_findLogFileByBackupID_ReadDirError(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	// Create the instance dir with a log file
	instanceDir := filepath.Join(tempDir, "logs", "camunda-readdir-fail2")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(instanceDir, "test-20240101-120000.log"), []byte("data"), 0644)

	// Make dir non-readable
	os.Chmod(instanceDir, 0000)
	defer os.Chmod(instanceDir, 0755)

	_, err := fs.ReadLogFile("camunda-readdir-fail2", "20240101-120000")
	if err == nil {
		t.Fatal("Expected error when ReadDir fails in findLogFileByBackupID")
	}
}

// --- listLogFilesHelper: exercises the ReadDir error path ---

func TestFileStorage_listLogFilesHelper_ReadDirError(t *testing.T) {
	fs, tempDir, cleanup := setupTestFileStorage(t)
	defer cleanup()

	instanceDir := filepath.Join(tempDir, "logs", "camunda-list-readdir")
	if err := os.MkdirAll(instanceDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Make the directory unreadable
	os.Chmod(instanceDir, 0000)
	defer os.Chmod(instanceDir, 0755)

	_, err := fs.ListLogFiles("camunda-list-readdir")
	if err == nil {
		t.Fatal("Expected error when ReadDir fails in listLogFilesHelper")
	}
	if !containsSubstring(err.Error(), "failed to read logs directory") {
		t.Errorf("Unexpected error message: %s", err.Error())
	}
}

// --- SaveConfiguration: rename error path ---

func TestFileStorage_SaveConfiguration_RenameError(t *testing.T) {
	// Construct a FileStorageImpl manually where configPath points to a path
	// whose parent dir will be removed between write and rename.
	// We trick it by setting configPath to a directory path.
	tempDir, err := os.MkdirTemp("", "fs-rename-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	// Create a directory at the config path location — rename of file onto directory should fail
	configAsDir := filepath.Join(tempDir, "config.json")
	if err := os.MkdirAll(configAsDir, 0755); err != nil {
		t.Fatal(err)
	}

	logger := utils.NewLogger("debug")
	fs := &FileStorageImpl{
		configPath: configAsDir,
		logsDir:    filepath.Join(tempDir, "logs"),
		config:     &config.Config{DataDir: tempDir},
		logger:     logger,
	}

	cfg := &models.Configuration{
		Version:          "1.0.0",
		CamundaInstances: []models.CamundaInstance{},
	}

	err = fs.SaveConfiguration(cfg)
	if err == nil {
		t.Fatal("Expected error when rename target is a directory")
	}
}