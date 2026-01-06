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