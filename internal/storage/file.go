package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
)

// FileStorageImpl implements the FileStorage interface
type FileStorageImpl struct {
	configPath string
	logsDir    string
	config     *config.Config
	logger     *utils.Logger
	
	// Mutexes for concurrent access protection
	configMutex sync.RWMutex
	logMutex    sync.RWMutex
}

// NewFileStorage creates a new file storage instance
func NewFileStorage(dataDir string, cfg *config.Config, logger *utils.Logger) (*FileStorageImpl, error) {
	// Create directories if they don't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	logsDir := filepath.Join(dataDir, "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create logs directory: %w", err)
	}
	
	configPath := filepath.Join(dataDir, "config.json")
	
	return &FileStorageImpl{
		configPath: configPath,
		logsDir:    logsDir,
		config:     cfg,
		logger:     logger,
	}, nil
}

// SaveConfiguration saves the configuration to JSON file
func (fs *FileStorageImpl) SaveConfiguration(config *models.Configuration) error {
	fs.configMutex.Lock()
	defer fs.configMutex.Unlock()
	
	// Create temporary file for atomic write
	tempPath := fs.configPath + ".tmp"
	
	// Marshal configuration with pretty printing
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal configuration: %w", err)
	}
	
	// Write to temporary file
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write configuration: %w", err)
	}
	
	// Atomic rename
	if err := os.Rename(tempPath, fs.configPath); err != nil {
		return fmt.Errorf("failed to rename configuration file: %w", err)
	}
	
	fs.logger.Debug("Configuration saved to %s", fs.configPath)
	return nil
}

// LoadConfiguration loads the configuration from JSON file
func (fs *FileStorageImpl) LoadConfiguration() (*models.Configuration, error) {
	fs.configMutex.RLock()
	defer fs.configMutex.RUnlock()
	
	// Check if config file exists
	if _, err := os.Stat(fs.configPath); os.IsNotExist(err) {
		// Return empty configuration if file doesn't exist
		return &models.Configuration{
			Version:          "1.0.0",
			CamundaInstances: []models.CamundaInstance{},
		}, nil
	}
	
	// Read configuration file
	data, err := os.ReadFile(fs.configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read configuration file: %w", err)
	}
	
	// Unmarshal configuration
	var config models.Configuration
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal configuration: %w", err)
	}
	
	fs.logger.Debug("Configuration loaded from %s", fs.configPath)
	return &config, nil
}

// CreateLogFile creates a new log file for a backup
func (fs *FileStorageImpl) CreateLogFile(camundaInstanceID, backupID string) error {
	fs.logMutex.Lock()
	defer fs.logMutex.Unlock()
	
	// Create instance log directory if it doesn't exist
	instanceLogsDir := filepath.Join(fs.logsDir, camundaInstanceID)
	if err := os.MkdirAll(instanceLogsDir, 0755); err != nil {
		return fmt.Errorf("failed to create instance logs directory: %w", err)
	}
	
	// Generate log file name: {camunda_instance_id}-{YYYYMMDD}-{HHMMSS}.log
	logFileName := fmt.Sprintf("%s-%s.log", camundaInstanceID, backupID)
	logFilePath := filepath.Join(instanceLogsDir, logFileName)
	
	// Create log file
	file, err := os.Create(logFilePath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer file.Close()
	
	// Write header
	header := fmt.Sprintf("Backup Log for Camunda Instance: %s\nBackup ID: %s\nStarted: %s\n\n", 
		camundaInstanceID, backupID, time.Now().Format(time.RFC3339))
	if _, err := file.WriteString(header); err != nil {
		return fmt.Errorf("failed to write log header: %w", err)
	}
	
	fs.logger.Debug("Created log file: %s", logFilePath)
	return nil
}

// WriteToLogFile writes a message to the log file
func (fs *FileStorageImpl) WriteToLogFile(camundaInstanceID, backupID, message string) error {
	fs.logMutex.Lock()
	defer fs.logMutex.Unlock()
	
	// Find the most recent log file for this instance
	logFilePath, err := fs.findLatestLogFile(camundaInstanceID)
	if err != nil {
		return err
	}
	
	// Open log file in append mode
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()
	
	// Write log entry with timestamp
	logEntry := fmt.Sprintf("%s %s\n", time.Now().Format(time.RFC3339), message)
	if _, err := file.WriteString(logEntry); err != nil {
		return fmt.Errorf("failed to write to log file: %w", err)
	}
	
	return nil
}

// ReadLogFile reads the contents of a log file
func (fs *FileStorageImpl) ReadLogFile(camundaInstanceID, backupID string) (string, error) {
	fs.logMutex.Lock()
	defer fs.logMutex.Unlock()
	
	// Find log file matching the backup ID
	logFilePath, err := fs.findLogFileByBackupID(camundaInstanceID, backupID)
	if err != nil {
		return "", err
	}
	
	// Read log file
	data, err := os.ReadFile(logFilePath)
	if err != nil {
		return "", fmt.Errorf("failed to read log file: %w", err)
	}
	
	return string(data), nil
}

// DeleteLogFile deletes a log file
func (fs *FileStorageImpl) DeleteLogFile(camundaInstanceID, backupID string) error {
	fs.logMutex.Lock()
	defer fs.logMutex.Unlock()
	
	// Find log file matching the backup ID
	logFilePath, err := fs.findLogFileByBackupID(camundaInstanceID, backupID)
	if err != nil {
		return err
	}
	
	// Delete log file
	if err := os.Remove(logFilePath); err != nil {
		return fmt.Errorf("failed to delete log file: %w", err)
	}
	
	fs.logger.Debug("Deleted log file: %s", logFilePath)
	return nil
}

// ListLogFiles lists all log files for a Camunda instance
// listLogFilesHelper lists all log files without locking
func (fs *FileStorageImpl) listLogFilesHelper(camundaInstanceID string) ([]string, error) {
	
	instanceLogsDir := filepath.Join(fs.logsDir, camundaInstanceID)
	
	// Check if directory exists
	if _, err := os.Stat(instanceLogsDir); os.IsNotExist(err) {
		return []string{}, nil
	}
	
	// Read directory entries
	entries, err := os.ReadDir(instanceLogsDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read logs directory: %w", err)
	}
	
	// Filter and sort log files
	var logFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			logFiles = append(logFiles, entry.Name())
		}
	}
	
	// Sort by filename (which includes timestamp)
	sort.Strings(logFiles)
	
	return logFiles, nil
}

func (fs *FileStorageImpl) ListLogFiles(camundaInstanceID string) ([]string, error) {
	fs.logMutex.RLock()
	defer fs.logMutex.RUnlock()
	return fs.listLogFilesHelper(camundaInstanceID)
}

func (fs *FileStorageImpl) CleanupOldLogFiles(camundaInstanceID string, keepCount int) error {
	fs.logMutex.Lock()
	defer fs.logMutex.Unlock()
	
	// List all log files
	logFiles, err := fs.listLogFilesHelper(camundaInstanceID)
	if err != nil {
		return err
	}
	
	// If we have fewer files than keepCount, nothing to delete
	if len(logFiles) <= keepCount {
		return nil
	}
	
	// Delete old files (keep the most recent 'keepCount' files)
	instanceLogsDir := filepath.Join(fs.logsDir, camundaInstanceID)
	for i := 0; i < len(logFiles)-keepCount; i++ {
		logFilePath := filepath.Join(instanceLogsDir, logFiles[i])
		if err := os.Remove(logFilePath); err != nil {
			fs.logger.Error("Failed to delete old log file %s: %v", logFilePath, err)
			continue
		}
		fs.logger.Debug("Deleted old log file: %s", logFilePath)
	}
	
	return nil
}

// Helper methods

// findLatestLogFile finds the most recent log file for a Camunda instance
func (fs *FileStorageImpl) findLatestLogFile(camundaInstanceID string) (string, error) {
	instanceLogsDir := filepath.Join(fs.logsDir, camundaInstanceID)
	
	// Check if directory exists
	if _, err := os.Stat(instanceLogsDir); os.IsNotExist(err) {
		return "", utils.ErrFileStorageFailed
	}
	
	// Read directory entries
	entries, err := os.ReadDir(instanceLogsDir)
	if err != nil {
		return "", fmt.Errorf("failed to read logs directory: %w", err)
	}
	
	// Find the most recent log file
	var latestLog os.FileInfo
	var latestLogName string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			if latestLog == nil || info.ModTime().After(latestLog.ModTime()) {
				latestLog = info
				latestLogName = entry.Name()
			}
		}
	}
	
	if latestLogName == "" {
		return "", utils.ErrFileStorageFailed
	}
	
	return filepath.Join(instanceLogsDir, latestLogName), nil
}

// findLogFileByBackupID finds a log file containing the backup ID in its name
func (fs *FileStorageImpl) findLogFileByBackupID(camundaInstanceID, backupID string) (string, error) {
	instanceLogsDir := filepath.Join(fs.logsDir, camundaInstanceID)
	
	// Check if directory exists
	if _, err := os.Stat(instanceLogsDir); os.IsNotExist(err) {
		return "", utils.ErrFileStorageFailed
	}
	
	// Read directory entries
	entries, err := os.ReadDir(instanceLogsDir)
	if err != nil {
		return "", fmt.Errorf("failed to read logs directory: %w", err)
	}
	
	// Find log file matching backup ID (in timestamp format)
	// Backup IDs are in format YYYYMMDD-HHMMSS
	for _, entry := range entries {
		if !entry.IsDir() && strings.Contains(entry.Name(), backupID) {
			return filepath.Join(instanceLogsDir, entry.Name()), nil
		}
	}
	
	return "", utils.ErrFileStorageFailed
}