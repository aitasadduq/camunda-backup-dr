package storage

import (
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// FileStorage defines the interface for file-based storage operations
type FileStorage interface {
	// Configuration operations
	SaveConfiguration(config *models.Configuration) error
	LoadConfiguration() (*models.Configuration, error)
	
	// Log file operations
	CreateLogFile(camundaInstanceID, backupID string) error
	WriteToLogFile(camundaInstanceID, backupID, message string) error
	ReadLogFile(camundaInstanceID, backupID string) (string, error)
	DeleteLogFile(camundaInstanceID, backupID string) error
	ListLogFiles(camundaInstanceID string) ([]string, error)
	
	// Cleanup operations
	CleanupOldLogFiles(camundaInstanceID string, keepCount int) error
}

// S3Storage defines the interface for S3-based storage operations
type S3Storage interface {
	// Backup ID operations
	StoreLatestBackupID(camundaInstanceID, backupID string) error
	GetLatestBackupID(camundaInstanceID string) (string, error)
	
	// Backup history operations
	StoreBackupHistory(history *models.BackupHistory) error
	GetBackupHistory(camundaInstanceID, backupID string) (*models.BackupHistory, error)
	ListBackupHistory(camundaInstanceID string, status types.BackupStatus) ([]*models.BackupHistory, error)
	
	// Backup state operations
	UpdateBackupStatus(camundaInstanceID, backupID string, status types.BackupStatus) error
	
	// Cleanup operations
	DeleteBackupHistory(camundaInstanceID, backupID string) error
	MoveToOrphaned(camundaInstanceID, backupID string) error
	MoveToIncomplete(camundaInstanceID, backupID string) error
	ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error)
	ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error)
}