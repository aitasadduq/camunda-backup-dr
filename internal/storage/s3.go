package storage

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// S3StorageImpl implements the S3Storage interface
// Note: This is a mock implementation. In production, this should use AWS SDK
type S3StorageImpl struct {
	// S3 configuration
	endpoint        string
	accessKey       string
	secretKey       string
	bucket          string
	prefix          string
	
	// In-memory storage for mock implementation
	backupHistory   map[string]map[string]*models.BackupHistory // camundaInstanceID -> backupID -> history
	latestBackupID  map[string]string // camundaInstanceID -> latest backup ID
	orphanedBackups map[string]map[string]*models.BackupHistory
	incompleteBackups map[string]map[string]*models.BackupHistory
	
	logger          *utils.Logger
	mutex           sync.RWMutex
}

// NewS3Storage creates a new S3 storage instance
func NewS3Storage(endpoint, accessKey, secretKey, bucket, prefix string, logger *utils.Logger) (*S3StorageImpl, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("S3 endpoint is required")
	}
	if accessKey == "" {
		return nil, fmt.Errorf("S3 access key is required")
	}
	if secretKey == "" {
		return nil, fmt.Errorf("S3 secret key is required")
	}
	if bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	
	return &S3StorageImpl{
		endpoint:          endpoint,
		accessKey:         accessKey,
		secretKey:         secretKey,
		bucket:            bucket,
		prefix:            prefix,
		backupHistory:     make(map[string]map[string]*models.BackupHistory),
		latestBackupID:    make(map[string]string),
		orphanedBackups:   make(map[string]map[string]*models.BackupHistory),
		incompleteBackups: make(map[string]map[string]*models.BackupHistory),
		logger:            logger,
	}, nil
}

// StoreLatestBackupID stores the latest backup ID for a Camunda instance
func (s3 *S3StorageImpl) StoreLatestBackupID(camundaInstanceID, backupID string) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()
	
	s3.latestBackupID[camundaInstanceID] = backupID
	s3.logger.Debug("Stored latest backup ID for %s: %s", camundaInstanceID, backupID)
	
	return nil
}

// GetLatestBackupID retrieves the latest backup ID for a Camunda instance
func (s3 *S3StorageImpl) GetLatestBackupID(camundaInstanceID string) (string, error) {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()
	
	backupID, exists := s3.latestBackupID[camundaInstanceID]
	if !exists {
		return "", utils.ErrBackupNotFound
	}
	
	return backupID, nil
}

// StoreBackupHistory stores a backup history entry
func (s3 *S3StorageImpl) StoreBackupHistory(history *models.BackupHistory) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()
	
	// Initialize instance history map if not exists
	if _, exists := s3.backupHistory[history.CamundaInstanceID]; !exists {
		s3.backupHistory[history.CamundaInstanceID] = make(map[string]*models.BackupHistory)
	}
	
	// Determine storage path based on status
	var storageMap map[string]map[string]*models.BackupHistory
	switch history.Status {
	case types.BackupStatusIncomplete:
		if _, exists := s3.incompleteBackups[history.CamundaInstanceID]; !exists {
			s3.incompleteBackups[history.CamundaInstanceID] = make(map[string]*models.BackupHistory)
		}
		storageMap = s3.incompleteBackups
	case types.BackupStatusCompleted, types.BackupStatusFailed, types.BackupStatusRunning:
		storageMap = s3.backupHistory
	default:
		storageMap = s3.backupHistory
	}
	
	// Store backup history
	storageMap[history.CamundaInstanceID][history.BackupID] = history
	s3.logger.Debug("Stored backup history for %s: %s (status: %s)", 
		history.CamundaInstanceID, history.BackupID, history.Status)
	
	return nil
}

// GetBackupHistory retrieves a backup history entry
func (s3 *S3StorageImpl) GetBackupHistory(camundaInstanceID, backupID string) (*models.BackupHistory, error) {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()
	
	// Check in main history
	if instanceHistory, exists := s3.backupHistory[camundaInstanceID]; exists {
		if history, exists := instanceHistory[backupID]; exists {
			return history, nil
		}
	}
	
	// Check in incomplete backups
	if instanceHistory, exists := s3.incompleteBackups[camundaInstanceID]; exists {
		if history, exists := instanceHistory[backupID]; exists {
			return history, nil
		}
	}
	
	// Check in orphaned backups
	if instanceHistory, exists := s3.orphanedBackups[camundaInstanceID]; exists {
		if history, exists := instanceHistory[backupID]; exists {
			return history, nil
		}
	}
	
	return nil, utils.ErrBackupNotFound
}

// ListBackupHistory lists backup history entries for a Camunda instance
func (s3 *S3StorageImpl) ListBackupHistory(camundaInstanceID string, status types.BackupStatus) ([]*models.BackupHistory, error) {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()
	
	var backups []*models.BackupHistory
	
	// Determine which map to search
	var searchMaps []map[string]map[string]*models.BackupHistory
	switch status {
	case types.BackupStatusIncomplete:
		searchMaps = []map[string]map[string]*models.BackupHistory{s3.incompleteBackups}
	default:
		searchMaps = []map[string]map[string]*models.BackupHistory{s3.backupHistory}
	}
	
	// Collect backups from relevant maps
	for _, searchMap := range searchMaps {
		if instanceHistory, exists := searchMap[camundaInstanceID]; exists {
			for _, history := range instanceHistory {
				if status == "" || history.Status == status {
					backups = append(backups, history)
				}
			}
		}
	}
	
	// Sort by start time (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartTime.After(backups[j].StartTime)
	})
	
	return backups, nil
}

// UpdateBackupStatus updates the status of a backup
func (s3 *S3StorageImpl) UpdateBackupStatus(camundaInstanceID, backupID string, status types.BackupStatus) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()
	
	// Find and update backup history
	var history *models.BackupHistory
	var found bool
	
	// Check in main history
	if instanceHistory, exists := s3.backupHistory[camundaInstanceID]; exists {
		if h, exists := instanceHistory[backupID]; exists {
			history = h
			found = true
		}
	}
	
	// Check in incomplete backups
	if !found {
		if instanceHistory, exists := s3.incompleteBackups[camundaInstanceID]; exists {
			if h, exists := instanceHistory[backupID]; exists {
				history = h
				found = true
				// Move from incomplete to appropriate map
				delete(s3.incompleteBackups[camundaInstanceID], backupID)
			}
		}
	}
	
	if !found {
		return utils.ErrBackupNotFound
	}
	
	// Update status
	history.Status = status
	now := time.Now()
	history.EndTime = &now
	duration := int(now.Sub(history.StartTime).Seconds())
	history.DurationSeconds = &duration
	
	// Store back in appropriate map based on new status
	switch status {
	case types.BackupStatusIncomplete:
		if _, exists := s3.incompleteBackups[camundaInstanceID]; !exists {
			s3.incompleteBackups[camundaInstanceID] = make(map[string]*models.BackupHistory)
		}
		s3.incompleteBackups[camundaInstanceID][backupID] = history
	default:
		if _, exists := s3.backupHistory[camundaInstanceID]; !exists {
			s3.backupHistory[camundaInstanceID] = make(map[string]*models.BackupHistory)
		}
		s3.backupHistory[camundaInstanceID][backupID] = history
	}
	
	s3.logger.Debug("Updated backup status for %s: %s -> %s", camundaInstanceID, backupID, status)
	
	return nil
}

// DeleteBackupHistory deletes a backup history entry
func (s3 *S3StorageImpl) DeleteBackupHistory(camundaInstanceID, backupID string) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()
	
	// Try to delete from main history
	if instanceHistory, exists := s3.backupHistory[camundaInstanceID]; exists {
		if _, exists := instanceHistory[backupID]; exists {
			delete(instanceHistory, backupID)
			s3.logger.Debug("Deleted backup history for %s: %s", camundaInstanceID, backupID)
			return nil
		}
	}
	
	// Try to delete from incomplete backups
	if instanceHistory, exists := s3.incompleteBackups[camundaInstanceID]; exists {
		if _, exists := instanceHistory[backupID]; exists {
			delete(instanceHistory, backupID)
			s3.logger.Debug("Deleted incomplete backup history for %s: %s", camundaInstanceID, backupID)
			return nil
		}
	}
	
	// Try to delete from orphaned backups
	if instanceHistory, exists := s3.orphanedBackups[camundaInstanceID]; exists {
		if _, exists := instanceHistory[backupID]; exists {
			delete(instanceHistory, backupID)
			s3.logger.Debug("Deleted orphaned backup history for %s: %s", camundaInstanceID, backupID)
			return nil
		}
	}
	
	return utils.ErrBackupNotFound
}

// MoveToOrphaned moves a backup to the orphaned directory
func (s3 *S3StorageImpl) MoveToOrphaned(camundaInstanceID, backupID string) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()
	
	// Find backup in main history
	var history *models.BackupHistory
	var found bool
	
	if instanceHistory, exists := s3.backupHistory[camundaInstanceID]; exists {
		if h, exists := instanceHistory[backupID]; exists {
			history = h
			found = true
			delete(instanceHistory, backupID)
		}
	}
	
	if !found {
		return utils.ErrBackupNotFound
	}
	
	// Initialize orphaned backups map if not exists
	if _, exists := s3.orphanedBackups[camundaInstanceID]; !exists {
		s3.orphanedBackups[camundaInstanceID] = make(map[string]*models.BackupHistory)
	}
	
	// Move to orphaned
	s3.orphanedBackups[camundaInstanceID][backupID] = history
	s3.logger.Debug("Moved backup to orphaned for %s: %s", camundaInstanceID, backupID)
	
	return nil
}

// MoveToIncomplete moves a backup to the incomplete directory
func (s3 *S3StorageImpl) MoveToIncomplete(camundaInstanceID, backupID string) error {
	s3.mutex.Lock()
	defer s3.mutex.Unlock()
	
	// Find backup in main history
	var history *models.BackupHistory
	var found bool
	
	if instanceHistory, exists := s3.backupHistory[camundaInstanceID]; exists {
		if h, exists := instanceHistory[backupID]; exists {
			history = h
			found = true
			delete(instanceHistory, backupID)
		}
	}
	
	if !found {
		return utils.ErrBackupNotFound
	}
	
	// Initialize incomplete backups map if not exists
	if _, exists := s3.incompleteBackups[camundaInstanceID]; !exists {
		s3.incompleteBackups[camundaInstanceID] = make(map[string]*models.BackupHistory)
	}
	
	// Move to incomplete
	history.Status = types.BackupStatusIncomplete
	s3.incompleteBackups[camundaInstanceID][backupID] = history
	s3.logger.Debug("Moved backup to incomplete for %s: %s", camundaInstanceID, backupID)
	
	return nil
}

// ListOrphanedBackups lists all orphaned backups for a Camunda instance
func (s3 *S3StorageImpl) ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()
	
	var backups []*models.BackupHistory
	
	if instanceHistory, exists := s3.orphanedBackups[camundaInstanceID]; exists {
		for _, history := range instanceHistory {
			backups = append(backups, history)
		}
	}
	
	// Sort by start time (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartTime.After(backups[j].StartTime)
	})
	
	return backups, nil
}

// ListIncompleteBackups lists all incomplete backups for a Camunda instance
func (s3 *S3StorageImpl) ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	s3.mutex.RLock()
	defer s3.mutex.RUnlock()
	
	var backups []*models.BackupHistory
	
	if instanceHistory, exists := s3.incompleteBackups[camundaInstanceID]; exists {
		for _, history := range instanceHistory {
			backups = append(backups, history)
		}
	}
	
	// Sort by start time (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartTime.After(backups[j].StartTime)
	})
	
	return backups, nil
}

// Helper methods

// getS3Key generates an S3 key for a backup history entry
func (s3 *S3StorageImpl) getS3Key(camundaInstanceID, backupID string, status types.BackupStatus) string {
	// Format: {prefix}/{camundaInstanceID}/{status}/{YYYY}/{MM}/{DD}/{backupID}.json
	date := time.Now().Format("2006/01/02")
	
	var statusPath string
	switch status {
	case types.BackupStatusIncomplete:
		statusPath = "incomplete"
	case types.BackupStatusFailed, types.BackupStatusCompleted, types.BackupStatusRunning:
		statusPath = "history"
	default:
		statusPath = "history"
	}
	
	return filepath.Join(s3.prefix, camundaInstanceID, statusPath, date, backupID+".json")
}

// serializeBackupHistory serializes a backup history entry to JSON
func (s3 *S3StorageImpl) serializeBackupHistory(history *models.BackupHistory) ([]byte, error) {
	return json.MarshalIndent(history, "", "  ")
}

// deserializeBackupHistory deserializes a backup history entry from JSON
func (s3 *S3StorageImpl) deserializeBackupHistory(data []byte) (*models.BackupHistory, error) {
	var history models.BackupHistory
	if err := json.Unmarshal(data, &history); err != nil {
		return nil, fmt.Errorf("failed to deserialize backup history: %w", err)
	}
	return &history, nil
}

// parseBackupIDFromKey extracts the backup ID from an S3 key
func (s3 *S3StorageImpl) parseBackupIDFromKey(key string) string {
	// Extract filename from path
	parts := strings.Split(key, "/")
	if len(parts) == 0 {
		return ""
	}
	
	// Remove .json extension
	filename := parts[len(parts)-1]
	return strings.TrimSuffix(filename, ".json")
}