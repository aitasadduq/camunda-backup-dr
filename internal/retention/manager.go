package retention

import (
	"fmt"
	"sort"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Manager handles backup retention policies including keep-last-N,
// orphaned backup detection, safe deletion, and INCOMPLETE cleanup.
type Manager struct {
	s3Storage   storage.S3Storage
	fileStorage storage.FileStorage
	logger      *utils.Logger
}

// NewManager creates a new retention manager.
func NewManager(s3Storage storage.S3Storage, fileStorage storage.FileStorage, logger *utils.Logger) *Manager {
	return &Manager{
		s3Storage:   s3Storage,
		fileStorage: fileStorage,
		logger:      logger,
	}
}

// RetentionResult summarises what the retention run did.
type RetentionResult struct {
	// Backups moved to orphaned because they exceeded keep-last-N.
	OrphanedByRetention []string
	// INCOMPLETE backups that were cleaned up (a newer COMPLETED backup exists).
	CleanedIncomplete []string
	// Number of log files removed during cleanup.
	LogFilesRemoved int
	// Errors encountered (non-fatal; retention is best-effort).
	Errors []string
}

// ApplyRetention is the main entry point called after a successful backup.
// It applies the keep-last-N policy, cleans up eligible INCOMPLETE backups,
// and prunes old log files. It never deletes the most recent successful backup.
func (m *Manager) ApplyRetention(camundaInstanceID string, retentionCount int) *RetentionResult {
	result := &RetentionResult{}

	m.logger.Info("[retention] Applying retention for instance %s (keep-last-%d)", camundaInstanceID, retentionCount)

	// 1. Apply keep-last-N for COMPLETED backups
	m.applyKeepLastN(camundaInstanceID, retentionCount, result)

	// 2. Clean up eligible INCOMPLETE backups
	m.cleanupIncompleteBackups(camundaInstanceID, result)

	// 3. Clean up old log files (keep count aligned with retention)
	m.cleanupLogFiles(camundaInstanceID, retentionCount, result)

	m.logger.Info("[retention] Retention complete for instance %s: orphaned=%d, cleaned_incomplete=%d, log_files_removed=%d, errors=%d",
		camundaInstanceID, len(result.OrphanedByRetention), len(result.CleanedIncomplete), result.LogFilesRemoved, len(result.Errors))

	return result
}

// applyKeepLastN keeps the N most recent COMPLETED backups and moves the rest
// to orphaned. The most recent COMPLETED backup is never moved.
func (m *Manager) applyKeepLastN(camundaInstanceID string, retentionCount int, result *RetentionResult) {
	if retentionCount <= 0 {
		m.logger.Warn("[retention] Retention count is %d for instance %s; skipping keep-last-N", retentionCount, camundaInstanceID)
		return
	}

	completed, err := m.s3Storage.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list completed backups: %v", err))
		return
	}

	if len(completed) <= retentionCount {
		m.logger.Debug("[retention] Instance %s has %d completed backups (<= retention %d); nothing to prune",
			camundaInstanceID, len(completed), retentionCount)
		return
	}

	// Sort newest first by StartTime
	sort.Slice(completed, func(i, j int) bool {
		return completed[i].StartTime.After(completed[j].StartTime)
	})

	// Keep the first retentionCount, orphan the rest.
	// Safety: never orphan index 0 (the most recent successful backup).
	toOrphan := completed[retentionCount:]
	for _, backup := range toOrphan {
		if err := m.s3Storage.MoveToOrphaned(camundaInstanceID, backup.BackupID); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to orphan backup %s: %v", backup.BackupID, err))
			continue
		}
		result.OrphanedByRetention = append(result.OrphanedByRetention, backup.BackupID)
		m.logger.Info("[retention] Moved backup %s to orphaned (exceeds keep-last-%d)", backup.BackupID, retentionCount)
	}
}

// cleanupIncompleteBackups removes INCOMPLETE backups only when a newer
// COMPLETED backup exists, as per the architecture spec.
func (m *Manager) cleanupIncompleteBackups(camundaInstanceID string, result *RetentionResult) {
	incomplete, err := m.s3Storage.ListIncompleteBackups(camundaInstanceID)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list incomplete backups: %v", err))
		return
	}

	if len(incomplete) == 0 {
		return
	}

	// We need at least one newer COMPLETED backup to justify deleting an INCOMPLETE one.
	completed, err := m.s3Storage.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list completed backups for incomplete cleanup: %v", err))
		return
	}

	if len(completed) == 0 {
		m.logger.Debug("[retention] No completed backups for instance %s; keeping all incomplete backups", camundaInstanceID)
		return
	}

	// Find the newest COMPLETED backup timestamp.
	newestCompleted := completed[0]
	for _, c := range completed[1:] {
		if c.StartTime.After(newestCompleted.StartTime) {
			newestCompleted = c
		}
	}

	// Delete INCOMPLETE backups that are older than the newest COMPLETED backup.
	for _, backup := range incomplete {
		if backup.StartTime.Before(newestCompleted.StartTime) {
			if err := m.s3Storage.DeleteBackupHistory(camundaInstanceID, backup.BackupID); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("failed to delete incomplete backup %s: %v", backup.BackupID, err))
				continue
			}
			result.CleanedIncomplete = append(result.CleanedIncomplete, backup.BackupID)
			m.logger.Info("[retention] Deleted incomplete backup %s (newer completed backup %s exists)", backup.BackupID, newestCompleted.BackupID)
		} else {
			m.logger.Debug("[retention] Keeping incomplete backup %s (no newer completed backup)", backup.BackupID)
		}
	}
}

// cleanupLogFiles removes old log files beyond the retention count.
func (m *Manager) cleanupLogFiles(camundaInstanceID string, retentionCount int, result *RetentionResult) {
	// Count log files before cleanup so we can report how many were removed.
	before, _ := m.fileStorage.ListLogFiles(camundaInstanceID)

	if err := m.fileStorage.CleanupOldLogFiles(camundaInstanceID, retentionCount); err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to cleanup old log files: %v", err))
		return
	}

	after, _ := m.fileStorage.ListLogFiles(camundaInstanceID)
	result.LogFilesRemoved = len(before) - len(after)
	if result.LogFilesRemoved > 0 {
		m.logger.Info("[retention] Cleaned up %d old log files for instance %s", result.LogFilesRemoved, camundaInstanceID)
	}
}

// ListOrphanedBackups returns all orphaned backups for user review.
func (m *Manager) ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	return m.s3Storage.ListOrphanedBackups(camundaInstanceID)
}

// ListIncompleteBackups returns all incomplete backups for user review.
func (m *Manager) ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	return m.s3Storage.ListIncompleteBackups(camundaInstanceID)
}

// ListFailedBackups returns all failed backups for user review.
func (m *Manager) ListFailedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	return m.s3Storage.ListBackupHistory(camundaInstanceID, types.BackupStatusFailed)
}

// DeleteBackup manually deletes a specific backup by ID.
// It checks orphaned, incomplete, and failed backups. It refuses to delete the
// most recent COMPLETED backup as a safety measure.
func (m *Manager) DeleteBackup(camundaInstanceID, backupID string) error {
	// Safety check: refuse to delete the most recent successful backup.
	completed, err := m.s3Storage.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		return fmt.Errorf("failed to verify backup safety: %w", err)
	}

	if len(completed) > 0 {
		// Find the most recent COMPLETED backup.
		mostRecent := completed[0]
		for _, c := range completed[1:] {
			if c.StartTime.After(mostRecent.StartTime) {
				mostRecent = c
			}
		}
		if mostRecent.BackupID == backupID {
			return fmt.Errorf("cannot delete the most recent successful backup (%s)", backupID)
		}
	}

	// Try deleting from main history first.
	if err := m.s3Storage.DeleteBackupHistory(camundaInstanceID, backupID); err != nil {
		// If not found in main history, it might be in orphaned.
		m.logger.Debug("[retention] Backup %s not in main history, checking orphaned/incomplete", backupID)
	} else {
		m.logger.Info("[retention] Deleted backup %s from history", backupID)
		return nil
	}

	// Check if it exists in orphaned
	orphaned, err := m.s3Storage.ListOrphanedBackups(camundaInstanceID)
	if err == nil {
		for _, o := range orphaned {
			if o.BackupID == backupID {
				if delErr := m.s3Storage.DeleteBackupHistory(camundaInstanceID, backupID); delErr != nil {
					return fmt.Errorf("failed to delete orphaned backup %s: %w", backupID, delErr)
				}
				m.logger.Info("[retention] Deleted orphaned backup %s", backupID)
				return nil
			}
		}
	}

	// Check if it exists in incomplete
	incomplete, err := m.s3Storage.ListIncompleteBackups(camundaInstanceID)
	if err == nil {
		for _, inc := range incomplete {
			if inc.BackupID == backupID {
				if delErr := m.s3Storage.DeleteBackupHistory(camundaInstanceID, backupID); delErr != nil {
					return fmt.Errorf("failed to delete incomplete backup %s: %w", backupID, delErr)
				}
				m.logger.Info("[retention] Deleted incomplete backup %s", backupID)
				return nil
			}
		}
	}

	return utils.ErrBackupNotFound
}
