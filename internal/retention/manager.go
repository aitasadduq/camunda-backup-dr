package retention

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/elasticsearch"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

type Manager struct {
	s3Storage   storage.S3Storage
	fileStorage storage.FileStorage
	httpClient  *camunda.HTTPClient
	cfg         *config.Config
	logger      *utils.Logger
	alerter     *utils.Alerter
}

func NewManager(s3Storage storage.S3Storage, fileStorage storage.FileStorage, httpClient *camunda.HTTPClient, cfg *config.Config, logger *utils.Logger) *Manager {
	return &Manager{
		s3Storage:   s3Storage,
		fileStorage: fileStorage,
		httpClient:  httpClient,
		cfg:         cfg,
		logger:      logger,
	}
}

// SetAlerter sets the alerter for cleanup failure notifications.
func (m *Manager) SetAlerter(alerter *utils.Alerter) {
	m.alerter = alerter
}

type RetentionResult struct {
	DeletedSuccessful []string
	DeletedFailed     []string
	CleanedIncomplete []string
	LogFilesRemoved   int
	Errors            []string
}

// ApplyRetention enforces success and failure retention policies.
// It keeps the N most recent successful backups and M most recent failed backups,
// deleting excess backups along with their component data (ES snapshots, Zeebe, Operate, etc.).
func (m *Manager) ApplyRetention(instance *models.CamundaInstance) *RetentionResult {
	result := &RetentionResult{}

	m.logger.Info("[retention] Applying retention for instance %s (success=%d, failure=%d)",
		instance.ID, instance.SuccessRetention, instance.FailureRetention)

	m.pruneByStatus(instance, types.BackupStatusCompleted, instance.SuccessRetention, &result.DeletedSuccessful, result)
	m.pruneFailedBackups(instance, result)
	m.cleanupIncompleteBackups(instance.ID, result)

	totalKeep := instance.SuccessRetention + instance.FailureRetention
	if totalKeep <= 0 {
		totalKeep = 1
	}
	m.cleanupLogFiles(instance.ID, totalKeep, result)

	m.logger.Info("[retention] Retention complete for instance %s: deleted_success=%d, deleted_failed=%d, cleaned_incomplete=%d, log_files_removed=%d, errors=%d",
		instance.ID, len(result.DeletedSuccessful), len(result.DeletedFailed), len(result.CleanedIncomplete), result.LogFilesRemoved, len(result.Errors))

	return result
}

// pruneByStatus keeps the N most recent backups with the given status
// and deletes the rest, including their component data.
func (m *Manager) pruneByStatus(instance *models.CamundaInstance, status types.BackupStatus, keep int, deleted *[]string, result *RetentionResult) {
	if keep <= 0 {
		m.logger.Warn("[retention] Retention count for %s is %d for instance %s; skipping", status, keep, instance.ID)
		return
	}

	backups, err := m.s3Storage.ListBackupHistory(instance.ID, status)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list %s backups: %v", status, err))
		return
	}

	if len(backups) <= keep {
		m.logger.Debug("[retention] Instance %s has %d %s backups (<= retention %d); nothing to prune",
			instance.ID, len(backups), status, keep)
		return
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartTime.After(backups[j].StartTime)
	})

	toDelete := backups[keep:]
	for _, backup := range toDelete {
		m.deleteBackupData(instance, backup, result)

		if err := m.s3Storage.DeleteBackupHistory(instance.ID, backup.BackupID); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to delete %s backup history %s: %v", status, backup.BackupID, err))
			continue
		}

		if err := m.fileStorage.DeleteLogFile(instance.ID, backup.BackupID); err != nil {
			m.logger.Debug("[retention] Could not delete log file for %s: %v", backup.BackupID, err)
		}

		*deleted = append(*deleted, backup.BackupID)
		m.logger.Info("[retention] Deleted %s backup %s (exceeds keep-last-%d)", status, backup.BackupID, keep)
	}
}

// pruneFailedBackups keeps the N most recent failed backups and deletes the rest,
// but only prunes a failed backup if a newer successful backup exists. This ensures
// failed backup data is preserved for investigation until a recovery point is established.
func (m *Manager) pruneFailedBackups(instance *models.CamundaInstance, result *RetentionResult) {
	keep := instance.FailureRetention
	if keep <= 0 {
		m.logger.Warn("[retention] Failure retention count is %d for instance %s; skipping", keep, instance.ID)
		return
	}

	failed, err := m.s3Storage.ListBackupHistory(instance.ID, types.BackupStatusFailed)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list FAILED backups: %v", err))
		return
	}

	if len(failed) <= keep {
		return
	}

	completed, err := m.s3Storage.ListBackupHistory(instance.ID, types.BackupStatusCompleted)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list completed backups for failed retention guard: %v", err))
		return
	}

	if len(completed) == 0 {
		m.logger.Info("[retention] No successful backups for instance %s; keeping all failed backups", instance.ID)
		return
	}

	// Find the newest successful backup.
	newestCompleted := completed[0]
	for _, c := range completed[1:] {
		if c.StartTime.After(newestCompleted.StartTime) {
			newestCompleted = c
		}
	}

	sort.Slice(failed, func(i, j int) bool {
		return failed[i].StartTime.After(failed[j].StartTime)
	})

	toDelete := failed[keep:]
	for _, backup := range toDelete {
		if !backup.StartTime.Before(newestCompleted.StartTime) {
			m.logger.Debug("[retention] Keeping failed backup %s (no newer successful backup)", backup.BackupID)
			continue
		}

		m.deleteBackupData(instance, backup, result)

		if err := m.s3Storage.DeleteBackupHistory(instance.ID, backup.BackupID); err != nil {
			result.Errors = append(result.Errors, fmt.Sprintf("failed to delete FAILED backup history %s: %v", backup.BackupID, err))
			continue
		}

		if err := m.fileStorage.DeleteLogFile(instance.ID, backup.BackupID); err != nil {
			m.logger.Debug("[retention] Could not delete log file for %s: %v", backup.BackupID, err)
		}

		result.DeletedFailed = append(result.DeletedFailed, backup.BackupID)
		m.logger.Info("[retention] Deleted failed backup %s (exceeds keep-last-%d, newer successful backup %s exists)", backup.BackupID, keep, newestCompleted.BackupID)
	}
}

// deleteBackupData deletes the actual component data for a backup by calling
// the relevant endpoints (Zeebe, Operate, Tasklist, Optimize, Elasticsearch).
func (m *Manager) deleteBackupData(instance *models.CamundaInstance, backup *models.BackupHistory, result *RetentionResult) {
	ctx := context.Background()

	for componentName, compInfo := range backup.Components {
		if !compInfo.Enabled || compInfo.Status == types.ComponentStatusSkipped {
			continue
		}

		switch componentName {
		case types.ComponentElasticsearch:
			m.deleteESSnapshot(ctx, instance, backup, compInfo, result)
		case types.ComponentZeebe:
			m.deleteComponentBackup(ctx, instance.ZeebeBackupEndpoint, instance.ID, backup.BackupID, "Zeebe", result)
		case types.ComponentOperate:
			m.deleteComponentBackup(ctx, instance.OperateBackupEndpoint, instance.ID, backup.BackupID, "Operate", result)
		case types.ComponentTasklist:
			m.deleteComponentBackup(ctx, instance.TasklistBackupEndpoint, instance.ID, backup.BackupID, "Tasklist", result)
		case types.ComponentOptimize:
			m.deleteComponentBackup(ctx, instance.OptimizeBackupEndpoint, instance.ID, backup.BackupID, "Optimize", result)
		}
	}
}

// deleteESSnapshot deletes an Elasticsearch snapshot for a backup.
func (m *Manager) deleteESSnapshot(ctx context.Context, instance *models.CamundaInstance, backup *models.BackupHistory, compInfo models.ComponentBackupInfo, result *RetentionResult) {
	if instance.ElasticsearchEndpoint == "" || m.cfg == nil {
		return
	}

	repository := compInfo.SnapshotRepository
	snapshotName := compInfo.SnapshotName

	if repository == "" {
		repository = m.cfg.GetElasticsearchSnapshotRepository(instance.ID)
	}
	if snapshotName == "" {
		namePrefix := m.cfg.GetElasticsearchSnapshotNamePrefix(instance.ID)
		if namePrefix != "" {
			snapshotName = fmt.Sprintf("%s-%s", namePrefix, backup.BackupID)
		} else {
			snapshotName = backup.BackupID
		}
	}

	if repository == "" || snapshotName == "" {
		return
	}

	password := m.cfg.GetElasticsearchPassword(instance.ID)
	esClient := elasticsearch.NewClient(
		instance.ElasticsearchEndpoint,
		instance.ElasticsearchUsername,
		password,
		m.httpClient,
		m.logger,
	)

	if err := esClient.DeleteSnapshot(ctx, repository, snapshotName); err != nil {
		errMsg := fmt.Sprintf("failed to delete ES snapshot %s/%s for backup %s: %v", repository, snapshotName, backup.BackupID, err)
		result.Errors = append(result.Errors, errMsg)
		if m.alerter != nil {
			m.alerter.AlertCleanupFailed(instance.ID, backup.BackupID, errMsg)
		}
	} else {
		m.logger.Info("[retention] Deleted ES snapshot %s/%s for backup %s", repository, snapshotName, backup.BackupID)
	}
}

// deleteComponentBackup deletes a Camunda component backup via its REST API (DELETE endpoint/{backupId}).
func (m *Manager) deleteComponentBackup(ctx context.Context, endpoint, instanceID, backupID, componentName string, result *RetentionResult) {
	if endpoint == "" || m.httpClient == nil {
		return
	}

	deleteURL := strings.TrimRight(endpoint, "/") + "/" + backupID
	resp, err := m.httpClient.Delete(ctx, deleteURL, nil)
	if err != nil {
		errMsg := fmt.Sprintf("failed to delete %s backup %s: %v", componentName, backupID, err)
		result.Errors = append(result.Errors, errMsg)
		if m.alerter != nil {
			m.alerter.AlertCleanupFailed(instanceID, backupID, errMsg)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		m.logger.Debug("[retention] %s backup %s not found (already deleted or never created)", componentName, backupID)
		return
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		m.logger.Info("[retention] Deleted %s backup %s for instance %s", componentName, backupID, instanceID)
	} else {
		errMsg := fmt.Sprintf("%s backup deletion returned status %d for backup %s", componentName, resp.StatusCode, backupID)
		result.Errors = append(result.Errors, errMsg)
		if m.alerter != nil {
			m.alerter.AlertCleanupFailed(instanceID, backupID, errMsg)
		}
	}
}

// cleanupIncompleteBackups removes INCOMPLETE backups when a newer COMPLETED backup exists.
func (m *Manager) cleanupIncompleteBackups(camundaInstanceID string, result *RetentionResult) {
	incomplete, err := m.s3Storage.ListIncompleteBackups(camundaInstanceID)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list incomplete backups: %v", err))
		return
	}

	if len(incomplete) == 0 {
		return
	}

	completed, err := m.s3Storage.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		result.Errors = append(result.Errors, fmt.Sprintf("failed to list completed backups for incomplete cleanup: %v", err))
		return
	}

	if len(completed) == 0 {
		m.logger.Debug("[retention] No completed backups for instance %s; keeping all incomplete backups", camundaInstanceID)
		return
	}

	newestCompleted := completed[0]
	for _, c := range completed[1:] {
		if c.StartTime.After(newestCompleted.StartTime) {
			newestCompleted = c
		}
	}

	for _, backup := range incomplete {
		if backup.StartTime.Before(newestCompleted.StartTime) {
			if err := m.s3Storage.DeleteBackupHistory(camundaInstanceID, backup.BackupID); err != nil {
				result.Errors = append(result.Errors, fmt.Sprintf("failed to delete incomplete backup %s: %v", backup.BackupID, err))
				continue
			}
			result.CleanedIncomplete = append(result.CleanedIncomplete, backup.BackupID)
			m.logger.Info("[retention] Deleted incomplete backup %s (newer completed backup %s exists)", backup.BackupID, newestCompleted.BackupID)
		}
	}
}

func (m *Manager) cleanupLogFiles(camundaInstanceID string, keepCount int, result *RetentionResult) {
	before, _ := m.fileStorage.ListLogFiles(camundaInstanceID)

	if err := m.fileStorage.CleanupOldLogFiles(camundaInstanceID, keepCount); err != nil {
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
// It refuses to delete the most recent COMPLETED backup as a safety measure.
func (m *Manager) DeleteBackup(camundaInstanceID, backupID string) error {
	completed, err := m.s3Storage.ListBackupHistory(camundaInstanceID, types.BackupStatusCompleted)
	if err != nil {
		return fmt.Errorf("failed to verify backup safety: %w", err)
	}

	if len(completed) > 0 {
		mostRecent := completed[0]
		for _, c := range completed[1:] {
			if c.StartTime.After(mostRecent.StartTime) {
				mostRecent = c
			}
		}
		if mostRecent.BackupID == backupID {
			return fmt.Errorf("%w (%s)", utils.ErrCannotDeleteMostRecentBackup, backupID)
		}
	}

	if err := m.s3Storage.DeleteBackupHistory(camundaInstanceID, backupID); err != nil {
		m.logger.Debug("[retention] Backup %s not in main history, checking orphaned/incomplete", backupID)
	} else {
		m.logger.Info("[retention] Deleted backup %s from history", backupID)
		return nil
	}

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
