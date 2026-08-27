package retention

import (
	"context"
	"errors"
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

// InstanceProvider resolves a Camunda instance by ID. Deleting a backup needs
// the instance's component endpoints, which live in the instance config rather
// than in the backup record.
type InstanceProvider interface {
	GetInstance(id string) (*models.CamundaInstance, error)
}

type Manager struct {
	s3Storage   storage.S3Storage
	fileStorage storage.FileStorage
	httpClient  *camunda.HTTPClient
	cfg         *config.Config
	logger      *utils.Logger
	alerter     *utils.Alerter
	instances   InstanceProvider
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

// SetInstanceProvider sets the lookup used to resolve component endpoints when
// deleting a backup by ID.
func (m *Manager) SetInstanceProvider(p InstanceProvider) {
	m.instances = p
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
	m.cleanupIncompleteBackups(instance, result)

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
		if !m.deleteBackupData(instance, backup, result) {
			m.logger.Warn("[retention] Keeping %s backup %s: some artifacts could not be deleted; retrying next cycle", status, backup.BackupID)
			continue
		}

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

		if !m.deleteBackupData(instance, backup, result) {
			m.logger.Warn("[retention] Keeping failed backup %s: some artifacts could not be deleted; retrying next cycle", backup.BackupID)
			continue
		}

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

// deleteBackupData deletes the component artifacts for a backup and records any
// failures on the retention result. It reports whether every artifact is gone,
// so callers can hold the metadata record back when one survives.
func (m *Manager) deleteBackupData(instance *models.CamundaInstance, backup *models.BackupHistory, result *RetentionResult) bool {
	errs := m.purgeBackupArtifacts(context.Background(), instance, backup)
	result.Errors = append(result.Errors, errs...)
	return len(errs) == 0
}

// purgeBackupArtifacts deletes everything a backup left outside the controller's
// own metadata: the Elasticsearch snapshot and each Camunda component's backup.
// It returns one message per failure; an empty slice means every artifact is
// gone (or was never there).
//
// The record's component map is the authority on what this backup could have
// written. The orchestrator seeds that map with every enabled component before
// any of them runs (see orchestrator.createBackupHistory), so a component that
// is absent was disabled at backup time and has no artifact to delete. Purging
// absent components instead would send DELETEs to components this backup never
// touched.
//
// A record with no components at all is a corrupt or pre-dating record: we
// cannot tell what it wrote, so we refuse rather than delete the metadata and
// strand whatever is out there.
func (m *Manager) purgeBackupArtifacts(ctx context.Context, instance *models.CamundaInstance, backup *models.BackupHistory) []string {
	if len(backup.Components) == 0 {
		return []string{fmt.Sprintf(
			"backup %s records no components, so its artifacts cannot be identified; delete them by hand or use force",
			backup.BackupID)}
	}

	var errs []string

	for _, componentName := range types.ValidComponents {
		compInfo, recorded := backup.Components[componentName]
		if !recorded || !compInfo.Enabled || compInfo.Status == types.ComponentStatusSkipped {
			continue
		}

		var err error
		switch componentName {
		case types.ComponentElasticsearch:
			err = m.deleteESSnapshot(ctx, instance, backup, compInfo)
		case types.ComponentZeebe:
			err = m.deleteComponentBackup(ctx, instance.ZeebeBackupEndpoint, backup.BackupID, "Zeebe")
		case types.ComponentOperate:
			err = m.deleteComponentBackup(ctx, instance.OperateBackupEndpoint, backup.BackupID, "Operate")
		case types.ComponentTasklist:
			err = m.deleteComponentBackup(ctx, instance.TasklistBackupEndpoint, backup.BackupID, "Tasklist")
		case types.ComponentOptimize:
			err = m.deleteComponentBackup(ctx, instance.OptimizeBackupEndpoint, backup.BackupID, "Optimize")
		}

		if err == nil {
			continue
		}

		errs = append(errs, err.Error())
		if m.alerter != nil {
			m.alerter.AlertCleanupFailed(instance.ID, backup.BackupID, err.Error())
		}
	}

	return errs
}

// deleteESSnapshot deletes an Elasticsearch snapshot for a backup.
func (m *Manager) deleteESSnapshot(ctx context.Context, instance *models.CamundaInstance, backup *models.BackupHistory, compInfo models.ComponentBackupInfo) error {
	if instance.ElasticsearchEndpoint == "" || m.cfg == nil {
		return nil
	}

	repository := compInfo.SnapshotRepository
	snapshotName := compInfo.SnapshotName

	if repository == "" {
		repository = m.cfg.GetElasticsearchSnapshotRepository(instance.ID, instance.ElasticsearchSnapshotRepository)
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
		return nil
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
		if errors.Is(err, elasticsearch.ErrSnapshotMissing) {
			m.logger.Debug("[retention] ES snapshot %s/%s not found (already deleted or never created)", repository, snapshotName)
			return nil
		}
		return fmt.Errorf("failed to delete ES snapshot %s/%s for backup %s: %w", repository, snapshotName, backup.BackupID, err)
	}

	m.logger.Info("[retention] Deleted ES snapshot %s/%s for backup %s", repository, snapshotName, backup.BackupID)
	return nil
}

// deleteComponentBackup deletes a Camunda component backup via its REST API (DELETE endpoint/{backupId}).
func (m *Manager) deleteComponentBackup(ctx context.Context, endpoint, backupID, componentName string) error {
	if endpoint == "" || m.httpClient == nil {
		return nil
	}

	deleteURL := strings.TrimRight(endpoint, "/") + "/" + backupID
	resp, err := m.httpClient.Delete(ctx, deleteURL, nil)
	if err != nil {
		return fmt.Errorf("failed to delete %s backup %s: %w", componentName, backupID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		m.logger.Debug("[retention] %s backup %s not found (already deleted or never created)", componentName, backupID)
		return nil
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		m.logger.Info("[retention] Deleted %s backup %s", componentName, backupID)
		return nil
	}

	return fmt.Errorf("%s backup deletion returned status %d for backup %s", componentName, resp.StatusCode, backupID)
}

// cleanupIncompleteBackups removes INCOMPLETE backups, and the partial artifacts
// they left behind, when a newer COMPLETED backup exists.
func (m *Manager) cleanupIncompleteBackups(instance *models.CamundaInstance, result *RetentionResult) {
	camundaInstanceID := instance.ID

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
			if !m.deleteBackupData(instance, backup, result) {
				m.logger.Warn("[retention] Keeping incomplete backup %s: some artifacts could not be deleted; retrying next cycle", backup.BackupID)
				continue
			}

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

// DeleteBackup manually deletes a specific backup by ID, removing it from every
// place it exists: the Elasticsearch snapshot repository, each Camunda
// component, the controller's own S3 metadata record, and the local log file.
//
// It refuses to delete the most recent COMPLETED backup, and any backup that is
// still RUNNING, as safety measures.
//
// The metadata record is deleted last and only once every artifact is gone.
// If an artifact cannot be deleted the record is kept, so the backup stays
// visible and the deletion can be retried, rather than the artifacts being
// stranded as orphans. Pass force to delete the record anyway; the surviving
// artifacts are then reported by the reconciler as orphans.
func (m *Manager) DeleteBackup(camundaInstanceID, backupID string, force bool) error {
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

	// GetBackupHistory spans history/, incomplete/ and orphaned/, so this also
	// covers backups that are not in the main history.
	backup, err := m.s3Storage.GetBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		return err
	}

	// Deleting a backup the orchestrator is still writing races it: we would
	// remove artifacts it is about to finish creating, and it would rewrite the
	// record we just deleted. force does not apply — this is never safe.
	if backup.Status == types.BackupStatusRunning {
		return fmt.Errorf("%w (%s)", utils.ErrCannotDeleteRunningBackup, backupID)
	}

	instance, err := m.resolveInstance(camundaInstanceID)
	if err != nil {
		return err
	}

	if errs := m.purgeBackupArtifacts(context.Background(), instance, backup); len(errs) > 0 {
		if !force {
			return fmt.Errorf("%w for %s: %s", utils.ErrBackupArtifactsRemain, backupID, strings.Join(errs, "; "))
		}
		m.logger.Warn("[retention] Force-deleting backup %s with %d artifact(s) left behind: %s",
			backupID, len(errs), strings.Join(errs, "; "))
	}

	if err := m.s3Storage.DeleteBackupHistory(camundaInstanceID, backupID); err != nil {
		return fmt.Errorf("failed to delete backup record %s: %w", backupID, err)
	}

	if err := m.fileStorage.DeleteLogFile(camundaInstanceID, backupID); err != nil {
		m.logger.Debug("[retention] Could not delete log file for %s: %v", backupID, err)
	}

	m.logger.Info("[retention] Deleted backup %s and all of its artifacts", backupID)
	return nil
}

// resolveInstance looks up the instance whose endpoints the artifacts live behind.
func (m *Manager) resolveInstance(camundaInstanceID string) (*models.CamundaInstance, error) {
	if m.instances == nil {
		return nil, utils.ErrInstanceProviderNotConfigured
	}
	instance, err := m.instances.GetInstance(camundaInstanceID)
	if err != nil {
		return nil, fmt.Errorf("failed to look up instance %s: %w", camundaInstanceID, err)
	}
	if instance == nil {
		return nil, fmt.Errorf("failed to look up instance %s: %w", camundaInstanceID, utils.ErrCamundaInstanceNotFound)
	}
	return instance, nil
}
