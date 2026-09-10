package utils

import "errors"

var (
	// ErrCamundaInstanceNotFound is returned when a Camunda instance is not found
	ErrCamundaInstanceNotFound = errors.New("camunda instance not found")

	// ErrCamundaInstanceAlreadyExists is returned when a Camunda instance with the same ID already exists
	ErrCamundaInstanceAlreadyExists = errors.New("camunda instance already exists")

	// ErrInvalidConfiguration is returned when configuration is invalid
	ErrInvalidConfiguration = errors.New("invalid configuration")

	// ErrBackupInProgress is returned when a backup is already in progress for an instance
	ErrBackupInProgress = errors.New("backup already in progress")

	// ErrBackupNotFound is returned when a backup is not found
	ErrBackupNotFound = errors.New("backup not found")

	// ErrS3ConnectionFailed is returned when S3 connection fails
	ErrS3ConnectionFailed = errors.New("failed to connect to S3")

	// ErrElasticsearchConnectionFailed is returned when Elasticsearch connection fails
	ErrElasticsearchConnectionFailed = errors.New("failed to connect to Elasticsearch")

	// ErrInvalidComponent is returned when an invalid component is specified
	ErrInvalidComponent = errors.New("invalid component")

	// ErrNoComponentsEnabled is returned when no components are enabled for backup
	ErrNoComponentsEnabled = errors.New("no components enabled for backup")

	// ErrBackupFailed is returned when a backup fails
	ErrBackupFailed = errors.New("backup failed")

	// ErrFileStorageFailed is returned when file storage operation fails
	ErrFileStorageFailed = errors.New("file storage operation failed")

	// ErrInvalidCamundaInstance is returned when Camunda instance configuration is invalid
	ErrInvalidCamundaInstance = errors.New("invalid camunda instance configuration")

	// ErrCannotDeleteMostRecentBackup is returned when attempting to delete the most recent successful backup
	ErrCannotDeleteMostRecentBackup = errors.New("cannot delete the most recent successful backup")

	// ErrCircuitBreakerOpen is returned when a circuit breaker is in the open state
	ErrCircuitBreakerOpen = errors.New("circuit breaker is open")

	// ErrBackupStuck is returned when a backup has been running longer than the stuck timeout
	ErrBackupStuck = errors.New("backup appears stuck")

	// ErrCleanupFailed is returned when post-failure cleanup encounters errors
	ErrCleanupFailed = errors.New("cleanup after failure encountered errors")

	// ErrRetryExhausted is returned when all retry attempts are exhausted
	ErrRetryExhausted = errors.New("all retry attempts exhausted")

	// ErrCannotDeleteRunningBackup is returned when attempting to delete a backup
	// that the orchestrator is still producing.
	ErrCannotDeleteRunningBackup = errors.New("cannot delete a backup that is still running")

	// ErrBackupArtifactsRemain is returned when a backup's metadata record was
	// left in place because its component artifacts could not all be deleted.
	// Removing the record anyway would strand those artifacts as orphans.
	ErrBackupArtifactsRemain = errors.New("backup artifacts could not be deleted")

	// ErrInstanceProviderNotConfigured is returned when a deletion needs the
	// instance's component endpoints but no instance provider is wired.
	ErrInstanceProviderNotConfigured = errors.New("instance provider not configured")

	// ErrNoReconcileReport is returned when an orphan deletion is requested but
	// no sweep has run. An orphan has no metadata record, so the scan is the only
	// thing that can say which artifacts it left behind.
	ErrNoReconcileReport = errors.New("no reconciliation report for this instance; run a scan first")

	// ErrNotAnOrphan is returned when a backup the caller asked to delete is
	// neither recorded by the controller nor reported as an orphan by the latest
	// scan. Nothing identifies its artifacts, so there is nothing safe to delete.
	ErrNotAnOrphan = errors.New("backup is not an orphan in the latest scan")

	// ErrOrphanRecordAppeared is returned when a backup reported as an orphan has
	// since acquired a controller record. Deleting it down the orphan path would
	// bypass the retention safety guards, so the caller must re-scan instead.
	ErrOrphanRecordAppeared = errors.New("backup now has a controller record; re-scan before deleting")

	// ErrOrphanArtifactsUnidentifiable is returned when an orphan's findings name
	// no artifact to delete. Guessing which components hold it would send DELETEs
	// to components the backup never touched.
	ErrOrphanArtifactsUnidentifiable = errors.New("orphan names no artifacts, so it cannot be deleted")

	// ErrBackupIDNotDeletable is returned for a backup ID that is not in the
	// controller's own YYYYMMDDHHMMSS format. Such an ID reached the controller
	// from a component API rather than being issued by it, and it would become a
	// path segment in a component DELETE URL, so it is reported but never
	// deleted automatically.
	ErrBackupIDNotDeletable = errors.New("backup ID is not in the controller's format, so it can only be deleted by hand")

	// ErrOrphanReportPartial is returned when the sweep an orphan deletion would
	// act on could not reach every source. Absence of evidence is not evidence
	// of absence, and it is not licence to delete either: a source that was not
	// enumerated may hold artifacts this deletion would leave behind while
	// reporting success.
	ErrOrphanReportPartial = errors.New("the last scan could not check every source, so what this backup left behind is not fully known")

	// ErrOrphanOwnershipUnverified is returned when another configured instance
	// could own the artifacts an orphan deletion would remove — because it holds
	// a record for the same backup ID, or because it shares the repository or
	// component endpoint the artifacts live behind. Backup IDs are timestamps,
	// so the same ID routinely exists in several instances.
	ErrOrphanOwnershipUnverified = errors.New("another configured instance may own this backup, so it will not be deleted automatically")

	// ErrOrphanEndpointDrift is returned when a component's backup endpoint has
	// changed since the sweep that found the orphan. The DELETE would go to an
	// endpoint that never reported this backup, where the same timestamp ID can
	// name a completely different, live backup.
	ErrOrphanEndpointDrift = errors.New("component endpoints have changed since the last scan; re-scan before deleting")

	// ErrOrphanReportStale is returned when the sweep an orphan deletion would
	// act on is too old to be trusted as a description of what exists now.
	ErrOrphanReportStale = errors.New("the last scan is too old to delete from; re-scan first")

	// ErrSnapshotNameNotDeletable is returned for a snapshot name that could
	// address something other than itself once placed in a URL path. Names come
	// from a repository listing, which is outside the controller.
	ErrSnapshotNameNotDeletable = errors.New("snapshot name contains characters that are not addressable, so it can only be deleted by hand")
)
