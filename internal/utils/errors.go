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
)
