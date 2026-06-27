package models

import (
	"time"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// BackupExecution represents a backup execution record
type BackupExecution struct {
	ID                string                           `json:"id"`
	CamundaInstanceID string                           `json:"camunda_instance_id"`
	StartTime         time.Time                        `json:"start_time"`
	EndTime           *time.Time                       `json:"end_time,omitempty"`
	Status            types.BackupStatus               `json:"status"`
	BackupID          string                           `json:"backup_id"`
	ComponentStatus   map[string]types.ComponentStatus `json:"component_status"`
	ComponentTimings  map[string]ComponentTiming       `json:"-"`
	ErrorMessage      string                           `json:"error_message,omitempty"`
	Logs              []string                         `json:"logs"`
}

// ComponentTiming captures per-component execution timing. Zero values mean
// the component has not started or not finished yet.
type ComponentTiming struct {
	StartTime time.Time
	EndTime   time.Time
}

// BackupHistory represents a backup history entry stored in S3
type BackupHistory struct {
	BackupID            string                         `json:"backup_id"`
	CamundaInstanceID   string                         `json:"camunda_instance_id"`
	CamundaInstanceName string                         `json:"camunda_instance_name"`
	StartTime           time.Time                      `json:"start_time"`
	EndTime             *time.Time                     `json:"end_time,omitempty"`
	DurationSeconds     *int                           `json:"duration_seconds,omitempty"`
	Status              types.BackupStatus             `json:"status"`
	TriggerType         types.TriggerType              `json:"trigger_type"`
	Components          map[string]ComponentBackupInfo `json:"components"`
	BackupStats         BackupStats                    `json:"backup_stats"`
	Metadata            BackupMetadata                 `json:"metadata"`
	ErrorMessage        string                         `json:"error_message,omitempty"`
}

// ComponentBackupInfo represents information about a component backup
type ComponentBackupInfo struct {
	Enabled            bool                  `json:"enabled"`
	Status             types.ComponentStatus `json:"status"`
	StartTime          *time.Time            `json:"start_time,omitempty"`
	EndTime            *time.Time            `json:"end_time,omitempty"`
	DurationSeconds    int                   `json:"duration_seconds"`
	ErrorMessage       string                `json:"error_message,omitempty"`
	SnapshotName       string                `json:"snapshot_name,omitempty"`
	SnapshotRepository string                `json:"snapshot_repository,omitempty"`
}

// BackupStats represents backup statistics
type BackupStats struct {
	TotalComponents      int `json:"total_components"`
	SuccessfulComponents int `json:"successful_components"`
	FailedComponents     int `json:"failed_components"`
	SkippedComponents    int `json:"skipped_components"`
	RunningComponents    int `json:"running_components,omitempty"`
	PendingComponents    int `json:"pending_components,omitempty"`
}

// BackupMetadata represents backup metadata
type BackupMetadata struct {
	ConfigVersion     string `json:"config_version"`
	ControllerVersion string `json:"controller_version"`
	ExecutionMode     string `json:"execution_mode"`
	LogFilePath       string `json:"log_file_path"`
	BackupReason      string `json:"backup_reason"`
}

// NewBackupExecution creates a new backup execution record
func NewBackupExecution(camundaInstanceID, backupID string) *BackupExecution {
	return &BackupExecution{
		ID:                backupID,
		CamundaInstanceID: camundaInstanceID,
		StartTime:         time.Now(),
		Status:            types.BackupStatusRunning,
		BackupID:          backupID,
		ComponentStatus:   make(map[string]types.ComponentStatus),
		ComponentTimings:  make(map[string]ComponentTiming),
		Logs:              []string{},
	}
}

// StartComponent records the start time for a component backup.
func (be *BackupExecution) StartComponent(component string) {
	if be.ComponentTimings == nil {
		be.ComponentTimings = make(map[string]ComponentTiming)
	}
	t := be.ComponentTimings[component]
	t.StartTime = time.Now()
	be.ComponentTimings[component] = t
}

// EndComponent records the end time for a component backup.
func (be *BackupExecution) EndComponent(component string) {
	if be.ComponentTimings == nil {
		be.ComponentTimings = make(map[string]ComponentTiming)
	}
	t := be.ComponentTimings[component]
	t.EndTime = time.Now()
	be.ComponentTimings[component] = t
}

// UpdateComponentStatus updates the status of a component
func (be *BackupExecution) UpdateComponentStatus(component string, status types.ComponentStatus) {
	if be.ComponentStatus == nil {
		be.ComponentStatus = make(map[string]types.ComponentStatus)
	}
	be.ComponentStatus[component] = status
}

// AddLog adds a log entry
func (be *BackupExecution) AddLog(message string) {
	be.Logs = append(be.Logs, message)
}

// MarkAsCompleted marks the backup as completed
func (be *BackupExecution) MarkAsCompleted() {
	now := time.Now()
	be.EndTime = &now
	be.Status = types.BackupStatusCompleted
}

// MarkAsFailed marks the backup as failed
func (be *BackupExecution) MarkAsFailed(errorMessage string) {
	now := time.Now()
	be.EndTime = &now
	be.Status = types.BackupStatusFailed
	be.ErrorMessage = errorMessage
}

// MarkAsIncomplete marks the backup as incomplete
func (be *BackupExecution) MarkAsIncomplete(errorMessage string) {
	now := time.Now()
	be.EndTime = &now
	be.Status = types.BackupStatusIncomplete
	be.ErrorMessage = errorMessage
}
