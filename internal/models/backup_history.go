package models

import (
	"time"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// BackupHistoryManager manages backup history operations
type BackupHistoryManager struct {
	// This will be implemented in Phase 2 with S3 storage integration
}

// NewBackupHistory creates a new backup history entry from a backup execution
func NewBackupHistory(camundaInstanceID, camundaInstanceName, backupID string, triggerType types.TriggerType, executionMode, logFilePath, backupReason, controllerVersion, configVersion string) *BackupHistory {
	now := time.Now()
	
	return &BackupHistory{
		BackupID:            backupID,
		CamundaInstanceID:   camundaInstanceID,
		CamundaInstanceName: camundaInstanceName,
		StartTime:           now,
		EndTime:             nil,
		DurationSeconds:     nil,
		Status:              types.BackupStatusRunning,
		TriggerType:         triggerType,
		Components:          make(map[string]ComponentBackupInfo),
		BackupStats: BackupStats{
			TotalComponents:  0,
			SuccessfulComponents: 0,
			FailedComponents: 0,
			SkippedComponents: 0,
		},
		Metadata: BackupMetadata{
			ConfigVersion:     configVersion,
			ControllerVersion: controllerVersion,
			ExecutionMode:     executionMode,
			LogFilePath:       logFilePath,
			BackupReason:      backupReason,
		},
	}
}

// UpdateComponentBackupInfo updates component backup information
func (bh *BackupHistory) UpdateComponentBackupInfo(component string, info ComponentBackupInfo) {
	if bh.Components == nil {
		bh.Components = make(map[string]ComponentBackupInfo)
	}
	bh.Components[component] = info
	bh.updateStats()
}

// MarkAsCompleted marks the backup history as completed
func (bh *BackupHistory) MarkAsCompleted() {
	now := time.Now()
	bh.EndTime = &now
	duration := int(now.Sub(bh.StartTime).Seconds())
	bh.DurationSeconds = &duration
	bh.Status = types.BackupStatusCompleted
}

// MarkAsFailed marks the backup history as failed
func (bh *BackupHistory) MarkAsFailed(errorMessage string) {
	now := time.Now()
	bh.EndTime = &now
	duration := int(now.Sub(bh.StartTime).Seconds())
	bh.DurationSeconds = &duration
	bh.Status = types.BackupStatusFailed
	bh.ErrorMessage = errorMessage
}

// MarkAsIncomplete marks the backup history as incomplete
func (bh *BackupHistory) MarkAsIncomplete(errorMessage string) {
	now := time.Now()
	bh.EndTime = &now
	duration := int(now.Sub(bh.StartTime).Seconds())
	bh.DurationSeconds = &duration
	bh.Status = types.BackupStatusIncomplete
	bh.ErrorMessage = errorMessage
}

// updateStats updates the backup statistics based on component status
func (bh *BackupHistory) updateStats() {
	total := 0
	successful := 0
	failed := 0
	skipped := 0
	running := 0
	pending := 0

	for _, comp := range bh.Components {
		if comp.Enabled {
			total++
			switch comp.Status {
			case types.ComponentStatusCompleted:
				successful++
			case types.ComponentStatusFailed:
				failed++
			case types.ComponentStatusSkipped:
				skipped++
			case types.ComponentStatusRunning:
				running++
			case types.ComponentStatusPending:
				pending++
			}
		} else {
			skipped++
		}
	}

	bh.BackupStats = BackupStats{
		TotalComponents:       total,
		SuccessfulComponents:  successful,
		FailedComponents:      failed,
		SkippedComponents:     skipped,
		RunningComponents:     running,
		PendingComponents:     pending,
	}
}