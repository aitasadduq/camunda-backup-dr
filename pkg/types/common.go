package types

// BackupStatus represents the status of a backup
type BackupStatus string

const (
	// BackupStatusRunning backup is currently in progress
	BackupStatusRunning BackupStatus = "RUNNING"
	// BackupStatusCompleted all enabled components completed successfully
	BackupStatusCompleted BackupStatus = "COMPLETED"
	// BackupStatusFailed one or more components failed
	BackupStatusFailed BackupStatus = "FAILED"
	// BackupStatusIncomplete backup was interrupted before completion
	BackupStatusIncomplete BackupStatus = "INCOMPLETE"
)

// ComponentStatus represents the status of a component backup
type ComponentStatus string

const (
	// ComponentStatusPending component backup not yet started
	ComponentStatusPending ComponentStatus = "PENDING"
	// ComponentStatusRunning component backup in progress
	ComponentStatusRunning ComponentStatus = "RUNNING"
	// ComponentStatusCompleted component backup completed successfully
	ComponentStatusCompleted ComponentStatus = "COMPLETED"
	// ComponentStatusFailed component backup failed
	ComponentStatusFailed ComponentStatus = "FAILED"
	// ComponentStatusSkipped component was disabled or skipped
	ComponentStatusSkipped ComponentStatus = "SKIPPED"
)

// TriggerType represents how a backup was triggered
type TriggerType string

const (
	// TriggerTypeScheduled backup was triggered by scheduler
	TriggerTypeScheduled TriggerType = "SCHEDULED"
	// TriggerTypeManual backup was triggered manually by user
	TriggerTypeManual TriggerType = "MANUAL"
)

// Component names
const (
	ComponentZeebe        = "zeebe"
	ComponentOperate      = "operate"
	ComponentTasklist     = "tasklist"
	ComponentOptimize     = "optimize"
	ComponentElasticsearch = "elasticsearch"
)

// Valid component names
var ValidComponents = []string{
	ComponentZeebe,
	ComponentOperate,
	ComponentTasklist,
	ComponentOptimize,
	ComponentElasticsearch,
}