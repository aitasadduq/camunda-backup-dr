package models

import (
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func TestNewBackupHistory(t *testing.T) {
	before := time.Now()
	bh := NewBackupHistory(
		"inst-1", "My Instance", "bk-100",
		types.TriggerTypeManual,
		"sequential", "/var/log/backup.log", "manual trigger",
		"1.0.0", "2.0.0",
	)
	after := time.Now()

	if bh.BackupID != "bk-100" {
		t.Errorf("BackupID = %q, want %q", bh.BackupID, "bk-100")
	}
	if bh.CamundaInstanceID != "inst-1" {
		t.Errorf("CamundaInstanceID = %q, want %q", bh.CamundaInstanceID, "inst-1")
	}
	if bh.CamundaInstanceName != "My Instance" {
		t.Errorf("CamundaInstanceName = %q, want %q", bh.CamundaInstanceName, "My Instance")
	}
	if bh.StartTime.Before(before) || bh.StartTime.After(after) {
		t.Errorf("StartTime %v not in expected range [%v, %v]", bh.StartTime, before, after)
	}
	if bh.EndTime != nil {
		t.Errorf("EndTime = %v, want nil", bh.EndTime)
	}
	if bh.DurationSeconds != nil {
		t.Errorf("DurationSeconds = %v, want nil", bh.DurationSeconds)
	}
	if bh.Status != types.BackupStatusRunning {
		t.Errorf("Status = %q, want %q", bh.Status, types.BackupStatusRunning)
	}
	if bh.TriggerType != types.TriggerTypeManual {
		t.Errorf("TriggerType = %q, want %q", bh.TriggerType, types.TriggerTypeManual)
	}
	if bh.Components == nil {
		t.Fatal("Components should be initialized, got nil")
	}
	if len(bh.Components) != 0 {
		t.Errorf("Components should be empty, got %d entries", len(bh.Components))
	}
	if bh.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", bh.ErrorMessage)
	}

	// Verify metadata
	if bh.Metadata.ConfigVersion != "2.0.0" {
		t.Errorf("Metadata.ConfigVersion = %q, want %q", bh.Metadata.ConfigVersion, "2.0.0")
	}
	if bh.Metadata.ControllerVersion != "1.0.0" {
		t.Errorf("Metadata.ControllerVersion = %q, want %q", bh.Metadata.ControllerVersion, "1.0.0")
	}
	if bh.Metadata.ExecutionMode != "sequential" {
		t.Errorf("Metadata.ExecutionMode = %q, want %q", bh.Metadata.ExecutionMode, "sequential")
	}
	if bh.Metadata.LogFilePath != "/var/log/backup.log" {
		t.Errorf("Metadata.LogFilePath = %q, want %q", bh.Metadata.LogFilePath, "/var/log/backup.log")
	}
	if bh.Metadata.BackupReason != "manual trigger" {
		t.Errorf("Metadata.BackupReason = %q, want %q", bh.Metadata.BackupReason, "manual trigger")
	}

	// Verify stats are zeroed
	if bh.BackupStats.TotalComponents != 0 {
		t.Errorf("BackupStats.TotalComponents = %d, want 0", bh.BackupStats.TotalComponents)
	}
	if bh.BackupStats.SuccessfulComponents != 0 {
		t.Errorf("BackupStats.SuccessfulComponents = %d, want 0", bh.BackupStats.SuccessfulComponents)
	}
	if bh.BackupStats.FailedComponents != 0 {
		t.Errorf("BackupStats.FailedComponents = %d, want 0", bh.BackupStats.FailedComponents)
	}
	if bh.BackupStats.SkippedComponents != 0 {
		t.Errorf("BackupStats.SkippedComponents = %d, want 0", bh.BackupStats.SkippedComponents)
	}
}

func TestNewBackupHistory_ScheduledTrigger(t *testing.T) {
	bh := NewBackupHistory(
		"inst-2", "Scheduled Instance", "bk-200",
		types.TriggerTypeScheduled,
		"parallel", "", "scheduled backup",
		"1.0.0", "1.0.0",
	)

	if bh.TriggerType != types.TriggerTypeScheduled {
		t.Errorf("TriggerType = %q, want %q", bh.TriggerType, types.TriggerTypeScheduled)
	}
}

func TestBackupHistory_UpdateComponentBackupInfo(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)

	now := time.Now()
	info := ComponentBackupInfo{
		Enabled:   true,
		Status:    types.ComponentStatusCompleted,
		StartTime: &now,
		EndTime:   &now,
	}

	bh.UpdateComponentBackupInfo(types.ComponentZeebe, info)

	got, ok := bh.Components[types.ComponentZeebe]
	if !ok {
		t.Fatal("ComponentZeebe not found in Components map")
	}
	if got.Status != types.ComponentStatusCompleted {
		t.Errorf("Component status = %q, want %q", got.Status, types.ComponentStatusCompleted)
	}

	// Stats should be updated
	if bh.BackupStats.TotalComponents != 1 {
		t.Errorf("TotalComponents = %d, want 1", bh.BackupStats.TotalComponents)
	}
	if bh.BackupStats.SuccessfulComponents != 1 {
		t.Errorf("SuccessfulComponents = %d, want 1", bh.BackupStats.SuccessfulComponents)
	}
}

func TestBackupHistory_UpdateComponentBackupInfo_NilMap(t *testing.T) {
	bh := &BackupHistory{
		Components: nil,
	}

	info := ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusPending,
	}

	bh.UpdateComponentBackupInfo(types.ComponentOperate, info)

	if bh.Components == nil {
		t.Fatal("Components should be initialized after update")
	}
	if _, ok := bh.Components[types.ComponentOperate]; !ok {
		t.Error("ComponentOperate should be in Components map")
	}
}

func TestBackupHistory_UpdateComponentBackupInfo_Overwrite(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)

	bh.UpdateComponentBackupInfo(types.ComponentZeebe, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusRunning,
	})

	if bh.BackupStats.RunningComponents != 1 {
		t.Errorf("RunningComponents = %d, want 1", bh.BackupStats.RunningComponents)
	}

	// Overwrite with completed
	bh.UpdateComponentBackupInfo(types.ComponentZeebe, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusCompleted,
	})

	if bh.BackupStats.SuccessfulComponents != 1 {
		t.Errorf("SuccessfulComponents = %d, want 1", bh.BackupStats.SuccessfulComponents)
	}
	if bh.BackupStats.RunningComponents != 0 {
		t.Errorf("RunningComponents = %d, want 0 after overwrite", bh.BackupStats.RunningComponents)
	}
}

func TestBackupHistory_UpdateStats_AllStatuses(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)

	bh.UpdateComponentBackupInfo(types.ComponentZeebe, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusCompleted,
	})
	bh.UpdateComponentBackupInfo(types.ComponentOperate, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusFailed,
	})
	bh.UpdateComponentBackupInfo(types.ComponentTasklist, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusSkipped,
	})
	bh.UpdateComponentBackupInfo(types.ComponentOptimize, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusRunning,
	})
	bh.UpdateComponentBackupInfo(types.ComponentElasticsearch, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusPending,
	})

	stats := bh.BackupStats
	if stats.TotalComponents != 5 {
		t.Errorf("TotalComponents = %d, want 5", stats.TotalComponents)
	}
	if stats.SuccessfulComponents != 1 {
		t.Errorf("SuccessfulComponents = %d, want 1", stats.SuccessfulComponents)
	}
	if stats.FailedComponents != 1 {
		t.Errorf("FailedComponents = %d, want 1", stats.FailedComponents)
	}
	if stats.SkippedComponents != 1 {
		t.Errorf("SkippedComponents = %d, want 1", stats.SkippedComponents)
	}
	if stats.RunningComponents != 1 {
		t.Errorf("RunningComponents = %d, want 1", stats.RunningComponents)
	}
	if stats.PendingComponents != 1 {
		t.Errorf("PendingComponents = %d, want 1", stats.PendingComponents)
	}
}

func TestBackupHistory_UpdateStats_DisabledComponent(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)

	// Disabled component should count as skipped but not in total
	bh.UpdateComponentBackupInfo(types.ComponentOptimize, ComponentBackupInfo{
		Enabled: false,
		Status:  types.ComponentStatusSkipped,
	})
	bh.UpdateComponentBackupInfo(types.ComponentZeebe, ComponentBackupInfo{
		Enabled: true,
		Status:  types.ComponentStatusCompleted,
	})

	if bh.BackupStats.TotalComponents != 1 {
		t.Errorf("TotalComponents = %d, want 1 (disabled not counted)", bh.BackupStats.TotalComponents)
	}
	if bh.BackupStats.SkippedComponents != 1 {
		t.Errorf("SkippedComponents = %d, want 1", bh.BackupStats.SkippedComponents)
	}
	if bh.BackupStats.SuccessfulComponents != 1 {
		t.Errorf("SuccessfulComponents = %d, want 1", bh.BackupStats.SuccessfulComponents)
	}
}

func TestBackupHistory_MarkAsCompleted(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)

	time.Sleep(time.Millisecond) // ensure non-zero duration
	before := time.Now()
	bh.MarkAsCompleted()
	after := time.Now()

	if bh.Status != types.BackupStatusCompleted {
		t.Errorf("Status = %q, want %q", bh.Status, types.BackupStatusCompleted)
	}
	if bh.EndTime == nil {
		t.Fatal("EndTime should be set after MarkAsCompleted")
	}
	if bh.EndTime.Before(before) || bh.EndTime.After(after) {
		t.Errorf("EndTime %v not in expected range [%v, %v]", *bh.EndTime, before, after)
	}
	if bh.DurationSeconds == nil {
		t.Fatal("DurationSeconds should be set after MarkAsCompleted")
	}
	if *bh.DurationSeconds < 0 {
		t.Errorf("DurationSeconds = %d, want >= 0", *bh.DurationSeconds)
	}
	if bh.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", bh.ErrorMessage)
	}
}

func TestBackupHistory_MarkAsFailed(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)

	before := time.Now()
	bh.MarkAsFailed("disk full")
	after := time.Now()

	if bh.Status != types.BackupStatusFailed {
		t.Errorf("Status = %q, want %q", bh.Status, types.BackupStatusFailed)
	}
	if bh.EndTime == nil {
		t.Fatal("EndTime should be set after MarkAsFailed")
	}
	if bh.EndTime.Before(before) || bh.EndTime.After(after) {
		t.Errorf("EndTime %v not in expected range [%v, %v]", *bh.EndTime, before, after)
	}
	if bh.DurationSeconds == nil {
		t.Fatal("DurationSeconds should be set after MarkAsFailed")
	}
	if bh.ErrorMessage != "disk full" {
		t.Errorf("ErrorMessage = %q, want %q", bh.ErrorMessage, "disk full")
	}
}

func TestBackupHistory_MarkAsFailed_EmptyMessage(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)
	bh.MarkAsFailed("")

	if bh.Status != types.BackupStatusFailed {
		t.Errorf("Status = %q, want %q", bh.Status, types.BackupStatusFailed)
	}
	if bh.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", bh.ErrorMessage)
	}
}

func TestBackupHistory_MarkAsIncomplete(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)

	before := time.Now()
	bh.MarkAsIncomplete("zeebe timed out")
	after := time.Now()

	if bh.Status != types.BackupStatusIncomplete {
		t.Errorf("Status = %q, want %q", bh.Status, types.BackupStatusIncomplete)
	}
	if bh.EndTime == nil {
		t.Fatal("EndTime should be set after MarkAsIncomplete")
	}
	if bh.EndTime.Before(before) || bh.EndTime.After(after) {
		t.Errorf("EndTime %v not in expected range [%v, %v]", *bh.EndTime, before, after)
	}
	if bh.DurationSeconds == nil {
		t.Fatal("DurationSeconds should be set after MarkAsIncomplete")
	}
	if bh.ErrorMessage != "zeebe timed out" {
		t.Errorf("ErrorMessage = %q, want %q", bh.ErrorMessage, "zeebe timed out")
	}
}

func TestBackupHistory_MarkAsIncomplete_EmptyMessage(t *testing.T) {
	bh := NewBackupHistory(
		"inst", "Inst", "bk",
		types.TriggerTypeManual,
		"sequential", "", "", "1.0.0", "1.0.0",
	)
	bh.MarkAsIncomplete("")

	if bh.Status != types.BackupStatusIncomplete {
		t.Errorf("Status = %q, want %q", bh.Status, types.BackupStatusIncomplete)
	}
	if bh.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", bh.ErrorMessage)
	}
}

func TestBackupHistory_FullLifecycle(t *testing.T) {
	bh := NewBackupHistory(
		"prod", "Production", "bk-lifecycle",
		types.TriggerTypeScheduled,
		"parallel", "/logs/bk.log", "nightly backup",
		"2.0.0", "3.0.0",
	)

	// Add components in various states
	now := time.Now()
	bh.UpdateComponentBackupInfo(types.ComponentZeebe, ComponentBackupInfo{
		Enabled:   true,
		Status:    types.ComponentStatusCompleted,
		StartTime: &now,
		EndTime:   &now,
	})
	bh.UpdateComponentBackupInfo(types.ComponentOperate, ComponentBackupInfo{
		Enabled:   true,
		Status:    types.ComponentStatusCompleted,
		StartTime: &now,
		EndTime:   &now,
	})
	bh.UpdateComponentBackupInfo(types.ComponentOptimize, ComponentBackupInfo{
		Enabled: false,
		Status:  types.ComponentStatusSkipped,
	})

	// Complete overall
	bh.MarkAsCompleted()

	if bh.Status != types.BackupStatusCompleted {
		t.Errorf("Status = %q, want %q", bh.Status, types.BackupStatusCompleted)
	}
	if bh.BackupStats.TotalComponents != 2 {
		t.Errorf("TotalComponents = %d, want 2", bh.BackupStats.TotalComponents)
	}
	if bh.BackupStats.SuccessfulComponents != 2 {
		t.Errorf("SuccessfulComponents = %d, want 2", bh.BackupStats.SuccessfulComponents)
	}
	if bh.BackupStats.SkippedComponents != 1 {
		t.Errorf("SkippedComponents = %d, want 1", bh.BackupStats.SkippedComponents)
	}
}
