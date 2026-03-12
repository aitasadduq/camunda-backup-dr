package models

import (
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func TestNewBackupExecution(t *testing.T) {
	before := time.Now()
	be := NewBackupExecution("instance-1", "backup-42")
	after := time.Now()

	if be.ID != "backup-42" {
		t.Errorf("ID = %q, want %q", be.ID, "backup-42")
	}
	if be.CamundaInstanceID != "instance-1" {
		t.Errorf("CamundaInstanceID = %q, want %q", be.CamundaInstanceID, "instance-1")
	}
	if be.BackupID != "backup-42" {
		t.Errorf("BackupID = %q, want %q", be.BackupID, "backup-42")
	}
	if be.Status != types.BackupStatusRunning {
		t.Errorf("Status = %q, want %q", be.Status, types.BackupStatusRunning)
	}
	if be.StartTime.Before(before) || be.StartTime.After(after) {
		t.Errorf("StartTime %v not in expected range [%v, %v]", be.StartTime, before, after)
	}
	if be.EndTime != nil {
		t.Errorf("EndTime = %v, want nil", be.EndTime)
	}
	if be.ComponentStatus == nil {
		t.Fatal("ComponentStatus should be initialized, got nil")
	}
	if len(be.ComponentStatus) != 0 {
		t.Errorf("ComponentStatus should be empty, got %d entries", len(be.ComponentStatus))
	}
	if be.Logs == nil {
		t.Fatal("Logs should be initialized, got nil")
	}
	if len(be.Logs) != 0 {
		t.Errorf("Logs should be empty, got %d entries", len(be.Logs))
	}
	if be.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", be.ErrorMessage)
	}
}

func TestBackupExecution_UpdateComponentStatus(t *testing.T) {
	be := NewBackupExecution("inst", "bk")

	be.UpdateComponentStatus(types.ComponentZeebe, types.ComponentStatusRunning)
	if got := be.ComponentStatus[types.ComponentZeebe]; got != types.ComponentStatusRunning {
		t.Errorf("ComponentStatus[zeebe] = %q, want %q", got, types.ComponentStatusRunning)
	}

	// Overwrite existing status
	be.UpdateComponentStatus(types.ComponentZeebe, types.ComponentStatusCompleted)
	if got := be.ComponentStatus[types.ComponentZeebe]; got != types.ComponentStatusCompleted {
		t.Errorf("ComponentStatus[zeebe] after update = %q, want %q", got, types.ComponentStatusCompleted)
	}

	// Multiple components
	be.UpdateComponentStatus(types.ComponentOperate, types.ComponentStatusFailed)
	if len(be.ComponentStatus) != 2 {
		t.Errorf("expected 2 components, got %d", len(be.ComponentStatus))
	}
}

func TestBackupExecution_UpdateComponentStatus_NilMap(t *testing.T) {
	be := &BackupExecution{
		ID:              "test",
		ComponentStatus: nil,
	}

	be.UpdateComponentStatus(types.ComponentTasklist, types.ComponentStatusPending)
	if be.ComponentStatus == nil {
		t.Fatal("ComponentStatus should be initialized after update")
	}
	if got := be.ComponentStatus[types.ComponentTasklist]; got != types.ComponentStatusPending {
		t.Errorf("ComponentStatus[tasklist] = %q, want %q", got, types.ComponentStatusPending)
	}
}

func TestBackupExecution_AddLog(t *testing.T) {
	be := NewBackupExecution("inst", "bk")

	be.AddLog("first message")
	if len(be.Logs) != 1 || be.Logs[0] != "first message" {
		t.Errorf("Logs after first add = %v, want [first message]", be.Logs)
	}

	be.AddLog("second message")
	be.AddLog("third message")
	if len(be.Logs) != 3 {
		t.Errorf("expected 3 log entries, got %d", len(be.Logs))
	}
	if be.Logs[1] != "second message" {
		t.Errorf("Logs[1] = %q, want %q", be.Logs[1], "second message")
	}
	if be.Logs[2] != "third message" {
		t.Errorf("Logs[2] = %q, want %q", be.Logs[2], "third message")
	}
}

func TestBackupExecution_AddLog_Empty(t *testing.T) {
	be := NewBackupExecution("inst", "bk")

	be.AddLog("")
	if len(be.Logs) != 1 {
		t.Errorf("expected 1 log entry for empty string, got %d", len(be.Logs))
	}
}

func TestBackupExecution_MarkAsCompleted(t *testing.T) {
	be := NewBackupExecution("inst", "bk")
	time.Sleep(time.Millisecond) // ensure EndTime > StartTime

	before := time.Now()
	be.MarkAsCompleted()
	after := time.Now()

	if be.Status != types.BackupStatusCompleted {
		t.Errorf("Status = %q, want %q", be.Status, types.BackupStatusCompleted)
	}
	if be.EndTime == nil {
		t.Fatal("EndTime should be set after MarkAsCompleted")
	}
	if be.EndTime.Before(before) || be.EndTime.After(after) {
		t.Errorf("EndTime %v not in expected range [%v, %v]", *be.EndTime, before, after)
	}
	if !be.EndTime.After(be.StartTime) {
		t.Error("EndTime should be after StartTime")
	}
	if be.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", be.ErrorMessage)
	}
}

func TestBackupExecution_MarkAsFailed(t *testing.T) {
	be := NewBackupExecution("inst", "bk")

	before := time.Now()
	be.MarkAsFailed("connection timeout")
	after := time.Now()

	if be.Status != types.BackupStatusFailed {
		t.Errorf("Status = %q, want %q", be.Status, types.BackupStatusFailed)
	}
	if be.EndTime == nil {
		t.Fatal("EndTime should be set after MarkAsFailed")
	}
	if be.EndTime.Before(before) || be.EndTime.After(after) {
		t.Errorf("EndTime %v not in expected range [%v, %v]", *be.EndTime, before, after)
	}
	if be.ErrorMessage != "connection timeout" {
		t.Errorf("ErrorMessage = %q, want %q", be.ErrorMessage, "connection timeout")
	}
}

func TestBackupExecution_MarkAsFailed_EmptyMessage(t *testing.T) {
	be := NewBackupExecution("inst", "bk")
	be.MarkAsFailed("")

	if be.Status != types.BackupStatusFailed {
		t.Errorf("Status = %q, want %q", be.Status, types.BackupStatusFailed)
	}
	if be.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", be.ErrorMessage)
	}
}

func TestBackupExecution_MarkAsIncomplete(t *testing.T) {
	be := NewBackupExecution("inst", "bk")

	before := time.Now()
	be.MarkAsIncomplete("partial failure")
	after := time.Now()

	if be.Status != types.BackupStatusIncomplete {
		t.Errorf("Status = %q, want %q", be.Status, types.BackupStatusIncomplete)
	}
	if be.EndTime == nil {
		t.Fatal("EndTime should be set after MarkAsIncomplete")
	}
	if be.EndTime.Before(before) || be.EndTime.After(after) {
		t.Errorf("EndTime %v not in expected range [%v, %v]", *be.EndTime, before, after)
	}
	if be.ErrorMessage != "partial failure" {
		t.Errorf("ErrorMessage = %q, want %q", be.ErrorMessage, "partial failure")
	}
}

func TestBackupExecution_MarkAsIncomplete_EmptyMessage(t *testing.T) {
	be := NewBackupExecution("inst", "bk")
	be.MarkAsIncomplete("")

	if be.Status != types.BackupStatusIncomplete {
		t.Errorf("Status = %q, want %q", be.Status, types.BackupStatusIncomplete)
	}
	if be.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", be.ErrorMessage)
	}
}

func TestBackupExecution_FullLifecycle(t *testing.T) {
	be := NewBackupExecution("prod-cluster", "bk-2024-01")

	// Start components
	be.UpdateComponentStatus(types.ComponentZeebe, types.ComponentStatusRunning)
	be.AddLog("Starting zeebe backup")

	be.UpdateComponentStatus(types.ComponentOperate, types.ComponentStatusRunning)
	be.AddLog("Starting operate backup")

	// Complete zeebe
	be.UpdateComponentStatus(types.ComponentZeebe, types.ComponentStatusCompleted)
	be.AddLog("Zeebe backup completed")

	// Fail operate
	be.UpdateComponentStatus(types.ComponentOperate, types.ComponentStatusFailed)
	be.AddLog("Operate backup failed")

	// Mark overall as failed
	be.MarkAsFailed("operate component failed")

	if be.Status != types.BackupStatusFailed {
		t.Errorf("Status = %q, want %q", be.Status, types.BackupStatusFailed)
	}
	if len(be.ComponentStatus) != 2 {
		t.Errorf("expected 2 component statuses, got %d", len(be.ComponentStatus))
	}
	if len(be.Logs) != 4 {
		t.Errorf("expected 4 log entries, got %d", len(be.Logs))
	}
}
