package retention

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// --- Mock storage implementations ---

type mockS3Storage struct {
	mu                sync.Mutex
	backupHistory     map[string]map[string]*models.BackupHistory
	orphaned          map[string]map[string]*models.BackupHistory
	incomplete        map[string]map[string]*models.BackupHistory
	latestBackupIDs   map[string]string
	listErr           error
	deleteErr         error
	moveErr           error
	incompleteListErr error
	orphanedListErr   error
}

func newMockS3Storage() *mockS3Storage {
	return &mockS3Storage{
		backupHistory:   make(map[string]map[string]*models.BackupHistory),
		orphaned:        make(map[string]map[string]*models.BackupHistory),
		incomplete:      make(map[string]map[string]*models.BackupHistory),
		latestBackupIDs: make(map[string]string),
	}
}

func (m *mockS3Storage) StoreLatestBackupID(camundaInstanceID, backupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestBackupIDs[camundaInstanceID] = backupID
	return nil
}

func (m *mockS3Storage) GetLatestBackupID(camundaInstanceID string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestBackupIDs[camundaInstanceID], nil
}

func (m *mockS3Storage) StoreBackupHistory(history *models.BackupHistory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.backupHistory[history.CamundaInstanceID] == nil {
		m.backupHistory[history.CamundaInstanceID] = make(map[string]*models.BackupHistory)
	}
	m.backupHistory[history.CamundaInstanceID][history.BackupID] = history
	return nil
}

func (m *mockS3Storage) GetBackupHistory(camundaInstanceID, backupID string) (*models.BackupHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, group := range []map[string]map[string]*models.BackupHistory{m.backupHistory, m.orphaned, m.incomplete} {
		if group[camundaInstanceID] == nil {
			continue
		}
		if h, ok := group[camundaInstanceID][backupID]; ok {
			return h, nil
		}
	}
	return nil, utils.ErrBackupNotFound
}

func (m *mockS3Storage) ListBackupHistory(camundaInstanceID string, status types.BackupStatus) ([]*models.BackupHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listErr != nil {
		return nil, m.listErr
	}
	var result []*models.BackupHistory
	for _, h := range m.backupHistory[camundaInstanceID] {
		if status == "" || h.Status == status {
			result = append(result, h)
		}
	}
	return result, nil
}

func (m *mockS3Storage) UpdateBackupStatus(camundaInstanceID, backupID string, status types.BackupStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.backupHistory[camundaInstanceID] != nil && m.backupHistory[camundaInstanceID][backupID] != nil {
		m.backupHistory[camundaInstanceID][backupID].Status = status
	}
	return nil
}

func (m *mockS3Storage) DeleteBackupHistory(camundaInstanceID, backupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	// Check main history
	if m.backupHistory[camundaInstanceID] != nil {
		if _, ok := m.backupHistory[camundaInstanceID][backupID]; ok {
			delete(m.backupHistory[camundaInstanceID], backupID)
			return nil
		}
	}
	// Check orphaned
	if m.orphaned[camundaInstanceID] != nil {
		if _, ok := m.orphaned[camundaInstanceID][backupID]; ok {
			delete(m.orphaned[camundaInstanceID], backupID)
			return nil
		}
	}
	// Check incomplete
	if m.incomplete[camundaInstanceID] != nil {
		if _, ok := m.incomplete[camundaInstanceID][backupID]; ok {
			delete(m.incomplete[camundaInstanceID], backupID)
			return nil
		}
	}
	return utils.ErrBackupNotFound
}

func (m *mockS3Storage) MoveToOrphaned(camundaInstanceID, backupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.moveErr != nil {
		return m.moveErr
	}
	if m.backupHistory[camundaInstanceID] == nil {
		return utils.ErrBackupNotFound
	}
	h, ok := m.backupHistory[camundaInstanceID][backupID]
	if !ok {
		return utils.ErrBackupNotFound
	}
	delete(m.backupHistory[camundaInstanceID], backupID)
	if m.orphaned[camundaInstanceID] == nil {
		m.orphaned[camundaInstanceID] = make(map[string]*models.BackupHistory)
	}
	m.orphaned[camundaInstanceID][backupID] = h
	return nil
}

func (m *mockS3Storage) MoveToIncomplete(camundaInstanceID, backupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.backupHistory[camundaInstanceID] == nil {
		return utils.ErrBackupNotFound
	}
	h, ok := m.backupHistory[camundaInstanceID][backupID]
	if !ok {
		return utils.ErrBackupNotFound
	}
	delete(m.backupHistory[camundaInstanceID], backupID)
	if m.incomplete[camundaInstanceID] == nil {
		m.incomplete[camundaInstanceID] = make(map[string]*models.BackupHistory)
	}
	m.incomplete[camundaInstanceID][backupID] = h
	return nil
}

func (m *mockS3Storage) ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.orphanedListErr != nil {
		return nil, m.orphanedListErr
	}
	var result []*models.BackupHistory
	for _, h := range m.orphaned[camundaInstanceID] {
		result = append(result, h)
	}
	return result, nil
}

func (m *mockS3Storage) ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.incompleteListErr != nil {
		return nil, m.incompleteListErr
	}
	var result []*models.BackupHistory
	for _, h := range m.incomplete[camundaInstanceID] {
		result = append(result, h)
	}
	return result, nil
}

func (m *mockS3Storage) addBackup(instanceID, backupID string, status types.BackupStatus, startTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.backupHistory[instanceID] == nil {
		m.backupHistory[instanceID] = make(map[string]*models.BackupHistory)
	}
	m.backupHistory[instanceID][backupID] = &models.BackupHistory{
		CamundaInstanceID: instanceID,
		BackupID:          backupID,
		Status:            status,
		StartTime:         startTime,
	}
}

func (m *mockS3Storage) addIncomplete(instanceID, backupID string, startTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.incomplete[instanceID] == nil {
		m.incomplete[instanceID] = make(map[string]*models.BackupHistory)
	}
	m.incomplete[instanceID][backupID] = &models.BackupHistory{
		CamundaInstanceID: instanceID,
		BackupID:          backupID,
		Status:            types.BackupStatusIncomplete,
		StartTime:         startTime,
	}
}

func (m *mockS3Storage) addOrphaned(instanceID, backupID string, startTime time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.orphaned[instanceID] == nil {
		m.orphaned[instanceID] = make(map[string]*models.BackupHistory)
	}
	m.orphaned[instanceID][backupID] = &models.BackupHistory{
		CamundaInstanceID: instanceID,
		BackupID:          backupID,
		Status:            types.BackupStatusCompleted,
		StartTime:         startTime,
	}
}

type mockFileStorage struct {
	mu       sync.Mutex
	logFiles map[string][]string
	cleanErr error
}

func newMockFileStorage() *mockFileStorage {
	return &mockFileStorage{
		logFiles: make(map[string][]string),
	}
}

func (m *mockFileStorage) SaveConfiguration(config *models.Configuration) error { return nil }
func (m *mockFileStorage) LoadConfiguration() (*models.Configuration, error)    { return nil, nil }
func (m *mockFileStorage) CreateLogFile(camundaInstanceID, backupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logFiles[camundaInstanceID] = append(m.logFiles[camundaInstanceID], backupID)
	return nil
}
func (m *mockFileStorage) WriteToLogFile(camundaInstanceID, backupID, message string) error {
	return nil
}
func (m *mockFileStorage) ReadLogFile(camundaInstanceID, backupID string) (string, error) {
	return "", nil
}
func (m *mockFileStorage) DeleteLogFile(camundaInstanceID, backupID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	files := m.logFiles[camundaInstanceID]
	for i, f := range files {
		if f == backupID {
			m.logFiles[camundaInstanceID] = append(files[:i], files[i+1:]...)
			return nil
		}
	}
	return nil
}

func (m *mockFileStorage) ListLogFiles(camundaInstanceID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files := make([]string, len(m.logFiles[camundaInstanceID]))
	copy(files, m.logFiles[camundaInstanceID])
	return files, nil
}

func (m *mockFileStorage) CleanupOldLogFiles(camundaInstanceID string, keepCount int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cleanErr != nil {
		return m.cleanErr
	}
	files := m.logFiles[camundaInstanceID]
	if len(files) > keepCount {
		m.logFiles[camundaInstanceID] = files[len(files)-keepCount:]
	}
	return nil
}

// mockInstanceProvider resolves instances for DeleteBackup.
type mockInstanceProvider struct {
	instances map[string]*models.CamundaInstance
	err       error
}

func newMockInstanceProvider() *mockInstanceProvider {
	return &mockInstanceProvider{instances: make(map[string]*models.CamundaInstance)}
}

func (m *mockInstanceProvider) GetInstance(id string) (*models.CamundaInstance, error) {
	if m.err != nil {
		return nil, m.err
	}
	if inst, ok := m.instances[id]; ok {
		return inst, nil
	}
	return nil, utils.ErrCamundaInstanceNotFound
}

func newTestManager() (*Manager, *mockS3Storage, *mockFileStorage) {
	mgr, s3, fs, _ := newTestManagerWithInstances()
	return mgr, s3, fs
}

// newTestManagerWithInstances wires a manager with an instance provider that
// already knows about "inst-1" with no component endpoints configured.
func newTestManagerWithInstances() (*Manager, *mockS3Storage, *mockFileStorage, *mockInstanceProvider) {
	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	logger := utils.NewLogger("debug")
	mgr := NewManager(s3, fs, nil, nil, logger)
	instances := newMockInstanceProvider()
	instances.instances["inst-1"] = &models.CamundaInstance{ID: "inst-1"}
	mgr.SetInstanceProvider(instances)
	return mgr, s3, fs, instances
}

func testInstance(id string, successRetention, failureRetention int) *models.CamundaInstance {
	return &models.CamundaInstance{
		ID:               id,
		SuccessRetention: successRetention,
		FailureRetention: failureRetention,
	}
}

// --- Tests ---

func TestNewManager(t *testing.T) {
	mgr, _, _ := newTestManager()
	if mgr == nil {
		t.Fatal("expected non-nil manager")
	}
}

func TestApplyRetention_KeepLastN_NothingToPrune(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-3*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b3", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	result := mgr.ApplyRetention(testInstance("inst-1", 5, 5))
	if len(result.DeletedSuccessful) != 0 {
		t.Errorf("expected 0 deleted, got %d", len(result.DeletedSuccessful))
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected 0 errors, got %v", result.Errors)
	}
}

func TestApplyRetention_KeepLastN_PrunesOldest(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-5*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-4*time.Hour))
	s3.addBackup("inst-1", "b3", types.BackupStatusCompleted, now.Add(-3*time.Hour))
	s3.addBackup("inst-1", "b4", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b5", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	result := mgr.ApplyRetention(testInstance("inst-1", 2, 2))
	if len(result.DeletedSuccessful) != 3 {
		t.Errorf("expected 3 deleted, got %d: %v", len(result.DeletedSuccessful), result.DeletedSuccessful)
	}
	deletedSet := make(map[string]bool)
	for _, id := range result.DeletedSuccessful {
		deletedSet[id] = true
	}
	for _, expected := range []string{"b1", "b2", "b3"} {
		if !deletedSet[expected] {
			t.Errorf("expected %s to be deleted", expected)
		}
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusCompleted)
	if len(remaining) != 2 {
		t.Errorf("expected 2 remaining completed, got %d", len(remaining))
	}
}

func TestApplyRetention_KeepLastN_NeverDeletesNewest(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	result := mgr.ApplyRetention(testInstance("inst-1", 1, 1))
	if len(result.DeletedSuccessful) != 1 {
		t.Fatalf("expected 1 deleted, got %d", len(result.DeletedSuccessful))
	}
	if result.DeletedSuccessful[0] != "b1" {
		t.Errorf("expected b1 to be deleted, got %s", result.DeletedSuccessful[0])
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusCompleted)
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining, got %d", len(remaining))
	}
	if remaining[0].BackupID != "b2" {
		t.Errorf("expected b2 to remain, got %s", remaining[0].BackupID)
	}
}

func TestApplyRetention_KeepLastN_ZeroRetention(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now)
	result := mgr.ApplyRetention(testInstance("inst-1", 0, 0))
	if len(result.DeletedSuccessful) != 0 {
		t.Errorf("expected 0 deleted for zero retention, got %d", len(result.DeletedSuccessful))
	}
}

func TestApplyRetention_CleanupIncomplete_WithNewerCompleted(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b-completed", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	s3.addIncomplete("inst-1", "b-incomplete-old", now.Add(-3*time.Hour))
	s3.addIncomplete("inst-1", "b-incomplete-newer", now)
	result := mgr.ApplyRetention(testInstance("inst-1", 10, 10))
	if len(result.CleanedIncomplete) != 1 {
		t.Fatalf("expected 1 cleaned incomplete, got %d: %v", len(result.CleanedIncomplete), result.CleanedIncomplete)
	}
	if result.CleanedIncomplete[0] != "b-incomplete-old" {
		t.Errorf("expected b-incomplete-old to be cleaned, got %s", result.CleanedIncomplete[0])
	}
}

func TestApplyRetention_CleanupIncomplete_NoCompletedBackups(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addIncomplete("inst-1", "b-incomplete", now)
	result := mgr.ApplyRetention(testInstance("inst-1", 10, 10))
	if len(result.CleanedIncomplete) != 0 {
		t.Errorf("expected 0 cleaned incomplete when no completed backups exist, got %d", len(result.CleanedIncomplete))
	}
}

func TestApplyRetention_CleanupLogFiles(t *testing.T) {
	mgr, _, fs := newTestManager()
	for i := 0; i < 5; i++ {
		fs.CreateLogFile("inst-1", fmt.Sprintf("backup-%d", i))
	}
	result := mgr.ApplyRetention(testInstance("inst-1", 2, 2))
	if result.LogFilesRemoved != 1 {
		t.Errorf("expected 1 log files removed, got %d", result.LogFilesRemoved)
	}
	remaining, _ := fs.ListLogFiles("inst-1")
	if len(remaining) != 4 {
		t.Errorf("expected 4 remaining log files, got %d", len(remaining))
	}
}

func TestApplyRetention_ErrorInListBackupHistory(t *testing.T) {
	mgr, s3, _ := newTestManager()
	s3.listErr = fmt.Errorf("S3 unavailable")
	result := mgr.ApplyRetention(testInstance("inst-1", 5, 5))
	if len(result.Errors) == 0 {
		t.Error("expected at least one error when S3 is unavailable")
	}
}

func TestApplyRetention_ErrorInDeleteBackupHistory(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-3*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b3", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	s3.deleteErr = fmt.Errorf("permission denied")
	result := mgr.ApplyRetention(testInstance("inst-1", 2, 2))
	if len(result.Errors) == 0 {
		t.Error("expected errors when DeleteBackupHistory fails")
	}
	if len(result.DeletedSuccessful) != 0 {
		t.Errorf("expected 0 deleted on delete error, got %d", len(result.DeletedSuccessful))
	}
}

func TestApplyRetention_LogFileCleanupError(t *testing.T) {
	mgr, _, fs := newTestManager()
	fs.cleanErr = fmt.Errorf("disk error")
	fs.CreateLogFile("inst-1", "backup-1")
	result := mgr.ApplyRetention(testInstance("inst-1", 1, 1))
	if len(result.Errors) == 0 {
		t.Fatal("expected at least one error from log cleanup failure")
	}
	hasLogErr := false
	for _, e := range result.Errors {
		if strings.Contains(e, "disk error") {
			hasLogErr = true
			break
		}
	}
	if !hasLogErr {
		t.Errorf("expected an error containing 'disk error', got: %v", result.Errors)
	}
}

func TestDeleteBackup_Success(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b1", false)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusCompleted)
	if len(remaining) != 1 {
		t.Errorf("expected 1 remaining, got %d", len(remaining))
	}
}

func TestDeleteBackup_RefusesMostRecentCompleted(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b2", false)
	if err == nil {
		t.Fatal("expected error when deleting most recent successful backup")
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusCompleted)
	if len(remaining) != 2 {
		t.Errorf("expected both backups to remain, got %d", len(remaining))
	}
}

func TestDeleteBackup_NotFound(t *testing.T) {
	mgr, _, _ := newTestManager()
	err := mgr.DeleteBackup("inst-1", "nonexistent", false)
	if err != utils.ErrBackupNotFound {
		t.Errorf("expected ErrBackupNotFound, got %v", err)
	}
}

func TestDeleteBackup_FromOrphaned(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addOrphaned("inst-1", "b-orphaned", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b-orphaned", false)
	if err != nil {
		t.Fatalf("expected nil error deleting orphaned backup, got %v", err)
	}
}

func TestDeleteBackup_FromIncomplete(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addIncomplete("inst-1", "b-incomplete", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b-incomplete", false)
	if err != nil {
		t.Fatalf("expected nil error deleting incomplete backup, got %v", err)
	}
}

func TestDeleteBackup_OnlyOneCompletedBackup(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now)
	err := mgr.DeleteBackup("inst-1", "b1", false)
	if err == nil {
		t.Fatal("expected error when deleting the only completed backup")
	}
}

func TestDeleteBackup_FailedBackupCanBeDeleted(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b-failed", types.BackupStatusFailed, now)
	err := mgr.DeleteBackup("inst-1", "b-failed", false)
	if err != nil {
		t.Fatalf("expected nil error deleting failed backup, got %v", err)
	}
}

func TestListOrphanedBackups(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addOrphaned("inst-1", "o1", now.Add(-2*time.Hour))
	s3.addOrphaned("inst-1", "o2", now.Add(-1*time.Hour))
	orphaned, err := mgr.ListOrphanedBackups("inst-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(orphaned) != 2 {
		t.Errorf("expected 2 orphaned backups, got %d", len(orphaned))
	}
}

func TestListIncompleteBackups(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addIncomplete("inst-1", "i1", now)
	incomplete, err := mgr.ListIncompleteBackups("inst-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(incomplete) != 1 {
		t.Errorf("expected 1 incomplete backup, got %d", len(incomplete))
	}
}

func TestListFailedBackups(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "f1", types.BackupStatusFailed, now)
	s3.addBackup("inst-1", "c1", types.BackupStatusCompleted, now)
	failed, err := mgr.ListFailedBackups("inst-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed backup, got %d", len(failed))
	}
	if failed[0].BackupID != "f1" {
		t.Errorf("expected f1, got %s", failed[0].BackupID)
	}
}

func TestApplyRetention_EmptyInstance(t *testing.T) {
	mgr, _, _ := newTestManager()
	result := mgr.ApplyRetention(testInstance("inst-empty", 5, 5))
	if len(result.DeletedSuccessful) != 0 {
		t.Errorf("expected 0 deleted, got %d", len(result.DeletedSuccessful))
	}
	if len(result.DeletedFailed) != 0 {
		t.Errorf("expected 0 deleted failed, got %d", len(result.DeletedFailed))
	}
	if len(result.CleanedIncomplete) != 0 {
		t.Errorf("expected 0 cleaned, got %d", len(result.CleanedIncomplete))
	}
	if result.LogFilesRemoved != 0 {
		t.Errorf("expected 0 log files removed, got %d", result.LogFilesRemoved)
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected 0 errors, got %v", result.Errors)
	}
}

func TestApplyRetention_FailedBackupsPruned(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	// A successful backup must exist (and be newer) for the safety guard to allow pruning
	s3.addBackup("inst-1", "c1", types.BackupStatusCompleted, now)
	s3.addBackup("inst-1", "f1", types.BackupStatusFailed, now.Add(-3*time.Hour))
	s3.addBackup("inst-1", "f2", types.BackupStatusFailed, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "f3", types.BackupStatusFailed, now.Add(-1*time.Hour))
	result := mgr.ApplyRetention(testInstance("inst-1", 1, 1))
	if len(result.DeletedFailed) != 2 {
		t.Errorf("expected 2 deleted failed, got %d: %v", len(result.DeletedFailed), result.DeletedFailed)
	}
	deletedSet := make(map[string]bool)
	for _, id := range result.DeletedFailed {
		deletedSet[id] = true
	}
	for _, expected := range []string{"f1", "f2"} {
		if !deletedSet[expected] {
			t.Errorf("expected %s to be deleted", expected)
		}
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusFailed)
	if len(remaining) != 1 {
		t.Errorf("expected 1 remaining failed, got %d", len(remaining))
	}
}

func TestApplyRetention_FailedBackupsKeptWhenNoSuccessful(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "f1", types.BackupStatusFailed, now.Add(-3*time.Hour))
	s3.addBackup("inst-1", "f2", types.BackupStatusFailed, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "f3", types.BackupStatusFailed, now.Add(-1*time.Hour))
	result := mgr.ApplyRetention(testInstance("inst-1", 1, 1))
	if len(result.DeletedFailed) != 0 {
		t.Errorf("expected 0 deleted failed (no successful backup exists), got %d: %v", len(result.DeletedFailed), result.DeletedFailed)
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusFailed)
	if len(remaining) != 3 {
		t.Errorf("expected all 3 failed backups kept, got %d", len(remaining))
	}
}

func TestApplyRetention_SortOrderDeterministic(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	for i := 0; i < 10; i++ {
		s3.addBackup("inst-1", fmt.Sprintf("b%02d", i), types.BackupStatusCompleted, now.Add(time.Duration(-10+i)*time.Hour))
	}
	result := mgr.ApplyRetention(testInstance("inst-1", 3, 3))
	if len(result.DeletedSuccessful) != 7 {
		t.Fatalf("expected 7 deleted, got %d: %v", len(result.DeletedSuccessful), result.DeletedSuccessful)
	}
	sort.Strings(result.DeletedSuccessful)
	for i, expected := range []string{"b00", "b01", "b02", "b03", "b04", "b05", "b06"} {
		if result.DeletedSuccessful[i] != expected {
			t.Errorf("deleted[%d]: expected %s, got %s", i, expected, result.DeletedSuccessful[i])
		}
	}
}

func TestDeleteBackup_ListErrorPreventsDelete(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now)
	s3.listErr = fmt.Errorf("S3 error")
	err := mgr.DeleteBackup("inst-1", "b1", false)
	if err == nil {
		t.Fatal("expected error when ListBackupHistory fails")
	}
}

// --- Additional coverage tests for cleanupIncompleteBackups ---

func TestCleanupIncompleteBackups_ListIncompleteError(t *testing.T) {
	mgr, s3, _ := newTestManager()
	s3.incompleteListErr = fmt.Errorf("S3 list incomplete error")
	result := mgr.ApplyRetention(testInstance("inst-1", 5, 5))
	hasErr := false
	for _, e := range result.Errors {
		if strings.Contains(e, "failed to list incomplete backups") {
			hasErr = true
			break
		}
	}
	if !hasErr {
		t.Errorf("expected error about listing incomplete backups, got: %v", result.Errors)
	}
}

func TestCleanupIncompleteBackups_ListCompletedErrorDuringCleanup(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	// Add an incomplete backup so the cleanup phase is entered
	s3.addIncomplete("inst-1", "b-inc", now.Add(-1*time.Hour))
	// Set listErr so ListBackupHistory (for completed) fails during cleanup.
	// Note: this also affects pruneByStatus but that's OK — both will record errors.
	s3.listErr = fmt.Errorf("S3 list completed error")
	result := mgr.ApplyRetention(testInstance("inst-1", 5, 5))
	hasErr := false
	for _, e := range result.Errors {
		if strings.Contains(e, "failed to list completed backups for incomplete cleanup") {
			hasErr = true
			break
		}
	}
	if !hasErr {
		t.Errorf("expected error about listing completed backups for incomplete cleanup, got: %v", result.Errors)
	}
}

func TestCleanupIncompleteBackups_DeleteError(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b-completed", types.BackupStatusCompleted, now)
	s3.addIncomplete("inst-1", "b-inc-old", now.Add(-2*time.Hour))
	s3.deleteErr = fmt.Errorf("delete permission denied")
	result := mgr.ApplyRetention(testInstance("inst-1", 5, 5))
	hasErr := false
	for _, e := range result.Errors {
		if strings.Contains(e, "failed to delete incomplete backup") {
			hasErr = true
			break
		}
	}
	if !hasErr {
		t.Errorf("expected error about deleting incomplete backup, got: %v", result.Errors)
	}
	if len(result.CleanedIncomplete) != 0 {
		t.Errorf("expected 0 cleaned incomplete on delete error, got %d", len(result.CleanedIncomplete))
	}
}

func TestCleanupIncompleteBackups_IncompleteNewerThanCompleted(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	// Completed backup is older than the incomplete
	s3.addBackup("inst-1", "b-completed", types.BackupStatusCompleted, now.Add(-3*time.Hour))
	s3.addIncomplete("inst-1", "b-inc-newer", now)
	result := mgr.ApplyRetention(testInstance("inst-1", 5, 5))
	// The incomplete is newer than the most recent completed, so it should NOT be cleaned
	if len(result.CleanedIncomplete) != 0 {
		t.Errorf("expected 0 cleaned incomplete (newer than completed), got %d: %v",
			len(result.CleanedIncomplete), result.CleanedIncomplete)
	}
	if len(result.Errors) != 0 {
		t.Errorf("expected 0 errors, got %v", result.Errors)
	}
}

// --- Additional coverage tests for DeleteBackup ---

func TestDeleteBackup_RecordDeleteError(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addOrphaned("inst-1", "b-orphan", now.Add(-5*time.Hour))
	s3.deleteErr = fmt.Errorf("delete failed")
	err := mgr.DeleteBackup("inst-1", "b-orphan", false)
	if err == nil {
		t.Fatal("expected error when DeleteBackupHistory fails")
	}
	if !strings.Contains(err.Error(), "failed to delete backup record") {
		t.Errorf("expected 'failed to delete backup record' error, got: %v", err)
	}
}

func TestDeleteBackup_NotFoundInAnyDirectory(t *testing.T) {
	mgr, _, _ := newTestManager()
	err := mgr.DeleteBackup("inst-1", "nonexistent", false)
	if !errors.Is(err, utils.ErrBackupNotFound) {
		t.Errorf("expected ErrBackupNotFound, got %v", err)
	}
}

func TestDeleteBackup_NoCompletedBackups_AllowsDelete(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	// Only orphaned backups exist (no completed)
	s3.addOrphaned("inst-1", "b-orphan", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b-orphan", false)
	if err != nil {
		t.Fatalf("expected nil error when no completed backups exist, got %v", err)
	}
}

func TestDeleteBackup_NoInstanceProvider(t *testing.T) {
	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	mgr := NewManager(s3, fs, nil, nil, utils.NewLogger("debug"))
	s3.addBackup("inst-1", "b-failed", types.BackupStatusFailed, time.Now())

	err := mgr.DeleteBackup("inst-1", "b-failed", false)
	if !errors.Is(err, utils.ErrInstanceProviderNotConfigured) {
		t.Fatalf("expected ErrInstanceProviderNotConfigured, got %v", err)
	}
	if _, getErr := s3.GetBackupHistory("inst-1", "b-failed"); getErr != nil {
		t.Error("expected the backup record to survive when the instance cannot be resolved")
	}
}

func TestDeleteBackup_UnknownInstance(t *testing.T) {
	mgr, s3, _ := newTestManager()
	s3.addBackup("inst-other", "b-failed", types.BackupStatusFailed, time.Now())

	err := mgr.DeleteBackup("inst-other", "b-failed", false)
	if !errors.Is(err, utils.ErrCamundaInstanceNotFound) {
		t.Fatalf("expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

// --- Artifact deletion on manual delete ---

// artifactServers stands in for the Camunda components and Elasticsearch,
// recording every DELETE it receives.
type artifactServers struct {
	server  *httptest.Server
	mu      sync.Mutex
	deletes []string
	status  int
}

func newArtifactServers() *artifactServers {
	a := &artifactServers{status: http.StatusOK}
	a.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusOK)
			return
		}
		a.mu.Lock()
		a.deletes = append(a.deletes, r.URL.Path)
		status := a.status
		a.mu.Unlock()
		w.WriteHeader(status)
		w.Write([]byte(`{"acknowledged":true}`))
	}))
	return a
}

func (a *artifactServers) recorded() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]string, len(a.deletes))
	copy(out, a.deletes)
	sort.Strings(out)
	return out
}

func (a *artifactServers) setStatus(code int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.status = code
}

func (a *artifactServers) Close() { a.server.Close() }

// newArtifactTestManager wires a manager whose instance points every component
// and Elasticsearch at the same recording server.
func newArtifactTestManager(t *testing.T) (*Manager, *mockS3Storage, *mockFileStorage, *artifactServers) {
	t.Helper()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:    5 * time.Second,
		MaxRetries: 0,
	}, logger)

	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	cfg := &config.Config{DefaultElasticsearchSnapshotRepository: "test-repo"}
	mgr := NewManager(s3, fs, httpClient, cfg, logger)

	servers := newArtifactServers()
	instances := newMockInstanceProvider()
	instances.instances["inst-1"] = &models.CamundaInstance{
		ID:                     "inst-1",
		ZeebeBackupEndpoint:    servers.server.URL + "/zeebe/backups",
		OperateBackupEndpoint:  servers.server.URL + "/operate/backups",
		TasklistBackupEndpoint: servers.server.URL + "/tasklist/backups",
		OptimizeBackupEndpoint: servers.server.URL + "/optimize/backups",
		ElasticsearchEndpoint:  servers.server.URL,
	}
	mgr.SetInstanceProvider(instances)

	return mgr, s3, fs, servers
}

func allComponentsCompleted() map[string]models.ComponentBackupInfo {
	return map[string]models.ComponentBackupInfo{
		types.ComponentZeebe:    {Enabled: true, Status: types.ComponentStatusCompleted},
		types.ComponentOperate:  {Enabled: true, Status: types.ComponentStatusCompleted},
		types.ComponentTasklist: {Enabled: true, Status: types.ComponentStatusCompleted},
		types.ComponentOptimize: {Enabled: true, Status: types.ComponentStatusCompleted},
		types.ComponentElasticsearch: {
			Enabled:            true,
			Status:             types.ComponentStatusCompleted,
			SnapshotRepository: "test-repo",
			SnapshotName:       "snap-b-old",
		},
	}
}

func TestDeleteBackup_DeletesArtifactsEverywhere(t *testing.T) {
	mgr, s3, fs, servers := newArtifactTestManager(t)
	defer servers.Close()

	now := time.Now()
	s3.addBackup("inst-1", "b-old", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b-new", types.BackupStatusCompleted, now)
	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b-old"].Components = allComponentsCompleted()
	s3.mu.Unlock()
	fs.logFiles["inst-1"] = []string{"b-old"}

	if err := mgr.DeleteBackup("inst-1", "b-old", false); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}

	want := []string{
		"/_snapshot/test-repo/snap-b-old",
		"/operate/backups/b-old",
		"/optimize/backups/b-old",
		"/tasklist/backups/b-old",
		"/zeebe/backups/b-old",
	}
	got := servers.recorded()
	if len(got) != len(want) {
		t.Fatalf("expected %d DELETE calls %v, got %d: %v", len(want), want, len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("delete[%d]: expected %s, got %s", i, want[i], got[i])
		}
	}

	if _, err := s3.GetBackupHistory("inst-1", "b-old"); !errors.Is(err, utils.ErrBackupNotFound) {
		t.Error("expected the metadata record to be gone")
	}
	if len(fs.logFiles["inst-1"]) != 0 {
		t.Errorf("expected the log file to be deleted, got %v", fs.logFiles["inst-1"])
	}
}

func TestDeleteBackup_SkipsSkippedComponents(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()

	now := time.Now()
	s3.addBackup("inst-1", "b-old", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b-new", types.BackupStatusCompleted, now)
	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b-old"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentZeebe:         {Enabled: true, Status: types.ComponentStatusCompleted},
		types.ComponentOperate:       {Enabled: false, Status: types.ComponentStatusSkipped},
		types.ComponentTasklist:      {Enabled: true, Status: types.ComponentStatusSkipped},
		types.ComponentOptimize:      {Enabled: false, Status: types.ComponentStatusSkipped},
		types.ComponentElasticsearch: {Enabled: false, Status: types.ComponentStatusSkipped},
	}
	s3.mu.Unlock()

	if err := mgr.DeleteBackup("inst-1", "b-old", false); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}

	got := servers.recorded()
	if len(got) != 1 || got[0] != "/zeebe/backups/b-old" {
		t.Errorf("expected only the Zeebe backup to be deleted, got %v", got)
	}
}

// An interrupted backup can leave artifacts it never recorded, so unrecorded
// components are still purged.
func TestDeleteBackup_PurgesComponentsMissingFromRecord(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()

	s3.addIncomplete("inst-1", "b-inc", time.Now().Add(-2*time.Hour))
	s3.mu.Lock()
	s3.incomplete["inst-1"]["b-inc"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentZeebe: {Enabled: true, Status: types.ComponentStatusCompleted},
	}
	s3.mu.Unlock()

	if err := mgr.DeleteBackup("inst-1", "b-inc", false); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}

	if got := servers.recorded(); len(got) != 5 {
		t.Errorf("expected all 5 components to be purged, got %v", got)
	}
}

func TestDeleteBackup_KeepsRecordWhenArtifactDeletionFails(t *testing.T) {
	mgr, s3, fs, servers := newArtifactTestManager(t)
	defer servers.Close()
	servers.setStatus(http.StatusInternalServerError)

	now := time.Now()
	s3.addBackup("inst-1", "b-old", types.BackupStatusFailed, now.Add(-2*time.Hour))
	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b-old"].Components = allComponentsCompleted()
	s3.mu.Unlock()
	fs.logFiles["inst-1"] = []string{"b-old"}

	err := mgr.DeleteBackup("inst-1", "b-old", false)
	if !errors.Is(err, utils.ErrBackupArtifactsRemain) {
		t.Fatalf("expected ErrBackupArtifactsRemain, got %v", err)
	}
	if !strings.Contains(err.Error(), "Zeebe") {
		t.Errorf("expected the error to name the failing component, got: %v", err)
	}
	if _, getErr := s3.GetBackupHistory("inst-1", "b-old"); getErr != nil {
		t.Error("expected the metadata record to survive so the delete can be retried")
	}
	if len(fs.logFiles["inst-1"]) != 1 {
		t.Error("expected the log file to survive alongside the record")
	}
}

func TestDeleteBackup_ForceDeletesRecordDespiteArtifactFailure(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()
	servers.setStatus(http.StatusInternalServerError)

	s3.addBackup("inst-1", "b-old", types.BackupStatusFailed, time.Now().Add(-2*time.Hour))
	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b-old"].Components = allComponentsCompleted()
	s3.mu.Unlock()

	if err := mgr.DeleteBackup("inst-1", "b-old", true); err != nil {
		t.Fatalf("expected force delete to succeed, got %v", err)
	}
	if _, err := s3.GetBackupHistory("inst-1", "b-old"); !errors.Is(err, utils.ErrBackupNotFound) {
		t.Error("expected the metadata record to be gone after a force delete")
	}
}

// A component that has already lost the backup answers 404; that is a success,
// not a reason to keep the record.
func TestDeleteBackup_TreatsMissingArtifactsAsDeleted(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()
	servers.setStatus(http.StatusNotFound)

	s3.addBackup("inst-1", "b-old", types.BackupStatusFailed, time.Now().Add(-2*time.Hour))
	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b-old"].Components = allComponentsCompleted()
	s3.mu.Unlock()

	if err := mgr.DeleteBackup("inst-1", "b-old", false); err != nil {
		t.Fatalf("expected 404s to count as deleted, got %v", err)
	}
}

func TestApplyRetention_CleanupIncomplete_PurgesArtifacts(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()

	now := time.Now()
	s3.addBackup("inst-1", "b-completed", types.BackupStatusCompleted, now)
	s3.addIncomplete("inst-1", "b-inc", now.Add(-2*time.Hour))
	s3.mu.Lock()
	s3.incomplete["inst-1"]["b-inc"].Components = allComponentsCompleted()
	s3.mu.Unlock()

	instance := &models.CamundaInstance{
		ID:                     "inst-1",
		SuccessRetention:       5,
		FailureRetention:       5,
		ZeebeBackupEndpoint:    servers.server.URL + "/zeebe/backups",
		OperateBackupEndpoint:  servers.server.URL + "/operate/backups",
		TasklistBackupEndpoint: servers.server.URL + "/tasklist/backups",
		OptimizeBackupEndpoint: servers.server.URL + "/optimize/backups",
		ElasticsearchEndpoint:  servers.server.URL,
	}

	result := mgr.ApplyRetention(instance)
	if len(result.CleanedIncomplete) != 1 {
		t.Fatalf("expected 1 cleaned incomplete backup, got %v (errors: %v)", result.CleanedIncomplete, result.Errors)
	}
	if got := servers.recorded(); len(got) != 5 {
		t.Errorf("expected the incomplete backup's artifacts to be purged, got %v", got)
	}
}

// --- Alerter tests ---

func TestSetAlerter(t *testing.T) {
	mgr, _, _ := newTestManager()
	if mgr.alerter != nil {
		t.Fatal("expected nil alerter initially")
	}
	alerter := utils.NewAlerter("http://example.com", utils.NewLogger("info"))
	mgr.SetAlerter(alerter)
	if mgr.alerter == nil {
		t.Fatal("expected non-nil alerter after SetAlerter")
	}
}

func TestDeleteComponentBackup_AlertsOnBadStatus(t *testing.T) {
	// Component server returns 500 for DELETE requests
	componentServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer componentServer.Close()

	// Alert webhook captures alerts
	var alertCount int32
	alertServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&alertCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer alertServer.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:    5 * time.Second,
		MaxRetries: 0,
	}, logger)

	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	mgr := NewManager(s3, fs, httpClient, nil, logger)

	alerter := utils.NewAlerter(alertServer.URL, logger)
	mgr.SetAlerter(alerter)

	now := time.Now()
	instance := &models.CamundaInstance{
		ID:                    "inst-1",
		SuccessRetention:      1,
		FailureRetention:      1,
		ZeebeBackupEndpoint:   componentServer.URL + "/zeebe",
	}

	// Add 3 completed backups so retention prunes the oldest
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-3*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b3", types.BackupStatusCompleted, now.Add(-1*time.Hour))

	// Add Zeebe component info to backups that will be pruned
	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b1"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentZeebe: {Enabled: true, Status: types.ComponentStatusCompleted},
	}
	s3.backupHistory["inst-1"]["b2"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentZeebe: {Enabled: true, Status: types.ComponentStatusCompleted},
	}
	s3.mu.Unlock()

	mgr.ApplyRetention(instance)

	// Wait for async alert delivery
	time.Sleep(500 * time.Millisecond)

	count := atomic.LoadInt32(&alertCount)
	if count < 1 {
		t.Errorf("expected at least 1 cleanup alert for bad HTTP status, got %d", count)
	}
}

func TestDeleteComponentBackup_NoAlertWhenAlerterNil(t *testing.T) {
	// Component server returns 500 for DELETE requests
	componentServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer componentServer.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:    5 * time.Second,
		MaxRetries: 0,
	}, logger)

	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	mgr := NewManager(s3, fs, httpClient, nil, logger)
	// No alerter set — should not panic

	now := time.Now()
	instance := &models.CamundaInstance{
		ID:                    "inst-1",
		SuccessRetention:      1,
		FailureRetention:      1,
		ZeebeBackupEndpoint:   componentServer.URL + "/zeebe",
	}

	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))

	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b1"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentZeebe: {Enabled: true, Status: types.ComponentStatusCompleted},
	}
	s3.mu.Unlock()

	// Should not panic with nil alerter
	result := mgr.ApplyRetention(instance)
	if len(result.Errors) == 0 {
		t.Error("expected errors from failed component deletion")
	}
}

func TestDeleteESSnapshot_AlertsOnError(t *testing.T) {
	// ES server that returns 500 for snapshot deletion
	esServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "snapshot deletion failed"})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer esServer.Close()

	// Alert webhook
	var alertCount int32
	alertServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&alertCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer alertServer.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:    5 * time.Second,
		MaxRetries: 0,
	}, logger)

	cfg := &config.Config{
		DefaultElasticsearchSnapshotRepository: "test-repo",
	}

	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	mgr := NewManager(s3, fs, httpClient, cfg, logger)

	alerter := utils.NewAlerter(alertServer.URL, logger)
	mgr.SetAlerter(alerter)

	now := time.Now()
	instance := &models.CamundaInstance{
		ID:                      "inst-1",
		SuccessRetention:        1,
		FailureRetention:        1,
		ElasticsearchEndpoint:   esServer.URL,
		ElasticsearchUsername:   "elastic",
	}

	// Add 2 completed backups with ES component so oldest gets pruned
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))

	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b1"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentElasticsearch: {
			Enabled:            true,
			Status:             types.ComponentStatusCompleted,
			SnapshotRepository: "test-repo",
			SnapshotName:       "b1",
		},
	}
	s3.mu.Unlock()

	mgr.ApplyRetention(instance)

	// Wait for async alert
	time.Sleep(500 * time.Millisecond)

	count := atomic.LoadInt32(&alertCount)
	if count < 1 {
		t.Errorf("expected at least 1 cleanup alert for ES snapshot deletion failure, got %d", count)
	}
}

func TestDeleteESSnapshot_UsesInstanceRepository(t *testing.T) {
	deletedRepo := ""
	esServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/_snapshot/") {
			parts := strings.Split(r.URL.Path, "/")
			if len(parts) >= 3 {
				deletedRepo = parts[2]
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"acknowledged":true}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer esServer.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:    5 * time.Second,
		MaxRetries: 0,
	}, logger)

	cfg := &config.Config{
		DefaultElasticsearchSnapshotRepository: "",
	}

	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	mgr := NewManager(s3, fs, httpClient, cfg, logger)

	now := time.Now()
	instance := &models.CamundaInstance{
		ID:                              "inst-repo",
		SuccessRetention:                1,
		FailureRetention:                1,
		ElasticsearchEndpoint:           esServer.URL,
		ElasticsearchSnapshotRepository: "instance-repo",
	}

	s3.addBackup("inst-repo", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-repo", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))

	s3.mu.Lock()
	s3.backupHistory["inst-repo"]["b1"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentElasticsearch: {
			Enabled:      true,
			Status:       types.ComponentStatusCompleted,
			SnapshotName: "b1",
		},
	}
	s3.mu.Unlock()

	mgr.ApplyRetention(instance)

	time.Sleep(200 * time.Millisecond)

	if deletedRepo != "instance-repo" {
		t.Errorf("expected ES snapshot deleted from 'instance-repo', got %q", deletedRepo)
	}
}

func (m *mockS3Storage) ListAllBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var all []*models.BackupHistory
	for _, group := range []map[string]map[string]*models.BackupHistory{m.backupHistory, m.orphaned, m.incomplete} {
		for _, h := range group[camundaInstanceID] {
			all = append(all, h)
		}
	}
	return all, nil
}

func (m *mockS3Storage) StoreReconcileReport(camundaInstanceID string, report []byte) error {
	return nil
}

func (m *mockS3Storage) GetLatestReconcileReport(camundaInstanceID string) ([]byte, error) {
	return nil, utils.ErrBackupNotFound
}
