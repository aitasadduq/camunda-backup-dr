package retention

import (
	"context"
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
	mu sync.Mutex
	// historyErr makes a record lookup fail for a reason other than not-found,
	// so a guard that must not read that as "no record exists" can be tested.
	historyErr        error
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
	if m.historyErr != nil {
		return nil, m.historyErr
	}
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

// defaultComponents mirrors what orchestrator.createBackupHistory writes: an
// entry for every component enabled on the instance, and nothing for the rest.
// Fixtures that omit this produce records the orchestrator would never create.
func defaultComponents() map[string]models.ComponentBackupInfo {
	return map[string]models.ComponentBackupInfo{
		types.ComponentZeebe: {Enabled: true, Status: types.ComponentStatusCompleted},
	}
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
		Components:        defaultComponents(),
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
		Components:        defaultComponents(),
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
		Components:        defaultComponents(),
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

func (m *mockInstanceProvider) ListInstances() ([]models.CamundaInstance, error) {
	if m.err != nil {
		return nil, m.err
	}
	out := make([]models.CamundaInstance, 0, len(m.instances))
	for _, inst := range m.instances {
		out = append(out, *inst)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
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
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b1", false)
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
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b2", false)
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
	err := mgr.DeleteBackup(context.Background(), "inst-1", "nonexistent", false)
	if err != utils.ErrBackupNotFound {
		t.Errorf("expected ErrBackupNotFound, got %v", err)
	}
}

func TestDeleteBackup_FromOrphaned(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addOrphaned("inst-1", "b-orphaned", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-orphaned", false)
	if err != nil {
		t.Fatalf("expected nil error deleting orphaned backup, got %v", err)
	}
}

func TestDeleteBackup_FromIncomplete(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addIncomplete("inst-1", "b-incomplete", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-incomplete", false)
	if err != nil {
		t.Fatalf("expected nil error deleting incomplete backup, got %v", err)
	}
}

func TestDeleteBackup_OnlyOneCompletedBackup(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now)
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b1", false)
	if err == nil {
		t.Fatal("expected error when deleting the only completed backup")
	}
}

func TestDeleteBackup_FailedBackupCanBeDeleted(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b-failed", types.BackupStatusFailed, now)
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-failed", false)
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
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b1", false)
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
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-orphan", false)
	if err == nil {
		t.Fatal("expected error when DeleteBackupHistory fails")
	}
	if !strings.Contains(err.Error(), "failed to delete backup record") {
		t.Errorf("expected 'failed to delete backup record' error, got: %v", err)
	}
}

func TestDeleteBackup_NotFoundInAnyDirectory(t *testing.T) {
	mgr, _, _ := newTestManager()
	err := mgr.DeleteBackup(context.Background(), "inst-1", "nonexistent", false)
	if !errors.Is(err, utils.ErrBackupNotFound) {
		t.Errorf("expected ErrBackupNotFound, got %v", err)
	}
}

func TestDeleteBackup_NoCompletedBackups_AllowsDelete(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	// Only orphaned backups exist (no completed)
	s3.addOrphaned("inst-1", "b-orphan", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-orphan", false)
	if err != nil {
		t.Fatalf("expected nil error when no completed backups exist, got %v", err)
	}
}

func TestDeleteBackup_NoInstanceProvider(t *testing.T) {
	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	mgr := NewManager(s3, fs, nil, nil, utils.NewLogger("debug"))
	s3.addBackup("inst-1", "b-failed", types.BackupStatusFailed, time.Now())

	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-failed", false)
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

	err := mgr.DeleteBackup(context.Background(), "inst-other", "b-failed", false)
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

// artifactInstance points every component and Elasticsearch at the recording server.
func artifactInstance(id string, servers *artifactServers) *models.CamundaInstance {
	return &models.CamundaInstance{
		ID:                     id,
		SuccessRetention:       5,
		FailureRetention:       5,
		ZeebeBackupEndpoint:    servers.server.URL + "/zeebe/backups",
		OperateBackupEndpoint:  servers.server.URL + "/operate/backups",
		TasklistBackupEndpoint: servers.server.URL + "/tasklist/backups",
		OptimizeBackupEndpoint: servers.server.URL + "/optimize/backups",
		ElasticsearchEndpoint:  servers.server.URL,
	}
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

	if err := mgr.DeleteBackup(context.Background(), "inst-1", "b-old", false); err != nil {
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
	// Runtime-skipped components stay in the map with SKIPPED status.
	s3.backupHistory["inst-1"]["b-old"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentZeebe:    {Enabled: true, Status: types.ComponentStatusCompleted},
		types.ComponentOperate:  {Enabled: true, Status: types.ComponentStatusSkipped},
		types.ComponentTasklist: {Enabled: false, Status: types.ComponentStatusSkipped},
	}
	s3.mu.Unlock()

	if err := mgr.DeleteBackup(context.Background(), "inst-1", "b-old", false); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}

	got := servers.recorded()
	if len(got) != 1 || got[0] != "/zeebe/backups/b-old" {
		t.Errorf("expected only the Zeebe backup to be deleted, got %v", got)
	}
}

// The orchestrator seeds the component map with every ENABLED component before
// any of them runs, so a component absent from the record was disabled at backup
// time and owns no artifact. Purging it anyway would send DELETEs to components
// this backup never touched.
func TestDeleteBackup_DoesNotPurgeComponentsAbsentFromRecord(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()

	s3.addIncomplete("inst-1", "b-inc", time.Now().Add(-2*time.Hour))
	s3.mu.Lock()
	// Only Zeebe was enabled; Operate/Tasklist/Optimize/ES are absent entirely.
	s3.incomplete["inst-1"]["b-inc"].Components = map[string]models.ComponentBackupInfo{
		types.ComponentZeebe: {Enabled: true, Status: types.ComponentStatusRunning},
	}
	s3.mu.Unlock()

	if err := mgr.DeleteBackup(context.Background(), "inst-1", "b-inc", false); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}

	got := servers.recorded()
	if len(got) != 1 || got[0] != "/zeebe/backups/b-inc" {
		t.Errorf("expected only the recorded component to be purged, got %v", got)
	}
}

// A record with no components at all cannot be purged safely: we cannot tell
// what it wrote, so deleting the metadata would strand whatever exists.
func TestDeleteBackup_RefusesRecordWithNoComponents(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()

	s3.addBackup("inst-1", "b-bare", types.BackupStatusFailed, time.Now().Add(-2*time.Hour))
	s3.mu.Lock()
	s3.backupHistory["inst-1"]["b-bare"].Components = nil
	s3.mu.Unlock()

	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-bare", false)
	if !errors.Is(err, utils.ErrBackupArtifactsRemain) {
		t.Fatalf("expected ErrBackupArtifactsRemain, got %v", err)
	}
	if _, getErr := s3.GetBackupHistory("inst-1", "b-bare"); getErr != nil {
		t.Error("expected the record to survive so it is not silently orphaned")
	}
	if len(servers.recorded()) != 0 {
		t.Errorf("expected no DELETEs for an unidentifiable backup, got %v", servers.recorded())
	}

	// force is the documented escape hatch.
	if err := mgr.DeleteBackup(context.Background(), "inst-1", "b-bare", true); err != nil {
		t.Fatalf("expected force to delete the bare record, got %v", err)
	}
}

// Deleting a backup the orchestrator is still writing races it, so it is
// refused outright — force does not apply.
func TestDeleteBackup_RefusesRunningBackup(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()

	s3.addBackup("inst-1", "b-running", types.BackupStatusRunning, time.Now())

	for _, force := range []bool{false, true} {
		err := mgr.DeleteBackup(context.Background(), "inst-1", "b-running", force)
		if !errors.Is(err, utils.ErrCannotDeleteRunningBackup) {
			t.Fatalf("force=%v: expected ErrCannotDeleteRunningBackup, got %v", force, err)
		}
	}

	if _, err := s3.GetBackupHistory("inst-1", "b-running"); err != nil {
		t.Error("expected the running backup's record to survive")
	}
	if len(servers.recorded()) != 0 {
		t.Errorf("expected no artifacts to be touched for a running backup, got %v", servers.recorded())
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

	err := mgr.DeleteBackup(context.Background(), "inst-1", "b-old", false)
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

	if err := mgr.DeleteBackup(context.Background(), "inst-1", "b-old", true); err != nil {
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

	if err := mgr.DeleteBackup(context.Background(), "inst-1", "b-old", false); err != nil {
		t.Fatalf("expected 404s to count as deleted, got %v", err)
	}
}

// Retention runs unattended, so it is the path where a silently orphaned
// artifact is most likely to go unnoticed. It must hold the metadata record
// back exactly like the manual path does.
func TestApplyRetention_KeepsRecordWhenArtifactDeletionFails(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()
	servers.setStatus(http.StatusInternalServerError)

	now := time.Now()
	for i, id := range []string{"b1", "b2", "b3"} {
		s3.addBackup("inst-1", id, types.BackupStatusCompleted, now.Add(-time.Duration(3-i)*time.Hour))
		s3.mu.Lock()
		s3.backupHistory["inst-1"][id].Components = allComponentsCompleted()
		s3.mu.Unlock()
	}

	instance := artifactInstance("inst-1", servers)
	instance.SuccessRetention = 1
	instance.FailureRetention = 1

	result := mgr.ApplyRetention(instance)

	if len(result.DeletedSuccessful) != 0 {
		t.Errorf("expected no backups reported deleted when the purge failed, got %v", result.DeletedSuccessful)
	}
	if len(result.Errors) == 0 {
		t.Error("expected the purge failures to be recorded on the result")
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusCompleted)
	if len(remaining) != 3 {
		t.Errorf("expected all 3 records to survive so retention can retry, got %d", len(remaining))
	}
}

func TestApplyRetention_CleanupIncomplete_KeepsRecordWhenPurgeFails(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()
	servers.setStatus(http.StatusInternalServerError)

	now := time.Now()
	s3.addBackup("inst-1", "b-completed", types.BackupStatusCompleted, now)
	s3.addIncomplete("inst-1", "b-inc", now.Add(-2*time.Hour))
	s3.mu.Lock()
	s3.incomplete["inst-1"]["b-inc"].Components = allComponentsCompleted()
	s3.mu.Unlock()

	instance := artifactInstance("inst-1", servers)
	result := mgr.ApplyRetention(instance)

	if len(result.CleanedIncomplete) != 0 {
		t.Errorf("expected the incomplete backup to be kept, got %v", result.CleanedIncomplete)
	}
	if _, err := s3.GetBackupHistory("inst-1", "b-inc"); err != nil {
		t.Error("expected the incomplete record to survive a failed purge")
	}
}

func TestApplyRetention_PrunesOnceArtifactsAreGone(t *testing.T) {
	mgr, s3, _, servers := newArtifactTestManager(t)
	defer servers.Close()

	now := time.Now()
	for i, id := range []string{"b1", "b2", "b3"} {
		s3.addBackup("inst-1", id, types.BackupStatusCompleted, now.Add(-time.Duration(3-i)*time.Hour))
		s3.mu.Lock()
		s3.backupHistory["inst-1"][id].Components = allComponentsCompleted()
		s3.mu.Unlock()
	}

	instance := artifactInstance("inst-1", servers)
	instance.SuccessRetention = 1
	instance.FailureRetention = 1

	result := mgr.ApplyRetention(instance)

	if len(result.DeletedSuccessful) != 2 {
		t.Fatalf("expected 2 pruned backups, got %v (errors: %v)", result.DeletedSuccessful, result.Errors)
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusCompleted)
	if len(remaining) != 1 || remaining[0].BackupID != "b3" {
		t.Errorf("expected only the newest backup to remain, got %v", backupIDsOf(remaining))
	}
}

func backupIDsOf(backups []*models.BackupHistory) []string {
	ids := make([]string, 0, len(backups))
	for _, b := range backups {
		ids = append(ids, b.BackupID)
	}
	return ids
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
		ID:                  "inst-1",
		SuccessRetention:    1,
		FailureRetention:    1,
		ZeebeBackupEndpoint: componentServer.URL + "/zeebe",
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
		ID:                  "inst-1",
		SuccessRetention:    1,
		FailureRetention:    1,
		ZeebeBackupEndpoint: componentServer.URL + "/zeebe",
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
		ID:                    "inst-1",
		SuccessRetention:      1,
		FailureRetention:      1,
		ElasticsearchEndpoint: esServer.URL,
		ElasticsearchUsername: "elastic",
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

// --- DeleteOrphan ---

// orphanTestEnv wires a manager against live component and Elasticsearch stubs,
// recording every DELETE they receive so a test can assert exactly which
// artifacts were addressed.
type orphanTestEnv struct {
	mgr       *Manager
	s3        *mockS3Storage
	fs        *mockFileStorage
	instances *mockInstanceProvider
	instance  *models.CamundaInstance

	mu      sync.Mutex
	deletes []string
	status  int
}

func newOrphanTestEnv(t *testing.T) *orphanTestEnv {
	t.Helper()

	env := &orphanTestEnv{status: http.StatusNoContent}
	record := func(w http.ResponseWriter, r *http.Request) {
		env.mu.Lock()
		if r.Method == http.MethodDelete {
			env.deletes = append(env.deletes, r.URL.Path)
		}
		status := env.status
		env.mu.Unlock()

		// Elasticsearch answers a snapshot delete with 200 and an
		// acknowledgement, not the 204 a component backup delete returns, and
		// the client checks for exactly that.
		if status == http.StatusNoContent && strings.HasPrefix(r.URL.Path, "/_snapshot/") {
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"acknowledged": true})
			return
		}
		w.WriteHeader(status)
	}

	server := httptest.NewServer(http.HandlerFunc(record))
	t.Cleanup(server.Close)

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:    5 * time.Second,
		MaxRetries: 0,
	}, logger)

	env.s3 = newMockS3Storage()
	env.fs = newMockFileStorage()
	env.mgr = NewManager(env.s3, env.fs, httpClient, &config.Config{DefaultElasticsearchSnapshotRepository: "camunda-backup"}, logger)

	env.instance = &models.CamundaInstance{
		ID:                    "inst-1",
		ZeebeBackupEndpoint:   server.URL + "/zeebe/actuator/backups",
		OperateBackupEndpoint: server.URL + "/operate/actuator/backups",
		ElasticsearchEndpoint: server.URL,
	}
	env.instances = newMockInstanceProvider()
	env.instances.instances["inst-1"] = env.instance
	env.mgr.SetInstanceProvider(env.instances)

	return env
}

// complete fills the report-provenance a real deletion carries: a sweep that
// reached every source, finished just now, and saw the endpoints the instance is
// configured with. A test exercising one of those guards overrides just that
// field, so the guard under test is the only thing that fails.
func (e *orphanTestEnv) complete(art OrphanArtifacts) OrphanArtifacts {
	art.Complete = true
	if art.SweptAt.IsZero() {
		art.SweptAt = time.Now()
	}
	if art.Endpoints == nil {
		art.Endpoints = map[string]string{}
		for _, component := range types.ValidComponents {
			if endpoint, ok := componentEndpoint(e.instance, component); ok && endpoint != "" {
				art.Endpoints[component] = endpoint
			}
		}
	}
	return art
}

// deleteOrphan is the call under test, with that provenance supplied.
func (e *orphanTestEnv) deleteOrphan(art OrphanArtifacts) error {
	return e.mgr.DeleteOrphan(context.Background(), "inst-1", e.complete(art))
}

func (e *orphanTestEnv) recorded() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := append([]string(nil), e.deletes...)
	sort.Strings(out)
	return out
}

func (e *orphanTestEnv) failComponents(status int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.status = status
}

func TestDeleteOrphan_DeletesOnlyWhatTheSweepFound(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.fs.logFiles["inst-1"] = []string{"20260320080000"}

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:      "20260320080000",
		Components:    []string{types.ComponentZeebe},
		SnapshotNames: []string{"camunda-20260320080000"},
		Repository:    "camunda-backup",
	})
	if err != nil {
		t.Fatalf("DeleteOrphan: %v", err)
	}

	got := env.recorded()
	want := []string{
		"/_snapshot/camunda-backup/camunda-20260320080000",
		"/zeebe/actuator/backups/20260320080000",
	}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("expected %v, got %v", want, got)
		}
	}

	// Operate was not in the sweep's findings, so it must not have been asked.
	for _, path := range got {
		if strings.Contains(path, "/operate/") {
			t.Errorf("deleted from Operate, which the sweep never found: %v", got)
		}
	}

	if len(env.fs.logFiles["inst-1"]) != 0 {
		t.Errorf("expected the stray log file to be removed, got %v", env.fs.logFiles["inst-1"])
	}
}

// The snapshot name comes from the report, never from the backup ID: a
// configured name prefix makes the snapshot unaddressable by ID alone.
func TestDeleteOrphan_UsesReportedSnapshotName(t *testing.T) {
	env := newOrphanTestEnv(t)

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:      "20260320080000",
		SnapshotNames: []string{"prod-prefix-20260320080000"},
		Repository:    "other-repo",
	})
	if err != nil {
		t.Fatalf("DeleteOrphan: %v", err)
	}

	got := env.recorded()
	if len(got) != 1 || got[0] != "/_snapshot/other-repo/prod-prefix-20260320080000" {
		t.Fatalf("expected the reported snapshot name to be deleted, got %v", got)
	}
}

// A record appearing between the sweep and the deletion means this is no longer
// an orphan, and the guards that only DeleteBackup applies would be skipped.
func TestDeleteOrphan_RefusesWhenRecordAppeared(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.s3.addBackup("inst-1", "20260320080000", types.BackupStatusCompleted, time.Now())

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	if !errors.Is(err, utils.ErrOrphanRecordAppeared) {
		t.Fatalf("expected ErrOrphanRecordAppeared, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing to be deleted, got %v", got)
	}
}

func TestDeleteOrphan_RefusesWithNoArtifactsNamed(t *testing.T) {
	env := newOrphanTestEnv(t)

	err := env.deleteOrphan(OrphanArtifacts{BackupID: "20260320080000"})
	if !errors.Is(err, utils.ErrOrphanArtifactsUnidentifiable) {
		t.Fatalf("expected ErrOrphanArtifactsUnidentifiable, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing to be deleted, got %v", got)
	}
}

func TestDeleteOrphan_ReportsSurvivingArtifacts(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.failComponents(http.StatusInternalServerError)

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	if !errors.Is(err, utils.ErrBackupArtifactsRemain) {
		t.Fatalf("expected ErrBackupArtifactsRemain, got %v", err)
	}
}

// A component with no endpoint configured cannot be deleted from, and saying so
// is the only honest answer: reporting success would hide the leftover.
func TestDeleteOrphan_ReportsUnconfiguredComponent(t *testing.T) {
	env := newOrphanTestEnv(t)

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentTasklist},
	})
	if !errors.Is(err, utils.ErrBackupArtifactsRemain) {
		t.Fatalf("expected ErrBackupArtifactsRemain, got %v", err)
	}
	if !strings.Contains(err.Error(), "no tasklist backup endpoint configured") {
		t.Errorf("expected the message to name the missing endpoint, got %v", err)
	}
}

// Elasticsearch is not a component with a backup endpoint. Naming it as one is a
// caller bug, and it has to fail loudly rather than being skipped in silence.
func TestDeleteOrphan_RejectsElasticsearchAsComponent(t *testing.T) {
	env := newOrphanTestEnv(t)

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentElasticsearch},
	})
	if !errors.Is(err, utils.ErrBackupArtifactsRemain) {
		t.Fatalf("expected ErrBackupArtifactsRemain, got %v", err)
	}
	if !strings.Contains(err.Error(), "unknown component") {
		t.Errorf("expected an unknown-component message, got %v", err)
	}
}

// A component answering 404 counts as deleted: the goal is the artifact's
// absence, not the act of removing it.
func TestDeleteOrphan_TreatsMissingArtifactAsDeleted(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.failComponents(http.StatusNotFound)

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe, types.ComponentOperate},
	})
	if err != nil {
		t.Fatalf("expected a 404 to count as deleted, got %v", err)
	}
}

func TestDeleteOrphan_RequiresBackupID(t *testing.T) {
	env := newOrphanTestEnv(t)

	if err := env.deleteOrphan(OrphanArtifacts{Components: []string{types.ComponentZeebe}}); err == nil {
		t.Fatal("expected an error for an empty backup ID")
	}
}

func TestDeleteOrphan_NoInstanceProvider(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.mgr.SetInstanceProvider(nil)

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	if !errors.Is(err, utils.ErrInstanceProviderNotConfigured) {
		t.Fatalf("expected ErrInstanceProviderNotConfigured, got %v", err)
	}
}

// --- DeleteOrphan guards ---

// A read failure is not "no record". Reading it as one sends the deletion down a
// path that applies none of the record's safety guards.
func TestDeleteOrphan_RefusesWhenRecordLookupFails(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.s3.historyErr = errors.New("s3: connection reset")

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	if err == nil || errors.Is(err, utils.ErrOrphanRecordAppeared) {
		t.Fatalf("expected a refusal naming the failed confirmation, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted while trackedness is unknown, got %v", got)
	}
}

// The Elasticsearch leg of the delete-everywhere invariant, which the
// component-only failure test does not reach.
func TestDeleteOrphan_ReportsSurvivingSnapshot(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.failComponents(http.StatusInternalServerError)

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:      "20260320080000",
		SnapshotNames: []string{"camunda-20260320080000"},
		Repository:    "camunda-backup",
	})
	if !errors.Is(err, utils.ErrBackupArtifactsRemain) {
		t.Fatalf("expected ErrBackupArtifactsRemain, got %v", err)
	}
	if !strings.Contains(err.Error(), "camunda-20260320080000") {
		t.Errorf("expected the surviving snapshot to be named, got %v", err)
	}
}

// A partial sweep does not describe the full artifact set, so acting on it would
// delete what it saw and silently strand the rest.
func TestDeleteOrphan_RefusesPartialReport(t *testing.T) {
	env := newOrphanTestEnv(t)

	art := env.complete(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	art.Complete = false

	if err := env.mgr.DeleteOrphan(context.Background(), "inst-1", art); !errors.Is(err, utils.ErrOrphanReportPartial) {
		t.Fatalf("expected ErrOrphanReportPartial, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted from a partial scan, got %v", got)
	}
}

// A deletion acts on a description of the world, and an old description is not
// one: endpoints move and backups appear in between.
func TestDeleteOrphan_RefusesStaleReport(t *testing.T) {
	env := newOrphanTestEnv(t)

	art := env.complete(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	art.SweptAt = time.Now().Add(-2 * maxReportAge)

	if err := env.mgr.DeleteOrphan(context.Background(), "inst-1", art); !errors.Is(err, utils.ErrOrphanReportStale) {
		t.Fatalf("expected ErrOrphanReportStale, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted from a stale scan, got %v", got)
	}
}

// An orphan has no record to read a RUNNING status from, and a live backup whose
// initial record write failed is indistinguishable from one.
func TestDeleteOrphan_RefusesWhileBackupRunning(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.mgr.SetBackupRunningFunc(func() bool { return true })

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	if !errors.Is(err, utils.ErrCannotDeleteRunningBackup) {
		t.Fatalf("expected ErrCannotDeleteRunningBackup, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted while a backup is in flight, got %v", got)
	}
}

// Backup IDs are timestamps, so the same ID exists in several instances. A
// record under another instance is proof this artifact is not ours.
func TestDeleteOrphan_RefusesWhenAnotherInstanceHasTheRecord(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.instances.instances["inst-2"] = &models.CamundaInstance{ID: "inst-2"}
	env.s3.addBackup("inst-2", "20260320080000", types.BackupStatusCompleted, time.Now())

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	if !errors.Is(err, utils.ErrOrphanOwnershipUnverified) {
		t.Fatalf("expected ErrOrphanOwnershipUnverified, got %v", err)
	}
	if !strings.Contains(err.Error(), "inst-2") {
		t.Errorf("expected the owning instance to be named, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted, got %v", got)
	}
}

// Two instances behind one component endpoint cannot be told apart at all, so
// neither may delete from it on the strength of its own missing record.
func TestDeleteOrphan_RefusesWhenComponentEndpointIsShared(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.instances.instances["inst-2"] = &models.CamundaInstance{
		ID:                  "inst-2",
		ZeebeBackupEndpoint: env.instance.ZeebeBackupEndpoint,
	}

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	if !errors.Is(err, utils.ErrOrphanOwnershipUnverified) {
		t.Fatalf("expected ErrOrphanOwnershipUnverified, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted, got %v", got)
	}
}

// The default configuration puts every instance in one repository with no name
// prefix, which is exactly the case the reason catalogue warns about.
func TestDeleteOrphan_RefusesWhenSnapshotRepositoryIsShared(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.instances.instances["inst-2"] = &models.CamundaInstance{
		ID:                    "inst-2",
		ElasticsearchEndpoint: env.instance.ElasticsearchEndpoint,
	}

	err := env.deleteOrphan(OrphanArtifacts{
		BackupID:      "20260320080000",
		SnapshotNames: []string{"20260320080000"},
		Repository:    "camunda-backup",
	})
	if !errors.Is(err, utils.ErrOrphanOwnershipUnverified) {
		t.Fatalf("expected ErrOrphanOwnershipUnverified, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted, got %v", got)
	}
}

// A second instance that shares nothing must not block a deletion.
func TestDeleteOrphan_AllowsWhenOtherInstanceIsSeparate(t *testing.T) {
	env := newOrphanTestEnv(t)
	env.instances.instances["inst-2"] = &models.CamundaInstance{
		ID:                    "inst-2",
		ZeebeBackupEndpoint:   "http://other-zeebe:9600/actuator/backups",
		ElasticsearchEndpoint: "http://other-es:9200",
	}

	if err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	}); err != nil {
		t.Fatalf("expected the deletion to proceed, got %v", err)
	}
	if got := env.recorded(); len(got) != 1 {
		t.Errorf("expected the Zeebe backup to be deleted, got %v", got)
	}
}

// The sweep records where it found the backup. Deleting at the currently
// configured endpoint instead would send the DELETE somewhere that never
// reported it, where the same timestamp ID can name a live backup.
func TestDeleteOrphan_RefusesOnEndpointDrift(t *testing.T) {
	env := newOrphanTestEnv(t)

	art := env.complete(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	art.Endpoints = map[string]string{types.ComponentZeebe: "http://zeebe-before-the-migration:9600/actuator/backups"}

	err := env.mgr.DeleteOrphan(context.Background(), "inst-1", art)
	if !errors.Is(err, utils.ErrOrphanEndpointDrift) {
		t.Fatalf("expected ErrOrphanEndpointDrift, got %v", err)
	}
	if got := env.recorded(); len(got) != 0 {
		t.Errorf("expected nothing deleted after an endpoint moved, got %v", got)
	}
}

// Credentials are stripped from the report's endpoints, so a byte comparison
// against live config would report drift that is not there.
func TestDeleteOrphan_IgnoresCredentialsWhenComparingEndpoints(t *testing.T) {
	env := newOrphanTestEnv(t)
	withCreds := strings.Replace(env.instance.ZeebeBackupEndpoint, "http://", "http://user:pass@", 1)
	env.instance.ZeebeBackupEndpoint = withCreds

	art := env.complete(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	})
	art.Endpoints = map[string]string{types.ComponentZeebe: stripUserinfo(withCreds)}

	if err := env.mgr.DeleteOrphan(context.Background(), "inst-1", art); err != nil {
		t.Fatalf("expected credentials to be ignored, got %v", err)
	}
}

// Elasticsearch forbids these characters in a snapshot name, so a name carrying
// one did not come from a healthy listing — and once path-cleaned it addresses
// something else entirely.
func TestDeleteOrphan_RefusesUnaddressableSnapshotName(t *testing.T) {
	for _, name := range []string{
		"camunda_operate_20260320080000_../../../_all",
		"camunda-*",
		"a,b",
		"..",
	} {
		env := newOrphanTestEnv(t)

		err := env.deleteOrphan(OrphanArtifacts{
			BackupID:      "20260320080000",
			SnapshotNames: []string{name},
			Repository:    "camunda-backup",
		})
		if !errors.Is(err, utils.ErrBackupArtifactsRemain) {
			t.Fatalf("%q: expected the deletion to be refused, got %v", name, err)
		}
		if !strings.Contains(err.Error(), "not addressable") {
			t.Errorf("%q: expected an addressability message, got %v", name, err)
		}
		if got := env.recorded(); len(got) != 0 {
			t.Errorf("%q: expected no request to be sent, got %v", name, got)
		}
	}
}

// The alert path on the orphan side, which had no equivalent of the tracked
// path's coverage.
func TestDeleteOrphan_AlertsOnSurvivingArtifacts(t *testing.T) {
	var alerts int32
	alertServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&alerts, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer alertServer.Close()

	env := newOrphanTestEnv(t)
	env.mgr.SetAlerter(utils.NewAlerter(alertServer.URL, utils.NewLogger("error")))
	env.failComponents(http.StatusInternalServerError)

	if err := env.deleteOrphan(OrphanArtifacts{
		BackupID:   "20260320080000",
		Components: []string{types.ComponentZeebe},
	}); !errors.Is(err, utils.ErrBackupArtifactsRemain) {
		t.Fatalf("expected ErrBackupArtifactsRemain, got %v", err)
	}

	time.Sleep(500 * time.Millisecond)
	if atomic.LoadInt32(&alerts) < 1 {
		t.Error("expected a cleanup alert for surviving orphan artifacts")
	}
}
