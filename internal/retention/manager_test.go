package retention

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// --- Mock storage implementations ---

type mockS3Storage struct {
	mu              sync.Mutex
	backupHistory   map[string]map[string]*models.BackupHistory
	orphaned        map[string]map[string]*models.BackupHistory
	incomplete      map[string]map[string]*models.BackupHistory
	latestBackupIDs map[string]string
	listErr         error
	deleteErr       error
	moveErr         error
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
	if m.backupHistory[camundaInstanceID] != nil {
		if h, ok := m.backupHistory[camundaInstanceID][backupID]; ok {
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
	var result []*models.BackupHistory
	for _, h := range m.orphaned[camundaInstanceID] {
		result = append(result, h)
	}
	return result, nil
}

func (m *mockS3Storage) ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func newTestManager() (*Manager, *mockS3Storage, *mockFileStorage) {
	s3 := newMockS3Storage()
	fs := newMockFileStorage()
	logger := utils.NewLogger("debug")
	mgr := NewManager(s3, fs, logger)
	return mgr, s3, fs
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
	result := mgr.ApplyRetention("inst-1", 5)
	if len(result.OrphanedByRetention) != 0 {
		t.Errorf("expected 0 orphaned, got %d", len(result.OrphanedByRetention))
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
	result := mgr.ApplyRetention("inst-1", 2)
	if len(result.OrphanedByRetention) != 3 {
		t.Errorf("expected 3 orphaned, got %d: %v", len(result.OrphanedByRetention), result.OrphanedByRetention)
	}
	orphanedSet := make(map[string]bool)
	for _, id := range result.OrphanedByRetention {
		orphanedSet[id] = true
	}
	for _, expected := range []string{"b1", "b2", "b3"} {
		if !orphanedSet[expected] {
			t.Errorf("expected %s to be orphaned", expected)
		}
	}
	orphaned, _ := s3.ListOrphanedBackups("inst-1")
	if len(orphaned) != 3 {
		t.Errorf("expected 3 orphaned in storage, got %d", len(orphaned))
	}
	remaining, _ := s3.ListBackupHistory("inst-1", types.BackupStatusCompleted)
	if len(remaining) != 2 {
		t.Errorf("expected 2 remaining completed, got %d", len(remaining))
	}
}

func TestApplyRetention_KeepLastN_NeverOrphansNewest(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	result := mgr.ApplyRetention("inst-1", 1)
	if len(result.OrphanedByRetention) != 1 {
		t.Fatalf("expected 1 orphaned, got %d", len(result.OrphanedByRetention))
	}
	if result.OrphanedByRetention[0] != "b1" {
		t.Errorf("expected b1 to be orphaned, got %s", result.OrphanedByRetention[0])
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
	result := mgr.ApplyRetention("inst-1", 0)
	if len(result.OrphanedByRetention) != 0 {
		t.Errorf("expected 0 orphaned for zero retention, got %d", len(result.OrphanedByRetention))
	}
}

func TestApplyRetention_CleanupIncomplete_WithNewerCompleted(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b-completed", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	s3.addIncomplete("inst-1", "b-incomplete-old", now.Add(-3*time.Hour))
	s3.addIncomplete("inst-1", "b-incomplete-newer", now)
	result := mgr.ApplyRetention("inst-1", 10)
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
	result := mgr.ApplyRetention("inst-1", 10)
	if len(result.CleanedIncomplete) != 0 {
		t.Errorf("expected 0 cleaned incomplete when no completed backups exist, got %d", len(result.CleanedIncomplete))
	}
}

func TestApplyRetention_CleanupLogFiles(t *testing.T) {
	mgr, _, fs := newTestManager()
	for i := 0; i < 5; i++ {
		fs.CreateLogFile("inst-1", fmt.Sprintf("backup-%d", i))
	}
	result := mgr.ApplyRetention("inst-1", 2)
	if result.LogFilesRemoved != 3 {
		t.Errorf("expected 3 log files removed, got %d", result.LogFilesRemoved)
	}
	remaining, _ := fs.ListLogFiles("inst-1")
	if len(remaining) != 2 {
		t.Errorf("expected 2 remaining log files, got %d", len(remaining))
	}
}

func TestApplyRetention_ErrorInListBackupHistory(t *testing.T) {
	mgr, s3, _ := newTestManager()
	s3.listErr = fmt.Errorf("S3 unavailable")
	result := mgr.ApplyRetention("inst-1", 5)
	if len(result.Errors) == 0 {
		t.Error("expected at least one error when S3 is unavailable")
	}
}

func TestApplyRetention_ErrorInMoveToOrphaned(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-3*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b3", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	s3.moveErr = fmt.Errorf("permission denied")
	result := mgr.ApplyRetention("inst-1", 2)
	if len(result.Errors) == 0 {
		t.Error("expected errors when MoveToOrphaned fails")
	}
	if len(result.OrphanedByRetention) != 0 {
		t.Errorf("expected 0 orphaned on move error, got %d", len(result.OrphanedByRetention))
	}
}

func TestApplyRetention_LogFileCleanupError(t *testing.T) {
	mgr, _, fs := newTestManager()
	fs.cleanErr = fmt.Errorf("disk error")
	fs.CreateLogFile("inst-1", "backup-1")
	result := mgr.ApplyRetention("inst-1", 1)
	hasLogErr := false
	for _, e := range result.Errors {
		if e == "failed to cleanup old log files: disk error" {
			hasLogErr = true
		}
	}
	if !hasLogErr {
		t.Errorf("expected log cleanup error, got: %v", result.Errors)
	}
}

func TestDeleteBackup_Success(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "b2", types.BackupStatusCompleted, now.Add(-1*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b1")
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
	err := mgr.DeleteBackup("inst-1", "b2")
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
	err := mgr.DeleteBackup("inst-1", "nonexistent")
	if err != utils.ErrBackupNotFound {
		t.Errorf("expected ErrBackupNotFound, got %v", err)
	}
}

func TestDeleteBackup_FromOrphaned(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addOrphaned("inst-1", "b-orphaned", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b-orphaned")
	if err != nil {
		t.Fatalf("expected nil error deleting orphaned backup, got %v", err)
	}
}

func TestDeleteBackup_FromIncomplete(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addIncomplete("inst-1", "b-incomplete", now.Add(-5*time.Hour))
	err := mgr.DeleteBackup("inst-1", "b-incomplete")
	if err != nil {
		t.Fatalf("expected nil error deleting incomplete backup, got %v", err)
	}
}

func TestDeleteBackup_OnlyOneCompletedBackup(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now)
	err := mgr.DeleteBackup("inst-1", "b1")
	if err == nil {
		t.Fatal("expected error when deleting the only completed backup")
	}
}

func TestDeleteBackup_FailedBackupCanBeDeleted(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b-failed", types.BackupStatusFailed, now)
	err := mgr.DeleteBackup("inst-1", "b-failed")
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
	result := mgr.ApplyRetention("inst-empty", 5)
	if len(result.OrphanedByRetention) != 0 {
		t.Errorf("expected 0 orphaned, got %d", len(result.OrphanedByRetention))
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

func TestApplyRetention_OnlyFailedBackups_NoOrphaning(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "f1", types.BackupStatusFailed, now.Add(-2*time.Hour))
	s3.addBackup("inst-1", "f2", types.BackupStatusFailed, now.Add(-1*time.Hour))
	result := mgr.ApplyRetention("inst-1", 1)
	if len(result.OrphanedByRetention) != 0 {
		t.Errorf("expected 0 orphaned for only-failed backups, got %d", len(result.OrphanedByRetention))
	}
}

func TestApplyRetention_SortOrderDeterministic(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	for i := 0; i < 10; i++ {
		s3.addBackup("inst-1", fmt.Sprintf("b%02d", i), types.BackupStatusCompleted, now.Add(time.Duration(-10+i)*time.Hour))
	}
	result := mgr.ApplyRetention("inst-1", 3)
	if len(result.OrphanedByRetention) != 7 {
		t.Fatalf("expected 7 orphaned, got %d: %v", len(result.OrphanedByRetention), result.OrphanedByRetention)
	}
	sort.Strings(result.OrphanedByRetention)
	for i, expected := range []string{"b00", "b01", "b02", "b03", "b04", "b05", "b06"} {
		if result.OrphanedByRetention[i] != expected {
			t.Errorf("orphaned[%d]: expected %s, got %s", i, expected, result.OrphanedByRetention[i])
		}
	}
}

func TestDeleteBackup_ListErrorPreventsDelete(t *testing.T) {
	mgr, s3, _ := newTestManager()
	now := time.Now()
	s3.addBackup("inst-1", "b1", types.BackupStatusCompleted, now)
	s3.listErr = fmt.Errorf("S3 error")
	err := mgr.DeleteBackup("inst-1", "b1")
	if err == nil {
		t.Fatal("expected error when ListBackupHistory fails")
	}
}
