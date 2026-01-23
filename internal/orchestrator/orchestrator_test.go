package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Mock storage implementations for testing
type mockFileStorage struct {
	logs   map[string]map[string][]string // camundaID -> backupID -> logs
	config *models.Configuration
	mutex  sync.RWMutex
}

func newMockFileStorage() *mockFileStorage {
	return &mockFileStorage{
		logs: make(map[string]map[string][]string),
		config: &models.Configuration{
			Version:          "1.0",
			CamundaInstances: []models.CamundaInstance{},
		},
	}
}

func (m *mockFileStorage) SaveConfiguration(config *models.Configuration) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.config = config
	return nil
}

func (m *mockFileStorage) LoadConfiguration() (*models.Configuration, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.config, nil
}

func (m *mockFileStorage) CreateLogFile(camundaInstanceID, backupID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.logs[camundaInstanceID] == nil {
		m.logs[camundaInstanceID] = make(map[string][]string)
	}
	m.logs[camundaInstanceID][backupID] = []string{}
	return nil
}

func (m *mockFileStorage) WriteToLogFile(camundaInstanceID, backupID, message string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.logs[camundaInstanceID] == nil {
		m.logs[camundaInstanceID] = make(map[string][]string)
	}
	if m.logs[camundaInstanceID][backupID] == nil {
		m.logs[camundaInstanceID][backupID] = []string{}
	}
	m.logs[camundaInstanceID][backupID] = append(m.logs[camundaInstanceID][backupID], message)
	return nil
}

func (m *mockFileStorage) ReadLogFile(camundaInstanceID, backupID string) (string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	logs := m.logs[camundaInstanceID][backupID]
	result := ""
	for _, log := range logs {
		result += log
	}
	return result, nil
}

func (m *mockFileStorage) DeleteLogFile(camundaInstanceID, backupID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.logs[camundaInstanceID] != nil {
		delete(m.logs[camundaInstanceID], backupID)
	}
	return nil
}

func (m *mockFileStorage) ListLogFiles(camundaInstanceID string) ([]string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	var files []string
	if m.logs[camundaInstanceID] != nil {
		for backupID := range m.logs[camundaInstanceID] {
			files = append(files, backupID)
		}
	}
	return files, nil
}

func (m *mockFileStorage) CleanupOldLogFiles(camundaInstanceID string, keepCount int) error {
	return nil
}

type mockS3Storage struct {
	backupIDs      map[string]string                           // camundaID -> latest backupID
	backupHistory  map[string]map[string]*models.BackupHistory // camundaID -> backupID -> history
	backupStatuses map[string]map[string]types.BackupStatus    // camundaID -> backupID -> status
	mutex          sync.RWMutex
}

func newMockS3Storage() *mockS3Storage {
	return &mockS3Storage{
		backupIDs:      make(map[string]string),
		backupHistory:  make(map[string]map[string]*models.BackupHistory),
		backupStatuses: make(map[string]map[string]types.BackupStatus),
	}
}

func (m *mockS3Storage) StoreLatestBackupID(camundaInstanceID, backupID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.backupIDs[camundaInstanceID] = backupID
	return nil
}

func (m *mockS3Storage) GetLatestBackupID(camundaInstanceID string) (string, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.backupIDs[camundaInstanceID], nil
}

func (m *mockS3Storage) StoreBackupHistory(history *models.BackupHistory) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.backupHistory[history.CamundaInstanceID] == nil {
		m.backupHistory[history.CamundaInstanceID] = make(map[string]*models.BackupHistory)
	}
	m.backupHistory[history.CamundaInstanceID][history.BackupID] = history
	return nil
}

func (m *mockS3Storage) GetBackupHistory(camundaInstanceID, backupID string) (*models.BackupHistory, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if m.backupHistory[camundaInstanceID] != nil {
		return m.backupHistory[camundaInstanceID][backupID], nil
	}
	return nil, nil
}

func (m *mockS3Storage) ListBackupHistory(camundaInstanceID string, status types.BackupStatus) ([]*models.BackupHistory, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	var histories []*models.BackupHistory
	if m.backupHistory[camundaInstanceID] != nil {
		for _, history := range m.backupHistory[camundaInstanceID] {
			if status == "" || history.Status == status {
				histories = append(histories, history)
			}
		}
	}
	return histories, nil
}

func (m *mockS3Storage) UpdateBackupStatus(camundaInstanceID, backupID string, status types.BackupStatus) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.backupStatuses[camundaInstanceID] == nil {
		m.backupStatuses[camundaInstanceID] = make(map[string]types.BackupStatus)
	}
	m.backupStatuses[camundaInstanceID][backupID] = status

	// Also update in history
	if m.backupHistory[camundaInstanceID] != nil && m.backupHistory[camundaInstanceID][backupID] != nil {
		m.backupHistory[camundaInstanceID][backupID].Status = status
	}
	return nil
}

func (m *mockS3Storage) DeleteBackupHistory(camundaInstanceID, backupID string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if m.backupHistory[camundaInstanceID] != nil {
		delete(m.backupHistory[camundaInstanceID], backupID)
	}
	return nil
}

func (m *mockS3Storage) MoveToOrphaned(camundaInstanceID, backupID string) error {
	return nil
}

func (m *mockS3Storage) MoveToIncomplete(camundaInstanceID, backupID string) error {
	return nil
}

func (m *mockS3Storage) ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	return []*models.BackupHistory{}, nil
}

func (m *mockS3Storage) ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	return []*models.BackupHistory{}, nil
}

// Test helper functions
func setupTestInstance(id, name string) *models.CamundaInstance {
	return &models.CamundaInstance{
		ID:                     id,
		Name:                   name,
		BaseURL:                "http://localhost:8080",
		Enabled:                true,
		ParallelExecution:      false,
		ZeebeBackupEndpoint:    "http://localhost:8080/zeebe/backup",
		ZeebeStatusEndpoint:    "http://localhost:8080/zeebe/backup/status",
		OperateBackupEndpoint:  "http://localhost:8080/operate/backup",
		OperateStatusEndpoint:  "http://localhost:8080/operate/backup/status",
		TasklistBackupEndpoint: "http://localhost:8080/tasklist/backup",
		TasklistStatusEndpoint: "http://localhost:8080/tasklist/backup/status",
		Components: []models.CamundaComponentConfig{
			{Name: types.ComponentZeebe, Enabled: true},
			{Name: types.ComponentOperate, Enabled: true},
			{Name: types.ComponentTasklist, Enabled: true},
		},
	}
}

func setupTestConfig() *config.Config {
	return &config.Config{
		DefaultElasticsearchSnapshotRepository: "camunda-backup",
		DefaultElasticsearchSnapshotNamePrefix: "",
	}
}

func TestNewOrchestrator(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	cfg := setupTestConfig()

	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 100*time.Millisecond, 50)

	if orchestrator == nil {
		t.Fatal("Expected orchestrator to be created")
	}
	if orchestrator.fileStorage == nil {
		t.Error("Expected fileStorage to be set")
	}
	if orchestrator.s3Storage == nil {
		t.Error("Expected s3Storage to be set")
	}
	if orchestrator.httpClient == nil {
		t.Error("Expected httpClient to be set")
	}
	if orchestrator.logger == nil {
		t.Error("Expected logger to be set")
	}
}

func TestExecuteBackup_SequentialMode_Success(t *testing.T) {
	// Create mock server for Camunda components
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with server URLs
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.OperateStatusEndpoint = server.URL + "/operate/backup/status"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.TasklistStatusEndpoint = server.URL + "/tasklist/backup/status"
	instance.ParallelExecution = false

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test backup",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution == nil {
		t.Fatal("Expected execution to be returned")
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}
	if execution.BackupID == "" {
		t.Error("Expected backup ID to be generated")
	}

	// Verify component statuses
	if len(execution.ComponentStatus) != 3 {
		t.Errorf("Expected 3 components, got: %d", len(execution.ComponentStatus))
	}
	for component, status := range execution.ComponentStatus {
		if status != types.ComponentStatusCompleted {
			t.Errorf("Expected component %s to be COMPLETED, got: %s", component, status)
		}
	}

	// Verify backup ID stored in S3
	storedID, err := s3Storage.GetLatestBackupID(instance.ID)
	if err != nil {
		t.Fatalf("Failed to get backup ID from S3: %v", err)
	}
	if storedID != execution.BackupID {
		t.Errorf("Expected backup ID %s in S3, got: %s", execution.BackupID, storedID)
	}

	// Verify backup history stored in S3
	history, err := s3Storage.GetBackupHistory(instance.ID, execution.BackupID)
	if err != nil {
		t.Fatalf("Failed to get backup history from S3: %v", err)
	}
	if history == nil {
		t.Fatal("Expected backup history to be stored")
	}
	if history.Status != types.BackupStatusCompleted {
		t.Errorf("Expected history status COMPLETED, got: %s", history.Status)
	}

	// Verify logs were written
	logs := fileStorage.logs[instance.ID][execution.BackupID]
	if len(logs) == 0 {
		t.Error("Expected logs to be written")
	}
}

func TestExecuteBackup_ParallelMode_Success(t *testing.T) {
	// Create mock server for Camunda components
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with server URLs
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.OperateStatusEndpoint = server.URL + "/operate/backup/status"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.TasklistStatusEndpoint = server.URL + "/tasklist/backup/status"
	instance.ParallelExecution = true // Enable parallel execution

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test parallel backup",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}

	// Verify all components completed
	for component, status := range execution.ComponentStatus {
		if status != types.ComponentStatusCompleted {
			t.Errorf("Expected component %s to be COMPLETED, got: %s", component, status)
		}
	}

	// Verify execution mode in history
	history, _ := s3Storage.GetBackupHistory(instance.ID, execution.BackupID)
	if history.Metadata.ExecutionMode != "parallel" {
		t.Errorf("Expected execution mode 'parallel', got: %s", history.Metadata.ExecutionMode)
	}
}

func TestExecuteBackup_ComponentFailure(t *testing.T) {
	// Create mock server that fails for Operate
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "operate") && r.Method == http.MethodPost {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "Backup failed"})
		} else if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	config := camunda.DefaultHTTPClientConfig()
	config.Timeout = 2 * time.Second // Short timeout for tests
	config.MaxRetries = 0

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(config, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with server URLs
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.OperateStatusEndpoint = server.URL + "/operate/backup/status"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.TasklistStatusEndpoint = server.URL + "/tasklist/backup/status"

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test failure",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}

	// Verify component statuses
	if execution.ComponentStatus[types.ComponentOperate] != types.ComponentStatusFailed {
		t.Errorf("Expected Operate to be FAILED, got: %s", execution.ComponentStatus[types.ComponentOperate])
	}
	if execution.ComponentStatus[types.ComponentZeebe] != types.ComponentStatusCompleted {
		t.Errorf("Expected Zeebe to be COMPLETED, got: %s", execution.ComponentStatus[types.ComponentZeebe])
	}

	// Verify status updated in S3
	status := s3Storage.backupStatuses[instance.ID][execution.BackupID]
	if status != types.BackupStatusFailed {
		t.Errorf("Expected S3 status FAILED, got: %s", status)
	}
}

func TestExecuteBackup_SkippedComponents(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with no endpoints configured
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = ""    // No endpoint
	instance.OperateBackupEndpoint = ""  // No endpoint
	instance.TasklistBackupEndpoint = "" // No endpoint

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test skipped",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// All components should be skipped, leading to completed status
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED (all skipped), got: %s", execution.Status)
	}

	// Verify all components are skipped
	for component, status := range execution.ComponentStatus {
		if status != types.ComponentStatusSkipped {
			t.Errorf("Expected component %s to be SKIPPED, got: %s", component, status)
		}
	}
}

func TestExecuteBackup_LogsWrittenToFile(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	// Disable other components to avoid failures due to missing endpoints
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = ""
	// Only enable Zeebe for this test
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeScheduled,
		BackupReason:    "Test logging",
	}

	execution, _ := orchestrator.ExecuteBackup(ctx, req)

	// Verify logs were written
	logs := fileStorage.logs[instance.ID][execution.BackupID]
	if len(logs) == 0 {
		t.Fatal("Expected logs to be written to file")
	}

	// Build complete log content
	logContent := ""
	for _, log := range logs {
		logContent += log
	}

	// Define expected log sequence with their order
	expectedLogSequence := []string{
		"Backup started",
		"Trigger type: SCHEDULED",
		"Execution mode: sequential",
		"Backup ID stored in S3",
		"Starting sequential execution",
		"Starting backup for component: zeebe",
		"Executing backup for component: zeebe",
		"Triggering Zeebe backup",
		"Zeebe backup triggered successfully",
		"Polling Zeebe backup status",
		"Zeebe backup completed",
		"Component zeebe completed successfully",
		"All components completed in sequential mode",
		"Backup completed successfully",
		"Backup completed with status",
	}

	// Verify each log entry exists
	for _, expected := range expectedLogSequence {
		if !strings.Contains(logContent, expected) {
			t.Errorf("Expected log to contain '%s'", expected)
		}
	}

	// Verify log entries appear in the correct order
	lastIndex := -1
	for i, expected := range expectedLogSequence {
		index := strings.Index(logContent, expected)
		if index == -1 {
			continue // Already reported as missing above
		}
		if index < lastIndex {
			t.Errorf("Log entry '%s' appears before expected. Expected order: %v", expected, expectedLogSequence[i-1:i+1])
		}
		lastIndex = index
	}

	// Verify minimum number of log entries (should have at least the expected sequence)
	if len(logs) < len(expectedLogSequence) {
		t.Errorf("Expected at least %d log entries, got %d", len(expectedLogSequence), len(logs))
	}
}

func TestBackupStats_Calculation(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test execution
	execution := &models.BackupExecution{
		ID:       "test-backup",
		BackupID: "test-backup",
		ComponentStatus: map[string]types.ComponentStatus{
			types.ComponentZeebe:    types.ComponentStatusCompleted,
			types.ComponentOperate:  types.ComponentStatusFailed,
			types.ComponentTasklist: types.ComponentStatusSkipped,
		},
	}

	// Calculate stats
	stats := orchestrator.calculateBackupStats(execution)

	// Verify stats
	if stats.TotalComponents != 3 {
		t.Errorf("Expected 3 total components, got: %d", stats.TotalComponents)
	}
	if stats.SuccessfulComponents != 1 {
		t.Errorf("Expected 1 successful component, got: %d", stats.SuccessfulComponents)
	}
	if stats.FailedComponents != 1 {
		t.Errorf("Expected 1 failed component, got: %d", stats.FailedComponents)
	}
	if stats.SkippedComponents != 1 {
		t.Errorf("Expected 1 skipped component, got: %d", stats.SkippedComponents)
	}
}

// Test 1: executeOptimizeBackup code paths
func TestExecuteOptimizeBackup_Success(t *testing.T) {
	// Create mock server for Optimize component
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with only Optimize enabled
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.OptimizeBackupEndpoint = server.URL + "/optimize/backup"
	instance.OptimizeStatusEndpoint = server.URL + "/optimize/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentOptimize, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Optimize backup",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentOptimize] != types.ComponentStatusCompleted {
		t.Errorf("Expected Optimize to be COMPLETED, got: %s", execution.ComponentStatus[types.ComponentOptimize])
	}
}

func TestExecuteOptimizeBackup_Skipped(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with Optimize enabled but no endpoint
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.OptimizeBackupEndpoint = "" // No endpoint configured
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentOptimize, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Optimize skipped",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.ComponentStatus[types.ComponentOptimize] != types.ComponentStatusSkipped {
		t.Errorf("Expected Optimize to be SKIPPED, got: %s", execution.ComponentStatus[types.ComponentOptimize])
	}
}

func TestExecuteOptimizeBackup_TriggerFailure(t *testing.T) {
	// Create mock server that returns error for Optimize
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "Internal error"})
	}))
	defer server.Close()

	// Set up orchestrator with short timeout
	config := camunda.DefaultHTTPClientConfig()
	config.Timeout = 1 * time.Second
	config.MaxRetries = 0

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(config, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with Optimize
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.OptimizeBackupEndpoint = server.URL + "/optimize/backup"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentOptimize, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Optimize failure",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentOptimize] != types.ComponentStatusFailed {
		t.Errorf("Expected Optimize to be FAILED, got: %s", execution.ComponentStatus[types.ComponentOptimize])
	}
}

func TestExecuteOptimizeBackup_NoStatusEndpoint(t *testing.T) {
	// Create mock server that accepts trigger but has no status endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted) // Test 202 Accepted
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with Optimize but no status endpoint
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.OptimizeBackupEndpoint = server.URL + "/optimize/backup"
	instance.OptimizeStatusEndpoint = "" // No status endpoint
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentOptimize, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Optimize no status endpoint",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should complete without polling
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.ComponentStatus[types.ComponentOptimize] != types.ComponentStatusCompleted {
		t.Errorf("Expected Optimize to be COMPLETED, got: %s", execution.ComponentStatus[types.ComponentOptimize])
	}
}

// Test 2: Scenario where a Camunda instance has no enabled components
func TestExecuteBackup_NoEnabledComponents(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with no enabled components
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: false},
		{Name: types.ComponentOperate, Enabled: false},
		{Name: types.ComponentTasklist, Enabled: false},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test no enabled components",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusIncomplete {
		t.Errorf("Expected status INCOMPLETE, got: %s", execution.Status)
	}
	if execution.ErrorMessage != "No components were executed" {
		t.Errorf("Expected error message 'No components were executed', got: %s", execution.ErrorMessage)
	}
	if len(execution.ComponentStatus) != 0 {
		t.Errorf("Expected 0 component statuses, got: %d", len(execution.ComponentStatus))
	}
}

func TestExecuteBackup_EmptyComponentsList(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with empty components list
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.Components = []models.CamundaComponentConfig{}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeScheduled,
		BackupReason:    "Test empty components list",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusIncomplete {
		t.Errorf("Expected status INCOMPLETE, got: %s", execution.Status)
	}
}

// Test 3: Polling timeout scenarios
func TestPollBackupStatus_Timeout(t *testing.T) {
	// Create mock server that always returns IN_PROGRESS
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "IN_PROGRESS"})
		}
	}))
	defer server.Close()

	// Set up orchestrator with very limited polling attempts
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	// Only 3 poll attempts with 50ms interval = 150ms max polling time
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 50*time.Millisecond, 3)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test polling timeout",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should fail due to timeout
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED due to timeout, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentZeebe] != types.ComponentStatusFailed {
		t.Errorf("Expected Zeebe to be FAILED due to timeout, got: %s", execution.ComponentStatus[types.ComponentZeebe])
	}
}

// Test 4: Duration calculation logic in createBackupHistory
func TestCreateBackupHistory_DurationCalculation(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test execution with known start and end times
	startTime := time.Now()
	endTime := startTime.Add(65 * time.Second) // 65 seconds duration

	execution := &models.BackupExecution{
		ID:                "test-backup",
		BackupID:          "test-backup",
		CamundaInstanceID: "test-instance",
		StartTime:         startTime,
		EndTime:           &endTime,
		Status:            types.BackupStatusCompleted,
		ComponentStatus: map[string]types.ComponentStatus{
			types.ComponentZeebe: types.ComponentStatusCompleted,
		},
	}

	instance := setupTestInstance("test-instance", "Test Instance")
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test duration calculation",
	}

	// Call createBackupHistory
	history := orchestrator.createBackupHistory(req, execution)

	// Verify duration calculation
	if history.DurationSeconds == nil {
		t.Fatal("Expected DurationSeconds to be set")
	}
	if *history.DurationSeconds != 65 {
		t.Errorf("Expected duration 65 seconds, got: %d", *history.DurationSeconds)
	}
}

func TestCreateBackupHistory_NoDurationWhenNotComplete(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test execution without end time
	execution := &models.BackupExecution{
		ID:                "test-backup",
		BackupID:          "test-backup",
		CamundaInstanceID: "test-instance",
		StartTime:         time.Now(),
		EndTime:           nil, // Not completed yet
		Status:            types.BackupStatusRunning,
		ComponentStatus:   map[string]types.ComponentStatus{},
	}

	instance := setupTestInstance("test-instance", "Test Instance")
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test no duration",
	}

	// Call createBackupHistory
	history := orchestrator.createBackupHistory(req, execution)

	// Verify no duration is set
	if history.DurationSeconds != nil {
		t.Errorf("Expected DurationSeconds to be nil, got: %d", *history.DurationSeconds)
	}
}

// Test 5: Context cancellation scenarios
func TestExecuteBackup_ContextCancellation(t *testing.T) {
	// Create mock server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			// Delay to allow cancellation
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Create context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test context cancellation",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should handle cancellation gracefully
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	// The backup should fail because the context was cancelled during polling
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED due to cancellation, got: %s", execution.Status)
	}
}

func TestExecuteBackup_ContextCancellation_Parallel(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance with parallel execution
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.OperateStatusEndpoint = server.URL + "/operate/backup/status"
	instance.ParallelExecution = true
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
		{Name: types.ComponentOperate, Enabled: true},
	}

	// Create context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test parallel context cancellation",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	// At least one component should have failed due to cancellation
	hasFailure := false
	for _, status := range execution.ComponentStatus {
		if status == types.ComponentStatusFailed {
			hasFailure = true
			break
		}
	}
	if !hasFailure {
		t.Error("Expected at least one component to fail due to context cancellation")
	}
}

// Test 6: Error scenarios during backup history storage
type failingS3Storage struct {
	*mockS3Storage
	failStoreHistory bool
	failUpdateStatus bool
}

func newFailingS3Storage(failStoreHistory, failUpdateStatus bool) *failingS3Storage {
	return &failingS3Storage{
		mockS3Storage:    newMockS3Storage(),
		failStoreHistory: failStoreHistory,
		failUpdateStatus: failUpdateStatus,
	}
}

func (f *failingS3Storage) StoreBackupHistory(history *models.BackupHistory) error {
	if f.failStoreHistory {
		return fmt.Errorf("simulated S3 storage failure")
	}
	return f.mockS3Storage.StoreBackupHistory(history)
}

func (f *failingS3Storage) UpdateBackupStatus(camundaInstanceID, backupID string, status types.BackupStatus) error {
	if f.failUpdateStatus {
		return fmt.Errorf("simulated S3 update failure")
	}
	return f.mockS3Storage.UpdateBackupStatus(camundaInstanceID, backupID, status)
}

func TestExecuteBackup_BackupHistoryStorageFailure(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator with failing S3 storage
	fileStorage := newMockFileStorage()
	s3Storage := newFailingS3Storage(true, false) // Fail on StoreBackupHistory
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test history storage failure",
	}

	// The backup should still complete even if history storage fails
	execution, err := orchestrator.ExecuteBackup(ctx, req)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	// Backup should complete successfully despite history storage failure
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED despite history failure, got: %s", execution.Status)
	}
}

func TestExecuteBackup_StatusUpdateFailure(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator with failing S3 storage
	fileStorage := newMockFileStorage()
	s3Storage := newFailingS3Storage(false, true) // Fail on UpdateBackupStatus
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test status update failure",
	}

	// The backup should still complete even if status update fails
	execution, err := orchestrator.ExecuteBackup(ctx, req)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	// Backup should complete successfully despite status update failure
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED despite update failure, got: %s", execution.Status)
	}
}

// Test 7: Various status response formats for pollBackupStatus
func TestPollBackupStatus_StatusField(t *testing.T) {
	// Create mock server that returns "status" field instead of "state"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			// Use "status" instead of "state"
			json.NewEncoder(w).Encode(map[string]string{"status": "COMPLETED"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test status field",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}
}

func TestPollBackupStatus_AlternativeCompletionStates(t *testing.T) {
	testCases := []struct {
		name          string
		state         string
		expectedState types.BackupStatus
	}{
		{"COMPLETE", "COMPLETE", types.BackupStatusCompleted},
		{"SUCCESS", "SUCCESS", types.BackupStatusCompleted},
		{"FAILURE", "FAILURE", types.BackupStatusFailed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create mock server
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
				} else if r.Method == http.MethodGet {
					w.WriteHeader(http.StatusOK)
					json.NewEncoder(w).Encode(map[string]string{"state": tc.state})
				}
			}))
			defer server.Close()

			// Set up orchestrator
			fileStorage := newMockFileStorage()
			s3Storage := newMockS3Storage()
			httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
			logger := utils.NewLogger("test")
			orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

			// Create test instance
			instance := setupTestInstance("test-instance", "Test Instance")
			instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
			instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
			instance.Components = []models.CamundaComponentConfig{
				{Name: types.ComponentZeebe, Enabled: true},
			}

			// Execute backup
			ctx := context.Background()
			req := BackupRequest{
				CamundaInstance: instance,
				TriggerType:     types.TriggerTypeManual,
				BackupReason:    "Test " + tc.name,
			}

			execution, err := orchestrator.ExecuteBackup(ctx, req)

			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}
			if execution.Status != tc.expectedState {
				t.Errorf("Expected status %s, got: %s", tc.expectedState, execution.Status)
			}
		})
	}
}

func TestPollBackupStatus_MissingStateField(t *testing.T) {
	callCount := 0
	// Create mock server that returns response without state/status field initially, then completes
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			callCount++
			w.WriteHeader(http.StatusOK)
			if callCount < 3 {
				// First two calls return response without state field
				json.NewEncoder(w).Encode(map[string]string{"backupId": "test-id"})
			} else {
				// Third call returns completed
				json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
			}
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 50*time.Millisecond, 10)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test missing state field",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Should eventually complete after retries
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED after retry, got: %s", execution.Status)
	}
}

func TestPollBackupStatus_404NotFound(t *testing.T) {
	// Create mock server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "Not found"})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 50*time.Millisecond, 10)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test 404 not found",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Should fail immediately on 404 (non-retryable)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED on 404, got: %s", execution.Status)
	}
}

func TestPollBackupStatus_TransientServerError(t *testing.T) {
	callCount := 0
	// Create mock server that returns 500 initially, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			callCount++
			if callCount < 3 {
				// First two calls return server error
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "Transient error"})
			} else {
				// Third call succeeds
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
			}
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 50*time.Millisecond, 10)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test transient error recovery",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Should eventually complete after recovering from transient errors
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED after transient error recovery, got: %s", execution.Status)
	}
}

func TestPollBackupStatus_InvalidJSON(t *testing.T) {
	callCount := 0
	// Create mock server that returns invalid JSON initially, then valid
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			callCount++
			w.WriteHeader(http.StatusOK)
			if callCount < 3 {
				// Return invalid JSON
				w.Write([]byte("not valid json"))
			} else {
				// Return valid JSON
				json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
			}
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 50*time.Millisecond, 10)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test invalid JSON recovery",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Should eventually complete after recovering from parse errors
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED after JSON error recovery, got: %s", execution.Status)
	}
}
