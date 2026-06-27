package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
		OperateBackupEndpoint:  "http://localhost:8080/operate/backup",
		TasklistBackupEndpoint: "http://localhost:8080/tasklist/backup",
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
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
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
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
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
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"

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

// Test: per-component timing is propagated into ComponentBackupInfo
func TestCreateBackupHistory_ComponentDuration(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	start := time.Now()
	zeebeStart := start.Add(1 * time.Second)
	zeebeEnd := zeebeStart.Add(12 * time.Second)

	execution := &models.BackupExecution{
		ID:                "test-backup",
		BackupID:          "test-backup",
		CamundaInstanceID: "test-instance",
		StartTime:         start,
		Status:            types.BackupStatusCompleted,
		ComponentStatus: map[string]types.ComponentStatus{
			types.ComponentZeebe:    types.ComponentStatusCompleted,
			types.ComponentOptimize: types.ComponentStatusSkipped, // never ran -> no timing
		},
		ComponentTimings: map[string]models.ComponentTiming{
			types.ComponentZeebe: {StartTime: zeebeStart, EndTime: zeebeEnd},
		},
	}

	req := BackupRequest{
		CamundaInstance: setupTestInstance("test-instance", "Test Instance"),
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test component duration",
	}

	history := orchestrator.createBackupHistory(req, execution)

	zeebe, ok := history.Components[types.ComponentZeebe]
	if !ok {
		t.Fatal("Expected Zeebe component in history")
	}
	if zeebe.DurationSeconds != 12 {
		t.Errorf("Expected Zeebe duration 12s, got: %d", zeebe.DurationSeconds)
	}
	if zeebe.StartTime == nil || zeebe.EndTime == nil {
		t.Error("Expected Zeebe StartTime and EndTime to be set")
	}

	// Component without timing should have zero duration and nil times
	optimize := history.Components[types.ComponentOptimize]
	if optimize.DurationSeconds != 0 {
		t.Errorf("Expected Optimize duration 0s, got: %d", optimize.DurationSeconds)
	}
	if optimize.StartTime != nil || optimize.EndTime != nil {
		t.Error("Expected Optimize StartTime and EndTime to be nil")
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
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
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

// =============================================================================
// Elasticsearch Backup Tests
// =============================================================================

// mockElasticsearchServer creates a mock Elasticsearch server for testing
func mockElasticsearchServer(t *testing.T, snapshotBehavior string) *httptest.Server {
	callCount := 0
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle snapshot creation: PUT /_snapshot/{repo}/{snapshot}
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/_snapshot/") {
			switch snapshotBehavior {
			case "success", "immediate-success", "polling-success":
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"accepted": true,
				})
			case "create-failure":
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": map[string]interface{}{
						"type":   "repository_exception",
						"reason": "Could not create snapshot",
					},
				})
			case "repo-not-found":
				w.WriteHeader(http.StatusNotFound)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": map[string]interface{}{
						"type":   "repository_missing_exception",
						"reason": "Repository not found",
					},
				})
			default:
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{"accepted": true})
			}
			return
		}

		// Handle snapshot status: GET /_snapshot/{repo}/{snapshot}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/_snapshot/") {
			callCount++
			switch snapshotBehavior {
			case "immediate-success":
				// Return SUCCESS immediately
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"snapshots": []map[string]interface{}{
						{"state": "SUCCESS"},
					},
				})
			case "polling-success":
				// First few calls return IN_PROGRESS, then SUCCESS
				w.WriteHeader(http.StatusOK)
				state := "IN_PROGRESS"
				if callCount >= 3 {
					state = "SUCCESS"
				}
				json.NewEncoder(w).Encode(map[string]interface{}{
					"snapshots": []map[string]interface{}{
						{"state": state},
					},
				})
			case "snapshot-failed":
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"snapshots": []map[string]interface{}{
						{"state": "FAILED"},
					},
				})
			case "snapshot-partial":
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"snapshots": []map[string]interface{}{
						{"state": "PARTIAL"},
					},
				})
			case "always-in-progress":
				// Always return IN_PROGRESS (for timeout testing)
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"snapshots": []map[string]interface{}{
						{"state": "IN_PROGRESS"},
					},
				})
			default:
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"snapshots": []map[string]interface{}{
						{"state": "SUCCESS"},
					},
				})
			}
			return
		}

		// Default: return 404
		w.WriteHeader(http.StatusNotFound)
	}))
}

func TestExecuteElasticsearchBackup_Success(t *testing.T) {
	// Create mock Elasticsearch server
	server := mockElasticsearchServer(t, "polling-success")
	defer server.Close()

	// Set up orchestrator with proper config
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance with only Elasticsearch enabled
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.ElasticsearchUsername = "elastic"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch backup",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusCompleted {
		t.Errorf("Expected Elasticsearch to be COMPLETED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_ImmediateSuccess(t *testing.T) {
	// Create mock Elasticsearch server that returns SUCCESS immediately
	server := mockElasticsearchServer(t, "immediate-success")
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.ElasticsearchUsername = "elastic"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test immediate Elasticsearch success",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should complete quickly without waiting for ticker
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusCompleted {
		t.Errorf("Expected Elasticsearch to be COMPLETED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_Skipped_NoEndpoint(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance with Elasticsearch enabled but no endpoint
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = "" // No endpoint configured
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch skipped",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusSkipped {
		t.Errorf("Expected Elasticsearch to be SKIPPED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_Failed_NoConfig(t *testing.T) {
	// Create mock Elasticsearch server
	server := mockElasticsearchServer(t, "success")
	defer server.Close()

	// Set up orchestrator WITHOUT config (nil)
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, nil, logger, 50*time.Millisecond, 10)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch no config",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should fail due to missing config
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusFailed {
		t.Errorf("Expected Elasticsearch to be FAILED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_Failed_NoRepository(t *testing.T) {
	// Create mock Elasticsearch server
	server := mockElasticsearchServer(t, "success")
	defer server.Close()

	// Set up orchestrator with config that has empty repository
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := &config.Config{
		DefaultElasticsearchSnapshotRepository: "", // Empty repository
		DefaultElasticsearchSnapshotNamePrefix: "",
	}
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch no repository",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should fail due to missing repository
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusFailed {
		t.Errorf("Expected Elasticsearch to be FAILED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_CreateSnapshotFailure(t *testing.T) {
	// Create mock Elasticsearch server that fails on snapshot creation
	server := mockElasticsearchServer(t, "create-failure")
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch create failure",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should fail due to snapshot creation error
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusFailed {
		t.Errorf("Expected Elasticsearch to be FAILED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_SnapshotFailed(t *testing.T) {
	// Create mock Elasticsearch server that returns FAILED state
	server := mockElasticsearchServer(t, "snapshot-failed")
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch snapshot failed",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should fail due to snapshot failure
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusFailed {
		t.Errorf("Expected Elasticsearch to be FAILED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_SnapshotPartial(t *testing.T) {
	// Create mock Elasticsearch server that returns PARTIAL state
	server := mockElasticsearchServer(t, "snapshot-partial")
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch partial snapshot",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should fail due to partial snapshot
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusFailed {
		t.Errorf("Expected Elasticsearch to be FAILED, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_PollingTimeout(t *testing.T) {
	// Create mock Elasticsearch server that always returns IN_PROGRESS
	server := mockElasticsearchServer(t, "always-in-progress")
	defer server.Close()

	// Set up orchestrator with very limited polling attempts
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	// Only 3 poll attempts with 50ms interval = fast timeout
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 3)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch polling timeout",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results - should fail due to timeout
	if err != nil {
		t.Fatalf("Expected no error from ExecuteBackup, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentElasticsearch] != types.ComponentStatusFailed {
		t.Errorf("Expected Elasticsearch to be FAILED due to timeout, got: %s", execution.ComponentStatus[types.ComponentElasticsearch])
	}
}

func TestExecuteElasticsearchBackup_ContextCancellation(t *testing.T) {
	// Create mock Elasticsearch server that delays response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"accepted": true})
			return
		}
		if r.Method == http.MethodGet {
			// Delay before returning IN_PROGRESS
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"snapshots": []map[string]interface{}{
					{"state": "IN_PROGRESS"},
				},
			})
		}
	}))
	defer server.Close()

	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := setupTestConfig()
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 100)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Create context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after a short delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch context cancellation",
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

func TestExecuteElasticsearchBackup_WithSnapshotNamePrefix(t *testing.T) {
	// Track the snapshot name used in the request
	var capturedSnapshotName string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/_snapshot/") {
			// Extract snapshot name from path: /_snapshot/{repo}/{snapshot}
			parts := strings.Split(r.URL.Path, "/")
			if len(parts) >= 4 {
				capturedSnapshotName = parts[3]
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"accepted": true})
			return
		}
		if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"snapshots": []map[string]interface{}{
					{"state": "SUCCESS"},
				},
			})
		}
	}))
	defer server.Close()

	// Set up orchestrator with a snapshot name prefix
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := &config.Config{
		DefaultElasticsearchSnapshotRepository: "camunda-backup",
		DefaultElasticsearchSnapshotNamePrefix: "my-prefix",
	}
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	// Create test instance with Elasticsearch
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	// Execute backup
	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test Elasticsearch with prefix",
	}

	execution, err := orchestrator.ExecuteBackup(ctx, req)

	// Verify results
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}

	// Verify the snapshot name includes the prefix
	if !strings.HasPrefix(capturedSnapshotName, "my-prefix-") {
		t.Errorf("Expected snapshot name to start with 'my-prefix-', got: %s", capturedSnapshotName)
	}
}

// =============================================================================
// Additional coverage tests
// =============================================================================

func TestExecuteBackup_NilInstance(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: nil,
		TriggerType:     types.TriggerTypeManual,
	}

	_, err := orch.ExecuteBackup(ctx, req)
	if err == nil {
		t.Fatal("Expected error for nil instance")
	}
	if !strings.Contains(err.Error(), "camunda instance is nil") {
		t.Errorf("Expected 'camunda instance is nil' error, got: %v", err)
	}
}

func TestExecuteBackup_BackupAlreadyInProgress(t *testing.T) {
	// Create mock server that delays so the backup stays "running"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "triggered"})
		} else if r.Method == http.MethodGet {
			// Simulate slow polling
			time.Sleep(2 * time.Second)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	// Start first backup in background
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		orch.ExecuteBackup(ctx, BackupRequest{
			CamundaInstance: instance,
			TriggerType:     types.TriggerTypeManual,
		})
	}()

	// Wait a short time for the first backup to acquire the lock
	time.Sleep(200 * time.Millisecond)

	// Try second backup — should fail
	ctx := context.Background()
	_, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})
	if err == nil {
		t.Fatal("Expected error for concurrent backup")
	}
	if !strings.Contains(err.Error(), "backup already in progress") {
		t.Errorf("Expected 'backup already in progress' error, got: %v", err)
	}

	<-done
}

// failingStoreLatestS3 fails on StoreLatestBackupID
type failingStoreLatestS3 struct {
	*mockS3Storage
}

func (f *failingStoreLatestS3) StoreLatestBackupID(camundaInstanceID, backupID string) error {
	return fmt.Errorf("simulated StoreLatestBackupID failure")
}

func TestExecuteBackup_StoreLatestBackupIDFailure(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := &failingStoreLatestS3{mockS3Storage: newMockS3Storage()}
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, _ := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	// Should return the execution with failed status
	if execution == nil {
		t.Fatal("Expected execution to be returned even on failure")
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected status FAILED, got: %s", execution.Status)
	}
	if !strings.Contains(execution.ErrorMessage, "Failed to store backup ID in S3") {
		t.Errorf("Expected error message about S3 failure, got: %s", execution.ErrorMessage)
	}
}

func TestSetAlerter(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	alerter := utils.NewAlerter("http://example.com/webhook", logger)
	orch.SetAlerter(alerter)

	// Verify alerter was set
	if orch.alerter == nil {
		t.Error("Expected alerter to be set")
	}
}

func TestSetRetentionFunc(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	orch.SetRetentionFunc(func(instance *models.CamundaInstance) {})

	if orch.retentionFunc == nil {
		t.Error("Expected retentionFunc to be set")
	}
}

func TestIsBackupRunning(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	// Initially not running
	if orch.IsBackupRunning() {
		t.Error("Expected backup to not be running initially")
	}
}

func TestSetRetentionFunc_CalledAfterSuccessfulBackup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	retentionCalled := make(chan struct{}, 1)
	orch.SetRetentionFunc(func(instance *models.CamundaInstance) {
		retentionCalled <- struct{}{}
	})

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.SuccessRetention = 5
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected COMPLETED, got: %s", execution.Status)
	}

	// Wait for retention function to be called (it runs async)
	select {
	case <-retentionCalled:
		// success
	case <-time.After(2 * time.Second):
		t.Error("Retention function was not called after successful backup")
	}
}

func TestHandleBackupFailure_WithAlerter(t *testing.T) {
	// Create a server that fails on Zeebe
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "fail"})
		}
	}))
	defer server.Close()

	// Create alert webhook
	var alertReceived int32
	alertServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&alertReceived, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer alertServer.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	clientCfg := camunda.DefaultHTTPClientConfig()
	clientCfg.MaxRetries = 0
	httpClient := camunda.NewHTTPClient(clientCfg, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	alerter := utils.NewAlerter(alertServer.URL, logger)
	orch.SetAlerter(alerter)

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = ""
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected FAILED, got: %s", execution.Status)
	}

	// Wait for async alert
	time.Sleep(500 * time.Millisecond)
	received := atomic.LoadInt32(&alertReceived)
	if received < 1 {
		t.Errorf("Expected at least 1 alert for backup failure, got %d", received)
	}
}

func TestHandleBackupFailure_NoAlerter(t *testing.T) {
	// Server that fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	clientCfg := camunda.DefaultHTTPClientConfig()
	clientCfg.MaxRetries = 0
	httpClient := camunda.NewHTTPClient(clientCfg, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)
	// Note: no alerter set

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = ""
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected FAILED, got: %s", execution.Status)
	}
}

// failingFileStorage returns errors for log writes
type failingFileStorage struct {
	*mockFileStorage
	failWrite bool
}

func (f *failingFileStorage) WriteToLogFile(camundaInstanceID, backupID, message string) error {
	if f.failWrite {
		return fmt.Errorf("simulated log write failure")
	}
	return f.mockFileStorage.WriteToLogFile(camundaInstanceID, backupID, message)
}

func TestWriteLog_FileWriteError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	base := newMockFileStorage()
	fileStorage := &failingFileStorage{mockFileStorage: base, failWrite: true}
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = ""
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	// Backup should still complete even if log writes fail
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected COMPLETED despite log write failures, got: %s", execution.Status)
	}
}

func TestExecuteZeebeBackup_TriggerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "internal error"})
	}))
	defer server.Close()

	clientCfg := camunda.DefaultHTTPClientConfig()
	clientCfg.MaxRetries = 0
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(clientCfg, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = ""
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentZeebe] != types.ComponentStatusFailed {
		t.Errorf("Expected Zeebe FAILED, got: %s", execution.ComponentStatus[types.ComponentZeebe])
	}
}

func TestExecuteTasklistBackup_TriggerFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "internal error"})
	}))
	defer server.Close()

	clientCfg := camunda.DefaultHTTPClientConfig()
	clientCfg.MaxRetries = 0
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(clientCfg, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = ""
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentTasklist, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected FAILED, got: %s", execution.Status)
	}
	if execution.ComponentStatus[types.ComponentTasklist] != types.ComponentStatusFailed {
		t.Errorf("Expected Tasklist FAILED, got: %s", execution.ComponentStatus[types.ComponentTasklist])
	}
}

func TestExecuteTasklistBackup_Skipped(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = ""
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = "" // No endpoint
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentTasklist, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.ComponentStatus[types.ComponentTasklist] != types.ComponentStatusSkipped {
		t.Errorf("Expected Tasklist SKIPPED, got: %s", execution.ComponentStatus[types.ComponentTasklist])
	}
}

// failingDeleteLogFileStorage fails on DeleteLogFile
type failingDeleteLogFileStorage struct {
	*mockFileStorage
}

func (f *failingDeleteLogFileStorage) DeleteLogFile(camundaInstanceID, backupID string) error {
	return fmt.Errorf("simulated DeleteLogFile failure")
}

func TestExecuteBackup_StoreLatestBackupIDFailure_DeleteLogFileError(t *testing.T) {
	base := newMockFileStorage()
	fileStorage := &failingDeleteLogFileStorage{mockFileStorage: base}
	s3Storage := &failingStoreLatestS3{mockS3Storage: newMockS3Storage()}
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, _ := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if execution == nil {
		t.Fatal("Expected execution even on failure")
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected FAILED, got: %s", execution.Status)
	}
}

func TestExecuteBackup_HandleBackupFailure_ErrorMessagePreserved(t *testing.T) {
	// Server that fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	clientCfg := camunda.DefaultHTTPClientConfig()
	clientCfg.MaxRetries = 0
	httpClient := camunda.NewHTTPClient(clientCfg, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = ""
	instance.TasklistBackupEndpoint = ""
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
	}

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	// Verify backup history was stored with failed status
	history, _ := s3Storage.GetBackupHistory(instance.ID, execution.BackupID)
	if history == nil {
		t.Fatal("Expected backup history to be stored")
	}
	if history.Status != types.BackupStatusFailed {
		t.Errorf("Expected history FAILED, got: %s", history.Status)
	}
}

func TestPauseExporting_Success(t *testing.T) {
	var pauseCalled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/actuator/exporting/pause" && r.Method == http.MethodPost {
			pauseCalled.Store(true)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.URL.Path == "/actuator/exporting/resume" && r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	cfg := setupTestConfig()
	cfg.ExporterPauseMaxRetries = 3
	cfg.ExporterPauseRetryDelay = 1
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.ExportingEndpoint = server.URL + "/actuator/exporting"

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected COMPLETED, got: %s", execution.Status)
	}
	if !pauseCalled.Load() {
		t.Error("Expected pause endpoint to be called")
	}
}

func TestPauseExporting_SoftPause(t *testing.T) {
	var receivedQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/actuator/exporting/pause" {
			receivedQuery = r.URL.RawQuery
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.URL.Path == "/actuator/exporting/resume" {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	cfg := setupTestConfig()
	cfg.ExporterPauseMaxRetries = 3
	cfg.ExporterPauseRetryDelay = 1
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.ExportingEndpoint = server.URL + "/actuator/exporting"
	instance.SoftExportPause = true

	ctx := context.Background()
	_, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if receivedQuery != "soft=true" {
		t.Errorf("Expected query 'soft=true', got: %q", receivedQuery)
	}
}

func TestPauseExporting_FailureAbortsBackup(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/actuator/exporting/pause" {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 500})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"message": "ok"})
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:       5 * time.Second,
		MaxRetries:    0,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 1 * time.Second,
	}, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	cfg := setupTestConfig()
	cfg.ExporterPauseMaxRetries = 1
	cfg.ExporterPauseRetryDelay = 1
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.ExportingEndpoint = server.URL + "/actuator/exporting"

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error (failure captured in execution), got: %v", err)
	}
	if execution.Status != types.BackupStatusFailed {
		t.Errorf("Expected FAILED, got: %s", execution.Status)
	}
	if !strings.Contains(execution.ErrorMessage, "pause exporting") {
		t.Errorf("Expected error message about pause, got: %s", execution.ErrorMessage)
	}
}

func TestPauseExporting_RetryThenSucceed(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/actuator/exporting/pause" {
			count := attempts.Add(1)
			if count < 3 {
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{"status": 500})
				return
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.URL.Path == "/actuator/exporting/resume" {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:       5 * time.Second,
		MaxRetries:    0,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 1 * time.Second,
	}, utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	cfg := setupTestConfig()
	cfg.ExporterPauseMaxRetries = 5
	cfg.ExporterPauseRetryDelay = 1
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.ExportingEndpoint = server.URL + "/actuator/exporting"

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected COMPLETED, got: %s", execution.Status)
	}
	if attempts.Load() < 3 {
		t.Errorf("Expected at least 3 pause attempts, got: %d", attempts.Load())
	}
}

func TestResumeExporting_CalledAfterBackup(t *testing.T) {
	var resumeCalled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/actuator/exporting/pause" {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.URL.Path == "/actuator/exporting/resume" {
			resumeCalled.Store(true)
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
			return
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	cfg := setupTestConfig()
	cfg.ExporterPauseMaxRetries = 3
	cfg.ExporterPauseRetryDelay = 1
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.ExportingEndpoint = server.URL + "/actuator/exporting"

	ctx := context.Background()
	_, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if !resumeCalled.Load() {
		t.Error("Expected resume endpoint to be called after backup")
	}
}

func TestNoExportingEndpoint_SkipsPauseResume(t *testing.T) {
	var exportingCalled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "exporting") {
			exportingCalled.Store(true)
		}
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		} else if r.Method == http.MethodGet {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(fileStorage, s3Storage, httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	// ExportingEndpoint intentionally left empty

	ctx := context.Background()
	execution, err := orch.ExecuteBackup(ctx, BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
	})

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected COMPLETED, got: %s", execution.Status)
	}
	if exportingCalled.Load() {
		t.Error("Expected exporting endpoints NOT to be called when ExportingEndpoint is empty")
	}
}

func TestCallExportingEndpoint_StatusAsString(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"status": "204"})
	}))
	defer server.Close()

	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(newMockFileStorage(), newMockS3Storage(), httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	err := orch.callExportingEndpoint(context.Background(), server.URL+"/pause")
	if err != nil {
		t.Errorf("Expected success for status '204' as string, got: %v", err)
	}
}

func TestCallExportingEndpoint_NonSuccessStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{"status": 500})
	}))
	defer server.Close()

	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orch := NewOrchestrator(newMockFileStorage(), newMockS3Storage(), httpClient, setupTestConfig(), logger, 100*time.Millisecond, 50)

	err := orch.callExportingEndpoint(context.Background(), server.URL+"/pause")
	if err == nil {
		t.Error("Expected error for non-204 status")
	}
}

func TestExecuteElasticsearchBackup_InstanceRepositoryOverridesDefault(t *testing.T) {
	capturedRepo := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/_snapshot/") {
			parts := strings.Split(r.URL.Path, "/")
			if len(parts) >= 3 {
				capturedRepo = parts[2]
			}
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"accepted": true})
			return
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/_snapshot/") {
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"snapshots": []map[string]interface{}{
					{"snapshot": "snap", "state": "SUCCESS"},
				},
			})
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	cfg := &config.Config{
		DefaultElasticsearchSnapshotRepository: "global-repo",
		DefaultElasticsearchSnapshotNamePrefix: "snap",
	}
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, cfg, logger, 50*time.Millisecond, 10)

	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ElasticsearchEndpoint = server.URL
	instance.ElasticsearchSnapshotRepository = "instance-repo"
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentElasticsearch, Enabled: true},
	}

	ctx := context.Background()
	req := BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test instance repo override",
	}

	orchestrator.ExecuteBackup(ctx, req)

	if capturedRepo != "instance-repo" {
		t.Errorf("expected snapshot created against 'instance-repo', got %q", capturedRepo)
	}
}
