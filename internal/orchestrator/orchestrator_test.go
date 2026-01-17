package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
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

func TestNewOrchestrator(t *testing.T) {
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")

	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, logger)

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
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, logger)

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
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, logger)

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
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, logger)

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
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, logger)

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
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, logger)

	// Create test instance
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.ZeebeStatusEndpoint = server.URL + "/zeebe/backup/status"

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

	// Check for specific log entries
	logContent := ""
	for _, log := range logs {
		logContent += log
	}

	expectedLogs := []string{
		"Backup started",
		"Trigger type: SCHEDULED",
		"Execution mode: sequential",
		"Backup ID stored in S3",
		"Executing backup for component: zeebe",
		"Backup completed with status",
	}

	for _, expected := range expectedLogs {
		if !strings.Contains(logContent, expected) {
			t.Errorf("Expected log to contain '%s'", expected)
		}
	}
}

func TestBackupStats_Calculation(t *testing.T) {
	// Set up orchestrator
	fileStorage := newMockFileStorage()
	s3Storage := newMockS3Storage()
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	logger := utils.NewLogger("test")
	orchestrator := NewOrchestrator(fileStorage, s3Storage, httpClient, logger)

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
