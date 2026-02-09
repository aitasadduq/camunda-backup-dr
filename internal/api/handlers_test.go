package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/orchestrator"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// mockCamundaManager implements CamundaManager for testing
type mockCamundaManager struct {
	instances []models.CamundaInstance
	err       error
}

func (m *mockCamundaManager) CreateInstance(instance *models.CamundaInstance) error {
	if m.err != nil {
		return m.err
	}
	m.instances = append(m.instances, *instance)
	return nil
}

func (m *mockCamundaManager) GetInstance(id string) (*models.CamundaInstance, error) {
	if m.err != nil {
		return nil, m.err
	}
	for i := range m.instances {
		if m.instances[i].ID == id {
			return &m.instances[i], nil
		}
	}
	return nil, utils.ErrCamundaInstanceNotFound
}

func (m *mockCamundaManager) ListInstances() ([]models.CamundaInstance, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.instances, nil
}

func (m *mockCamundaManager) UpdateInstance(id string, updates *models.CamundaInstance) error {
	if m.err != nil {
		return m.err
	}
	for i := range m.instances {
		if m.instances[i].ID == id {
			m.instances[i] = *updates
			return nil
		}
	}
	return utils.ErrCamundaInstanceNotFound
}

func (m *mockCamundaManager) DeleteInstance(id string) error {
	if m.err != nil {
		return m.err
	}
	for i := range m.instances {
		if m.instances[i].ID == id {
			m.instances = append(m.instances[:i], m.instances[i+1:]...)
			return nil
		}
	}
	return utils.ErrCamundaInstanceNotFound
}

func (m *mockCamundaManager) EnableInstance(id string) error {
	if m.err != nil {
		return m.err
	}
	for i := range m.instances {
		if m.instances[i].ID == id {
			m.instances[i].Enabled = true
			return nil
		}
	}
	return utils.ErrCamundaInstanceNotFound
}

func (m *mockCamundaManager) DisableInstance(id string) error {
	if m.err != nil {
		return m.err
	}
	for i := range m.instances {
		if m.instances[i].ID == id {
			m.instances[i].Enabled = false
			return nil
		}
	}
	return utils.ErrCamundaInstanceNotFound
}

// mockOrchestrator implements BackupOrchestrator for testing
type mockOrchestrator struct {
	backupRunning bool
	execution     *models.BackupExecution
	err           error
}

func (m *mockOrchestrator) ExecuteBackup(ctx context.Context, req orchestrator.BackupRequest) (*models.BackupExecution, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.execution != nil {
		return m.execution, nil
	}
	return &models.BackupExecution{
		ID:                "test-backup-id",
		CamundaInstanceID: req.CamundaInstance.ID,
		BackupID:          "test-backup-id",
		Status:            types.BackupStatusCompleted,
	}, nil
}

func (m *mockOrchestrator) IsBackupRunning() bool {
	return m.backupRunning
}

// mockHistoryProvider implements BackupHistoryProvider for testing
type mockHistoryProvider struct {
	history []*models.BackupHistory
	err     error
}

func (m *mockHistoryProvider) GetBackupHistory(camundaInstanceID, backupID string) (*models.BackupHistory, error) {
	if m.err != nil {
		return nil, m.err
	}
	for _, h := range m.history {
		if h.CamundaInstanceID == camundaInstanceID && h.BackupID == backupID {
			return h, nil
		}
	}
	return nil, utils.ErrBackupNotFound
}

func (m *mockHistoryProvider) ListBackupHistory(camundaInstanceID string, status types.BackupStatus) ([]*models.BackupHistory, error) {
	if m.err != nil {
		return nil, m.err
	}
	var result []*models.BackupHistory
	for _, h := range m.history {
		if h.CamundaInstanceID == camundaInstanceID {
			if status == "" || h.Status == status {
				result = append(result, h)
			}
		}
	}
	return result, nil
}

// mockScheduler implements SchedulerInterface for testing
type mockScheduler struct {
	running      bool
	jobsCount    int
	enabledJobs  int
	lockAcquired bool
}

func (m *mockScheduler) IsRunning() bool {
	return m.running
}

func (m *mockScheduler) GetJobsCount() int {
	return m.jobsCount
}

func (m *mockScheduler) GetEnabledJobsCount() int {
	return m.enabledJobs
}

func (m *mockScheduler) TryAcquireBackupLock(instanceID string) bool {
	return !m.lockAcquired
}

func (m *mockScheduler) ReleaseBackupLock() {}

func (m *mockScheduler) RegisterJob(instanceID, schedule string, enabled bool) error {
	return nil
}

func (m *mockScheduler) DeregisterJob(instanceID string) error {
	return nil
}

func (m *mockScheduler) UpdateJob(instanceID, schedule string, enabled bool) error {
	return nil
}

func newTestHandlers() (*Handlers, *mockCamundaManager, *mockOrchestrator, *mockHistoryProvider, *mockScheduler) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	handlers := NewHandlers(cm, orch, hist, sched, logger)
	return handlers, cm, orch, hist, sched
}

func TestHealthzHandler(t *testing.T) {
	handlers, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	handlers.HealthzHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != "healthy" {
		t.Errorf("expected status 'healthy', got '%s'", resp.Status)
	}
}

func TestReadyzHandler(t *testing.T) {
	handlers, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handlers.ReadyzHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != "ready" {
		t.Errorf("expected status 'ready', got '%s'", resp.Status)
	}
}

func TestSystemStatusHandler(t *testing.T) {
	handlers, cm, _, _, sched := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance", Enabled: true},
	}
	sched.jobsCount = 1
	sched.enabledJobs = 1

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	handlers.SystemStatusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp SystemStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.Status != "ok" {
		t.Errorf("expected status 'ok', got '%s'", resp.Status)
	}
	if resp.CamundaInstances.Total != 1 {
		t.Errorf("expected 1 total instance, got %d", resp.CamundaInstances.Total)
	}
	if resp.CamundaInstances.Enabled != 1 {
		t.Errorf("expected 1 enabled instance, got %d", resp.CamundaInstances.Enabled)
	}
}

func TestListCamundaInstancesHandler(t *testing.T) {
	handlers, cm, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
		{ID: "test-2", Name: "Test Instance 2"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas", nil)
	w := httptest.NewRecorder()

	handlers.ListCamundaInstancesHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var instances []models.CamundaInstance
	if err := json.Unmarshal(w.Body.Bytes(), &instances); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(instances) != 2 {
		t.Errorf("expected 2 instances, got %d", len(instances))
	}
}

func TestCreateCamundaInstanceHandler(t *testing.T) {
	handlers, _, _, _, _ := newTestHandlers()

	instance := models.CamundaInstance{
		ID:      "new-instance",
		Name:    "New Instance",
		BaseURL: "http://localhost:8080",
	}

	body, _ := json.Marshal(instance)
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("expected status %d, got %d: %s", http.StatusCreated, w.Code, w.Body.String())
	}
}

func TestCreateCamundaInstanceHandler_MissingID(t *testing.T) {
	handlers, _, _, _, _ := newTestHandlers()

	instance := models.CamundaInstance{
		Name:    "New Instance",
		BaseURL: "http://localhost:8080",
	}

	body, _ := json.Marshal(instance)
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestGetCamundaInstanceHandler(t *testing.T) {
	handlers, cm, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()

	handlers.GetCamundaInstanceHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var instance models.CamundaInstance
	if err := json.Unmarshal(w.Body.Bytes(), &instance); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if instance.ID != "test-1" {
		t.Errorf("expected ID 'test-1', got '%s'", instance.ID)
	}
}

func TestGetCamundaInstanceHandler_NotFound(t *testing.T) {
	handlers, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent", nil)
	w := httptest.NewRecorder()

	handlers.GetCamundaInstanceHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestDeleteCamundaInstanceHandler(t *testing.T) {
	handlers, cm, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteCamundaInstanceHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	if len(cm.instances) != 0 {
		t.Errorf("expected 0 instances after delete, got %d", len(cm.instances))
	}
}

func TestEnableCamundaInstanceHandler(t *testing.T) {
	handlers, cm, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1", Enabled: false},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/enable", nil)
	w := httptest.NewRecorder()

	handlers.EnableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	if !cm.instances[0].Enabled {
		t.Error("expected instance to be enabled")
	}
}

func TestDisableCamundaInstanceHandler(t *testing.T) {
	handlers, cm, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1", Enabled: true},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/disable", nil)
	w := httptest.NewRecorder()

	handlers.DisableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	if cm.instances[0].Enabled {
		t.Error("expected instance to be disabled")
	}
}

func TestTriggerBackupHandler(t *testing.T) {
	handlers, cm, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1", Enabled: true},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backup", nil)
	w := httptest.NewRecorder()

	handlers.TriggerBackupHandler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d: %s", http.StatusAccepted, w.Code, w.Body.String())
	}

	var resp BackupTriggerResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if resp.BackupID == "" {
		t.Error("expected backup ID in response")
	}
}

func TestTriggerBackupHandler_NotFound(t *testing.T) {
	handlers, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/non-existent/backup", nil)
	w := httptest.NewRecorder()

	handlers.TriggerBackupHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestTriggerBackupHandler_BackupInProgress(t *testing.T) {
	handlers, cm, _, _, sched := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1", Enabled: true},
	}
	sched.lockAcquired = true

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backup", nil)
	w := httptest.NewRecorder()

	handlers.TriggerBackupHandler(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status %d, got %d: %s", http.StatusConflict, w.Code, w.Body.String())
	}
}

func TestListBackupHistoryHandler(t *testing.T) {
	handlers, cm, _, hist, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	hist.history = []*models.BackupHistory{
		{BackupID: "backup-1", CamundaInstanceID: "test-1", Status: types.BackupStatusCompleted},
		{BackupID: "backup-2", CamundaInstanceID: "test-1", Status: types.BackupStatusCompleted},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups", nil)
	w := httptest.NewRecorder()

	handlers.ListBackupHistoryHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	var history []*models.BackupHistory
	if err := json.Unmarshal(w.Body.Bytes(), &history); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(history) != 2 {
		t.Errorf("expected 2 history entries, got %d", len(history))
	}
}

func TestGetBackupDetailsHandler(t *testing.T) {
	handlers, cm, _, hist, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	hist.history = []*models.BackupHistory{
		{BackupID: "backup-1", CamundaInstanceID: "test-1", Status: types.BackupStatusCompleted},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupDetailsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	var history models.BackupHistory
	if err := json.Unmarshal(w.Body.Bytes(), &history); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if history.BackupID != "backup-1" {
		t.Errorf("expected backup ID 'backup-1', got '%s'", history.BackupID)
	}
}

func TestGetBackupDetailsHandler_NotFound(t *testing.T) {
	handlers, cm, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/non-existent", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupDetailsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}
