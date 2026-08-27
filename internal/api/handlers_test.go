package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
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

// mockRetentionManager implements RetentionManager for testing
type mockRetentionManager struct {
	orphaned    []*models.BackupHistory
	incomplete  []*models.BackupHistory
	failed      []*models.BackupHistory
	deleteErr   error
	listErr     error
	deleteForce bool
}

func (m *mockRetentionManager) DeleteBackup(camundaInstanceID, backupID string, force bool) error {
	m.deleteForce = force
	return m.deleteErr
}

func (m *mockRetentionManager) ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.orphaned, nil
}

func (m *mockRetentionManager) ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.incomplete, nil
}

func (m *mockRetentionManager) ListFailedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.failed, nil
}

// mockLogFileReader implements LogFileReader for testing
type mockLogFileReader struct {
	logs map[string]string // key is "instanceID/backupID"
	err  error
}

func (m *mockLogFileReader) ReadLogFile(camundaInstanceID, backupID string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	key := camundaInstanceID + "/" + backupID
	if content, ok := m.logs[key]; ok {
		return content, nil
	}
	return "", utils.ErrFileStorageFailed
}

func newTestHandlers() (*Handlers, *mockCamundaManager, *mockOrchestrator, *mockHistoryProvider, *mockScheduler, *mockRetentionManager, *mockLogFileReader) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, ret, lfr, logger, nil)
	return handlers, cm, orch, hist, sched, ret, lfr
}

func TestHealthzHandler(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

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
	handlers, _, _, _, _, _, _ := newTestHandlers()

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
	handlers, cm, _, _, sched, _, _ := newTestHandlers()

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
	handlers, cm, _, _, _, _, _ := newTestHandlers()

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
	handlers, _, _, _, _, _, _ := newTestHandlers()

	instance := models.CamundaInstance{
		ID:                  "new-instance",
		Name:                "New Instance",
		BaseURL:             "http://localhost:8080",
		BackupIDS3Endpoint:  "https://s3.example.com",
		BackupIDS3AccessKey: "AKIAIOSFODNN7EXAMPLE",
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
	handlers, _, _, _, _, _, _ := newTestHandlers()

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
	handlers, cm, _, _, _, _, _ := newTestHandlers()

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
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent", nil)
	w := httptest.NewRecorder()

	handlers.GetCamundaInstanceHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestDeleteCamundaInstanceHandler(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()

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
	handlers, cm, _, _, _, _, _ := newTestHandlers()

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
	handlers, cm, _, _, _, _, _ := newTestHandlers()

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
	handlers, cm, _, _, _, _, _ := newTestHandlers()

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
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/non-existent/backup", nil)
	w := httptest.NewRecorder()

	handlers.TriggerBackupHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestTriggerBackupHandler_BackupInProgress(t *testing.T) {
	handlers, cm, _, _, sched, _, _ := newTestHandlers()

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
	handlers, cm, _, hist, _, _, _ := newTestHandlers()

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
	handlers, cm, _, hist, _, _, _ := newTestHandlers()

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
	handlers, cm, _, _, _, _, _ := newTestHandlers()

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

// --- Retention Handler Tests ---

func TestDeleteBackupHandler_Success(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.deleteErr = nil

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestDeleteBackupHandler_NotFound(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.deleteErr = utils.ErrBackupNotFound

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d: %s", http.StatusNotFound, w.Code, w.Body.String())
	}
}

func TestDeleteBackupHandler_SafetyRefusal(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.deleteErr = fmt.Errorf("%w (backup-1)", utils.ErrCannotDeleteMostRecentBackup)

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status %d, got %d: %s", http.StatusConflict, w.Code, w.Body.String())
	}
}

func TestDeleteBackupHandler_ArtifactsRemain(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.deleteErr = fmt.Errorf("%w for backup-1: Zeebe backup deletion returned status 500", utils.ErrBackupArtifactsRemain)

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status %d, got %d: %s", http.StatusConflict, w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "artifacts_remain") {
		t.Errorf("expected an artifacts_remain error code, got: %s", w.Body.String())
	}
	if ret.deleteForce {
		t.Error("expected force to default to false")
	}
}

func TestDeleteBackupHandler_ForceQueryParam(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1?force=true", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
	if !ret.deleteForce {
		t.Error("expected force=true to reach the retention manager")
	}
}

func TestDeleteBackupHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/non-existent/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d: %s", http.StatusNotFound, w.Code, w.Body.String())
	}
}

func TestListOrphanedBackupsHandler(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.orphaned = []*models.BackupHistory{
		{BackupID: "orphaned-1", CamundaInstanceID: "test-1", Status: types.BackupStatusCompleted},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/orphaned", nil)
	w := httptest.NewRecorder()

	handlers.ListOrphanedBackupsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	var result []*models.BackupHistory
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("expected 1 orphaned backup, got %d", len(result))
	}
}

func TestListIncompleteBackupsHandler(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.incomplete = []*models.BackupHistory{
		{BackupID: "incomplete-1", CamundaInstanceID: "test-1", Status: types.BackupStatusIncomplete},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/incomplete", nil)
	w := httptest.NewRecorder()

	handlers.ListIncompleteBackupsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	var result []*models.BackupHistory
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("expected 1 incomplete backup, got %d", len(result))
	}
}

func TestListFailedBackupsHandler(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.failed = []*models.BackupHistory{
		{BackupID: "failed-1", CamundaInstanceID: "test-1", Status: types.BackupStatusFailed},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/failed", nil)
	w := httptest.NewRecorder()

	handlers.ListFailedBackupsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	var result []*models.BackupHistory
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	if len(result) != 1 {
		t.Errorf("expected 1 failed backup, got %d", len(result))
	}
}

func TestListOrphanedBackupsHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent/backups/orphaned", nil)
	w := httptest.NewRecorder()

	handlers.ListOrphanedBackupsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

// --- Backup Logs Handler Tests ---

func TestGetBackupLogsHandler_Success(t *testing.T) {
	handlers, cm, _, _, _, _, lfr := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	lfr.logs["test-1/backup-1"] = "2026-02-16 10:00:00 Starting backup...\n2026-02-16 10:01:00 Backup completed."

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "text/plain; charset=utf-8" {
		t.Errorf("expected Content-Type 'text/plain; charset=utf-8', got '%s'", contentType)
	}

	body := w.Body.String()
	if body != "2026-02-16 10:00:00 Starting backup...\n2026-02-16 10:01:00 Backup completed." {
		t.Errorf("unexpected log content: %s", body)
	}
}

func TestGetBackupLogsHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d: %s", http.StatusNotFound, w.Code, w.Body.String())
	}
}

func TestGetBackupLogsHandler_LogFileNotFound(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/non-existent/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d: %s", http.StatusNotFound, w.Code, w.Body.String())
	}
}

func TestGetBackupLogsHandler_InvalidPath(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

func TestGetBackupLogsHandler_ReadError(t *testing.T) {
	handlers, cm, _, _, _, _, lfr := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	lfr.err = fmt.Errorf("disk I/O error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d: %s", http.StatusInternalServerError, w.Code, w.Body.String())
	}
}

// --- UpdateCamundaInstanceHandler Tests ---

func TestUpdateCamundaInstanceHandler_Success(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1", Schedule: "0 2 * * *"},
	}

	updates := models.CamundaInstance{
		Name:     "Updated Instance",
		BaseURL:  "http://localhost:9090",
		Schedule: "0 3 * * *",
	}
	body, _ := json.Marshal(updates)
	req := httptest.NewRequest(http.MethodPut, "/api/camundas/test-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.UpdateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestUpdateCamundaInstanceHandler_NotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	updates := models.CamundaInstance{Name: "Updated"}
	body, _ := json.Marshal(updates)
	req := httptest.NewRequest(http.MethodPut, "/api/camundas/non-existent", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.UpdateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestUpdateCamundaInstanceHandler_InvalidJSON(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}

	req := httptest.NewRequest(http.MethodPut, "/api/camundas/test-1", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.UpdateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestUpdateCamundaInstanceHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	updates := models.CamundaInstance{Name: "Updated"}
	body, _ := json.Marshal(updates)
	req := httptest.NewRequest(http.MethodPut, "/api/camundas/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.UpdateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestUpdateCamundaInstanceHandler_ValidationError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	cm.err = utils.ErrInvalidCamundaInstance

	updates := models.CamundaInstance{Name: "Updated"}
	body, _ := json.Marshal(updates)
	req := httptest.NewRequest(http.MethodPut, "/api/camundas/test-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.UpdateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

func TestUpdateCamundaInstanceHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	cm.err = fmt.Errorf("database connection lost")

	updates := models.CamundaInstance{Name: "Updated"}
	body, _ := json.Marshal(updates)
	req := httptest.NewRequest(http.MethodPut, "/api/camundas/test-1", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.UpdateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d: %s", http.StatusInternalServerError, w.Code, w.Body.String())
	}
}

// --- ListCamundaInstancesHandler Error Tests ---

func TestListCamundaInstancesHandler_Error(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("storage error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas", nil)
	w := httptest.NewRecorder()

	handlers.ListCamundaInstancesHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

// --- CreateCamundaInstanceHandler Error Tests ---

func TestCreateCamundaInstanceHandler_InvalidJSON(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestCreateCamundaInstanceHandler_MissingName(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	instance := models.CamundaInstance{
		ID:      "test-1",
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

func TestCreateCamundaInstanceHandler_MissingBaseURL(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	instance := models.CamundaInstance{
		ID:   "test-1",
		Name: "Test Instance",
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

func TestCreateCamundaInstanceHandler_AlreadyExists(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = utils.ErrCamundaInstanceAlreadyExists

	instance := models.CamundaInstance{
		ID:      "test-1",
		Name:    "Test Instance",
		BaseURL: "http://localhost:8080",
	}
	body, _ := json.Marshal(instance)
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("expected status %d, got %d: %s", http.StatusConflict, w.Code, w.Body.String())
	}
}

func TestCreateCamundaInstanceHandler_InvalidInstance(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = utils.ErrInvalidCamundaInstance

	instance := models.CamundaInstance{
		ID:      "test-1",
		Name:    "Test Instance",
		BaseURL: "http://localhost:8080",
	}
	body, _ := json.Marshal(instance)
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

func TestCreateCamundaInstanceHandler_NoComponentsEnabled(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = utils.ErrNoComponentsEnabled

	instance := models.CamundaInstance{
		ID:      "test-1",
		Name:    "Test Instance",
		BaseURL: "http://localhost:8080",
	}
	body, _ := json.Marshal(instance)
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, w.Code, w.Body.String())
	}
}

func TestCreateCamundaInstanceHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	instance := models.CamundaInstance{
		ID:      "test-1",
		Name:    "Test Instance",
		BaseURL: "http://localhost:8080",
	}
	body, _ := json.Marshal(instance)
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d: %s", http.StatusInternalServerError, w.Code, w.Body.String())
	}
}

// --- GetCamundaInstanceHandler Error Tests ---

func TestGetCamundaInstanceHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()

	handlers.GetCamundaInstanceHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestGetCamundaInstanceHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/", nil)
	w := httptest.NewRecorder()

	handlers.GetCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestGetCamundaInstanceHandler_SubPath(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	// Request with a sub-path like /api/camundas/test-1/something
	// The handler checks for "/" in the extracted ID
	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/something", nil)
	w := httptest.NewRecorder()

	handlers.GetCamundaInstanceHandler(w, req)

	// extractIDFromPath returns "test-1" (stops at first /), not "test-1/something"
	// so this should result in not-found (test-1 doesn't exist in empty mock)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

// --- DeleteCamundaInstanceHandler Error Tests ---

func TestDeleteCamundaInstanceHandler_NotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/non-existent", nil)
	w := httptest.NewRecorder()

	handlers.DeleteCamundaInstanceHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestDeleteCamundaInstanceHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteCamundaInstanceHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestDeleteCamundaInstanceHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/", nil)
	w := httptest.NewRecorder()

	handlers.DeleteCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- EnableCamundaInstanceHandler Error Tests ---

func TestEnableCamundaInstanceHandler_NotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/non-existent/enable", nil)
	w := httptest.NewRecorder()

	handlers.EnableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestEnableCamundaInstanceHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/enable", nil)
	w := httptest.NewRecorder()

	handlers.EnableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestEnableCamundaInstanceHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas//enable", nil)
	w := httptest.NewRecorder()

	handlers.EnableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- DisableCamundaInstanceHandler Error Tests ---

func TestDisableCamundaInstanceHandler_NotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/non-existent/disable", nil)
	w := httptest.NewRecorder()

	handlers.DisableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestDisableCamundaInstanceHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/disable", nil)
	w := httptest.NewRecorder()

	handlers.DisableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestDisableCamundaInstanceHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas//disable", nil)
	w := httptest.NewRecorder()

	handlers.DisableCamundaInstanceHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- TriggerBackupHandler Error Tests ---

func TestTriggerBackupHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodPost, "/api/camundas//backup", nil)
	w := httptest.NewRecorder()

	handlers.TriggerBackupHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestTriggerBackupHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backup", nil)
	w := httptest.NewRecorder()

	handlers.TriggerBackupHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

// --- ListBackupHistoryHandler Error Tests ---

func TestListBackupHistoryHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent/backups", nil)
	w := httptest.NewRecorder()

	handlers.ListBackupHistoryHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestListBackupHistoryHandler_InstanceInternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups", nil)
	w := httptest.NewRecorder()

	handlers.ListBackupHistoryHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListBackupHistoryHandler_HistoryError(t *testing.T) {
	handlers, cm, _, hist, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	hist.err = fmt.Errorf("history storage error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups", nil)
	w := httptest.NewRecorder()

	handlers.ListBackupHistoryHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListBackupHistoryHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas//backups", nil)
	w := httptest.NewRecorder()

	handlers.ListBackupHistoryHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

func TestListBackupHistoryHandler_WithStatusFilter(t *testing.T) {
	handlers, cm, _, hist, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	hist.history = []*models.BackupHistory{
		{BackupID: "backup-1", CamundaInstanceID: "test-1", Status: types.BackupStatusCompleted},
		{BackupID: "backup-2", CamundaInstanceID: "test-1", Status: types.BackupStatusFailed},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups?status=completed", nil)
	w := httptest.NewRecorder()

	handlers.ListBackupHistoryHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}

	var history []*models.BackupHistory
	if err := json.Unmarshal(w.Body.Bytes(), &history); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if len(history) != 1 {
		t.Errorf("expected 1 filtered entry, got %d", len(history))
	}
}

// --- GetBackupDetailsHandler Error Tests ---

func TestGetBackupDetailsHandler_InstanceInternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupDetailsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestGetBackupDetailsHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupDetailsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestGetBackupDetailsHandler_HistoryInternalError(t *testing.T) {
	handlers, cm, _, hist, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	hist.err = fmt.Errorf("history storage error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupDetailsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestGetBackupDetailsHandler_InvalidPath(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	// Path without a backup ID segment
	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupDetailsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- DeleteBackupHandler Error Tests ---

func TestDeleteBackupHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.deleteErr = fmt.Errorf("storage error")

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestDeleteBackupHandler_InstanceInternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestDeleteBackupHandler_NilRetentionManager(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	sched := &mockScheduler{running: true}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, nil, lfr, logger, nil) // nil retentionManager

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestDeleteBackupHandler_InvalidPath(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/", nil)
	w := httptest.NewRecorder()

	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- ListOrphanedBackupsHandler Error Tests ---

func TestListOrphanedBackupsHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/orphaned", nil)
	w := httptest.NewRecorder()

	handlers.ListOrphanedBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListOrphanedBackupsHandler_NilRetentionManager(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	sched := &mockScheduler{running: true}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, nil, lfr, logger, nil) // nil retentionManager

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/orphaned", nil)
	w := httptest.NewRecorder()

	handlers.ListOrphanedBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListOrphanedBackupsHandler_ListError(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.listErr = fmt.Errorf("storage error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/orphaned", nil)
	w := httptest.NewRecorder()

	handlers.ListOrphanedBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListOrphanedBackupsHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas//backups/orphaned", nil)
	w := httptest.NewRecorder()

	handlers.ListOrphanedBackupsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- ListIncompleteBackupsHandler Error Tests ---

func TestListIncompleteBackupsHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent/backups/incomplete", nil)
	w := httptest.NewRecorder()

	handlers.ListIncompleteBackupsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestListIncompleteBackupsHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/incomplete", nil)
	w := httptest.NewRecorder()

	handlers.ListIncompleteBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListIncompleteBackupsHandler_NilRetentionManager(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	sched := &mockScheduler{running: true}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, nil, lfr, logger, nil) // nil retentionManager

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/incomplete", nil)
	w := httptest.NewRecorder()

	handlers.ListIncompleteBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListIncompleteBackupsHandler_ListError(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.listErr = fmt.Errorf("storage error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/incomplete", nil)
	w := httptest.NewRecorder()

	handlers.ListIncompleteBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListIncompleteBackupsHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas//backups/incomplete", nil)
	w := httptest.NewRecorder()

	handlers.ListIncompleteBackupsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- ListFailedBackupsHandler Error Tests ---

func TestListFailedBackupsHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/non-existent/backups/failed", nil)
	w := httptest.NewRecorder()

	handlers.ListFailedBackupsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

func TestListFailedBackupsHandler_InternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/failed", nil)
	w := httptest.NewRecorder()

	handlers.ListFailedBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListFailedBackupsHandler_NilRetentionManager(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	sched := &mockScheduler{running: true}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, nil, lfr, logger, nil) // nil retentionManager

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/failed", nil)
	w := httptest.NewRecorder()

	handlers.ListFailedBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListFailedBackupsHandler_ListError(t *testing.T) {
	handlers, cm, _, _, _, ret, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	ret.listErr = fmt.Errorf("storage error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/failed", nil)
	w := httptest.NewRecorder()

	handlers.ListFailedBackupsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestListFailedBackupsHandler_EmptyID(t *testing.T) {
	handlers, _, _, _, _, _, _ := newTestHandlers()

	req := httptest.NewRequest(http.MethodGet, "/api/camundas//backups/failed", nil)
	w := httptest.NewRecorder()

	handlers.ListFailedBackupsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status %d, got %d", http.StatusBadRequest, w.Code)
	}
}

// --- ReadyzHandler Extended Tests ---

func TestReadyzHandler_SchedulerNotRunning(t *testing.T) {
	handlers, _, _, _, sched, _, _ := newTestHandlers()
	sched.running = false

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handlers.ReadyzHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Checks["scheduler"] != "not_running" {
		t.Errorf("expected scheduler check 'not_running', got '%s'", resp.Checks["scheduler"])
	}
}

func TestReadyzHandler_NilScheduler(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, nil, ret, lfr, logger, nil) // nil scheduler

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handlers.ReadyzHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Checks["scheduler"] != "not_running" {
		t.Errorf("expected scheduler check 'not_running', got '%s'", resp.Checks["scheduler"])
	}
}

func TestReadyzHandler_CamundaManagerError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("storage error")

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handlers.ReadyzHandler(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected status %d, got %d", http.StatusServiceUnavailable, w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Status != "not_ready" {
		t.Errorf("expected status 'not_ready', got '%s'", resp.Status)
	}
	if resp.Checks["camunda_manager"] != "error" {
		t.Errorf("expected camunda_manager check 'error', got '%s'", resp.Checks["camunda_manager"])
	}
}

func TestReadyzHandler_NilCamundaManager(t *testing.T) {
	logger := utils.NewLogger("error")
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(nil, orch, hist, sched, ret, lfr, logger, nil) // nil camundaManager

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	handlers.ReadyzHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp HealthResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.Status != "ready" {
		t.Errorf("expected status 'ready', got '%s'", resp.Status)
	}
}

// --- GetBackupLogsHandler Extended Error Tests ---

func TestGetBackupLogsHandler_InstanceInternalError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("database error")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestGetBackupLogsHandler_NilLogFileReader(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	handlers := NewHandlers(cm, orch, hist, sched, ret, nil, logger, nil) // nil logFileReader

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestGetBackupLogsHandler_BackupNotFoundError(t *testing.T) {
	handlers, cm, _, _, _, _, lfr := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test Instance 1"},
	}
	lfr.err = utils.ErrBackupNotFound

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	handlers.GetBackupLogsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d", http.StatusNotFound, w.Code)
	}
}

// --- extractIDFromPath Tests ---

func TestExtractIDFromPath(t *testing.T) {
	tests := []struct {
		path     string
		prefix   string
		expected string
	}{
		{"/api/camundas/test-1", "/api/camundas/", "test-1"},
		{"/api/camundas/TEST-1", "/api/camundas/", "test-1"}, // lowercase normalization
		{"/api/camundas/test-1/backups", "/api/camundas/", "test-1"},
		{"/api/camundas/", "/api/camundas/", ""},
		{"/other/path", "/api/camundas/", ""},
	}

	for _, tt := range tests {
		got := extractIDFromPath(tt.path, tt.prefix)
		if got != tt.expected {
			t.Errorf("extractIDFromPath(%q, %q) = %q, want %q", tt.path, tt.prefix, got, tt.expected)
		}
	}
}

// --- SystemStatusHandler Extended Tests ---

func TestSystemStatusHandler_NilScheduler(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, nil, ret, lfr, logger, nil) // nil scheduler

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	handlers.SystemStatusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestSystemStatusHandler_BackupRunning(t *testing.T) {
	handlers, _, orch, _, _, _, _ := newTestHandlers()
	orch.backupRunning = true

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	handlers.SystemStatusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp SystemStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.ActiveBackups != 1 {
		t.Errorf("expected 1 active backup, got %d", resp.ActiveBackups)
	}
}

func TestSystemStatusHandler_ManagerError(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.err = fmt.Errorf("storage error")

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	handlers.SystemStatusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var resp SystemStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	// When manager errors, instances should be zero (error is silently handled)
	if resp.CamundaInstances.Total != 0 {
		t.Errorf("expected 0 instances when manager errors, got %d", resp.CamundaInstances.Total)
	}
}

func TestSystemStatusHandler_DisabledInstances(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1", Enabled: true},
		{ID: "test-2", Name: "Test 2", Enabled: false},
		{ID: "test-3", Name: "Test 3", Enabled: false},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	handlers.SystemStatusHandler(w, req)

	var resp SystemStatusResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}
	if resp.CamundaInstances.Total != 3 {
		t.Errorf("expected 3 total, got %d", resp.CamundaInstances.Total)
	}
	if resp.CamundaInstances.Enabled != 1 {
		t.Errorf("expected 1 enabled, got %d", resp.CamundaInstances.Enabled)
	}
	if resp.CamundaInstances.Disabled != 2 {
		t.Errorf("expected 2 disabled, got %d", resp.CamundaInstances.Disabled)
	}
}

func TestGetDefaultsHandler(t *testing.T) {
	logger := utils.NewLogger("error")
	cfg := &config.Config{
		DefaultSchedule:                        "0 3 * * *",
		DefaultSuccessRetention:                14,
		DefaultFailureRetention:                14,
		DefaultElasticsearchEndpoint:           "http://es:9200",
		DefaultElasticsearchUsername:            "elastic",
		DefaultElasticsearchSnapshotRepository: "my-repo",
		DefaultElasticsearchSnapshotNamePrefix: "snap-",
		DefaultS3Endpoint:                      "http://minio:9000",
		DefaultS3AccessKey:                     "AKID",
	}
	handlers := NewHandlers(nil, nil, nil, nil, nil, nil, logger, cfg)

	req := httptest.NewRequest(http.MethodGet, "/api/defaults", nil)
	w := httptest.NewRecorder()
	handlers.GetDefaultsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var defaults map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &defaults); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	checks := map[string]interface{}{
		"schedule":                          "0 3 * * *",
		"success_retention":                 float64(14),
		"failure_retention":                 float64(14),
		"elasticsearch_endpoint":            "http://es:9200",
		"elasticsearch_username":            "elastic",
		"elasticsearch_snapshot_repository":  "my-repo",
		"elasticsearch_snapshot_name_prefix": "snap-",
		"s3_endpoint":                       "http://minio:9000",
		"s3_accesskey":                      "AKID",
	}
	for key, want := range checks {
		if got := defaults[key]; got != want {
			t.Errorf("defaults[%q] = %v, want %v", key, got, want)
		}
	}
}

func TestGetDefaultsHandler_NilConfig(t *testing.T) {
	logger := utils.NewLogger("error")
	handlers := NewHandlers(nil, nil, nil, nil, nil, nil, logger, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/defaults", nil)
	w := httptest.NewRecorder()
	handlers.GetDefaultsHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var defaults map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &defaults); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Should return hardcoded defaults when cfg is nil
	if defaults["schedule"] != "0 2 * * *" {
		t.Errorf("expected hardcoded schedule default, got %v", defaults["schedule"])
	}
}

func TestCreateCamundaInstanceHandler_ESDefaults(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	sched := &mockScheduler{running: true}
	cfg := &config.Config{
		DefaultElasticsearchEndpoint: "http://es-default:9200",
		DefaultElasticsearchUsername: "admin",
	}
	handlers := NewHandlers(cm, nil, nil, sched, nil, nil, logger, cfg)

	body := `{
		"id": "a",
		"name": "Test",
		"base_url": "http://camunda.local",
		"s3_endpoint": "http://s3.local",
		"s3_accesskey": "key"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	if len(cm.instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(cm.instances))
	}
	inst := cm.instances[0]
	if inst.ElasticsearchEndpoint != "http://es-default:9200" {
		t.Errorf("ES endpoint = %q, want default", inst.ElasticsearchEndpoint)
	}
	if inst.ElasticsearchUsername != "admin" {
		t.Errorf("ES username = %q, want default", inst.ElasticsearchUsername)
	}
}

func TestCreateCamundaInstanceHandler_ESExplicitOverridesDefaults(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	sched := &mockScheduler{running: true}
	cfg := &config.Config{
		DefaultElasticsearchEndpoint: "http://es-default:9200",
		DefaultElasticsearchUsername: "admin",
	}
	handlers := NewHandlers(cm, nil, nil, sched, nil, nil, logger, cfg)

	body := `{
		"id": "b",
		"name": "Test",
		"base_url": "http://camunda.local",
		"elasticsearch_endpoint": "http://es-custom:9200",
		"elasticsearch_username": "custom-user",
		"s3_endpoint": "http://s3.local",
		"s3_accesskey": "key"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}

	inst := cm.instances[0]
	if inst.ElasticsearchEndpoint != "http://es-custom:9200" {
		t.Errorf("ES endpoint = %q, want explicit value", inst.ElasticsearchEndpoint)
	}
	if inst.ElasticsearchUsername != "custom-user" {
		t.Errorf("ES username = %q, want explicit value", inst.ElasticsearchUsername)
	}
}

func TestCreateCamundaInstanceHandler_ESSnapshotRepositoryStored(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	sched := &mockScheduler{running: true}
	handlers := NewHandlers(cm, nil, nil, sched, nil, nil, logger, nil)

	body := `{
		"id": "c",
		"name": "Test",
		"base_url": "http://camunda.local",
		"s3_endpoint": "http://s3.local",
		"s3_accesskey": "key",
		"elasticsearch_snapshot_repository": "my-custom-repo"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if len(cm.instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(cm.instances))
	}
	if cm.instances[0].ElasticsearchSnapshotRepository != "my-custom-repo" {
		t.Errorf("ElasticsearchSnapshotRepository = %q, want %q", cm.instances[0].ElasticsearchSnapshotRepository, "my-custom-repo")
	}
}

func TestCreateCamundaInstanceHandler_ESSnapshotRepositoryEmpty(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	sched := &mockScheduler{running: true}
	handlers := NewHandlers(cm, nil, nil, sched, nil, nil, logger, nil)

	body := `{
		"id": "d",
		"name": "Test",
		"base_url": "http://camunda.local",
		"s3_endpoint": "http://s3.local",
		"s3_accesskey": "key"
	}`
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handlers.CreateCamundaInstanceHandler(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", w.Code, w.Body.String())
	}
	if cm.instances[0].ElasticsearchSnapshotRepository != "" {
		t.Errorf("ElasticsearchSnapshotRepository should be empty when not provided, got %q", cm.instances[0].ElasticsearchSnapshotRepository)
	}
}
