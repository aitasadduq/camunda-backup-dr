package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func newTestRouter() (*Router, *mockCamundaManager, *mockOrchestrator, *mockHistoryProvider, *mockScheduler, *mockRetentionManager) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	handlers := NewHandlers(cm, orch, hist, sched, ret, logger)
	router := NewRouter(handlers)
	return router, cm, orch, hist, sched, ret
}

func TestRouter_HealthzEndpoint(t *testing.T) {
	router, _, _, _, _, _ := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestRouter_ReadyzEndpoint(t *testing.T) {
	router, _, _, _, _, _ := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestRouter_StatusEndpoint(t *testing.T) {
	router, _, _, _, _, _ := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestRouter_ListCamundaInstances(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}

	var instances []models.CamundaInstance
	if err := json.Unmarshal(w.Body.Bytes(), &instances); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if len(instances) != 1 {
		t.Errorf("expected 1 instance, got %d", len(instances))
	}
}

func TestRouter_GetCamundaInstance(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestRouter_EnableInstance(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1", Enabled: false},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/enable", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_DisableInstance(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1", Enabled: true},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/disable", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_TriggerBackup(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1", Enabled: true},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backup", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("expected status %d, got %d: %s", http.StatusAccepted, w.Code, w.Body.String())
	}
}

func TestRouter_ListBackups(t *testing.T) {
	router, cm, _, hist, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	hist.history = []*models.BackupHistory{
		{BackupID: "backup-1", CamundaInstanceID: "test-1", Status: types.BackupStatusCompleted},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_GetBackupDetails(t *testing.T) {
	router, cm, _, hist, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	hist.history = []*models.BackupHistory{
		{BackupID: "backup-1", CamundaInstanceID: "test-1", Status: types.BackupStatusCompleted},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_MethodNotAllowed(t *testing.T) {
	router, _, _, _, _, _ := newTestRouter()

	// Try to GET on /api/status (should work)
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("GET /api/status: expected status %d, got %d", http.StatusOK, w.Code)
	}

	// Try to POST on /api/status (should fail with method not allowed)
	req = httptest.NewRequest(http.MethodPost, "/api/status", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /api/status: expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_DeleteBackup(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_ListOrphanedBackups(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/orphaned", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_ListIncompleteBackups(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/incomplete", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_ListFailedBackups(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/failed", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_DeleteBackup_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	// POST on a specific backup path should not be allowed
	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups/backup-1", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d: %s", http.StatusMethodNotAllowed, w.Code, w.Body.String())
	}
}
