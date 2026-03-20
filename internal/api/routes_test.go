package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

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
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, ret, lfr, logger, nil)
	router := NewRouter(handlers, nil)
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

func TestRouter_GetBackupLogs(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// Should route to GetBackupLogsHandler; log not found is expected (returns 404)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected status %d, got %d: %s", http.StatusNotFound, w.Code, w.Body.String())
	}
}

func TestRouter_GetBackupLogs_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()

	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups/backup-1/logs", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d: %s", http.StatusMethodNotAllowed, w.Code, w.Body.String())
	}
}

// --- Static file serving tests ---

func newTestFS() fstest.MapFS {
	return fstest.MapFS{
		"index.html": &fstest.MapFile{
			Data: []byte("<!DOCTYPE html><html><head><title>Test</title></head><body>Hello</body></html>"),
		},
		"css/styles.css": &fstest.MapFile{
			Data: []byte("body { margin: 0; }"),
		},
		"js/app.js": &fstest.MapFile{
			Data: []byte("console.log('hello');"),
		},
	}
}

func newTestRouterWithWebFS() *Router {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, ret, lfr, logger, nil)
	return NewRouter(handlers, newTestFS())
}

func TestRouter_StaticServing_RootServesIndexHTML(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<!DOCTYPE html>") {
		t.Errorf("expected index.html content, got: %s", body)
	}
}

func TestRouter_StaticServing_CSSFile(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/css/styles.css", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	contentType := w.Header().Get("Content-Type")
	if !strings.Contains(contentType, "text/css") {
		t.Errorf("expected Content-Type to contain text/css, got: %s", contentType)
	}
	body := w.Body.String()
	if !strings.Contains(body, "body") {
		t.Errorf("expected CSS content, got: %s", body)
	}
}

func TestRouter_StaticServing_JSFile(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/js/app.js", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "console.log") {
		t.Errorf("expected JS content, got: %s", body)
	}
}

func TestRouter_StaticServing_SPAFallback(t *testing.T) {
	router := newTestRouterWithWebFS()

	// A random non-API, non-asset path should serve index.html
	req := httptest.NewRequest(http.MethodGet, "/some/unknown/path", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "<!DOCTYPE html>") {
		t.Errorf("expected index.html for SPA fallback, got: %s", body)
	}
}

func TestRouter_StaticServing_APIRoutesStillWork(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	// API routes should still return JSON, not index.html
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
	body := w.Body.String()
	if strings.Contains(body, "<!DOCTYPE html>") {
		t.Error("API route should not return index.html")
	}
}

func TestRouter_StaticServing_NilWebFS(t *testing.T) {
	// When webFS is nil, non-API routes should 404
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}
	handlers := NewHandlers(cm, orch, hist, sched, ret, lfr, logger, nil)
	router := NewRouter(handlers, nil)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 when webFS is nil, got %d", w.Code)
	}
}

func TestRouter_StaticServing_BareCSSPath_Returns404(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/css", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for /css without trailing slash, got %d", w.Code)
	}
}

func TestRouter_StaticServing_BareJSPath_Returns404(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/js", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for /js without trailing slash, got %d", w.Code)
	}
}

func TestRouter_StaticServing_DirectoryTraversal_Returns404(t *testing.T) {
	router := newTestRouterWithWebFS()

	// Go's net/http automatically redirects paths containing ".." segments
	// with a 301 before the handler runs. Our containsDotDot guard provides
	// defense-in-depth for cases that bypass the mux (e.g. direct handler calls).
	paths := []string{
		"/css/../../../etc/passwd",
		"/js/../../secret",
		"/../index.html",
	}

	for _, p := range paths {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		// Either a 301 redirect (mux cleanup) or a 404 (our guard) is acceptable
		if w.Code != http.StatusNotFound && w.Code != http.StatusMovedPermanently {
			t.Errorf("expected 404 or 301 for traversal path %q, got %d", p, w.Code)
		}
	}
}

// --- Method Not Allowed Tests for Resource Routes ---

func TestRouter_EnableInstance_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	// GET on /enable should not be allowed
	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/enable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_DisableInstance_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/disable", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_TriggerBackup_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/backup", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_ListOrphanedBackups_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups/orphaned", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_ListIncompleteBackups_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups/incomplete", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_ListFailedBackups_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups/failed", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_ListBackups_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_CamundaResource_MethodNotAllowed(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	// PATCH on /api/camundas/{id} should not be allowed
	req := httptest.NewRequest(http.MethodPatch, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status %d, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestRouter_UpdateCamundaInstance(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	updates := models.CamundaInstance{Name: "Updated"}
	body, _ := json.Marshal(updates)
	req := httptest.NewRequest(http.MethodPut, "/api/camundas/test-1", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_DeleteCamundaInstance(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestRouter_MethodHandler_OPTIONS(t *testing.T) {
	router, _, _, _, _, _ := newTestRouter()

	// OPTIONS on /api/status should return 204 (preflight)
	req := httptest.NewRequest(http.MethodOptions, "/api/status", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status %d for OPTIONS, got %d", http.StatusNoContent, w.Code)
	}
}

func TestRouter_UnmatchedAPIRoute(t *testing.T) {
	router := newTestRouterWithWebFS()

	// An unknown /api/ route should get a JSON 404, not index.html
	req := httptest.NewRequest(http.MethodGet, "/api/nonexistent", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}

	var errResp ErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("expected JSON error response, got: %s", w.Body.String())
	}
	if errResp.Error != "not_found" {
		t.Errorf("expected error 'not_found', got '%s'", errResp.Error)
	}
}

func TestRouter_StaticServing_BareCSSSlash_Returns404(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/css/", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for /css/, got %d", w.Code)
	}
}

func TestRouter_StaticServing_BareJSSlash_Returns404(t *testing.T) {
	router := newTestRouterWithWebFS()

	req := httptest.NewRequest(http.MethodGet, "/js/", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 for /js/, got %d", w.Code)
	}
}

func TestRouter_CamundaResourceTrailingSlash(t *testing.T) {
	router, cm, _, _, _, _ := newTestRouter()
	cm.instances = []models.CamundaInstance{
		{ID: "test-1", Name: "Test 1"},
	}

	// Trailing slash should still work due to TrimSuffix
	req := httptest.NewRequest(http.MethodGet, "/api/camundas/test-1/", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d: %s", http.StatusOK, w.Code, w.Body.String())
	}
}

func TestContainsDotDot(t *testing.T) {
	tests := []struct {
		path     string
		expected bool
	}{
		{"/css/styles.css", false},
		{"/js/app.js", false},
		{"/", false},
		{"/some/path", false},
		{"/../etc/passwd", true},
		{"/css/../../../etc/passwd", true},
		{"/js/../../secret", true},
		{"/..hidden", false}, // not a ".." segment
	}

	for _, tt := range tests {
		got := containsDotDot(tt.path)
		if got != tt.expected {
			t.Errorf("containsDotDot(%q) = %v, want %v", tt.path, got, tt.expected)
		}
	}
}
