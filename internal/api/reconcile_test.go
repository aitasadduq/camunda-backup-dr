package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

type mockReconciler struct {
	report      *reconcile.Report
	latestErr   error
	sweepErr    error
	sweepCalls  int
	latestCalls int
	lastWindow  *reconcile.Window
	ctxErr      error // the sweep's context error at the moment it ran
}

func (m *mockReconciler) Reconcile(ctx context.Context, instance *models.CamundaInstance, window *reconcile.Window) (*reconcile.Report, error) {
	m.sweepCalls++
	m.lastWindow = window
	m.ctxErr = ctx.Err()
	if m.sweepErr != nil {
		return nil, m.sweepErr
	}
	return m.report, nil
}

func (m *mockReconciler) LatestReport(camundaInstanceID string) (*reconcile.Report, error) {
	m.latestCalls++
	if m.latestErr != nil {
		return nil, m.latestErr
	}
	return m.report, nil
}

func reconcileTestSetup(t *testing.T) (*Handlers, *mockReconciler) {
	t.Helper()
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.instances = []models.CamundaInstance{{ID: "prod", Name: "Production"}}

	rec := &mockReconciler{report: &reconcile.Report{
		CamundaInstanceID: "prod",
		StartedAt:         time.Now(),
		FinishedAt:        time.Now(),
		SourcesChecked: map[string]reconcile.SourceStatus{
			reconcile.SourceControllerS3: {Name: reconcile.SourceControllerS3, Reachable: true},
		},
		BackupIssues: []reconcile.BackupIssue{{
			BackupID:      "20260320080000",
			PrimaryReason: reconcile.ReasonSplitRestorePair,
			Severity:      reconcile.SeverityCritical,
		}},
	}}
	handlers.SetReconciler(rec)
	return handlers, rec
}

func TestGetReconcileReportHandler(t *testing.T) {
	handlers, _ := reconcileTestSetup(t)

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/prod/backups/reconcile", nil)
	w := httptest.NewRecorder()
	handlers.GetReconcileReportHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	var report reconcile.Report
	if err := json.Unmarshal(w.Body.Bytes(), &report); err != nil {
		t.Fatalf("response is not a report: %v", err)
	}
	if len(report.BackupIssues) != 1 || report.BackupIssues[0].PrimaryReason != reconcile.ReasonSplitRestorePair {
		t.Errorf("report did not round-trip: %+v", report.BackupIssues)
	}
}

// "No sweep has run" and "the sweep found nothing" are different answers, and
// the UI relies on being able to tell them apart.
func TestGetReconcileReportHandlerNoReportYet(t *testing.T) {
	handlers, rec := reconcileTestSetup(t)
	rec.latestErr = utils.ErrBackupNotFound

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/prod/backups/reconcile", nil)
	w := httptest.NewRecorder()
	handlers.GetReconcileReportHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", w.Code)
	}
}

func TestRunReconcileHandler(t *testing.T) {
	handlers, rec := reconcileTestSetup(t)

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/prod/backups/reconcile", nil)
	w := httptest.NewRecorder()
	handlers.RunReconcileHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}
	if rec.sweepCalls != 1 {
		t.Errorf("sweep ran %d times, want 1", rec.sweepCalls)
	}
}

func TestReconcileHandlersUnknownInstance(t *testing.T) {
	handlers, _ := reconcileTestSetup(t)

	for name, fn := range map[string]http.HandlerFunc{
		"get": handlers.GetReconcileReportHandler,
		"run": handlers.RunReconcileHandler,
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/camundas/nope/backups/reconcile", nil)
			w := httptest.NewRecorder()
			fn(w, req)
			if w.Code != http.StatusNotFound {
				t.Errorf("status = %d, want 404", w.Code)
			}
		})
	}
}

func TestReconcileHandlersWithoutReconciler(t *testing.T) {
	handlers, cm, _, _, _, _, _ := newTestHandlers()
	cm.instances = []models.CamundaInstance{{ID: "prod"}}

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/prod/backups/reconcile", nil)
	w := httptest.NewRecorder()
	handlers.GetReconcileReportHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestRunReconcileHandlerSweepFailure(t *testing.T) {
	handlers, rec := reconcileTestSetup(t)
	rec.sweepErr = context.DeadlineExceeded

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/prod/backups/reconcile", nil)
	w := httptest.NewRecorder()
	handlers.RunReconcileHandler(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

// The catalogue is what turns a reason code into something a human can act on,
// so the endpoint must serve every code with its remediation text.
func TestReasonCatalogHandler(t *testing.T) {
	handlers, _ := reconcileTestSetup(t)

	req := httptest.NewRequest(http.MethodGet, "/api/reconcile/reasons", nil)
	w := httptest.NewRecorder()
	handlers.ReasonCatalogHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var catalog []reconcile.ReasonInfo
	if err := json.Unmarshal(w.Body.Bytes(), &catalog); err != nil {
		t.Fatalf("response is not a catalogue: %v", err)
	}
	if len(catalog) == 0 {
		t.Fatal("catalogue is empty")
	}
	for _, info := range catalog {
		if info.Code == "" || info.Label == "" || info.Remediation == "" {
			t.Errorf("incomplete catalogue entry: %+v", info)
		}
	}
}

func TestReconcileRoutes(t *testing.T) {
	handlers, _ := reconcileTestSetup(t)
	router := NewRouter(handlers, nil, "")

	tests := []struct {
		method     string
		path       string
		wantStatus int
	}{
		{http.MethodGet, "/api/camundas/prod/backups/reconcile", http.StatusOK},
		{http.MethodPost, "/api/camundas/prod/backups/reconcile", http.StatusOK},
		{http.MethodDelete, "/api/camundas/prod/backups/reconcile", http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/reconcile/reasons", http.StatusOK},
		{http.MethodPost, "/api/reconcile/reasons", http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
			if w.Code != tt.wantStatus {
				t.Errorf("status = %d, want %d; body: %s", w.Code, tt.wantStatus, w.Body.String())
			}
		})
	}
}

// The reconcile route must not shadow the existing /backups/{backupId} route.
func TestReconcileRouteDoesNotShadowBackupDetails(t *testing.T) {
	handlers, _ := reconcileTestSetup(t)
	router := NewRouter(handlers, nil, "")

	req := httptest.NewRequest(http.MethodGet, "/api/camundas/prod/backups/20260320080000", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code == http.StatusMethodNotAllowed {
		t.Error("backup details route was captured by the reconcile route")
	}
}

func runReconcile(handlers *Handlers, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/camundas/prod/backups/reconcile", strings.NewReader(body))
	w := httptest.NewRecorder()
	handlers.RunReconcileHandler(w, req)
	return w
}

func TestRunReconcileHandlerPassesTheWindow(t *testing.T) {
	handlers, rec := reconcileTestSetup(t)

	if w := runReconcile(handlers, `{"from":"2026-11-01","to":"2026-11-17"}`); w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	if rec.lastWindow == nil || rec.lastWindow.From != "2026-11-01" || rec.lastWindow.To != "2026-11-17" {
		t.Errorf("window = %+v, want 2026-11-01..2026-11-17", rec.lastWindow)
	}

	// No body, or an empty one, is all time.
	for _, body := range []string{"", "{}"} {
		rec.lastWindow = &reconcile.Window{}
		if w := runReconcile(handlers, body); w.Code != http.StatusOK {
			t.Fatalf("body %q: status = %d: %s", body, w.Code, w.Body.String())
		}
		if rec.lastWindow != nil {
			t.Errorf("body %q: window = %+v, want all time", body, rec.lastWindow)
		}
	}
}

func TestRunReconcileHandlerRefusesABadWindow(t *testing.T) {
	handlers, rec := reconcileTestSetup(t)
	for _, body := range []string{
		`{"from":"2026-11-01"}`,
		`{"from":"2026-11-17","to":"2026-11-01"}`,
		`{"from":"yesterday","to":"today"}`,
		`{"from":"2020-01-01","to":"2026-11-01"}`,
		`{not json`,
	} {
		w := runReconcile(handlers, body)
		if w.Code != http.StatusBadRequest {
			t.Errorf("body %s: status = %d, want 400", body, w.Code)
		}
	}
	if rec.sweepCalls != 0 {
		t.Errorf("a refused request still ran %d sweeps", rec.sweepCalls)
	}
}

// A browser tab closing, or an ingress with a shorter timeout, cancels the
// request's context. A sweep tied to it records every source in flight as
// "context canceled" and stores that as the latest report.
func TestRunReconcileHandlerSweepSurvivesTheRequestEnding(t *testing.T) {
	handlers, rec := reconcileTestSetup(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the client has already gone
	req := httptest.NewRequest(http.MethodPost, "/api/camundas/prod/backups/reconcile", nil).WithContext(ctx)
	handlers.RunReconcileHandler(httptest.NewRecorder(), req)

	if rec.sweepCalls != 1 {
		t.Fatalf("sweep ran %d times, want 1", rec.sweepCalls)
	}
	if rec.ctxErr != nil {
		t.Errorf("the sweep's context was already %v; it must not be cancelled with the request", rec.ctxErr)
	}
}

func TestTrackedListsFilterByWindow(t *testing.T) {
	handlers, cm, _, hist, _, ret, _ := newTestHandlers()
	cm.instances = []models.CamundaInstance{{ID: "test-1", Name: "Test"}}
	all := []*models.BackupHistory{
		{BackupID: "20261109080000", CamundaInstanceID: "test-1"},
		{BackupID: "20261111080000", CamundaInstanceID: "test-1"},
		{BackupID: "20261120080000", CamundaInstanceID: "test-1"},
	}
	hist.history = all
	ret.incomplete = all
	ret.failed = all

	tests := []struct {
		name    string
		path    string
		handler func(http.ResponseWriter, *http.Request)
	}{
		{"history", "/api/camundas/test-1/backups", handlers.ListBackupHistoryHandler},
		{"incomplete", "/api/camundas/test-1/backups/incomplete", handlers.ListIncompleteBackupsHandler},
		{"failed", "/api/camundas/test-1/backups/failed", handlers.ListFailedBackupsHandler},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			get := func(query string) (int, []*models.BackupHistory) {
				req := httptest.NewRequest(http.MethodGet, tt.path+query, nil)
				w := httptest.NewRecorder()
				tt.handler(w, req)
				var out []*models.BackupHistory
				_ = json.Unmarshal(w.Body.Bytes(), &out)
				return w.Code, out
			}

			if code, got := get(""); code != http.StatusOK || len(got) != 3 {
				t.Errorf("no range: status %d, %d backups; want 200 and 3", code, len(got))
			}
			code, got := get("?from=2026-11-10&to=2026-11-12")
			if code != http.StatusOK || len(got) != 1 || got[0].BackupID != "20261111080000" {
				t.Errorf("range: status %d, %v; want only 20261111080000", code, got)
			}
			if code, _ := get("?from=2026-11-10"); code != http.StatusBadRequest {
				t.Errorf("half a range: status %d, want 400", code)
			}
		})
	}
}
