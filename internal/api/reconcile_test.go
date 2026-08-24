package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

type mockReconciler struct {
	report     *reconcile.Report
	latestErr  error
	sweepErr   error
	sweepCalls int
}

func (m *mockReconciler) Reconcile(ctx context.Context, instance *models.CamundaInstance) (*reconcile.Report, error) {
	m.sweepCalls++
	if m.sweepErr != nil {
		return nil, m.sweepErr
	}
	return m.report, nil
}

func (m *mockReconciler) LatestReport(camundaInstanceID string) (*reconcile.Report, error) {
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
