package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// deleteTestSetup wires handlers whose retention manager reports every backup as
// untracked, so deletions fall through to the orphan path, and whose latest
// report describes one orphan spanning Zeebe and Elasticsearch.
func deleteTestSetup(t *testing.T) (*Handlers, *mockRetentionManager, *mockReconciler) {
	t.Helper()

	handlers, cm, _, _, _, ret, _ := newTestHandlers()
	cm.instances = []models.CamundaInstance{{ID: "test-1", Name: "Test Instance 1"}}

	rec := &mockReconciler{report: &reconcile.Report{
		CamundaInstanceID:  "test-1",
		SnapshotRepository: "camunda-backup",
		BackupIssues: []reconcile.BackupIssue{{
			BackupID:      "20260320080000",
			Tracked:       false,
			PrimaryReason: reconcile.ReasonUntrackedComponentBackup,
			PresentIn: []string{
				reconcile.SourceZeebe,
				reconcile.SourceElasticsearch,
				reconcile.SourceLogs,
			},
			SnapshotNames: []string{"camunda-20260320080000"},
		}},
	}}
	handlers.SetReconciler(rec)
	return handlers, ret, rec
}

func postBulkDelete(t *testing.T, handlers *Handlers, body any) *httptest.ResponseRecorder {
	t.Helper()

	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups/delete", bytes.NewReader(encoded))
	w := httptest.NewRecorder()
	handlers.BulkDeleteBackupsHandler(w, req)
	return w
}

func decodeBulkDelete(t *testing.T, w *httptest.ResponseRecorder) bulkDeleteResponse {
	t.Helper()

	var resp bulkDeleteResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body: %s)", err, w.Body.String())
	}
	return resp
}

// --- Orphan dispatch on the single-backup endpoint ---

// The artifacts to delete come from the stored report, not from the request, and
// only the sources the sweep positively found are included. Elasticsearch and
// the controller's own log source are not component endpoints, so they must not
// arrive as components.
func TestDeleteBackupHandler_OrphanUsesReportedArtifacts(t *testing.T) {
	handlers, ret, _ := deleteTestSetup(t)
	ret.deleteErr = utils.ErrBackupNotFound

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/20260320080000", nil)
	w := httptest.NewRecorder()
	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(ret.deletedOrphans) != 1 {
		t.Fatalf("expected one orphan deletion, got %d", len(ret.deletedOrphans))
	}

	got := ret.deletedOrphans[0]
	if got.BackupID != "20260320080000" {
		t.Errorf("expected backup ID 20260320080000, got %q", got.BackupID)
	}
	if len(got.Components) != 1 || got.Components[0] != reconcile.SourceZeebe {
		t.Errorf("expected components [zeebe], got %v", got.Components)
	}
	if len(got.SnapshotNames) != 1 || got.SnapshotNames[0] != "camunda-20260320080000" {
		t.Errorf("expected the reported snapshot name, got %v", got.SnapshotNames)
	}
	if got.Repository != "camunda-backup" {
		t.Errorf("expected repository camunda-backup, got %q", got.Repository)
	}
}

// A tracked backup must never take the orphan path, because the orphan path
// applies none of the retention guards.
func TestDeleteBackupHandler_TrackedDoesNotUseOrphanPath(t *testing.T) {
	handlers, ret, _ := deleteTestSetup(t)

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/20260320080000", nil)
	w := httptest.NewRecorder()
	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}
	if len(ret.deletedOrphans) != 0 {
		t.Errorf("expected no orphan deletion for a tracked backup, got %v", ret.deletedOrphans)
	}
}

// An orphan whose ID the controller could not have issued is reported but never
// deleted: that ID came from a component API and would become a path segment in
// the DELETEs built from it.
func TestDeleteBackupHandler_RefusesForeignOrphanID(t *testing.T) {
	handlers, ret, rec := deleteTestSetup(t)
	ret.deleteErr = utils.ErrBackupNotFound
	rec.report.BackupIssues = []reconcile.BackupIssue{{
		BackupID:  "someone-elses-backup",
		Tracked:   false,
		PresentIn: []string{reconcile.SourceZeebe},
	}}

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/someone-elses-backup", nil)
	w := httptest.NewRecorder()
	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusConflict {
		t.Fatalf("expected status 409, got %d: %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "not_deletable") {
		t.Errorf("expected a not_deletable error code, got: %s", w.Body.String())
	}
	if len(ret.deletedOrphans) != 0 {
		t.Errorf("expected nothing to be deleted, got %v", ret.deletedOrphans)
	}
}

// A backup reported as tracked in the sweep is not an orphan, so a request for
// it must not be satisfied down the orphan path even when no record exists.
func TestDeleteBackupHandler_TrackedIssueIsNotAnOrphan(t *testing.T) {
	handlers, ret, rec := deleteTestSetup(t)
	ret.deleteErr = utils.ErrBackupNotFound
	rec.report.BackupIssues[0].Tracked = true

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1/backups/20260320080000", nil)
	w := httptest.NewRecorder()
	handlers.DeleteBackupHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d: %s", w.Code, w.Body.String())
	}
	if len(ret.deletedOrphans) != 0 {
		t.Errorf("expected nothing to be deleted, got %v", ret.deletedOrphans)
	}
}

// --- Bulk deletion ---

func TestBulkDeleteBackupsHandler_DeletesEach(t *testing.T) {
	handlers, _, _ := deleteTestSetup(t)

	w := postBulkDelete(t, handlers, bulkDeleteRequest{
		BackupIDs: []string{"20260320080000", "20260321080000", "20260322080000"},
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", w.Code, w.Body.String())
	}

	resp := decodeBulkDelete(t, w)
	if resp.Requested != 3 {
		t.Errorf("expected requested 3, got %d", resp.Requested)
	}
	if len(resp.Deleted) != 3 {
		t.Errorf("expected 3 deleted, got %v", resp.Deleted)
	}
	if len(resp.Failed) != 0 {
		t.Errorf("expected no failures, got %v", resp.Failed)
	}
}

// A batch is not a transaction. One backup refused by a safety guard must not
// stop the others, and the reason has to survive per backup.
func TestBulkDeleteBackupsHandler_ReportsPerBackupOutcome(t *testing.T) {
	handlers, ret, _ := deleteTestSetup(t)
	ret.deleteErrByID = map[string]error{
		"20260321080000": fmt.Errorf("%w (20260321080000)", utils.ErrCannotDeleteMostRecentBackup),
		"20260322080000": fmt.Errorf("%w for 20260322080000: Operate returned 500", utils.ErrBackupArtifactsRemain),
	}

	w := postBulkDelete(t, handlers, bulkDeleteRequest{
		BackupIDs: []string{"20260320080000", "20260321080000", "20260322080000"},
	})
	resp := decodeBulkDelete(t, w)

	if len(resp.Deleted) != 1 || resp.Deleted[0] != "20260320080000" {
		t.Errorf("expected only 20260320080000 deleted, got %v", resp.Deleted)
	}
	if len(resp.Failed) != 2 {
		t.Fatalf("expected 2 failures, got %v", resp.Failed)
	}

	codes := map[string]string{}
	for _, f := range resp.Failed {
		codes[f.BackupID] = f.Error
		if f.Message == "" {
			t.Errorf("failure for %s carries no message", f.BackupID)
		}
	}
	if codes["20260321080000"] != "safety_refusal" {
		t.Errorf("expected safety_refusal, got %q", codes["20260321080000"])
	}
	if codes["20260322080000"] != "artifacts_remain" {
		t.Errorf("expected artifacts_remain, got %q", codes["20260322080000"])
	}
}

func TestBulkDeleteBackupsHandler_ForwardsForce(t *testing.T) {
	handlers, ret, _ := deleteTestSetup(t)

	postBulkDelete(t, handlers, bulkDeleteRequest{BackupIDs: []string{"20260320080000"}, Force: true})
	if !ret.deleteForce {
		t.Error("expected force to reach the retention manager")
	}
}

func TestBulkDeleteBackupsHandler_DeduplicatesIDs(t *testing.T) {
	handlers, ret, _ := deleteTestSetup(t)

	w := postBulkDelete(t, handlers, bulkDeleteRequest{
		BackupIDs: []string{"20260320080000", "20260320080000"},
	})
	resp := decodeBulkDelete(t, w)

	if resp.Requested != 1 {
		t.Errorf("expected requested 1 after dedupe, got %d", resp.Requested)
	}
	if len(ret.deletedBackups) != 1 {
		t.Errorf("expected one deletion attempt, got %v", ret.deletedBackups)
	}
}

// A batch arrives in a JSON body, which no router has cleaned as a path, and
// each ID goes on to address an S3 key and a component URL.
func TestBulkDeleteBackupsHandler_RejectsPathSeparators(t *testing.T) {
	for _, backupID := range []string{"../other-instance/20260320080000", "a/b", `a\b`, ".."} {
		handlers, ret, _ := deleteTestSetup(t)

		w := postBulkDelete(t, handlers, bulkDeleteRequest{BackupIDs: []string{backupID}})
		if w.Code != http.StatusBadRequest {
			t.Errorf("%q: expected status 400, got %d: %s", backupID, w.Code, w.Body.String())
		}
		if len(ret.deletedBackups) != 0 {
			t.Errorf("%q: expected no deletion attempt, got %v", backupID, ret.deletedBackups)
		}
	}
}

func TestBulkDeleteBackupsHandler_RejectsEmptyBatch(t *testing.T) {
	handlers, _, _ := deleteTestSetup(t)

	w := postBulkDelete(t, handlers, bulkDeleteRequest{BackupIDs: []string{}})
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBulkDeleteBackupsHandler_RejectsOversizedBatch(t *testing.T) {
	handlers, ret, _ := deleteTestSetup(t)

	ids := make([]string, maxBulkDeleteBatch+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("2026032008%04d", i)
	}

	w := postBulkDelete(t, handlers, bulkDeleteRequest{BackupIDs: ids})
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
	if len(ret.deletedBackups) != 0 {
		t.Errorf("expected no deletion attempt, got %v", ret.deletedBackups)
	}
}

func TestBulkDeleteBackupsHandler_RejectsInvalidBody(t *testing.T) {
	handlers, _, _ := deleteTestSetup(t)

	req := httptest.NewRequest(http.MethodPost, "/api/camundas/test-1/backups/delete", strings.NewReader("not json"))
	w := httptest.NewRecorder()
	handlers.BulkDeleteBackupsHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d: %s", w.Code, w.Body.String())
	}
}

func TestBulkDeleteBackupsHandler_InstanceNotFound(t *testing.T) {
	handlers, _, _ := deleteTestSetup(t)

	encoded, _ := json.Marshal(bulkDeleteRequest{BackupIDs: []string{"20260320080000"}})
	req := httptest.NewRequest(http.MethodPost, "/api/camundas/nope/backups/delete", bytes.NewReader(encoded))
	w := httptest.NewRecorder()
	handlers.BulkDeleteBackupsHandler(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d: %s", w.Code, w.Body.String())
	}
}

// --- componentsHolding ---

// Only components with a backup API belong here. Elasticsearch snapshots are
// addressed by name, and the controller's own S3 and log sources hold nothing an
// untracked backup could be deleted from.
func TestComponentsHolding_KeepsOnlyBackupAPIComponents(t *testing.T) {
	got := componentsHolding([]string{
		reconcile.SourceControllerS3,
		reconcile.SourceElasticsearch,
		reconcile.SourceLogs,
		reconcile.SourceZeebe,
		reconcile.SourceOperate,
		reconcile.SourceTasklist,
		reconcile.SourceOptimize,
	})

	want := []string{
		reconcile.SourceZeebe,
		reconcile.SourceOperate,
		reconcile.SourceTasklist,
		reconcile.SourceOptimize,
	}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestOrphanArtifactsFor_NoReconcilerConfigured(t *testing.T) {
	handlers, _, _ := deleteTestSetup(t)
	handlers.SetReconciler(nil)

	_, err := handlers.orphanArtifactsFor("test-1", "20260320080000")
	if err != utils.ErrNoReconcileReport {
		t.Fatalf("expected ErrNoReconcileReport, got %v", err)
	}
}

func TestDeleteFailureResponse_MapsEveryKnownFailure(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{utils.ErrBackupNotFound, http.StatusNotFound, "not_found"},
		{utils.ErrNotAnOrphan, http.StatusNotFound, "not_found"},
		{utils.ErrCannotDeleteMostRecentBackup, http.StatusConflict, "safety_refusal"},
		{utils.ErrCannotDeleteRunningBackup, http.StatusConflict, "safety_refusal"},
		{utils.ErrBackupArtifactsRemain, http.StatusConflict, "artifacts_remain"},
		{utils.ErrNoReconcileReport, http.StatusConflict, "no_report"},
		{utils.ErrOrphanRecordAppeared, http.StatusConflict, "stale_report"},
		{utils.ErrOrphanArtifactsUnidentifiable, http.StatusConflict, "unidentifiable"},
		{utils.ErrBackupIDNotDeletable, http.StatusConflict, "not_deletable"},
		{fmt.Errorf("disk on fire"), http.StatusInternalServerError, "internal_error"},
	}

	for _, tc := range cases {
		// Wrapped, because every real caller wraps the sentinel with context.
		status, code, _ := deleteFailureResponse(fmt.Errorf("context: %w", tc.err))
		if status != tc.status || code != tc.code {
			t.Errorf("%v: expected %d/%s, got %d/%s", tc.err, tc.status, tc.code, status, code)
		}
	}
}
