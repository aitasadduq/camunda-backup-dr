package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/retention"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// maxBulkDeleteBatch caps one bulk request. Each backup in a batch fans out to
// every component plus Elasticsearch, so an unbounded batch would hold a request
// open long past any sensible timeout.
const maxBulkDeleteBatch = 200

// maxBulkDeleteBody caps the request body. The only content is a list of
// fourteen-character IDs, so this is generous by an order of magnitude.
const maxBulkDeleteBody = 1 << 16

// bulkDeleteRequest is the body of POST /api/camundas/{id}/backups/delete.
type bulkDeleteRequest struct {
	BackupIDs []string `json:"backup_ids"`
	Force     bool     `json:"force"`
}

// bulkDeleteFailure explains why one backup in a batch was not deleted. Error
// carries the same code the single-delete endpoint would have returned, so a
// client can react per backup instead of parsing prose.
type bulkDeleteFailure struct {
	BackupID string `json:"backup_id"`
	Error    string `json:"error"`
	Message  string `json:"message"`
}

// bulkDeleteResponse reports a batch one backup at a time.
//
// A batch is not a transaction and must not pretend to be one: deletions span
// several systems, so some can succeed while others are refused by a safety
// guard or blocked by an unreachable component. Reporting per backup lets the
// caller see exactly what survived and why, which a single status code cannot.
type bulkDeleteResponse struct {
	Requested int                 `json:"requested"`
	Deleted   []string            `json:"deleted"`
	Failed    []bulkDeleteFailure `json:"failed"`
}

// BulkDeleteBackupsHandler deletes several backups in one request, each through
// the same path the single-backup endpoint uses. It answers 200 whenever the
// request itself was valid, even if every individual deletion failed; the
// per-backup outcome is in the body.
func (h *Handlers) BulkDeleteBackupsHandler(w http.ResponseWriter, r *http.Request) {
	id := extractIDFromPath(r.URL.Path, "/api/camundas/")
	id = strings.TrimSuffix(strings.TrimSuffix(id, "/"), "/backups/delete")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	if _, err := h.camundaManager.GetInstance(id); err != nil {
		if errors.Is(err, utils.ErrCamundaInstanceNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to get Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get Camunda instance")
		return
	}

	if h.retentionManager == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Retention manager not configured")
		return
	}

	var req bulkDeleteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBulkDeleteBody)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_error", "Invalid request body")
		return
	}

	ids, invalid := validateBulkDeleteIDs(req.BackupIDs)
	if invalid != "" {
		writeError(w, http.StatusBadRequest, "validation_error", invalid)
		return
	}

	resp := bulkDeleteResponse{Requested: len(ids), Deleted: []string{}, Failed: []bulkDeleteFailure{}}
	for _, backupID := range ids {
		if err := h.deleteBackupEverywhere(id, backupID, req.Force); err != nil {
			_, code, message := deleteFailureResponse(err)
			resp.Failed = append(resp.Failed, bulkDeleteFailure{BackupID: backupID, Error: code, Message: message})
			continue
		}
		resp.Deleted = append(resp.Deleted, backupID)
	}

	h.logger.Info("Bulk delete for instance %s: requested=%d, deleted=%d, failed=%d",
		id, resp.Requested, len(resp.Deleted), len(resp.Failed))

	writeJSON(w, http.StatusOK, resp)
}

// validateBulkDeleteIDs normalises a requested batch: it rejects an empty or
// oversized one and drops duplicates while preserving order.
//
// An ID that is merely undeletable is not rejected here. It fails on its own in
// the loop, so one foreign artifact among a selection does not cost the caller
// the whole batch.
//
// The second return is a message for the user, not an error: it is only ever
// written into a 400 response, never wrapped or compared against.
func validateBulkDeleteIDs(requested []string) ([]string, string) {
	if len(requested) == 0 {
		return nil, "At least one backup ID is required"
	}
	if len(requested) > maxBulkDeleteBatch {
		return nil, "Too many backups in one request; delete them in smaller batches"
	}

	seen := make(map[string]struct{}, len(requested))
	ids := make([]string, 0, len(requested))
	for _, backupID := range requested {
		if backupID == "" {
			return nil, "Backup IDs must not be empty"
		}
		// A single-backup delete gets its ID from a URL path, which ServeMux has
		// already cleaned. A batch arrives in a JSON body with no such pass, and
		// each ID goes on to address an S3 key and a component URL, so it has to
		// be one path segment and nothing more.
		if strings.ContainsAny(backupID, "/\\") || backupID == "." || backupID == ".." {
			return nil, "Backup IDs must not contain path separators"
		}
		if _, dup := seen[backupID]; dup {
			continue
		}
		seen[backupID] = struct{}{}
		ids = append(ids, backupID)
	}
	return ids, ""
}

// deleteBackupEverywhere deletes one backup, tracked or orphaned, choosing the
// path from what the controller actually holds rather than from anything the
// caller says.
//
// A tracked backup goes through the retention manager and all of its guards. A
// backup with no record is an orphan: its artifacts cannot be read off a record
// that does not exist, so the latest sweep becomes the authority for what it
// left behind.
//
// DeleteBackup's own not-found answer is what distinguishes the two. It reaches
// that answer before touching anything, so using it as the probe costs nothing
// and keeps a single definition of "tracked".
func (h *Handlers) deleteBackupEverywhere(instanceID, backupID string, force bool) error {
	err := h.retentionManager.DeleteBackup(instanceID, backupID, force)
	if !errors.Is(err, utils.ErrBackupNotFound) {
		return err
	}

	artifacts, err := h.orphanArtifactsFor(instanceID, backupID)
	if err != nil {
		return err
	}
	return h.retentionManager.DeleteOrphan(instanceID, artifacts)
}

// orphanArtifactsFor reads, from the latest sweep, exactly what an orphaned
// backup left behind.
//
// Only sources the sweep positively found the backup in are returned. A source
// it could not reach contributes nothing, so its artifacts survive and the next
// sweep reports them again — the same rule that keeps the reconciler from
// claiming a backup is missing from a source it never enumerated.
//
// The report is read server-side rather than accepted from the client. A client
// that could name the artifacts to delete could name any snapshot in the
// repository.
func (h *Handlers) orphanArtifactsFor(instanceID, backupID string) (retention.OrphanArtifacts, error) {
	if h.reconciler == nil {
		return retention.OrphanArtifacts{}, utils.ErrNoReconcileReport
	}

	report, err := h.reconciler.LatestReport(instanceID)
	if err != nil {
		if errors.Is(err, utils.ErrBackupNotFound) {
			return retention.OrphanArtifacts{}, utils.ErrNoReconcileReport
		}
		return retention.OrphanArtifacts{}, err
	}

	for _, issue := range report.BackupIssues {
		if issue.BackupID != backupID || issue.Tracked {
			continue
		}

		// Whether the controller may act on it is a separate question from
		// whether it exists, so it is asked only once the orphan is found.
		//
		// Unlike a tracked deletion, nothing about this ID has been through the
		// controller: it is whatever a component API or snapshot repository
		// reported, and it becomes a path segment in the DELETEs built from it.
		// Acting only on IDs the controller could itself have issued keeps a
		// component's answer from steering those DELETEs somewhere else. The
		// rest stay report-only, with the exact command shown to run by hand.
		if !camunda.IsBackupIDShaped(backupID) {
			return retention.OrphanArtifacts{}, fmt.Errorf("%w (%s)", utils.ErrBackupIDNotDeletable, backupID)
		}

		return retention.OrphanArtifacts{
			BackupID:      backupID,
			Components:    componentsHolding(issue.PresentIn),
			SnapshotNames: issue.SnapshotNames,
			Repository:    report.SnapshotRepository,
		}, nil
	}

	// Neither recorded nor reported as an orphan: there is nothing to delete and
	// nothing that could describe it, which is the same answer as not existing.
	return retention.OrphanArtifacts{}, utils.ErrBackupNotFound
}

// componentsHolding filters a finding's source list down to the components that
// expose a backup API. Elasticsearch is excluded because its artifacts are
// snapshots addressed by name, and the controller's own S3 and log sources are
// excluded because an untracked backup has nothing in them to address.
func componentsHolding(presentIn []string) []string {
	components := make([]string, 0, len(presentIn))
	for _, source := range presentIn {
		switch source {
		case reconcile.SourceZeebe, reconcile.SourceOperate,
			reconcile.SourceTasklist, reconcile.SourceOptimize:
			components = append(components, source)
		}
	}
	return components
}

// deleteFailureResponse maps a deletion failure onto the HTTP status and error
// code the API reports it as. Both the single and bulk endpoints go through it,
// so one failure never gets two different names.
func deleteFailureResponse(err error) (int, string, string) {
	switch {
	case errors.Is(err, utils.ErrBackupNotFound):
		return http.StatusNotFound, "not_found", "Backup not found"
	case errors.Is(err, utils.ErrNotAnOrphan):
		return http.StatusNotFound, "not_found", err.Error()
	case errors.Is(err, utils.ErrCannotDeleteMostRecentBackup),
		errors.Is(err, utils.ErrCannotDeleteRunningBackup):
		return http.StatusConflict, "safety_refusal", err.Error()
	case errors.Is(err, utils.ErrBackupArtifactsRemain):
		return http.StatusConflict, "artifacts_remain", err.Error()
	case errors.Is(err, utils.ErrNoReconcileReport):
		return http.StatusConflict, "no_report", err.Error()
	case errors.Is(err, utils.ErrOrphanRecordAppeared):
		return http.StatusConflict, "stale_report", err.Error()
	case errors.Is(err, utils.ErrOrphanArtifactsUnidentifiable):
		return http.StatusConflict, "unidentifiable", err.Error()
	case errors.Is(err, utils.ErrBackupIDNotDeletable):
		return http.StatusConflict, "not_deletable", err.Error()
	default:
		return http.StatusInternalServerError, "internal_error", "Failed to delete backup"
	}
}
