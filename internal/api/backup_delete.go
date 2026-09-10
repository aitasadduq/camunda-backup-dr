package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/retention"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// maxBulkDeleteBatch caps one bulk request.
//
// The cap is a time budget, not a taste. Each backup costs an instance-wide S3
// listing plus a fan-out to every component and to Elasticsearch, and one
// unreachable component burns four 30s attempts with backoff before it gives
// up. The server's WriteTimeout is 120s, so a batch large enough to exceed it
// cannot deliver the per-backup report that is the whole point of the endpoint.
const maxBulkDeleteBatch = 25

// bulkDeleteBudget bounds a whole batch, and singleDeleteTimeout one deletion.
// Both sit under the server's 120s WriteTimeout so the handler stops with time
// left to write its result, rather than being cut off mid-flight with the
// outcome lost.
const (
	bulkDeleteBudget    = 90 * time.Second
	singleDeleteTimeout = 90 * time.Second
)

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
	// extractIDFromPath already stops at the first "/", so the instance ID
	// arrives without the "/backups/delete" suffix.
	id := extractIDFromPath(r.URL.Path, "/api/camundas/")
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
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBulkDeleteBody))
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_error", "Invalid request body")
		return
	}
	// Decode stops at the end of the first JSON value, so without this a body
	// of "{...}" followed by anything at all would be accepted and go on to
	// delete. A destructive endpoint should not act on a body it only partly
	// understood.
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeError(w, http.StatusBadRequest, "validation_error", "Request body must contain exactly one JSON object")
		return
	}

	ids, invalid := validateBulkDeleteIDs(req.BackupIDs)
	if invalid != "" {
		writeError(w, http.StatusBadRequest, "validation_error", invalid)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), bulkDeleteBudget)
	defer cancel()

	// Read the sweep once. It cannot change mid-request, and re-reading it per
	// backup would both re-transfer the whole document and let a concurrent
	// sweep judge different backups in one batch against different reports.
	orphans := h.orphanIndexFor(id)

	resp := bulkDeleteResponse{Requested: len(ids), Deleted: []string{}, Failed: []bulkDeleteFailure{}}
	for _, backupID := range ids {
		// A cancelled or expired batch stops rather than deleting into a
		// connection nobody is reading. The remainder is reported as untried so
		// the caller knows exactly where it stopped.
		if err := ctx.Err(); err != nil {
			resp.Failed = append(resp.Failed, bulkDeleteFailure{
				BackupID: backupID,
				Error:    "not_attempted",
				Message:  "the batch ran out of time before reaching this backup; retry it in a smaller batch",
			})
			continue
		}

		if err := h.deleteBackupEverywhere(ctx, id, backupID, req.Force, orphans); err != nil {
			status, code, message := deleteFailureResponse(err)
			// The batch answers 200 regardless, so an unmapped failure would
			// otherwise leave no trace anywhere: not in the status code, and
			// not in the logs. The single-delete handler logs these too.
			if status == http.StatusInternalServerError {
				h.logger.Error("Failed to delete backup %s in batch for instance %s: %v", backupID, id, err)
			}
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

// orphanIndex is one sweep, indexed for deletion: the untracked issues by
// backup ID, plus the report-wide facts a deletion has to check against.
//
// It is resolved once per request. A sweep cannot change while a request runs,
// and re-reading it per backup would let a concurrent sweep judge different
// backups in one batch against different reports.
type orphanIndex struct {
	issues     map[string]reconcile.BackupIssue
	repository string
	endpoints  map[string]string
	sweptAt    time.Time
	complete   bool
	err        error
}

// orphanIndexFor loads the latest sweep for an instance. A failure to load it is
// carried on the index rather than returned, because most deletions are of
// tracked backups and never consult it — a missing report must not fail those.
func (h *Handlers) orphanIndexFor(instanceID string) *orphanIndex {
	idx := &orphanIndex{issues: map[string]reconcile.BackupIssue{}}

	if h.reconciler == nil {
		idx.err = utils.ErrNoReconcileReport
		return idx
	}

	report, err := h.reconciler.LatestReport(instanceID)
	if err != nil {
		if errors.Is(err, utils.ErrBackupNotFound) {
			idx.err = utils.ErrNoReconcileReport
			return idx
		}
		idx.err = err
		return idx
	}

	idx.repository = report.SnapshotRepository
	idx.endpoints = report.ComponentEndpoints
	idx.sweptAt = report.FinishedAt
	idx.complete = report.AllSourcesReachable()
	for _, issue := range report.BackupIssues {
		if !issue.Tracked {
			idx.issues[issue.BackupID] = issue
		}
	}
	return idx
}

// deleteBackupEverywhere deletes one backup, tracked or orphaned, choosing the
// path from what the controller actually holds rather than from anything the
// caller says.
//
// The choice is made by looking the record up, not by pattern-matching an error
// out of a call that has already had side effects. DeleteBackup reports
// ErrBackupNotFound from two places — before it touches anything, and again
// from the record deletion at the very end, after every artifact is gone — so
// treating that error as "not tracked" would reroute an already-successful
// deletion onto the orphan path and report it as a failure.
func (h *Handlers) deleteBackupEverywhere(ctx context.Context, instanceID, backupID string, force bool, orphans *orphanIndex) error {
	tracked, err := h.isTracked(instanceID, backupID)
	if err != nil {
		return err
	}
	if tracked {
		return h.retentionManager.DeleteBackup(ctx, instanceID, backupID, force)
	}

	artifacts, err := orphanArtifactsFor(orphans, backupID)
	if err != nil {
		return err
	}
	return h.retentionManager.DeleteOrphan(ctx, instanceID, artifacts)
}

// isTracked reports whether the controller holds a record for this backup.
//
// A read failure is not "no record": answering that would send a deletion down
// the orphan path, which applies none of the record's safety guards. It is
// returned as an error instead.
func (h *Handlers) isTracked(instanceID, backupID string) (bool, error) {
	if h.historyProvider == nil {
		return false, fmt.Errorf("backup history provider not configured")
	}
	_, err := h.historyProvider.GetBackupHistory(instanceID, backupID)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, utils.ErrBackupNotFound) {
		return false, nil
	}
	return false, fmt.Errorf("failed to determine whether backup %s is tracked: %w", backupID, err)
}

// orphanArtifactsFor reads, from the latest sweep, exactly what an orphaned
// backup left behind.
//
// Only sources the sweep positively found the backup in are returned, and the
// report's own reachability, age and observed endpoints travel with them so the
// retention manager can refuse on any of the three. The report is read
// server-side rather than accepted from the client: a client able to name the
// artifacts to delete could name any snapshot in the repository.
func orphanArtifactsFor(orphans *orphanIndex, backupID string) (retention.OrphanArtifacts, error) {
	if orphans.err != nil {
		return retention.OrphanArtifacts{}, orphans.err
	}

	issue, found := orphans.issues[backupID]
	if !found {
		// Neither recorded nor reported as an orphan: nothing to delete, and
		// nothing that could describe it.
		return retention.OrphanArtifacts{}, fmt.Errorf("%w (%s)", utils.ErrNotAnOrphan, backupID)
	}

	// Whether the controller may act on it is a separate question from whether
	// it exists, so it is asked only once the orphan is found.
	//
	// Unlike a tracked deletion, nothing about this ID has been through the
	// controller: it is whatever a component API or snapshot repository
	// reported, and it becomes a path segment in the DELETEs built from it.
	// Acting only on IDs the controller could itself have issued keeps a
	// component's answer from steering those DELETEs somewhere else. The rest
	// stay report-only, with the exact command shown to run by hand.
	if !camunda.IsBackupIDShaped(backupID) {
		return retention.OrphanArtifacts{}, fmt.Errorf("%w (%s)", utils.ErrBackupIDNotDeletable, backupID)
	}

	return retention.OrphanArtifacts{
		BackupID:   backupID,
		Components: componentsHolding(issue.PresentIn),
		// AllSnapshotNames, not SnapshotNames: the latter is de-duplicated for
		// display and omits snapshots whose finding another finding already
		// explains, which for the commonest orphan shape is all of them.
		SnapshotNames: issue.AllSnapshotNames,
		Repository:    orphans.repository,
		Endpoints:     orphans.endpoints,
		SweptAt:       orphans.sweptAt,
		Complete:      orphans.complete,
	}, nil
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
		// 404, not 409: on main an unknown ID answered "not found", and clients
		// rely on that for a repeat DELETE to be idempotent. The code still says
		// which of the two reasons applies, and the message says what to do.
		return http.StatusNotFound, "no_report",
			"Backup not found. If you expected an orphaned backup here, run a reconciliation scan first."
	case errors.Is(err, utils.ErrOrphanRecordAppeared):
		return http.StatusConflict, "stale_report", err.Error()
	case errors.Is(err, utils.ErrOrphanArtifactsUnidentifiable):
		return http.StatusConflict, "unidentifiable", err.Error()
	case errors.Is(err, utils.ErrOrphanReportPartial):
		return http.StatusConflict, "report_partial", err.Error()
	case errors.Is(err, utils.ErrOrphanReportStale):
		return http.StatusConflict, "report_stale", err.Error()
	case errors.Is(err, utils.ErrOrphanEndpointDrift):
		return http.StatusConflict, "endpoint_drift", err.Error()
	case errors.Is(err, utils.ErrOrphanOwnershipUnverified):
		return http.StatusConflict, "ownership_unverified", err.Error()
	case errors.Is(err, utils.ErrSnapshotNameNotDeletable):
		return http.StatusConflict, "not_deletable", err.Error()
	case errors.Is(err, utils.ErrBackupIDNotDeletable):
		return http.StatusConflict, "not_deletable", err.Error()
	default:
		return http.StatusInternalServerError, "internal_error", "Failed to delete backup"
	}
}
