package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Reconciler cross-references controller metadata against the artifacts that
// actually exist and reports the differences. It never deletes anything.
type Reconciler interface {
	Reconcile(ctx context.Context, instance *models.CamundaInstance) (*reconcile.Report, error)
	LatestReport(camundaInstanceID string) (*reconcile.Report, error)
}

// defaultReconcileTimeout caps an on-demand sweep when the config does not.
// A sweep fans out to every component API and to Elasticsearch, so it needs more
// headroom than an ordinary request.
const defaultReconcileTimeout = 2 * time.Minute

// reconcileTimeout honours RECONCILE_TIMEOUT_SECONDS, which the docs describe as
// capping a sweep. Reading it here keeps the on-demand endpoint consistent with
// the post-backup path rather than silently pinning the API to two minutes.
func (h *Handlers) reconcileTimeout() time.Duration {
	if h.cfg != nil && h.cfg.ReconcileTimeoutSeconds > 0 {
		return time.Duration(h.cfg.ReconcileTimeoutSeconds) * time.Second
	}
	return defaultReconcileTimeout
}

// SetReconciler registers the reconciler used by the reconcile endpoints.
func (h *Handlers) SetReconciler(r Reconciler) {
	h.reconciler = r
}

// resolveInstanceForReconcile extracts and validates the instance from the path.
// It writes the error response itself and returns nil when the request cannot
// proceed.
func (h *Handlers) resolveInstanceForReconcile(w http.ResponseWriter, r *http.Request) *models.CamundaInstance {
	id := extractIDFromPath(r.URL.Path, "/api/camundas/")
	id = strings.TrimSuffix(strings.TrimSuffix(id, "/"), "/backups/reconcile")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return nil
	}

	instance, err := h.camundaManager.GetInstance(id)
	if err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return nil
		}
		h.logger.Error("Failed to get Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get Camunda instance")
		return nil
	}

	if h.reconciler == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Reconciler not configured")
		return nil
	}
	return instance
}

// GetReconcileReportHandler returns the most recent reconciliation report.
//
// It answers 404 when no sweep has run yet, which the UI must distinguish from
// an empty report: "not checked" and "nothing wrong" are different answers.
func (h *Handlers) GetReconcileReportHandler(w http.ResponseWriter, r *http.Request) {
	instance := h.resolveInstanceForReconcile(w, r)
	if instance == nil {
		return
	}

	report, err := h.reconciler.LatestReport(instance.ID)
	if err != nil {
		if err == utils.ErrBackupNotFound {
			// Distinct code from the instance-not-found 404 above: a client must
			// be able to tell "never scanned" from "no such instance".
			writeError(w, http.StatusNotFound, "no_report", "No reconciliation report yet for this instance")
			return
		}
		h.logger.Error("Failed to read reconcile report: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to read reconciliation report")
		return
	}

	writeJSON(w, http.StatusOK, report)
}

// RunReconcileHandler runs a sweep now and returns the fresh report.
func (h *Handlers) RunReconcileHandler(w http.ResponseWriter, r *http.Request) {
	instance := h.resolveInstanceForReconcile(w, r)
	if instance == nil {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.reconcileTimeout())
	defer cancel()

	report, err := h.reconciler.Reconcile(ctx, instance)
	if err != nil {
		if errors.Is(err, reconcile.ErrSweepInProgress) {
			writeError(w, http.StatusConflict, "sweep_in_progress",
				"A reconciliation sweep is already running for this instance")
			return
		}
		h.logger.Error("Reconciliation failed for %s: %v", instance.ID, err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Reconciliation failed")
		return
	}

	writeJSON(w, http.StatusOK, report)
}

// ReasonCatalogHandler serves the static descriptions of every reason code. The
// UI fetches this once and caches it, so reports carry only the codes.
func (h *Handlers) ReasonCatalogHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, reconcile.Catalog())
}
