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

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Reconciler cross-references controller metadata against the artifacts that
// actually exist and reports the differences. It never deletes anything.
type Reconciler interface {
	Reconcile(ctx context.Context, instance *models.CamundaInstance, window *reconcile.Window) (*reconcile.Report, error)
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

// SetLifetimeContext registers the context that is cancelled when the server
// shuts down. Work that outlives a request, such as a sweep, is bound to it.
func (h *Handlers) SetLifetimeContext(ctx context.Context) {
	h.lifetime = ctx
}

// lifetimeContext is the server's lifetime, or a background context when none
// was registered.
func (h *Handlers) lifetimeContext() context.Context {
	if h.lifetime != nil {
		return h.lifetime
	}
	return context.Background()
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

	window, err := windowFromPost(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "validation_error", err.Error())
		return
	}

	// The sweep must not die with the request. A browser tab closing, or an
	// ingress with a shorter timeout than the sweep, cancels r.Context(), and a
	// sweep tied to it records every source in flight as "context canceled".
	// It is tied to the server's lifetime instead: it runs to its own deadline
	// and stores a real result whatever the client does, and is cancelled (and
	// its partial result discarded) when the server shuts down, so a shutdown
	// never waits on it.
	ctx, cancel := context.WithTimeout(h.lifetimeContext(), h.reconcileTimeout())
	defer cancel()

	report, err := h.reconciler.Reconcile(ctx, instance, window)
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

// windowFromQuery reads the optional date range from ?from=&to=. Neither means
// all time. The list endpoints use it: a GET carries no body to read.
func windowFromQuery(r *http.Request) (*reconcile.Window, error) {
	return reconcile.NewWindow(r.URL.Query().Get("from"), r.URL.Query().Get("to"))
}

// windowFromPost reads the optional date range of a POST: the query parameters,
// with a JSON body {"from":"YYYY-MM-DD","to":"YYYY-MM-DD"} overriding them field
// by field. A body that sets only one field therefore keeps the other from the
// query, and both must be present in the end, or neither for all time.
func windowFromPost(r *http.Request) (*reconcile.Window, error) {
	from, to := r.URL.Query().Get("from"), r.URL.Query().Get("to")

	if r.Body != nil && r.ContentLength != 0 {
		var body struct {
			From string `json:"from"`
			To   string `json:"to"`
		}
		dec := json.NewDecoder(io.LimitReader(r.Body, 4096))
		if err := dec.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("request body must be JSON like {\"from\":\"2026-11-01\",\"to\":\"2026-11-17\"}: %w", err)
		}
		// Anything after the first object is a malformed request, not something
		// to ignore: it may be a second range the caller meant.
		var extra json.RawMessage
		if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("request body must hold a single JSON object")
		}
		if body.From != "" {
			from = body.From
		}
		if body.To != "" {
			to = body.To
		}
	}

	return reconcile.NewWindow(from, to)
}
