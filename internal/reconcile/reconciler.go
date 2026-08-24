package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/elasticsearch"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Reconciler cross-references controller metadata against the artifacts that
// actually exist, and reports the differences.
//
// It never deletes anything. Removing a backup stays a deliberate, manual act
// through internal/retention, because a detector acting on its own conclusions
// would turn a false positive into data loss.
type Reconciler struct {
	s3Storage    storage.S3Storage
	fileStorage  storage.FileStorage
	backupLister camunda.BackupLister
	httpClient   *camunda.HTTPClient
	esFactory    ESClientFactory
	cfg          *config.Config
	logger       *utils.Logger
	opts         Options

	// backupRunning reports whether a backup is in flight. Without it the F4
	// exporter check cannot tell "a backup failed to resume exporting" from
	// "a backup is pausing exporting right now, as designed".
	backupRunning func() bool

	// sweepRunning serialises sweeps per reconciler. A sweep fans out to every
	// component plus Elasticsearch and holds the whole record set in memory, so
	// overlapping sweeps multiply both the load on the cluster and the memory.
	sweepRunning atomic.Bool

	// nowFunc is swappable so tests can drive the time-based guards.
	nowFunc func() time.Time
}

// NewReconciler builds a Reconciler wired to the real Camunda and Elasticsearch
// clients.
func NewReconciler(
	s3Storage storage.S3Storage,
	fileStorage storage.FileStorage,
	httpClient *camunda.HTTPClient,
	cfg *config.Config,
	logger *utils.Logger,
) *Reconciler {
	r := &Reconciler{
		s3Storage:    s3Storage,
		fileStorage:  fileStorage,
		backupLister: httpClient,
		httpClient:   httpClient,
		cfg:          cfg,
		logger:       logger,
		opts:         DefaultOptions(),
	}
	r.esFactory = func(instance *models.CamundaInstance) SnapshotLister {
		return elasticsearch.NewClient(
			instance.ElasticsearchEndpoint,
			instance.ElasticsearchUsername,
			cfg.GetElasticsearchPassword(instance.ID),
			httpClient,
			logger,
		)
	}
	return r
}

// SetOptions overrides the time-based guard settings.
func (r *Reconciler) SetOptions(opts Options) {
	r.opts = opts
}

// SetBackupRunningFunc registers the check that tells the sweep whether a backup
// is currently in flight. Without it the sweep assumes none is.
func (r *Reconciler) SetBackupRunningFunc(fn func() bool) {
	r.backupRunning = fn
}

// ErrSweepInProgress is returned when a sweep is already running for this
// reconciler. Callers surface it as 409 rather than starting a second sweep.
var ErrSweepInProgress = errors.New("reconciliation already in progress")

// isBackupRunning reports whether a backup is in flight, defaulting to false
// when no check has been registered.
func (r *Reconciler) isBackupRunning() bool {
	return r.backupRunning != nil && r.backupRunning()
}

func (r *Reconciler) now() time.Time {
	if r.nowFunc != nil {
		return r.nowFunc()
	}
	return time.Now()
}

// Reconcile sweeps one instance and stores the resulting report.
//
// A sweep does not fail because a source is down: the unreachable source is
// recorded as such and every conclusion that would have depended on it is
// withheld. The report says what it could not check.
func (r *Reconciler) Reconcile(ctx context.Context, instance *models.CamundaInstance) (*Report, error) {
	if instance == nil {
		return nil, fmt.Errorf("camunda instance is nil")
	}

	if !r.sweepRunning.CompareAndSwap(false, true) {
		return nil, ErrSweepInProgress
	}
	defer r.sweepRunning.Store(false)

	started := r.now()
	r.logger.Info("[reconcile] Starting sweep for instance %s", instance.ID)

	ev := r.collect(ctx, instance)
	findings := classify(ev, r.opts)

	tracked := make(map[string]bool, len(ev.records))
	for _, rec := range ev.records {
		tracked[rec.BackupID] = true
	}

	report := Rollup(instance.ID, findings, tracked, ev.sources, started, r.now())
	report.SnapshotRepository = ev.repository
	report.ComponentEndpoints = make(map[string]string)
	for name, endpoint := range componentEndpoints(instance) {
		if endpoint != "" {
			report.ComponentEndpoints[name] = stripUserinfo(endpoint)
		}
	}

	if unreachable := report.UnreachableSources(); len(unreachable) > 0 {
		r.logger.Warn("[reconcile] Sweep for %s is partial; could not check: %s",
			instance.ID, strings.Join(unreachable, ", "))
	}
	r.logger.Info("[reconcile] Instance %s: %d findings across %d backups",
		instance.ID, report.TotalFindings(), len(report.BackupIssues))

	if err := r.store(report); err != nil {
		// The report is still valid and worth returning even if it could not be
		// persisted for later reads.
		r.logger.Error("[reconcile] Failed to store report for %s: %v", instance.ID, err)
	}

	return report, nil
}

// LatestReport returns the most recent stored report for an instance.
func (r *Reconciler) LatestReport(camundaInstanceID string) (*Report, error) {
	data, err := r.s3Storage.GetLatestReconcileReport(camundaInstanceID)
	if err != nil {
		return nil, err
	}
	var report Report
	if err := json.Unmarshal(data, &report); err != nil {
		return nil, fmt.Errorf("failed to parse stored reconcile report: %w", err)
	}
	return &report, nil
}

func (r *Reconciler) store(report *Report) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize reconcile report: %w", err)
	}
	return r.s3Storage.StoreReconcileReport(report.CamundaInstanceID, data)
}

// stripUserinfo removes any embedded credentials from a URL while keeping the
// path, which the UI needs to build a remediation command. The report is written
// to S3 and served over the API, so a "https://user:pass@host/actuator/backups"
// endpoint would otherwise persist the password in both.
func stripUserinfo(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.User = nil
	return u.String()
}

// exporterPaused reports whether Zeebe's exporters are currently stopped.
func (r *Reconciler) exporterPaused(ctx context.Context, endpoint string) (bool, error) {
	resp, err := r.httpClient.Get(ctx, endpoint, nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return false, fmt.Errorf("exporter status returned %d", resp.StatusCode)
	}

	var exporters []struct {
		ExporterID string `json:"exporterId"`
		Status     string `json:"status"`
	}
	if err := camunda.ReadJSONResponse(resp, &exporters); err != nil {
		return false, err
	}

	// Only claim "paused" from a positive signal. An empty list means the broker
	// has no exporters configured, not that exporting was left paused.
	for _, e := range exporters {
		if strings.EqualFold(e.Status, "ENABLED") {
			return false, nil
		}
	}
	return len(exporters) > 0, nil
}
