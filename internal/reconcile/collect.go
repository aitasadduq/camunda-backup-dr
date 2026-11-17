package reconcile

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/elasticsearch"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// SnapshotLister is the Elasticsearch read side the reconciler needs.
type SnapshotLister interface {
	ListSnapshots(ctx context.Context, repository string) ([]elasticsearch.SnapshotInfo, error)
}

// ESClientFactory builds a snapshot lister for an instance. Injected so tests
// can supply a fake without standing up an Elasticsearch server.
type ESClientFactory func(instance *models.CamundaInstance) SnapshotLister

// evidence is everything one sweep observed. Every field is paired with a
// SourceStatus in sources, so the classifier can tell "not there" apart from
// "could not look".
type evidence struct {
	instance   *models.CamundaInstance
	now        time.Time
	repository string
	namePrefix string
	// externalPrefixes are the declared external backup jobs' snapshot name
	// prefixes. See elasticsearch.SnapshotNaming.
	externalPrefixes []string

	// records keeps duplicates from ListAllBackups on purpose: an ID appearing
	// twice means it is filed in two directories.
	records       []*models.BackupHistory
	latestPointer string

	// componentBackups maps component name -> backup ID -> what it reported.
	componentBackups map[string]map[string]camunda.ComponentBackupRecord
	// snapshots carry Owner/BackupID/Component once classifySnapshots has run,
	// which every reader triggers. Classification happens once per sweep, so
	// the index, the deletable snapshot set and the log cannot disagree.
	snapshots           []elasticsearch.SnapshotInfo
	snapshotsClassified bool
	logBackupIDs     map[string]bool
	exporterPaused   bool
	backupInFlight   bool

	sources map[string]SourceStatus
}

// reachable reports whether a source was enumerated successfully. Anything the
// classifier concludes from absence must be gated on this.
func (e *evidence) reachable(source string) bool {
	s, ok := e.sources[source]
	return ok && s.Reachable
}

// naming is how this sweep attributes snapshot names to owners.
func (e *evidence) naming() elasticsearch.SnapshotNaming {
	return elasticsearch.SnapshotNaming{Prefix: e.namePrefix, ExternalPrefixes: e.externalPrefixes}
}

// classifySnapshots resolves every snapshot's owner and backup ID, once. Not
// safe for concurrent use; collect calls it after its goroutines finish.
func (e *evidence) classifySnapshots() {
	if e.snapshotsClassified {
		return
	}
	naming := e.naming()
	for i := range e.snapshots {
		snap := &e.snapshots[i]
		snap.Owner, snap.BackupID, snap.Component = naming.Classify(snap.Name)
	}
	e.snapshotsClassified = true
}

// untrackedInFlight lists the backup IDs with no controller record that some
// source still reports as being written: a component backup IN_PROGRESS, or a
// snapshot IN_PROGRESS. Such a backup is someone's live backup (an external
// job's, typically), not an orphan yet, and must never be offered for
// deletion: Elasticsearch aborts an in-progress snapshot that is deleted. The
// grace period alone cannot ensure this, because it assumes the backup ID's
// timestamp is UTC and that every backup finishes within it.
func (e *evidence) untrackedInFlight() map[string]bool {
	e.classifySnapshots()
	tracked := make(map[string]bool, len(e.records))
	for _, rec := range e.records {
		tracked[rec.BackupID] = true
	}

	inFlight := make(map[string]bool)
	for _, byID := range e.componentBackups {
		for id, rec := range byID {
			if !tracked[id] && strings.EqualFold(rec.State, camunda.BackupStateInProgress) {
				inFlight[id] = true
			}
		}
	}
	for _, snap := range e.snapshots {
		if snap.BackupID != "" && !tracked[snap.BackupID] && snap.State == elasticsearch.SnapshotStateInProgress {
			inFlight[snap.BackupID] = true
		}
	}
	return inFlight
}

// componentEndpoints maps a component name to its configured backup endpoint.
func componentEndpoints(instance *models.CamundaInstance) map[string]string {
	return map[string]string{
		types.ComponentZeebe:    instance.ZeebeBackupEndpoint,
		types.ComponentOperate:  instance.OperateBackupEndpoint,
		types.ComponentTasklist: instance.TasklistBackupEndpoint,
		types.ComponentOptimize: instance.OptimizeBackupEndpoint,
	}
}

// collect gathers evidence from every configured source concurrently. It never
// fails as a whole: an unreachable source is recorded as unreachable and the
// sweep continues, because a partial report that says which parts are missing is
// far more useful than no report.
func (r *Reconciler) collect(ctx context.Context, instance *models.CamundaInstance) *evidence {
	ev := &evidence{
		instance:         instance,
		now:              r.now(),
		repository:       r.cfg.GetElasticsearchSnapshotRepository(instance.ID, instance.ElasticsearchSnapshotRepository),
		namePrefix:       r.cfg.GetElasticsearchSnapshotNamePrefix(instance.ID),
		externalPrefixes: r.cfg.GetElasticsearchExternalSnapshotPrefixes(instance.ID),
		componentBackups: make(map[string]map[string]camunda.ComponentBackupRecord),
		logBackupIDs:     make(map[string]bool),
		sources:          make(map[string]SourceStatus),
		backupInFlight:   r.isBackupRunning(),
	}

	// Wall-clock, not r.now: this measures how long each source took to answer,
	// which is what tells a slow source apart from a broken one in the logs.
	collectStarted := time.Now()

	var mu sync.Mutex
	setSource := func(s SourceStatus) {
		r.logSourceResult(instance.ID, s, time.Since(collectStarted))
		mu.Lock()
		defer mu.Unlock()
		ev.sources[s.Name] = s
	}

	var wg sync.WaitGroup

	// Controller metadata.
	wg.Add(1)
	go func() {
		defer wg.Done()
		records, err := r.s3Storage.ListAllBackups(instance.ID)
		if err != nil {
			setSource(SourceStatus{Name: SourceControllerS3, Error: err.Error()})
			return
		}
		mu.Lock()
		ev.records = records
		mu.Unlock()
		setSource(SourceStatus{Name: SourceControllerS3, Reachable: true, Count: len(records)})

		if pointer, ptrErr := r.s3Storage.GetLatestBackupID(instance.ID); ptrErr == nil {
			mu.Lock()
			ev.latestPointer = pointer
			mu.Unlock()
		}
	}()

	// Component backup APIs.
	for name, endpoint := range componentEndpoints(instance) {
		if endpoint == "" {
			setSource(SourceStatus{Name: name, Skipped: true})
			continue
		}
		wg.Add(1)
		go func(component, ep string) {
			defer wg.Done()
			listed, err := r.backupLister.ListBackups(ctx, ep)
			if err != nil {
				setSource(SourceStatus{Name: component, Error: err.Error()})
				return
			}
			byID := make(map[string]camunda.ComponentBackupRecord, len(listed))
			for _, rec := range listed {
				byID[rec.BackupID] = rec
			}
			mu.Lock()
			ev.componentBackups[component] = byID
			mu.Unlock()
			setSource(SourceStatus{Name: component, Reachable: true, Count: len(listed)})
		}(name, endpoint)
	}

	// Elasticsearch snapshot repository.
	if instance.ElasticsearchEndpoint == "" || ev.repository == "" {
		setSource(SourceStatus{Name: SourceElasticsearch, Skipped: true})
	} else {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snapshots, err := r.esFactory(instance).ListSnapshots(ctx, ev.repository)
			if err != nil {
				setSource(SourceStatus{Name: SourceElasticsearch, Error: err.Error()})
				return
			}
			mu.Lock()
			ev.snapshots = snapshots
			mu.Unlock()
			setSource(SourceStatus{Name: SourceElasticsearch, Reachable: true, Count: len(snapshots)})
		}()
	}

	// Local log files.
	wg.Add(1)
	go func() {
		defer wg.Done()
		files, err := r.fileStorage.ListLogFiles(instance.ID)
		if err != nil {
			setSource(SourceStatus{Name: SourceLogs, Error: err.Error()})
			return
		}
		ids := make(map[string]bool, len(files))
		for _, f := range files {
			if id := backupIDFromLogFile(f); id != "" {
				ids[id] = true
			}
		}
		mu.Lock()
		ev.logBackupIDs = ids
		mu.Unlock()
		setSource(SourceStatus{Name: SourceLogs, Reachable: true, Count: len(files)})
	}()

	// Exporter state. Best-effort: a failure here suppresses only the F4 check.
	if endpoint := exportersURL(instance.ExportingEndpoint); endpoint != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if paused, err := r.exporterPaused(ctx, endpoint); err == nil {
				mu.Lock()
				ev.exporterPaused = paused
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	ev.classifySnapshots()
	r.logSnapshotBreakdown(ev)
	return ev
}

// logSourceResult records what one source answered and how long it took. The
// report keeps only the outcome; the timing is what says whether raising a
// timeout would help.
func (r *Reconciler) logSourceResult(instanceID string, s SourceStatus, elapsed time.Duration) {
	elapsed = elapsed.Round(time.Millisecond)
	switch {
	case s.Skipped:
		r.logger.Debug("[reconcile] %s: %s not configured, skipped", instanceID, s.Name)
	case s.Reachable:
		r.logger.Info("[reconcile] %s: %s listed %d item(s) in %v", instanceID, s.Name, s.Count, elapsed)
	default:
		r.logger.Warn("[reconcile] %s: %s could not be checked (gave up after %v): %s",
			instanceID, s.Name, elapsed, s.Error)
	}
}

// snapshotBreakdownSamples caps how many unrecognised names one log line shows.
const snapshotBreakdownSamples = 5

// logSnapshotBreakdown says how the sweep read the snapshot repository: how
// many names it took as the controller's, as a component's, and as another
// tool's. Without this, a repository full of snapshots that match no known
// convention looks, from the outside, like a sweep that found nothing.
func (r *Reconciler) logSnapshotBreakdown(ev *evidence) {
	if !ev.reachable(SourceElasticsearch) {
		return
	}

	var controller, component, external int
	var foreign []string
	for _, snap := range ev.snapshots {
		switch snap.Owner {
		case elasticsearch.OwnerController:
			controller++
		case elasticsearch.OwnerComponent:
			component++
		case elasticsearch.OwnerExternal:
			external++
		default:
			foreign = append(foreign, snap.Name)
		}
	}

	declared := "none declared"
	if len(ev.externalPrefixes) > 0 {
		declared = fmt.Sprintf("%q", ev.externalPrefixes)
	}
	r.logger.Info("[reconcile] %s: repository %q holds %d snapshot(s): %d named like this controller's (%q), %d component-owned (camunda_<component>_<id>_...), %d from declared external jobs (%s), %d match none",
		ev.instance.ID, ev.repository, len(ev.snapshots), controller,
		elasticsearch.BuildSnapshotName(ev.namePrefix, "<backupID>"), component, external, declared, len(foreign))
	if len(foreign) > 0 {
		samples := foreign
		if len(samples) > snapshotBreakdownSamples {
			samples = samples[:snapshotBreakdownSamples]
		}
		r.logger.Info("[reconcile] %s: unmatched snapshots are reported but never deleted; declare their job in ELASTICSEARCH_EXTERNAL_SNAPSHOT_PREFIXES_<ID> to manage them. e.g. %s",
			ev.instance.ID, strings.Join(samples, ", "))
	}
}

// backupIDFromLogFile extracts a backup ID from a log file name. Log files are
// created as {backupID}.log by FileStorage.CreateLogFile.
func backupIDFromLogFile(name string) string {
	base := name
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	base = strings.TrimSuffix(base, ".log")
	if camunda.IsBackupIDShaped(base) {
		return base
	}
	return ""
}

// exportersURL derives the exporter status URL from the configured pause/resume
// endpoint. Returns "" when the endpoint does not follow the expected shape, in
// which case the exporter check is skipped rather than guessed at.
func exportersURL(exportingEndpoint string) string {
	trimmed := strings.TrimRight(exportingEndpoint, "/")
	if trimmed == "" {
		return ""
	}
	if !strings.HasSuffix(trimmed, "/exporting") {
		return ""
	}
	return strings.TrimSuffix(trimmed, "/exporting") + "/exporters"
}
