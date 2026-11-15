package reconcile

import (
	"context"
	"sort"
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

	// records keeps duplicates from ListAllBackups on purpose: an ID appearing
	// twice means it is filed in two directories.
	records       []*models.BackupHistory
	latestPointer string

	// componentBackups maps component name -> backup ID -> what it reported.
	componentBackups map[string]map[string]camunda.ComponentBackupRecord
	snapshots        []elasticsearch.SnapshotInfo
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
		componentBackups: make(map[string]map[string]camunda.ComponentBackupRecord),
		logBackupIDs:     make(map[string]bool),
		sources:          make(map[string]SourceStatus),
		backupInFlight:   r.isBackupRunning(),
	}

	var mu sync.Mutex
	setSource := func(s SourceStatus) {
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
	endpoints := componentEndpoints(instance)
	for name, endpoint := range endpoints {
		if endpoint == "" {
			setSource(SourceStatus{Name: name, Skipped: true})
			continue
		}
		wg.Add(1)
		go func(component, ep string) {
			defer wg.Done()
			listCtx, cancel := bareListContext(ctx)
			defer cancel()
			listed, err := r.backupLister.ListBackups(listCtx, ep)
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

	// Fallback for components whose unfiltered GET /actuator/backups failed
	// (for example, Zeebe backed by S3/OCI with hundreds or thousands of backups
	// where S3BackupStore.list("*") times out or returns 500, or older Camunda
	// versions that require a prefix or backup ID path segment).
	if candidateLister, ok := r.backupLister.(camunda.CandidateBackupLister); ok && ctx.Err() == nil {
		candidates := candidateBackupIDs(ev)
		var fallbackWG sync.WaitGroup
		for name, endpoint := range endpoints {
			if endpoint == "" {
				continue
			}
			status, exists := ev.sources[name]
			if !exists || status.Reachable || status.Skipped {
				continue
			}
			fallbackWG.Add(1)
			go func(component, ep string) {
				defer fallbackWG.Done()
				listed, err := candidateLister.ListBackupsByCandidates(ctx, ep, candidates)
				if err != nil {
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
		fallbackWG.Wait()
	}

	return ev
}

// bareListContext bounds the initial unfiltered ListBackups attempt so that if a
// component's full-bucket listing times out, at least half of the sweep's
// remaining time budget stays available for prefix/per-ID fallback queries.
func bareListContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	remaining := time.Until(deadline)
	budget := remaining / 2
	if budget > 45*time.Second {
		budget = 45 * time.Second
	}
	if budget < 5*time.Second {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, budget)
}

// candidateBackupIDs collects every backup ID observed in the reachable sources,
// placing tracked backup IDs first (newest first) followed by untracked backup
// IDs (newest first).
func candidateBackupIDs(ev *evidence) []string {
	trackedSet := make(map[string]struct{}, len(ev.records))
	var tracked []string
	for _, rec := range ev.records {
		if rec.BackupID == "" {
			continue
		}
		if _, dup := trackedSet[rec.BackupID]; !dup {
			trackedSet[rec.BackupID] = struct{}{}
			tracked = append(tracked, rec.BackupID)
		}
	}
	sort.Slice(tracked, func(i, j int) bool { return tracked[i] > tracked[j] })

	untrackedSet := make(map[string]struct{})
	addUntracked := func(id string) {
		if id == "" {
			return
		}
		if _, isTracked := trackedSet[id]; isTracked {
			return
		}
		untrackedSet[id] = struct{}{}
	}

	for _, snap := range ev.snapshots {
		owner, backupID, _ := elasticsearch.ClassifySnapshot(snap.Name, ev.namePrefix)
		if owner != elasticsearch.OwnerForeign && backupID != "" {
			addUntracked(backupID)
		}
	}
	for _, byID := range ev.componentBackups {
		for id := range byID {
			addUntracked(id)
		}
	}
	for id := range ev.logBackupIDs {
		addUntracked(id)
	}

	untracked := make([]string, 0, len(untrackedSet))
	for id := range untrackedSet {
		untracked = append(untracked, id)
	}
	sort.Slice(untracked, func(i, j int) bool { return untracked[i] > untracked[j] })

	return append(tracked, untracked...)
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
