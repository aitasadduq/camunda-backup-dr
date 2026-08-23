package reconcile

import (
	"context"
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
	return ev
}

// backupIDFromLogFile extracts a backup ID from a log file name. Log files are
// created as {backupID}.log by FileStorage.CreateLogFile.
func backupIDFromLogFile(name string) string {
	base := name
	if idx := strings.LastIndex(base, "/"); idx >= 0 {
		base = base[idx+1:]
	}
	base = strings.TrimSuffix(base, ".log")
	if isBackupIDShaped(base) {
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

// isBackupIDShaped reports whether a string looks like a YYYYMMDDHHMMSS ID.
func isBackupIDShaped(s string) bool {
	if len(s) != 14 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}
