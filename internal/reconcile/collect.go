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
	// window is the range of backup IDs this sweep covers; nil means all time.
	// collect restricts every list in the evidence to it, so no later rule can
	// draw a conclusion about a backup the sweep did not look at.
	window *Window
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
	// snapshotsByDay says the repository was listed by name pattern, one date
	// and prefix at a time, not in full. Such a listing holds only snapshots a
	// prefix claims, so nothing may be concluded from what it lacks about the
	// repository as a whole, such as the snapshots no one has claimed (A4).
	snapshotsByDay bool
	logBackupIDs   map[string]bool
	exporterPaused bool
	backupInFlight bool

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
func (r *Reconciler) collect(ctx context.Context, instance *models.CamundaInstance, window *Window) *evidence {
	ev := &evidence{
		window:           window,
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
			listed, err := r.listComponent(ctx, component, ep, window)
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
			snapshots, byDay, err := r.listSnapshots(ctx, ev, r.esFactory(instance))
			if err != nil {
				setSource(SourceStatus{Name: SourceElasticsearch, Error: err.Error()})
				return
			}
			mu.Lock()
			ev.snapshots = snapshots
			ev.snapshotsByDay = byDay
			mu.Unlock()
			status := SourceStatus{Name: SourceElasticsearch, Reachable: true, Count: len(snapshots)}
			if byDay {
				status.Note = byDayNote
			}
			setSource(status)
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
	ev.finalize()
	r.logSnapshotBreakdown(ev)
	return ev
}

// finalize turns what the sources returned into the evidence the rules read:
// every snapshot gets its owner, then everything is restricted to the window.
// The order matters, because the window places a snapshot by the backup ID that
// classification resolves.
func (e *evidence) finalize() {
	e.classifySnapshots()
	e.applyWindow()
}

// byDayNote is what the report says about a repository listed by day.
const byDayNote = "listed by day: only snapshots named by a known prefix were looked for, so snapshots of other tools are not counted or reported"

// esDayFirstMaxDays is the longest window for which the snapshot repository is
// listed by day straight away. Up to this many days it is a handful of small
// requests, cheaper and surer than a full listing of a large repository that
// would then need to be cut off to leave time for a fallback.
const esDayFirstMaxDays = 31

// noWindowHint is appended to a listing failure that a date range could avoid.
const noWindowHint = " (scan a date range to list by day instead: choose one in the UI, or set RECONCILE_WINDOW_DAYS for automatic scans)"

// listComponent lists a component's backups. Zeebe is listed one day at a time
// when the sweep has a window, because its full listing is the one that fails
// on a large backup store; every other component answers a full listing, which
// the window then filters. Operate, Tasklist and Optimize do not accept a
// prefix, so they have no per-day form.
func (r *Reconciler) listComponent(ctx context.Context, component, endpoint string, window *Window) ([]camunda.ComponentBackupRecord, error) {
	if component == types.ComponentZeebe {
		if daily, ok := r.backupLister.(camunda.DayBackupLister); ok {
			if window != nil {
				return daily.ListBackupsByDays(ctx, endpoint, window.Days())
			}
			listed, err := r.backupLister.ListBackups(ctx, endpoint)
			if err != nil {
				return nil, fmt.Errorf("%w%s", err, noWindowHint)
			}
			return listed, nil
		}
	}
	return r.backupLister.ListBackups(ctx, endpoint)
}

// listSnapshots lists the snapshot repository. The second result says it was
// listed by day (see evidence.snapshotsByDay).
//
// Without a window it is listed in full. With one:
//   - up to esDayFirstMaxDays days, by day straight away: a few small requests,
//     with no full listing to cut off;
//   - for longer windows, in full first with two thirds of the scan's time, and
//     by day if that fails.
func (r *Reconciler) listSnapshots(ctx context.Context, ev *evidence, lister SnapshotLister) ([]elasticsearch.SnapshotInfo, bool, error) {
	daily, canFallBack := lister.(elasticsearch.DaySnapshotLister)
	if ev.window == nil || !canFallBack {
		snaps, err := lister.ListSnapshots(ctx, ev.repository)
		if err != nil && canFallBack {
			err = fmt.Errorf("%w%s", err, noWindowHint)
		}
		return snaps, false, err
	}

	days := ev.window.Days()
	prefixes := elasticsearch.SnapshotNamePrefixes(ev.naming())
	if len(days) <= esDayFirstMaxDays {
		snaps, err := daily.ListSnapshotsByDays(ctx, ev.repository, days, prefixes)
		return snaps, true, err
	}

	firstCtx, cancel := firstAttemptContext(ctx)
	snaps, err := lister.ListSnapshots(firstCtx, ev.repository)
	cancel()
	if err == nil {
		return snaps, false, nil
	}
	if ctx.Err() != nil {
		return nil, false, err
	}

	r.logger.Warn("[reconcile] %s: listing the whole snapshot repository failed (%v); listing %s by day instead",
		ev.instance.ID, err, ev.window)
	snaps, dayErr := daily.ListSnapshotsByDays(ctx, ev.repository, days, prefixes)
	if dayErr != nil {
		return nil, false, fmt.Errorf("%w; the listing by day also failed: %v", err, dayErr)
	}
	return snaps, true, nil
}

// firstAttemptContext gives the first, full-listing attempt at most two thirds
// of what is left of the scan's budget, so a fallback can still run after it.
func firstAttemptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, time.Until(deadline)*2/3)
}

// applyWindow restricts the evidence to the window. Everything outside it
// becomes "not assessed": no list holds it, so no rule can call it present,
// missing or untracked. Snapshots whose names carry no backup ID cannot be
// placed in a date and are kept; they are only ever reported at repository
// level.
func (e *evidence) applyWindow() {
	w := e.window
	if w == nil {
		return
	}
	// Snapshots are placed by the backup ID classification resolved; finalize
	// classifies before it gets here.

	records := e.records[:0:0]
	for _, rec := range e.records {
		if w.Contains(rec.BackupID) {
			records = append(records, rec)
		}
	}
	e.records = records

	for component, byID := range e.componentBackups {
		kept := make(map[string]camunda.ComponentBackupRecord, len(byID))
		for id, rec := range byID {
			if w.Contains(id) {
				kept[id] = rec
			}
		}
		e.componentBackups[component] = kept
	}

	snaps := e.snapshots[:0:0]
	for _, snap := range e.snapshots {
		if snap.BackupID == "" || w.Contains(snap.BackupID) {
			snaps = append(snaps, snap)
		}
	}
	e.snapshots = snaps

	for id := range e.logBackupIDs {
		if !w.Contains(id) {
			delete(e.logBackupIDs, id)
		}
	}

	// The pointer names the latest backup overall. If that is outside the window
	// the window holds no record of it, and "dangling" would be a false alarm.
	if !w.Contains(e.latestPointer) {
		e.latestPointer = ""
	}

	// A source's count is what the report says it found; with a window that is
	// what lies inside it.
	count := func(name string, n int) {
		if s, ok := e.sources[name]; ok && s.Reachable {
			s.Count = n
			e.sources[name] = s
		}
	}
	count(SourceControllerS3, len(e.records))
	count(SourceElasticsearch, len(e.snapshots))
	count(SourceLogs, len(e.logBackupIDs))
	for component, byID := range e.componentBackups {
		count(component, len(byID))
	}
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

	if ev.snapshotsByDay {
		r.logger.Info("[reconcile] %s: repository %q was listed by day for %s: %d snapshot(s) named by a known prefix; snapshots of other tools were not looked for",
			ev.instance.ID, ev.repository, ev.window, len(ev.snapshots))
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
