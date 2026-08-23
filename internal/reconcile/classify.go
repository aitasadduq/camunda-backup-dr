package reconcile

import (
	"fmt"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/elasticsearch"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Options tunes the classifier's time-based guards.
type Options struct {
	// GracePeriod excludes backups younger than this from every check, because
	// their artifacts may still be landing.
	GracePeriod time.Duration
	// StaleAfter is how long a backup may stay in progress before it is called
	// stuck. Should exceed the orchestrator's poll window.
	StaleAfter time.Duration
}

// DefaultOptions returns conservative guard settings.
func DefaultOptions() Options {
	return Options{
		GracePeriod: 15 * time.Minute,
		StaleAfter:  30 * time.Minute,
	}
}

// classify turns collected evidence into findings.
//
// It is pure: same evidence in, same findings out, no I/O and no clock reads
// (evidence carries its own timestamp). That is what makes the whole taxonomy
// testable from a table.
//
// Every conclusion drawn from something being *absent* is gated on that source
// having been reachable. A component that could not be listed produces no
// missing-artifact findings at all - reporting data loss because a service was
// briefly down would make the whole feature untrustworthy.
func classify(ev *evidence, opts Options) []Finding {
	idx := indexEvidence(ev)

	var findings []Finding
	findings = append(findings, classifyRecords(ev, idx, opts)...)
	findings = append(findings, classifyUntracked(ev, idx, opts)...)
	findings = append(findings, classifyInstance(ev, idx)...)

	// Sub-classifiers set only the fields specific to their finding; severity,
	// scope and timestamp come from the catalogue so they can never disagree
	// with it.
	for i := range findings {
		findings[i].DetectedAt = ev.now
		findings[i].Severity = severityOf(findings[i].Reason)
		findings[i].Scope = scopeOf(findings[i].Reason)
	}
	return findings
}

// evidenceIndex is the derived view the classifiers work from.
type evidenceIndex struct {
	// recordsByID keeps every record for an ID; more than one means the backup
	// is filed in multiple directories.
	recordsByID map[string][]*models.BackupHistory
	// snapshotsByBackupID groups controller and component snapshots by the
	// backup they belong to.
	controllerSnapshots map[string]elasticsearch.SnapshotInfo
	componentSnapshots  map[string][]elasticsearch.SnapshotInfo
	foreignSnapshots    []elasticsearch.SnapshotInfo
	// allComponentIDs is every backup ID any component reported.
	allComponentIDs map[string]bool
}

func indexEvidence(ev *evidence) *evidenceIndex {
	idx := &evidenceIndex{
		recordsByID:         make(map[string][]*models.BackupHistory),
		controllerSnapshots: make(map[string]elasticsearch.SnapshotInfo),
		componentSnapshots:  make(map[string][]elasticsearch.SnapshotInfo),
		allComponentIDs:     make(map[string]bool),
	}

	for _, rec := range ev.records {
		idx.recordsByID[rec.BackupID] = append(idx.recordsByID[rec.BackupID], rec)
	}

	for _, snap := range ev.snapshots {
		owner, backupID, _ := elasticsearch.ClassifySnapshot(snap.Name, ev.namePrefix)
		snap.Owner = owner
		snap.BackupID = backupID
		switch owner {
		case elasticsearch.OwnerController:
			idx.controllerSnapshots[backupID] = snap
		case elasticsearch.OwnerComponent:
			idx.componentSnapshots[backupID] = append(idx.componentSnapshots[backupID], snap)
		default:
			idx.foreignSnapshots = append(idx.foreignSnapshots, snap)
		}
	}

	for _, byID := range ev.componentBackups {
		for id := range byID {
			idx.allComponentIDs[id] = true
		}
	}
	return idx
}

// withinGrace reports whether a backup is too young to judge.
func withinGrace(backupID string, ev *evidence, opts Options) bool {
	ts, err := camunda.ParseBackupIDTimestamp(backupID)
	if err != nil {
		return false
	}
	return ev.now.Sub(ts) < opts.GracePeriod
}

// isStale reports whether an in-progress artifact has run past its window.
func isStale(backupID string, ev *evidence, opts Options) bool {
	ts, err := camunda.ParseBackupIDTimestamp(backupID)
	if err != nil {
		return false
	}
	return ev.now.Sub(ts) > opts.StaleAfter
}

// classifyRecords walks the backups the controller knows about, checking each
// against the artifacts that should exist for it.
func classifyRecords(ev *evidence, idx *evidenceIndex, opts Options) []Finding {
	var out []Finding

	for backupID, records := range idx.recordsByID {
		if withinGrace(backupID, ev, opts) {
			continue
		}
		record := records[0]

		// E1: the same backup filed in more than one directory.
		if len(records) > 1 {
			out = append(out, Finding{
				BackupID: backupID,
				Reason:   ReasonDuplicateRecord,
				Detail:   fmt.Sprintf("%d records exist for this backup ID", len(records)),
			})
		}

		// E4: record attributed to a different instance than its prefix.
		if record.CamundaInstanceID != "" && record.CamundaInstanceID != ev.instance.ID {
			out = append(out, Finding{
				BackupID: backupID,
				Reason:   ReasonCrossInstanceRecord,
				Detail:   fmt.Sprintf("record names instance %q, filed under %q", record.CamundaInstanceID, ev.instance.ID),
			})
		}

		// E2: stored under a date path that contradicts the record's own start
		// time. The ID is itself a timestamp, so the two must agree.
		if ts, err := camunda.ParseBackupIDTimestamp(backupID); err == nil {
			if !record.StartTime.IsZero() && record.StartTime.UTC().Format("2006-01-02") != ts.UTC().Format("2006-01-02") {
				out = append(out, Finding{
					BackupID: backupID,
					Reason:   ReasonDatePathMismatch,
					Detail: fmt.Sprintf("backup ID encodes %s but the record starts at %s",
						ts.UTC().Format("2006-01-02"), record.StartTime.UTC().Format("2006-01-02")),
				})
			}
		}

		// C5: still RUNNING although no backup survives a controller restart.
		if record.Status == types.BackupStatusRunning && isStale(backupID, ev, opts) {
			out = append(out, Finding{
				BackupID: backupID,
				Reason:   ReasonStaleRunningRecord,
				Detail:   "record has been RUNNING since before the last restart",
			})
		}

		// B5: the recorded log file is gone.
		if ev.reachable(SourceLogs) && !ev.logBackupIDs[backupID] && record.Metadata.LogFilePath != "" {
			out = append(out, Finding{
				BackupID: backupID,
				Reason:   ReasonMissingLogFile,
				Detail:   "no log file on disk for this backup",
			})
		}

		out = append(out, classifyComponents(ev, idx, record, backupID, opts)...)
	}

	return out
}

// classifyComponents compares one record's component results against what the
// components and the snapshot repository actually hold.
func classifyComponents(ev *evidence, idx *evidenceIndex, record *models.BackupHistory, backupID string, opts Options) []Finding {
	var out []Finding

	var missingComponents, presentComponents, unverified []string
	zeebeMissing, zeebePresent := false, false

	for component, endpoint := range componentEndpoints(ev.instance) {
		if endpoint == "" {
			continue
		}

		// G1: judge against what the backup itself recorded, never against the
		// instance's current component config. A component enabled after this
		// backup ran was legitimately never part of it.
		info, tracked := record.Components[component]
		if !tracked || !info.Enabled || info.Status == types.ComponentStatusSkipped {
			continue
		}

		// G5: a component that could not be listed proves nothing.
		if !ev.reachable(component) {
			unverified = append(unverified, component)
			continue
		}

		reported, exists := ev.componentBackups[component][backupID]

		if !exists {
			if info.Status == types.ComponentStatusCompleted {
				missingComponents = append(missingComponents, component)
				if component == types.ComponentZeebe {
					zeebeMissing = true
				}
				out = append(out, Finding{
					BackupID:  backupID,
					Reason:    ReasonDanglingComponentBackup,
					MissingIn: []string{component},
					Detail:    fmt.Sprintf("%s no longer lists this backup", component),
				})
			}
			continue
		}

		presentComponents = append(presentComponents, component)
		if component == types.ComponentZeebe {
			zeebePresent = true
		}

		// D3: this component's data survived a backup that failed overall, and
		// nothing in the retention path will ever clean it up.
		if record.Status == types.BackupStatusFailed || record.Status == types.BackupStatusIncomplete {
			out = append(out, Finding{
				BackupID:  backupID,
				Reason:    ReasonOrphanedInFailedSet,
				PresentIn: []string{component},
				Detail:    fmt.Sprintf("%s holds data for a backup recorded as %s", component, record.Status),
			})
			continue
		}

		if info.Status != types.ComponentStatusCompleted {
			continue
		}

		// C3: in progress long past the polling window.
		if strings.EqualFold(reported.State, camunda.BackupStateInProgress) {
			if isStale(backupID, ev, opts) {
				out = append(out, Finding{
					BackupID:  backupID,
					Reason:    ReasonStaleInProgressComponent,
					PresentIn: []string{component},
					Detail:    fmt.Sprintf("%s still reports IN_PROGRESS", component),
				})
			}
			continue
		}

		// C1: recorded as completed, the component disagrees.
		if !reported.IsCompleted() {
			detail := fmt.Sprintf("%s reports %s", component, reported.State)
			if reported.FailureReason != "" {
				detail += ": " + reported.FailureReason
			}
			out = append(out, Finding{
				BackupID:  backupID,
				Reason:    ReasonStateDivergenceComponent,
				PresentIn: []string{component},
				Detail:    detail,
			})
			continue
		}

		// B3: the component says the backup is fine, but the snapshots behind it
		// are gone. Nothing else surfaces this until a restore is attempted.
		if ev.reachable(SourceElasticsearch) {
			if missing := missingSnapshots(reported, ev); len(missing) > 0 {
				out = append(out, Finding{
					BackupID:  backupID,
					Reason:    ReasonDanglingAppESSnapshot,
					PresentIn: []string{component},
					MissingIn: []string{SourceElasticsearch},
					Detail: fmt.Sprintf("%s reports COMPLETED but %d of its snapshots are absent from %q",
						component, len(missing), ev.repository),
				})
			}
		}
	}

	out = append(out, classifyESComponent(ev, idx, record, backupID, opts, &missingComponents, &presentComponents, &unverified)...)

	// D2 / D1: the set is broken. D2 is the specific, most dangerous shape.
	if len(missingComponents) > 0 && len(presentComponents) > 0 {
		esMissing := containsString(missingComponents, types.ComponentElasticsearch)
		esPresent := containsString(presentComponents, types.ComponentElasticsearch)

		reason := ReasonPartialSet
		if (zeebePresent && esMissing) || (zeebeMissing && esPresent) {
			reason = ReasonSplitRestorePair
		}

		out = append(out, Finding{
			BackupID:   backupID,
			Reason:     reason,
			PresentIn:  presentComponents,
			MissingIn:  missingComponents,
			Unverified: unverified,
			Detail: fmt.Sprintf("present in %s; missing from %s",
				strings.Join(presentComponents, ", "), strings.Join(missingComponents, ", ")),
		})
	}

	return out
}

// classifyESComponent checks the controller's own Elasticsearch snapshot for a
// backup, and the config drift that can make it look absent when it is not.
func classifyESComponent(ev *evidence, idx *evidenceIndex, record *models.BackupHistory, backupID string, opts Options, missing, present, unverified *[]string) []Finding {
	var out []Finding

	info, tracked := record.Components[types.ComponentElasticsearch]
	if !tracked || !info.Enabled || info.Status == types.ComponentStatusSkipped {
		return nil
	}

	if !ev.reachable(SourceElasticsearch) {
		*unverified = append(*unverified, types.ComponentElasticsearch)
		return nil
	}

	// F1: the record's snapshot lives in a repository the instance no longer
	// uses, so its absence here says nothing about whether the data survives.
	if info.SnapshotRepository != "" && info.SnapshotRepository != ev.repository {
		return append(out, Finding{
			BackupID:  backupID,
			Reason:    ReasonRepoRebound,
			MissingIn: []string{SourceElasticsearch},
			Detail: fmt.Sprintf("snapshot was written to repository %q; the instance now uses %q",
				info.SnapshotRepository, ev.repository),
		})
	}

	snap, exists := idx.controllerSnapshots[backupID]

	// F2: the recorded snapshot name does not match what the current prefix
	// would produce, which is config drift rather than data loss.
	if !exists && info.SnapshotName != "" && info.SnapshotName != expectedSnapshotName(backupID, ev.namePrefix) {
		if found := findSnapshotByName(ev.snapshots, info.SnapshotName); found {
			return append(out, Finding{
				BackupID:  backupID,
				Reason:    ReasonNamePrefixDrift,
				PresentIn: []string{SourceElasticsearch},
				Detail: fmt.Sprintf("snapshot is named %q but the configured prefix now produces %q",
					info.SnapshotName, expectedSnapshotName(backupID, ev.namePrefix)),
			})
		}
	}

	if !exists {
		if info.Status == types.ComponentStatusCompleted {
			*missing = append(*missing, types.ComponentElasticsearch)
			out = append(out, Finding{
				BackupID:  backupID,
				Reason:    ReasonDanglingESSnapshot,
				MissingIn: []string{SourceElasticsearch},
				Detail:    fmt.Sprintf("no snapshot for this backup in repository %q", ev.repository),
			})
		}
		return out
	}

	*present = append(*present, types.ComponentElasticsearch)

	if info.Status != types.ComponentStatusCompleted {
		return out
	}

	switch snap.State {
	case elasticsearch.SnapshotStateInProgress:
		if isStale(backupID, ev, opts) {
			out = append(out, Finding{
				BackupID:  backupID,
				Reason:    ReasonStaleInProgressES,
				PresentIn: []string{SourceElasticsearch},
				Detail:    "snapshot has been IN_PROGRESS past the polling window",
			})
		}
	case elasticsearch.SnapshotStatePartial, elasticsearch.SnapshotStateFailed:
		out = append(out, Finding{
			BackupID:  backupID,
			Reason:    ReasonStateDivergenceES,
			PresentIn: []string{SourceElasticsearch},
			Detail:    fmt.Sprintf("snapshot state is %s with %d failed shards", snap.State, snap.FailedShards),
		})
	}

	return out
}

// classifyUntracked finds artifacts that exist with no controller record.
func classifyUntracked(ev *evidence, idx *evidenceIndex, opts Options) []Finding {
	var out []Finding

	// A1: a component holds a backup the controller never recorded.
	for component, byID := range ev.componentBackups {
		for backupID := range byID {
			if len(idx.recordsByID[backupID]) > 0 || withinGrace(backupID, ev, opts) {
				continue
			}
			out = append(out, Finding{
				BackupID:  backupID,
				Reason:    untrackedReason(ev, idx, backupID, ReasonUntrackedComponentBackup),
				PresentIn: []string{component},
				Detail:    fmt.Sprintf("%s holds this backup but the controller has no record of it", component),
			})
		}
	}

	if !ev.reachable(SourceElasticsearch) {
		return out
	}

	// A2: a controller-shaped snapshot with no record behind it.
	for backupID, snap := range idx.controllerSnapshots {
		if len(idx.recordsByID[backupID]) > 0 || withinGrace(backupID, ev, opts) {
			continue
		}
		out = append(out, Finding{
			BackupID:  backupID,
			Reason:    untrackedReason(ev, idx, backupID, ReasonUntrackedESSnapshot),
			PresentIn: []string{SourceElasticsearch},
			Detail:    fmt.Sprintf("snapshot %q has no backup record", snap.Name),
		})
	}

	// A3: component snapshots whose owning component no longer knows about them.
	for backupID, snaps := range idx.componentSnapshots {
		if len(idx.recordsByID[backupID]) > 0 || withinGrace(backupID, ev, opts) {
			continue
		}
		if idx.allComponentIDs[backupID] {
			continue // the component still tracks it; that is A1, already recorded
		}
		out = append(out, Finding{
			BackupID:  backupID,
			Reason:    ReasonUntrackedAppESSnapshot,
			PresentIn: []string{SourceElasticsearch},
			Detail:    fmt.Sprintf("%d component snapshots remain with no owning backup", len(snaps)),
		})
	}

	// A4: snapshots belonging to something else entirely. Reported once as a
	// repository-level note, never as per-backup rows.
	if len(idx.foreignSnapshots) > 0 {
		names := make([]string, 0, len(idx.foreignSnapshots))
		for _, s := range idx.foreignSnapshots {
			names = append(names, s.Name)
		}
		out = append(out, Finding{
			Reason:    ReasonForeignSnapshot,
			PresentIn: []string{SourceElasticsearch},
			Detail: fmt.Sprintf("%d snapshots in %q match no known convention: %s",
				len(names), ev.repository, summarize(names, 5)),
		})
	}

	// A5: log files with no record behind them.
	if ev.reachable(SourceLogs) && ev.reachable(SourceControllerS3) {
		var stray []string
		for backupID := range ev.logBackupIDs {
			if len(idx.recordsByID[backupID]) == 0 && !withinGrace(backupID, ev, opts) {
				stray = append(stray, backupID)
			}
		}
		if len(stray) > 0 {
			out = append(out, Finding{
				Reason: ReasonUntrackedLogFile,
				Detail: fmt.Sprintf("%d log files have no backup record: %s", len(stray), summarize(stray, 5)),
			})
		}
	}

	return out
}

// untrackedReason distinguishes retention residue from a genuinely external
// backup. Residue is data the controller created and then lost track of while
// deleting; the give-away is that only some of its parts remain.
func untrackedReason(ev *evidence, idx *evidenceIndex, backupID string, fallback ReasonCode) ReasonCode {
	if !isBackupIDShaped(backupID) {
		return fallback
	}

	// Artifacts spread across several sources with no record between them are
	// far more likely to be a half-finished deletion than an external backup.
	sources := 0
	for _, byID := range ev.componentBackups {
		if _, ok := byID[backupID]; ok {
			sources++
		}
	}
	if _, ok := idx.controllerSnapshots[backupID]; ok {
		sources++
	}
	if sources > 1 {
		return ReasonRetentionResidue
	}
	return fallback
}

// classifyInstance produces findings that describe the instance as a whole.
func classifyInstance(ev *evidence, idx *evidenceIndex) []Finding {
	var out []Finding

	// B4: the latest-backup pointer resolves to nothing.
	if ev.reachable(SourceControllerS3) && ev.latestPointer != "" {
		if len(idx.recordsByID[ev.latestPointer]) == 0 {
			out = append(out, Finding{
				Reason: ReasonDanglingLatestPointer,
				Detail: fmt.Sprintf("latest-backup-id.txt names %q, which has no record", ev.latestPointer),
			})
		}
	}

	// F3: Zeebe reports none of the backups the controller recorded. One cause
	// with one symptom per backup, so it is reported once at instance scope and
	// suppresses the per-backup findings during rollup.
	if ev.reachable(types.ComponentZeebe) && ev.reachable(SourceControllerS3) {
		expected, found := 0, 0
		for backupID, records := range idx.recordsByID {
			info, tracked := records[0].Components[types.ComponentZeebe]
			if !tracked || !info.Enabled || info.Status != types.ComponentStatusCompleted {
				continue
			}
			expected++
			if _, ok := ev.componentBackups[types.ComponentZeebe][backupID]; ok {
				found++
			}
		}
		if expected >= zeebeReboundThreshold && found == 0 {
			out = append(out, Finding{
				Reason:    ReasonZeebeStoreRebound,
				MissingIn: []string{types.ComponentZeebe},
				Detail:    fmt.Sprintf("Zeebe reports none of the %d backups recorded as completed", expected),
			})
		}
	}

	// F4: exporting left paused by a backup that never resumed it.
	if ev.exporterPaused {
		out = append(out, Finding{
			Reason: ReasonExporterLeftPaused,
			Detail: "Zeebe exporting is paused although no backup is running",
		})
	}

	return out
}

// zeebeReboundThreshold is how many completed backups must all be missing from
// Zeebe before the cause is treated as the store itself rather than individual
// deletions. Below this, per-backup findings are the honest description.
const zeebeReboundThreshold = 3

func missingSnapshots(rec camunda.ComponentBackupRecord, ev *evidence) []string {
	var missing []string
	for _, name := range rec.SnapshotNames() {
		if !findSnapshotByName(ev.snapshots, name) {
			missing = append(missing, name)
		}
	}
	return missing
}

func findSnapshotByName(snapshots []elasticsearch.SnapshotInfo, name string) bool {
	for _, s := range snapshots {
		if s.Name == name {
			return true
		}
	}
	return false
}

func expectedSnapshotName(backupID, namePrefix string) string {
	if namePrefix == "" {
		return backupID
	}
	return namePrefix + "-" + backupID
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// summarize renders at most max items, noting how many were left out.
func summarize(items []string, max int) string {
	if len(items) <= max {
		return strings.Join(items, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(items[:max], ", "), len(items)-max)
}
