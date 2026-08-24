// Package reconcile cross-references the controller's S3 backup metadata against
// the artifacts that actually exist in Zeebe, the Camunda component APIs and the
// Elasticsearch snapshot repository, and reports where they disagree.
//
// Detection is report-only: nothing in this package deletes a backup or an
// artifact. Deletion stays behind the manual API path in internal/retention.
package reconcile

// Scope says what a finding is about. Instance-scoped findings describe the
// whole instance (one cause, many symptoms); repository-scoped findings describe
// artifacts that belong to no backup the controller knows about.
type Scope string

const (
	ScopeInstance   Scope = "instance"
	ScopeBackup     Scope = "backup"
	ScopeRepository Scope = "repository"
)

// Severity orders findings by how much damage the underlying condition does.
type Severity string

const (
	SeverityInfo          Severity = "info"
	SeverityWarn          Severity = "warn"
	SeverityBlocksRestore Severity = "blocks_restore"
	SeverityCritical      Severity = "critical"
)

// severityRank orders severities for sorting and for picking a primary reason.
var severityRank = map[Severity]int{
	SeverityInfo:          0,
	SeverityWarn:          1,
	SeverityBlocksRestore: 2,
	SeverityCritical:      3,
}

// Rank returns the sort weight of a severity. Higher is worse.
func (s Severity) Rank() int {
	return severityRank[s]
}

// ReasonCode identifies a specific way a backup can be inconsistent. These are
// part of the public API contract: they appear in reconcile reports and are safe
// to match on from scripts, so their string values must stay stable.
type ReasonCode string

// Group A - untracked: the artifact exists but the controller has no record of it.
const (
	ReasonUntrackedComponentBackup ReasonCode = "A1_UNTRACKED_COMPONENT_BACKUP"
	ReasonUntrackedESSnapshot      ReasonCode = "A2_UNTRACKED_ES_SNAPSHOT"
	ReasonUntrackedAppESSnapshot   ReasonCode = "A3_UNTRACKED_APP_ES_SNAPSHOT"
	ReasonForeignSnapshot          ReasonCode = "A4_FOREIGN_SNAPSHOT"
	ReasonUntrackedLogFile         ReasonCode = "A5_UNTRACKED_LOG_FILE"
)

// Group B - dangling: the controller has a record but the artifact is gone.
const (
	ReasonDanglingComponentBackup ReasonCode = "B1_DANGLING_COMPONENT_BACKUP"
	ReasonDanglingESSnapshot      ReasonCode = "B2_DANGLING_ES_SNAPSHOT"
	ReasonDanglingAppESSnapshot   ReasonCode = "B3_DANGLING_APP_ES_SNAPSHOT"
	ReasonDanglingLatestPointer   ReasonCode = "B4_DANGLING_LATEST_POINTER"
	ReasonMissingLogFile          ReasonCode = "B5_MISSING_LOG_FILE"
)

// Group C - divergence: both sides exist but report different states.
const (
	ReasonStateDivergenceComponent ReasonCode = "C1_STATE_DIVERGENCE_COMPONENT"
	ReasonStateDivergenceES        ReasonCode = "C2_STATE_DIVERGENCE_ES"
	ReasonStaleInProgressComponent ReasonCode = "C3_STALE_IN_PROGRESS_COMPONENT"
	ReasonStaleInProgressES        ReasonCode = "C4_STALE_IN_PROGRESS_ES"
	ReasonStaleRunningRecord       ReasonCode = "C5_STALE_RUNNING_RECORD"
)

// Group D - partial or split sets, which cannot be restored as a unit.
const (
	ReasonPartialSet          ReasonCode = "D1_PARTIAL_SET"
	ReasonSplitRestorePair    ReasonCode = "D2_SPLIT_RESTORE_PAIR"
	ReasonOrphanedInFailedSet ReasonCode = "D3_ORPHANED_COMPONENT_IN_FAILED_SET"
	ReasonRetentionResidue    ReasonCode = "D4_RETENTION_RESIDUE"
)

// Group E - anomalies inside the controller's own S3 metadata.
const (
	ReasonDuplicateRecord         ReasonCode = "E1_DUPLICATE_RECORD"
	ReasonDatePathMismatch        ReasonCode = "E2_DATE_PATH_MISMATCH"
	ReasonUnparseableRecord       ReasonCode = "E3_UNPARSEABLE_RECORD"
	ReasonCrossInstanceRecord     ReasonCode = "E4_CROSS_INSTANCE_RECORD"
	ReasonAbandonedInstancePrefix ReasonCode = "E5_ABANDONED_INSTANCE_PREFIX"
)

// Group F - environment and configuration drift.
const (
	ReasonRepoRebound        ReasonCode = "F1_REPO_REBOUND"
	ReasonNamePrefixDrift    ReasonCode = "F2_NAME_PREFIX_DRIFT"
	ReasonZeebeStoreRebound  ReasonCode = "F3_ZEEBE_BACKUP_STORE_REBOUND"
	ReasonExporterLeftPaused ReasonCode = "F4_EXPORTER_LEFT_PAUSED"
)

// ReasonInfo is the human-facing description of a reason code. The UI reads this
// catalogue once from GET /api/reconcile/reasons and renders Label as the primary
// text, keeping the code itself as secondary detail.
type ReasonInfo struct {
	Code        ReasonCode `json:"code"`
	Scope       Scope      `json:"scope"`
	Severity    Severity   `json:"severity"`
	Label       string     `json:"label"`
	Explanation string     `json:"explanation"`
	Impact      string     `json:"impact"`
	Remediation string     `json:"remediation"`
}

// reasonCatalog describes every reason code. Every ReasonCode constant must have
// an entry here; TestReasonCatalogIsComplete enforces that.
var reasonCatalog = map[ReasonCode]ReasonInfo{
	ReasonUntrackedComponentBackup: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Backup not tracked by the controller",
		Explanation: "A Camunda component still holds this backup, but the controller has no history record for it.",
		Impact:      "The backup consumes storage and will never be cleaned up by retention.",
		Remediation: "If the backup was taken deliberately outside the controller, leave it. Otherwise delete it from each component that lists it: DELETE {component_backup_endpoint}/{backup_id}",
	},
	ReasonUntrackedESSnapshot: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Elasticsearch snapshot not tracked by the controller",
		Explanation: "A snapshot matching the controller's naming convention exists in the repository, but no history record refers to it.",
		Impact:      "The snapshot consumes repository storage and retention will never remove it.",
		Remediation: "Delete it once you are sure it is not needed: DELETE /_snapshot/{repository}/{snapshot_name}",
	},
	ReasonUntrackedAppESSnapshot: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Component snapshot left behind",
		Explanation: "An Operate, Tasklist or Optimize snapshot exists in the repository, but neither the controller nor the owning component lists the backup it belongs to.",
		Impact:      "Storage is consumed by a snapshot nothing can restore from, because the component metadata that describes it is gone.",
		Remediation: "Delete the snapshot parts directly: DELETE /_snapshot/{repository}/{snapshot_name}",
	},
	ReasonForeignSnapshot: {
		Scope:       ScopeRepository,
		Severity:    SeverityInfo,
		Label:       "Snapshot from another tool",
		Explanation: "The repository contains a snapshot that matches neither the controller's nor Camunda's naming convention.",
		Impact:      "None for this controller. It may belong to an SLM policy, another cluster, or a different backup tool.",
		Remediation: "No action. Confirm ownership before deleting anything in a shared repository.",
	},
	ReasonUntrackedLogFile: {
		Scope:       ScopeRepository,
		Severity:    SeverityInfo,
		Label:       "Log file with no backup record",
		Explanation: "A backup log file exists on the controller's data volume but the matching history record is gone.",
		Impact:      "Negligible - a small amount of local disk.",
		Remediation: "Safe to delete from $DATA_DIR/logs/{instance_id}/.",
	},

	ReasonDanglingComponentBackup: {
		Scope:       ScopeBackup,
		Severity:    SeverityBlocksRestore,
		Label:       "Component backup is missing",
		Explanation: "The controller recorded this component as backed up, but the component no longer lists the backup.",
		Impact:      "This backup cannot be restored. The controller would report it as available.",
		Remediation: "Take a fresh backup. Investigate why the component's backup store lost the data before trusting the next one.",
	},
	ReasonDanglingESSnapshot: {
		Scope:       ScopeBackup,
		Severity:    SeverityBlocksRestore,
		Label:       "Elasticsearch snapshot is missing",
		Explanation: "The controller recorded a successful Elasticsearch snapshot, but it is absent from the repository.",
		Impact:      "This backup cannot be restored.",
		Remediation: "Take a fresh backup. Check whether the snapshot repository was pruned or re-bound to different storage.",
	},
	ReasonDanglingAppESSnapshot: {
		Scope:       ScopeBackup,
		Severity:    SeverityCritical,
		Label:       "Component reports a backup whose snapshots are gone",
		Explanation: "The component still lists this backup as complete, but the Elasticsearch snapshots backing it no longer exist in the repository.",
		Impact:      "Every status surface reports this backup as healthy. It will fail only at the moment you try to restore it.",
		Remediation: "Treat this backup as lost. Take a fresh one and verify the repository's retention and lifecycle settings.",
	},
	ReasonDanglingLatestPointer: {
		Scope:       ScopeInstance,
		Severity:    SeverityWarn,
		Label:       "Latest-backup pointer is stale",
		Explanation: "latest-backup-id.txt names a backup that has no history record in any directory.",
		Impact:      "Tooling that reads the pointer resolves it to a backup that cannot be described.",
		Remediation: "Resolved automatically by the next successful backup. No manual action needed.",
	},
	ReasonMissingLogFile: {
		Scope:       ScopeBackup,
		Severity:    SeverityInfo,
		Label:       "Backup log file is missing",
		Explanation: "The history record points at a log file that no longer exists on disk.",
		Impact:      "The backup's logs cannot be viewed. The backup data itself is unaffected.",
		Remediation: "No action. Log files are pruned on their own retention schedule.",
	},

	ReasonStateDivergenceComponent: {
		Scope:       ScopeBackup,
		Severity:    SeverityCritical,
		Label:       "Component disagrees with the recorded status",
		Explanation: "The controller recorded this component as completed, but the component itself reports the backup as failed or incomplete.",
		Impact:      "The backup looks healthy in the controller and is not restorable in reality.",
		Remediation: "Trust the component, not the record. Take a fresh backup and check the controller's logs for the original poll result.",
	},
	ReasonStateDivergenceES: {
		Scope:       ScopeBackup,
		Severity:    SeverityCritical,
		Label:       "Elasticsearch snapshot is partial or failed",
		Explanation: "The controller recorded a successful snapshot, but Elasticsearch reports it as PARTIAL or FAILED.",
		Impact:      "Restoring from this snapshot would silently omit shards.",
		Remediation: "Take a fresh backup. Check Elasticsearch cluster health and the failed_shards count on the snapshot.",
	},
	ReasonStaleInProgressComponent: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Component backup stuck in progress",
		Explanation: "The component has reported this backup as in-progress for far longer than the controller's polling window allows.",
		Impact:      "The backup will never complete and may hold resources in the component.",
		Remediation: "Check the component's logs. Delete the stuck backup once you have confirmed it is not advancing.",
	},
	ReasonStaleInProgressES: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Snapshot stuck in progress",
		Explanation: "Elasticsearch has reported this snapshot as IN_PROGRESS for longer than the polling window allows.",
		Impact:      "The snapshot holds repository resources and blocks concurrent snapshot operations.",
		Remediation: "Check Elasticsearch cluster health. A genuinely stuck snapshot can be aborted with DELETE /_snapshot/{repository}/{snapshot_name}.",
	},
	ReasonStaleRunningRecord: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Backup record stuck in RUNNING",
		Explanation: "This record is still marked RUNNING although no backup is in flight, which means the controller stopped mid-backup.",
		Impact:      "The record misrepresents the backup's real state, and any artifacts it created are unaccounted for.",
		Remediation: "Reclassified as INCOMPLETE automatically at startup. Review any artifacts the interrupted run left behind.",
	},

	ReasonPartialSet: {
		Scope:       ScopeBackup,
		Severity:    SeverityBlocksRestore,
		Label:       "Backup set is incomplete",
		Explanation: "Some components that were enabled when this backup ran no longer hold their part of it.",
		Impact:      "A Camunda restore needs every component at the same backup ID, so this set cannot be restored.",
		Remediation: "Take a fresh backup. Delete the surviving parts of this one once a newer backup has succeeded.",
	},
	ReasonSplitRestorePair: {
		Scope:       ScopeBackup,
		Severity:    SeverityCritical,
		Label:       "Zeebe and Elasticsearch halves are split",
		Explanation: "Only one half of the Zeebe plus Elasticsearch pair survives for this backup ID.",
		Impact:      "A Camunda restore requires both halves at the same backup ID. This backup is unrestorable, and the surviving half may look healthy on its own.",
		Remediation: "Take a fresh backup immediately, then remove the orphaned half.",
	},
	ReasonOrphanedInFailedSet: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Artifacts left by a failed backup",
		Explanation: "The backup as a whole failed or was interrupted, but individual components succeeded and left real data behind.",
		Impact:      "Storage is consumed by data that will never be restored and that retention does not clean up.",
		Remediation: "Delete the surviving component backups and snapshots for this backup ID by hand.",
	},
	ReasonRetentionResidue: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Retention deleted the record but not the data",
		Explanation: "Retention removed this backup's history record, but the component or snapshot deletion failed, so the data survives untracked.",
		Impact:      "Storage is consumed permanently: with no history record, retention will never retry the deletion.",
		Remediation: "Delete the remaining artifacts by hand. Check the controller's alerts for the original deletion failure.",
	},

	ReasonDuplicateRecord: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Backup recorded in more than one place",
		Explanation: "The same backup ID has records in more than one of history/, incomplete/ and orphaned/.",
		Impact:      "Listings show the backup more than once and its true status is ambiguous.",
		Remediation: "Keep the record that matches the backup's real state and delete the others.",
	},
	ReasonDatePathMismatch: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Record filed under the wrong date",
		Explanation: "The record is stored under a YYYY/MM/DD path that contradicts the start time inside it.",
		Impact:      "Date-keyed lookups cannot find the record; only a full scan will.",
		Remediation: "Rewrite the record under the correct date path.",
	},
	ReasonUnparseableRecord: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Unreadable backup record",
		Explanation: "An object under the instance prefix is not valid backup history JSON, or its contents contradict its key.",
		Impact:      "The record is ignored by every listing, so the backup it describes is invisible to the controller.",
		Remediation: "Inspect the object in S3. Delete it if it is corrupt.",
	},
	ReasonCrossInstanceRecord: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Record filed under the wrong instance",
		Explanation: "The record names a different Camunda instance than the prefix it is stored under.",
		Impact:      "The backup is attributed to the wrong instance, and retention may apply the wrong policy to it.",
		Remediation: "Move the record under the correct instance prefix.",
	},
	ReasonAbandonedInstancePrefix: {
		Scope:       ScopeInstance,
		Severity:    SeverityWarn,
		Label:       "Backups left by a deleted instance",
		Explanation: "The bucket holds backups for an instance ID that is no longer in the controller's configuration.",
		Impact:      "Every backup under this prefix is unmanaged: no retention applies and nothing will ever delete it.",
		Remediation: "Re-add the instance to adopt the backups, or delete the whole prefix once you are sure they are not needed.",
	},

	ReasonRepoRebound: {
		Scope:       ScopeBackup,
		Severity:    SeverityBlocksRestore,
		Label:       "Snapshot repository has changed",
		Explanation: "This backup's snapshots live in a repository the instance is no longer configured to use.",
		Impact:      "The controller cannot reach the snapshots, so the backup cannot be restored through it.",
		Remediation: "Point the instance back at the original repository, or register the old repository alongside the new one.",
	},
	ReasonNamePrefixDrift: {
		Scope:       ScopeBackup,
		Severity:    SeverityWarn,
		Label:       "Snapshot name prefix has changed",
		Explanation: "The configured snapshot name prefix no longer matches the name this backup's snapshot was created with.",
		Impact:      "Older snapshots are not recognised as belonging to their backups, which produces false missing-snapshot reports.",
		Remediation: "Restore the previous ELASTICSEARCH_SNAPSHOT_NAME_PREFIX, or accept that pre-change backups are only addressable by their literal names.",
	},
	ReasonZeebeStoreRebound: {
		Scope:       ScopeInstance,
		Severity:    SeverityCritical,
		Label:       "Zeebe backup store has changed",
		Explanation: "Zeebe reports none of the backups the controller has recorded, which means the broker's backup store was re-pointed or emptied.",
		Impact:      "Every existing backup for this instance is unrestorable, not just one.",
		Remediation: "Check ZEEBE_BROKER_DATA_BACKUP_* on the broker. Restore the original bucket if this was unintentional, then take a fresh backup.",
	},
	ReasonExporterLeftPaused: {
		Scope:       ScopeInstance,
		Severity:    SeverityCritical,
		Label:       "Zeebe exporting is still paused",
		Explanation: "Exporting is paused although no backup is running, so a previous backup failed to resume it.",
		Impact:      "Zeebe is not exporting to Elasticsearch. Operate and Tasklist fall further behind for as long as this lasts.",
		Remediation: "Resume it immediately: POST {exporting_endpoint}/resume",
	},
}

func init() {
	// Stamp each entry with its own key so callers get a self-describing struct
	// and the catalogue cannot drift out of sync with its map keys.
	for code, info := range reasonCatalog {
		info.Code = code
		reasonCatalog[code] = info
	}
}

// Describe returns the catalogue entry for a reason code. The second result is
// false for codes that have no entry, which the completeness test rules out.
func Describe(code ReasonCode) (ReasonInfo, bool) {
	info, ok := reasonCatalog[code]
	return info, ok
}

// Catalog returns every reason code description, ordered by code, for serving at
// GET /api/reconcile/reasons.
func Catalog() []ReasonInfo {
	out := make([]ReasonInfo, 0, len(reasonCatalog))
	for _, info := range reasonCatalog {
		out = append(out, info)
	}
	sortReasonInfos(out)
	return out
}

// severityOf reports the catalogued severity for a code, defaulting to warn so an
// uncatalogued code is never silently treated as harmless.
func severityOf(code ReasonCode) Severity {
	if info, ok := reasonCatalog[code]; ok {
		return info.Severity
	}
	return SeverityWarn
}

// scopeOf reports the catalogued scope for a code, defaulting to backup scope.
func scopeOf(code ReasonCode) Scope {
	if info, ok := reasonCatalog[code]; ok {
		return info.Scope
	}
	return ScopeBackup
}
