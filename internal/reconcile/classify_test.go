package reconcile

import (
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/elasticsearch"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Fixed clock and two backup IDs: one old enough to judge, one inside the
// grace period.
var (
	testNow      = time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	oldBackupID  = "20260320080000" // 4h before testNow
	freshBackupI = "20260320115900" // 1m before testNow
)

// evidenceBuilder assembles the evidence a sweep would have collected. Every
// source starts reachable and empty, so a test only states what it cares about.
type evidenceBuilder struct{ ev *evidence }

func newEvidence() *evidenceBuilder {
	instance := &models.CamundaInstance{
		ID:                     "prod",
		Name:                   "Production",
		ZeebeBackupEndpoint:    "http://zeebe/actuator/backups",
		OperateBackupEndpoint:  "http://operate/actuator/backups",
		ElasticsearchEndpoint:  "http://es:9200",
		TasklistBackupEndpoint: "",
		OptimizeBackupEndpoint: "",
	}
	return &evidenceBuilder{ev: &evidence{
		instance:         instance,
		now:              testNow,
		repository:       "camunda-backup",
		componentBackups: map[string]map[string]camunda.ComponentBackupRecord{},
		logBackupIDs:     map[string]bool{},
		sources: map[string]SourceStatus{
			SourceControllerS3:      {Name: SourceControllerS3, Reachable: true},
			types.ComponentZeebe:    {Name: types.ComponentZeebe, Reachable: true},
			types.ComponentOperate:  {Name: types.ComponentOperate, Reachable: true},
			SourceElasticsearch:     {Name: SourceElasticsearch, Reachable: true},
			SourceLogs:              {Name: SourceLogs, Reachable: true},
			types.ComponentTasklist: {Name: types.ComponentTasklist, Skipped: true},
			types.ComponentOptimize: {Name: types.ComponentOptimize, Skipped: true},
		},
	}}
}

// record adds a history record whose listed components are all COMPLETED.
func (b *evidenceBuilder) record(backupID string, status types.BackupStatus, components ...string) *evidenceBuilder {
	ts, _ := camunda.ParseBackupIDTimestamp(backupID)
	h := &models.BackupHistory{
		BackupID:          backupID,
		CamundaInstanceID: b.ev.instance.ID,
		StartTime:         ts,
		Status:            status,
		Components:        map[string]models.ComponentBackupInfo{},
	}
	for _, c := range components {
		h.Components[c] = models.ComponentBackupInfo{Enabled: true, Status: types.ComponentStatusCompleted}
	}
	b.ev.records = append(b.ev.records, h)
	return b
}

// componentInfo overrides one component entry on the most recent record.
func (b *evidenceBuilder) componentInfo(component string, info models.ComponentBackupInfo) *evidenceBuilder {
	b.ev.records[len(b.ev.records)-1].Components[component] = info
	return b
}

// holds registers a component as holding a backup in the given state.
func (b *evidenceBuilder) holds(component, backupID, state string, snapshots ...string) *evidenceBuilder {
	if b.ev.componentBackups[component] == nil {
		b.ev.componentBackups[component] = map[string]camunda.ComponentBackupRecord{}
	}
	rec := camunda.ComponentBackupRecord{BackupID: backupID, State: state}
	for _, s := range snapshots {
		rec.Details = append(rec.Details, camunda.ComponentBackupDetail{SnapshotName: s, State: "SUCCESS"})
	}
	b.ev.componentBackups[component][backupID] = rec
	return b
}

// snapshot registers a snapshot present in the repository.
func (b *evidenceBuilder) snapshot(name string, state elasticsearch.SnapshotState) *evidenceBuilder {
	b.ev.snapshots = append(b.ev.snapshots, elasticsearch.SnapshotInfo{Name: name, State: state})
	return b
}

func (b *evidenceBuilder) unreachable(source string) *evidenceBuilder {
	b.ev.sources[source] = SourceStatus{Name: source, Reachable: false, Error: "connection refused"}
	return b
}

func (b *evidenceBuilder) build() *evidence { return b.ev }

// reasons returns the set of reason codes present in a finding list.
func reasons(findings []Finding) map[ReasonCode]int {
	out := map[ReasonCode]int{}
	for _, f := range findings {
		out[f.Reason]++
	}
	return out
}

func hasReason(findings []Finding, code ReasonCode) bool {
	_, ok := reasons(findings)[code]
	return ok
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name       string
		evidence   *evidence
		wantCodes  []ReasonCode
		absentCode []ReasonCode
	}{
		{
			name: "healthy backup produces no findings",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe, types.ComponentElasticsearch).
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
				snapshot(oldBackupID, elasticsearch.SnapshotStateSuccess).
				build(),
			wantCodes: nil,
		},
		{
			name: "A1 component holds a backup the controller never recorded",
			evidence: newEvidence().
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
				build(),
			wantCodes: []ReasonCode{ReasonUntrackedComponentBackup},
		},
		{
			name: "A2 controller snapshot with no record",
			evidence: newEvidence().
				snapshot(oldBackupID, elasticsearch.SnapshotStateSuccess).
				build(),
			wantCodes: []ReasonCode{ReasonUntrackedESSnapshot},
		},
		{
			name: "A3 component snapshot whose owner forgot it",
			evidence: newEvidence().
				snapshot("camunda_operate_"+oldBackupID+"_8.6.0_part_1_of_6", elasticsearch.SnapshotStateSuccess).
				build(),
			wantCodes: []ReasonCode{ReasonUntrackedAppESSnapshot},
		},
		{
			name: "A4 foreign snapshot is repository scoped info",
			evidence: newEvidence().
				snapshot("slm-daily-2026.03.20", elasticsearch.SnapshotStateSuccess).
				build(),
			wantCodes: []ReasonCode{ReasonForeignSnapshot},
		},
		{
			name: "B1 recorded component backup is gone",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe).
				build(),
			wantCodes: []ReasonCode{ReasonDanglingComponentBackup},
		},
		{
			name: "B2 recorded snapshot is gone",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentElasticsearch).
				build(),
			wantCodes: []ReasonCode{ReasonDanglingESSnapshot},
		},
		{
			name: "B3 component reports complete but its snapshots are gone",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentOperate).
				holds(types.ComponentOperate, oldBackupID, camunda.BackupStateCompleted,
					"camunda_operate_"+oldBackupID+"_8.6.0_part_1_of_6").
				build(),
			wantCodes: []ReasonCode{ReasonDanglingAppESSnapshot},
		},
		{
			name: "C1 component disagrees with the record",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe).
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateFailed).
				build(),
			wantCodes: []ReasonCode{ReasonStateDivergenceComponent},
		},
		{
			name: "C2 snapshot is partial",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentElasticsearch).
				snapshot(oldBackupID, elasticsearch.SnapshotStatePartial).
				build(),
			wantCodes: []ReasonCode{ReasonStateDivergenceES},
		},
		{
			name: "C3 component stuck in progress past the window",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe).
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateInProgress).
				build(),
			wantCodes: []ReasonCode{ReasonStaleInProgressComponent},
		},
		{
			name: "C4 snapshot stuck in progress past the window",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentElasticsearch).
				snapshot(oldBackupID, elasticsearch.SnapshotStateInProgress).
				build(),
			wantCodes: []ReasonCode{ReasonStaleInProgressES},
		},
		{
			name: "C5 record still RUNNING long after the fact",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusRunning).
				build(),
			wantCodes: []ReasonCode{ReasonStaleRunningRecord},
		},
		{
			name: "D2 zeebe survives but the snapshot is gone",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe, types.ComponentElasticsearch).
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
				build(),
			wantCodes: []ReasonCode{ReasonSplitRestorePair, ReasonDanglingESSnapshot},
		},
		{
			name: "D1 a non-pair component is missing",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe, types.ComponentOperate).
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
				build(),
			wantCodes:  []ReasonCode{ReasonPartialSet},
			absentCode: []ReasonCode{ReasonSplitRestorePair},
		},
		{
			name: "D3 failed backup left real artifacts behind",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusFailed, types.ComponentZeebe).
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
				build(),
			wantCodes: []ReasonCode{ReasonOrphanedInFailedSet},
		},
		{
			name: "D4 artifacts in several sources with no record is retention residue",
			evidence: newEvidence().
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
				snapshot(oldBackupID, elasticsearch.SnapshotStateSuccess).
				build(),
			wantCodes: []ReasonCode{ReasonRetentionResidue},
		},
		{
			name: "E1 the same backup filed twice",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted).
				record(oldBackupID, types.BackupStatusIncomplete).
				build(),
			wantCodes: []ReasonCode{ReasonDuplicateRecord},
		},
		{
			name: "F1 snapshot repository has been re-pointed",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentElasticsearch).
				componentInfo(types.ComponentElasticsearch, models.ComponentBackupInfo{
					Enabled: true, Status: types.ComponentStatusCompleted, SnapshotRepository: "old-repo",
				}).
				build(),
			wantCodes:  []ReasonCode{ReasonRepoRebound},
			absentCode: []ReasonCode{ReasonDanglingESSnapshot},
		},
		{
			name: "F4 exporting left paused",
			evidence: func() *evidence {
				b := newEvidence()
				b.ev.exporterPaused = true
				return b.build()
			}(),
			wantCodes: []ReasonCode{ReasonExporterLeftPaused},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.evidence, DefaultOptions())

			for _, want := range tt.wantCodes {
				if !hasReason(got, want) {
					t.Errorf("missing expected reason %s; got %v", want, reasons(got))
				}
			}
			for _, absent := range tt.absentCode {
				if hasReason(got, absent) {
					t.Errorf("reason %s should not have been reported; got %v", absent, reasons(got))
				}
			}
			if tt.wantCodes == nil && len(got) != 0 {
				t.Errorf("expected no findings, got %v", reasons(got))
			}
		})
	}
}

// The guards are what separate a useful detector from an alarm nobody trusts,
// so each gets an explicit test that a finding is NOT produced.
func TestClassifyGuards(t *testing.T) {
	tests := []struct {
		name     string
		evidence *evidence
		absent   []ReasonCode
	}{
		{
			name: "G1 component disabled at backup time is not missing",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe).
				componentInfo(types.ComponentOperate, models.ComponentBackupInfo{
					Enabled: false, Status: types.ComponentStatusSkipped,
				}).
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
				snapshot(oldBackupID, elasticsearch.SnapshotStateSuccess).
				build(),
			absent: []ReasonCode{ReasonDanglingComponentBackup, ReasonPartialSet},
		},
		{
			name: "G5 an unreachable component yields no missing-artifact findings",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe).
				unreachable(types.ComponentZeebe).
				build(),
			absent: []ReasonCode{ReasonDanglingComponentBackup, ReasonPartialSet, ReasonSplitRestorePair},
		},
		{
			name: "G5 unreachable elasticsearch yields no missing-snapshot findings",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentElasticsearch).
				unreachable(SourceElasticsearch).
				build(),
			absent: []ReasonCode{ReasonDanglingESSnapshot, ReasonUntrackedESSnapshot},
		},
		{
			name: "G9 a backup inside the grace period is never judged",
			evidence: newEvidence().
				record(freshBackupI, types.BackupStatusCompleted, types.ComponentZeebe, types.ComponentElasticsearch).
				build(),
			absent: []ReasonCode{ReasonDanglingComponentBackup, ReasonDanglingESSnapshot, ReasonSplitRestorePair},
		},
		{
			name: "G4 an in-progress backup inside the window is not called stuck",
			evidence: newEvidence().
				record(freshBackupI, types.BackupStatusCompleted, types.ComponentZeebe).
				holds(types.ComponentZeebe, freshBackupI, camunda.BackupStateInProgress).
				build(),
			absent: []ReasonCode{ReasonStaleInProgressComponent},
		},
		{
			name: "G7 component snapshots are not mistaken for foreign ones",
			evidence: newEvidence().
				record(oldBackupID, types.BackupStatusCompleted, types.ComponentOperate).
				holds(types.ComponentOperate, oldBackupID, camunda.BackupStateCompleted,
					"camunda_operate_"+oldBackupID+"_8.6.0_part_1_of_6").
				snapshot("camunda_operate_"+oldBackupID+"_8.6.0_part_1_of_6", elasticsearch.SnapshotStateSuccess).
				build(),
			absent: []ReasonCode{ReasonForeignSnapshot, ReasonDanglingAppESSnapshot},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.evidence, DefaultOptions())
			for _, code := range tt.absent {
				if hasReason(got, code) {
					t.Errorf("guard failed: %s was reported; got %v", code, reasons(got))
				}
			}
		})
	}
}

// F3 exists so that one re-pointed Zeebe store does not read as N independent
// data-loss events.
func TestClassifyZeebeStoreRebound(t *testing.T) {
	b := newEvidence()
	for _, id := range []string{"20260320080000", "20260320090000", "20260320100000"} {
		b.record(id, types.BackupStatusCompleted, types.ComponentZeebe)
	}
	got := classify(b.build(), DefaultOptions())

	if !hasReason(got, ReasonZeebeStoreRebound) {
		t.Fatalf("expected F3 when every recorded Zeebe backup is absent; got %v", reasons(got))
	}
}

func TestClassifyZeebeReboundNotTriggeredBelowThreshold(t *testing.T) {
	got := classify(newEvidence().
		record(oldBackupID, types.BackupStatusCompleted, types.ComponentZeebe).
		build(), DefaultOptions())

	if hasReason(got, ReasonZeebeStoreRebound) {
		t.Error("a single missing Zeebe backup must be reported per-backup, not as a store rebound")
	}
	if !hasReason(got, ReasonDanglingComponentBackup) {
		t.Error("expected the per-backup finding instead")
	}
}

// Codes that are emitted but were not covered by the main table above.
func TestClassifyRemainingCodes(t *testing.T) {
	tests := []struct {
		name     string
		evidence *evidence
		want     ReasonCode
	}{
		{
			name: "A5 log file on disk with no backup record",
			evidence: func() *evidence {
				b := newEvidence()
				b.ev.logBackupIDs[oldBackupID] = true
				return b.build()
			}(),
			want: ReasonUntrackedLogFile,
		},
		{
			name: "B5 record points at a log file that is gone",
			evidence: func() *evidence {
				b := newEvidence().record(oldBackupID, types.BackupStatusCompleted)
				b.ev.records[0].Metadata.LogFilePath = "/data/logs/prod/" + oldBackupID + ".log"
				return b.build()
			}(),
			want: ReasonMissingLogFile,
		},
		{
			name: "B4 latest-backup pointer resolves to nothing",
			evidence: func() *evidence {
				b := newEvidence()
				b.ev.latestPointer = oldBackupID
				return b.build()
			}(),
			want: ReasonDanglingLatestPointer,
		},
		{
			name: "E2 record filed under a date that contradicts its own ID",
			evidence: func() *evidence {
				b := newEvidence().record(oldBackupID, types.BackupStatusCompleted)
				b.ev.records[0].StartTime = time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
				return b.build()
			}(),
			want: ReasonDatePathMismatch,
		},
		{
			name: "E4 record naming a different instance",
			evidence: func() *evidence {
				b := newEvidence().record(oldBackupID, types.BackupStatusCompleted)
				b.ev.records[0].CamundaInstanceID = "staging"
				return b.build()
			}(),
			want: ReasonCrossInstanceRecord,
		},
		{
			name: "F2 snapshot exists under its old prefixed name",
			evidence: func() *evidence {
				b := newEvidence().
					record(oldBackupID, types.BackupStatusCompleted, types.ComponentElasticsearch).
					componentInfo(types.ComponentElasticsearch, models.ComponentBackupInfo{
						Enabled: true, Status: types.ComponentStatusCompleted,
						SnapshotName: "legacy-" + oldBackupID,
					}).
					snapshot("legacy-"+oldBackupID, elasticsearch.SnapshotStateSuccess)
				b.ev.namePrefix = "prod"
				return b.build()
			}(),
			want: ReasonNamePrefixDrift,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classify(tt.evidence, DefaultOptions())
			if !hasReason(got, tt.want) {
				t.Errorf("expected %s; got %v", tt.want, reasons(got))
			}
		})
	}
}

// Two declared codes have no detection path yet. This test pins that fact so the
// gap is visible rather than being mistaken for coverage: both need information
// the current per-instance sweep does not have.
//
//   - E3 needs ListAllBackups to surface objects it failed to parse; today the
//     storage layer skips them silently.
//   - E5 needs to enumerate bucket prefixes and compare them against the
//     configured instances, which a sweep scoped to one instance cannot do.
func TestUnemittedCodesAreKnown(t *testing.T) {
	unemitted := []ReasonCode{ReasonUnparseableRecord, ReasonAbandonedInstancePrefix}

	for _, code := range unemitted {
		if _, ok := Describe(code); !ok {
			t.Errorf("%s is declared but has no catalogue entry", code)
		}
	}
}
