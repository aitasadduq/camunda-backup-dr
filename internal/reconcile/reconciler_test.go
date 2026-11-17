package reconcile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/elasticsearch"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Reports are persisted to S3 and served over the API, so an endpoint carrying
// credentials must not survive into either.
func TestStripUserinfo(t *testing.T) {
	cases := map[string]string{
		"https://user:pass@operate:8080/actuator/backups": "https://operate:8080/actuator/backups",
		"https://user@operate:8080/actuator/backups":      "https://operate:8080/actuator/backups",
		"http://operate:8080/actuator/backups":            "http://operate:8080/actuator/backups",
		"":                                                "",
	}
	for in, want := range cases {
		if got := stripUserinfo(in); got != want {
			t.Errorf("stripUserinfo(%q) = %q, want %q", in, got, want)
		}
	}
}

// A sweep fans out to every component and holds the whole record set in memory,
// so a second concurrent sweep must be refused rather than doubling the load.
func TestReconcileRefusesConcurrentSweep(t *testing.T) {
	r := &Reconciler{}
	if !r.sweepRunning.CompareAndSwap(false, true) {
		t.Fatal("expected the guard to start unclaimed")
	}
	defer r.sweepRunning.Store(false)

	_, err := r.Reconcile(context.Background(), &models.CamundaInstance{ID: "prod"})
	if !errors.Is(err, ErrSweepInProgress) {
		t.Errorf("err = %v, want ErrSweepInProgress", err)
	}
}

func TestIsBackupRunningDefaultsToFalse(t *testing.T) {
	r := &Reconciler{}
	if r.isBackupRunning() {
		t.Error("with no check registered the sweep must assume no backup is running")
	}
	r.SetBackupRunningFunc(func() bool { return true })
	if !r.isBackupRunning() {
		t.Error("registered check was not consulted")
	}
}

// The cron-job shape seen in production: an Operate and a Zeebe backup plus a
// camunda_zeebe_records_backup_<id> snapshot under one ID, with no record.
func cronJobBackup() *evidenceBuilder {
	return newEvidence().
		holds(types.ComponentOperate, "20260320080000", camunda.BackupStateCompleted, "camunda_operate_20260320080000_8.6.0_part_1_of_6").
		holds(types.ComponentZeebe, "20260320080000", camunda.BackupStateCompleted).
		snapshot("camunda_operate_20260320080000_8.6.0_part_1_of_6", elasticsearch.SnapshotStateSuccess).
		snapshot("camunda_zeebe_records_backup_20260320080000", elasticsearch.SnapshotStateSuccess)
}

// Undeclared, the records snapshot is another tool's: reported once at
// repository level, and not among what deleting the backup would remove.
func TestExternalSnapshotUndeclaredStaysForeign(t *testing.T) {
	ev := cronJobBackup().ev
	report := Rollup("prod", classify(ev, DefaultOptions()), map[string]bool{}, ev.sources, observedSnapshots(ev), testNow, testNow)

	if len(report.BackupIssues) != 1 {
		t.Fatalf("expected one untracked backup, got %+v", report.BackupIssues)
	}
	for _, name := range report.BackupIssues[0].AllSnapshotNames {
		if name == "camunda_zeebe_records_backup_20260320080000" {
			t.Error("an undeclared external snapshot must not be offered for deletion")
		}
	}
	if len(report.RepositoryFindings) != 1 || report.RepositoryFindings[0].Reason != ReasonForeignSnapshot {
		t.Errorf("expected one A4 repository finding, got %+v", report.RepositoryFindings)
	}
}

// Declared, it joins its backup's row so deleting the backup removes it too,
// and the row says it came from that job rather than calling it residue.
func TestExternalSnapshotDeclaredJoinsItsBackup(t *testing.T) {
	ev := cronJobBackup().ev
	ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}
	report := Rollup("prod", classify(ev, DefaultOptions()), map[string]bool{}, ev.sources, observedSnapshots(ev), testNow, testNow)

	if len(report.BackupIssues) != 1 {
		t.Fatalf("expected one untracked backup, got %+v", report.BackupIssues)
	}
	issue := report.BackupIssues[0]
	if issue.Tracked || issue.PrimaryReason != ReasonExternalBackup {
		t.Errorf("got tracked=%v primary=%s, want an untracked %s", issue.Tracked, issue.PrimaryReason, ReasonExternalBackup)
	}
	want := map[string]bool{
		"camunda_operate_20260320080000_8.6.0_part_1_of_6": true,
		"camunda_zeebe_records_backup_20260320080000":      true,
	}
	if len(issue.AllSnapshotNames) != len(want) {
		t.Fatalf("AllSnapshotNames = %v, want %v", issue.AllSnapshotNames, want)
	}
	for _, name := range issue.AllSnapshotNames {
		if !want[name] {
			t.Errorf("unexpected snapshot %q in AllSnapshotNames", name)
		}
	}
	for _, source := range []string{types.ComponentOperate, types.ComponentZeebe, SourceElasticsearch} {
		if !containsString(issue.PresentIn, source) {
			t.Errorf("PresentIn = %v, missing %s", issue.PresentIn, source)
		}
	}
	for _, f := range report.RepositoryFindings {
		if f.Reason == ReasonForeignSnapshot {
			t.Errorf("a declared snapshot must not also be reported as foreign: %+v", f)
		}
	}
}

// A backup the cron job started minutes ago may still be writing artifacts.
func TestExternalSnapshotWithinGraceIsNotReported(t *testing.T) {
	ev := newEvidence().snapshot("camunda_zeebe_records_backup_"+testNow.Add(-time.Minute).Format("20060102150405"), elasticsearch.SnapshotStateSuccess).ev
	ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}
	if issues := classify(ev, DefaultOptions()); len(issues) != 0 {
		t.Errorf("expected nothing within the grace period, got %+v", issues)
	}
}

// An untracked backup that some source still reports as being written is a
// live backup, not an orphan. Reporting it would offer it for deletion, and
// Elasticsearch aborts an in-progress snapshot that is deleted. The grace
// period alone is not enough: this one is hours old.
func TestUntrackedBackupStillBeingWrittenIsNotReported(t *testing.T) {
	tests := []struct {
		name string
		ev   *evidence
	}{
		{
			name: "external records snapshot in progress",
			ev: newEvidence().
				holds(types.ComponentOperate, oldBackupID, camunda.BackupStateCompleted).
				snapshot("camunda_zeebe_records_backup_"+oldBackupID, elasticsearch.SnapshotStateInProgress).ev,
		},
		{
			name: "component backup in progress",
			ev: newEvidence().
				holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateInProgress).
				snapshot("camunda_zeebe_records_backup_"+oldBackupID, elasticsearch.SnapshotStateSuccess).ev,
		},
		{
			name: "component snapshot in progress",
			ev: newEvidence().
				snapshot("camunda_operate_"+oldBackupID+"_8.6.0_part_1_of_6", elasticsearch.SnapshotStateInProgress).
				snapshot("camunda_zeebe_records_backup_"+oldBackupID, elasticsearch.SnapshotStateSuccess).ev,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}
			for _, f := range classify(tt.ev, DefaultOptions()) {
				if f.BackupID == oldBackupID {
					t.Errorf("a backup still being written was reported as %s", f.Reason)
				}
			}
			if !tt.ev.untrackedInFlight()[oldBackupID] {
				t.Error("expected the backup to be recognised as in flight")
			}
		})
	}
}

// Once it finishes, the same backup is reported as usual.
func TestUntrackedBackupIsReportedOnceFinished(t *testing.T) {
	ev := newEvidence().
		holds(types.ComponentZeebe, oldBackupID, camunda.BackupStateCompleted).
		snapshot("camunda_zeebe_records_backup_"+oldBackupID, elasticsearch.SnapshotStateSuccess).ev
	ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}

	var reported bool
	for _, f := range classify(ev, DefaultOptions()) {
		reported = reported || (f.BackupID == oldBackupID && f.Reason == ReasonExternalBackup)
	}
	if !reported {
		t.Error("a finished external backup must be reported")
	}
}

// The index, the deletable snapshot set and the log all read one
// classification, done once per sweep.
func TestSnapshotsAreClassifiedOnce(t *testing.T) {
	ev := newEvidence().snapshot("camunda_zeebe_records_backup_"+oldBackupID, elasticsearch.SnapshotStateSuccess).ev
	ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}
	ev.classifySnapshots()
	if ev.snapshots[0].Owner != elasticsearch.OwnerExternal || ev.snapshots[0].BackupID != oldBackupID {
		t.Fatalf("got %+v", ev.snapshots[0])
	}

	// A later change to the naming must not reclassify the same sweep.
	ev.externalPrefixes = nil
	ev.classifySnapshots()
	if got := observedSnapshots(ev)[oldBackupID]; len(got) != 1 {
		t.Errorf("observedSnapshots disagrees with the stored classification: %v", got)
	}
}
