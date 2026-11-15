package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
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

type fakeCandidateBackupLister struct {
	listErr    map[string]error
	byEndpoint map[string][]camunda.ComponentBackupRecord
	byCand     map[string][]camunda.ComponentBackupRecord
}

func (f *fakeCandidateBackupLister) ListBackups(_ context.Context, endpoint string) ([]camunda.ComponentBackupRecord, error) {
	if err := f.listErr[endpoint]; err != nil {
		return nil, err
	}
	return f.byEndpoint[endpoint], nil
}

func (f *fakeCandidateBackupLister) ListBackupsByCandidates(_ context.Context, endpoint string, _ []string) ([]camunda.ComponentBackupRecord, error) {
	if recs, ok := f.byCand[endpoint]; ok {
		return recs, nil
	}
	return nil, errors.New("candidate lookup failed")
}

func TestClassify_CamundaZeebeRecordsBackupSnapshotsBecomeOrphanedIssues(t *testing.T) {
	ev := newEvidence().
		snapshot("camunda_zeebe_records_backup_20260111154511", elasticsearch.SnapshotStateSuccess).
		snapshot("camunda_zeebe_records_backup_", elasticsearch.SnapshotStateSuccess).
		ev

	findings := classify(ev, DefaultOptions())
	report := Rollup("hajj", findings, map[string]bool{}, ev.sources, observedSnapshots(ev), testNow, testNow)

	if len(report.BackupIssues) != 1 {
		t.Fatalf("expected 1 orphaned backup issue, got %d: %+v", len(report.BackupIssues), report.BackupIssues)
	}
	issue := report.BackupIssues[0]
	if issue.BackupID != "20260111154511" || issue.Tracked {
		t.Errorf("unexpected issue: %+v", issue)
	}
	if len(issue.AllSnapshotNames) != 1 || issue.AllSnapshotNames[0] != "camunda_zeebe_records_backup_20260111154511" {
		t.Errorf("expected AllSnapshotNames=[camunda_zeebe_records_backup_20260111154511], got %v", issue.AllSnapshotNames)
	}
	if len(report.RepositoryFindings) != 1 || report.RepositoryFindings[0].Reason != ReasonForeignSnapshot {
		t.Errorf("expected malformed camunda_zeebe_records_backup_ to remain foreign, got %+v", report.RepositoryFindings)
	}
}

func TestCandidateBackupIDs_OrdersTrackedFirstThenNewestUntracked(t *testing.T) {
	ev := newEvidence().
		record("20260319080000", types.BackupStatusCompleted, types.ComponentZeebe).
		snapshot("camunda_zeebe_records_backup_20260318080000", elasticsearch.SnapshotStateSuccess).
		holds(types.ComponentOperate, "20260320080000", camunda.BackupStateCompleted).
		ev

	got := candidateBackupIDs(ev)
	want := []string{"20260319080000", "20260320080000", "20260318080000"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("index %d: got %q, want %q", i, got[i], want[i])
		}
	}
	_ = config.Config{}
}
