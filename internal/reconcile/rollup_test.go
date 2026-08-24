package reconcile

import (
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func f(backupID string, reason ReasonCode, missing ...string) Finding {
	return Finding{
		BackupID:  backupID,
		Scope:     scopeOf(reason),
		Reason:    reason,
		Severity:  severityOf(reason),
		MissingIn: missing,
	}
}

func findIssue(t *testing.T, report *Report, backupID string) BackupIssue {
	t.Helper()
	for _, issue := range report.BackupIssues {
		if issue.BackupID == backupID {
			return issue
		}
	}
	t.Fatalf("no issue for backup %s", backupID)
	return BackupIssue{}
}

func containsReason(codes []ReasonCode, want ReasonCode) bool {
	for _, c := range codes {
		if c == want {
			return true
		}
	}
	return false
}

func TestRollupImplications(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name         string
		findings     []Finding
		wantPrimary  ReasonCode
		wantImplied  []ReasonCode
		wantReasons  int
		wantIssueLen int
	}{
		{
			name: "D2 absorbs the B1, B2 and D1 it was derived from",
			findings: []Finding{
				f(oldBackupID, ReasonDanglingComponentBackup, types.ComponentZeebe),
				f(oldBackupID, ReasonDanglingESSnapshot, SourceElasticsearch),
				f(oldBackupID, ReasonPartialSet),
				f(oldBackupID, ReasonSplitRestorePair),
			},
			wantPrimary: ReasonSplitRestorePair,
			wantImplied: []ReasonCode{
				ReasonDanglingComponentBackup, ReasonDanglingESSnapshot, ReasonPartialSet,
			},
			wantReasons:  1,
			wantIssueLen: 1,
		},
		{
			name: "D1 absorbs the individual dangling components",
			findings: []Finding{
				f(oldBackupID, ReasonDanglingComponentBackup, types.ComponentOperate),
				f(oldBackupID, ReasonPartialSet),
			},
			wantPrimary:  ReasonPartialSet,
			wantImplied:  []ReasonCode{ReasonDanglingComponentBackup},
			wantReasons:  1,
			wantIssueLen: 1,
		},
		{
			name: "F1 suppresses B2 so a re-pointed repo is not reported as data loss",
			findings: []Finding{
				f(oldBackupID, ReasonRepoRebound, SourceElasticsearch),
				f(oldBackupID, ReasonDanglingESSnapshot, SourceElasticsearch),
			},
			wantPrimary:  ReasonRepoRebound,
			wantImplied:  []ReasonCode{ReasonDanglingESSnapshot},
			wantReasons:  1,
			wantIssueLen: 1,
		},
		{
			name: "unrelated findings on one backup both survive",
			findings: []Finding{
				f(oldBackupID, ReasonDuplicateRecord),
				f(oldBackupID, ReasonMissingLogFile),
			},
			wantPrimary:  ReasonDuplicateRecord,
			wantReasons:  2,
			wantIssueLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := Rollup("prod", tt.findings, nil, nil, now, now)

			if len(report.BackupIssues) != tt.wantIssueLen {
				t.Fatalf("got %d issues, want %d", len(report.BackupIssues), tt.wantIssueLen)
			}
			issue := findIssue(t, report, oldBackupID)

			if issue.PrimaryReason != tt.wantPrimary {
				t.Errorf("primary reason = %s, want %s", issue.PrimaryReason, tt.wantPrimary)
			}
			if len(issue.Reasons) != tt.wantReasons {
				t.Errorf("got %d surviving reasons, want %d: %v", len(issue.Reasons), tt.wantReasons, issue.Reasons)
			}
			for _, want := range tt.wantImplied {
				if !containsReason(issue.Implied, want) {
					t.Errorf("expected %s in Implied, got %v", want, issue.Implied)
				}
			}
		})
	}
}

// F3 is the reason a re-pointed Zeebe store reads as one problem rather than one
// per backup, which is the difference between an actionable report and a wall of
// noise.
func TestRollupZeebeReboundCollapsesPerBackupFindings(t *testing.T) {
	now := time.Now()
	ids := []string{"20260320080000", "20260320090000", "20260320100000"}

	findings := []Finding{{
		Scope:     ScopeInstance,
		Reason:    ReasonZeebeStoreRebound,
		Severity:  severityOf(ReasonZeebeStoreRebound),
		MissingIn: []string{types.ComponentZeebe},
	}}
	for _, id := range ids {
		findings = append(findings, f(id, ReasonDanglingComponentBackup, types.ComponentZeebe))
	}

	report := Rollup("prod", findings, nil, nil, now, now)

	if len(report.InstanceFindings) != 1 {
		t.Fatalf("want 1 instance finding, got %d", len(report.InstanceFindings))
	}
	if len(report.BackupIssues) != 0 {
		t.Errorf("per-backup rows should be collapsed into the instance finding, got %d rows", len(report.BackupIssues))
	}
}

func TestRollupSeparatesScopes(t *testing.T) {
	now := time.Now()
	findings := []Finding{
		{Scope: ScopeInstance, Reason: ReasonDanglingLatestPointer, Severity: SeverityWarn},
		{Scope: ScopeRepository, Reason: ReasonForeignSnapshot, Severity: SeverityInfo},
		f(oldBackupID, ReasonDuplicateRecord),
	}

	report := Rollup("prod", findings, nil, nil, now, now)

	if len(report.InstanceFindings) != 1 || len(report.RepositoryFindings) != 1 || len(report.BackupIssues) != 1 {
		t.Fatalf("scopes not separated: instance=%d repository=%d backups=%d",
			len(report.InstanceFindings), len(report.RepositoryFindings), len(report.BackupIssues))
	}
}

func TestRollupOrdersWorstFirst(t *testing.T) {
	now := time.Now()
	report := Rollup("prod", []Finding{
		f("20260320080000", ReasonMissingLogFile),
		f("20260320090000", ReasonSplitRestorePair),
		f("20260320100000", ReasonDanglingESSnapshot),
	}, nil, nil, now, now)

	got := make([]Severity, 0, len(report.BackupIssues))
	for _, issue := range report.BackupIssues {
		got = append(got, issue.Severity)
	}
	want := []Severity{SeverityCritical, SeverityBlocksRestore, SeverityInfo}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("position %d: severity %s, want %s (full order: %v)", i, got[i], want[i], got)
		}
	}
}

// Every row shows a real date even when no history record survives, because the
// backup ID is itself a timestamp.
func TestRollupParsesBackupTimeFromID(t *testing.T) {
	now := time.Now()
	report := Rollup("prod", []Finding{f(oldBackupID, ReasonUntrackedComponentBackup)}, nil, nil, now, now)

	issue := findIssue(t, report, oldBackupID)
	if issue.BackupTime == nil {
		t.Fatal("expected backup time parsed from the ID")
	}
	if got := issue.BackupTime.UTC().Format("2006-01-02 15:04:05"); got != "2026-03-20 08:00:00" {
		t.Errorf("backup time = %s", got)
	}
}

func TestReportSourceHealth(t *testing.T) {
	report := &Report{SourcesChecked: map[string]SourceStatus{
		SourceControllerS3:     {Name: SourceControllerS3, Reachable: true},
		types.ComponentOperate: {Name: types.ComponentOperate, Reachable: false, Error: "refused"},
		types.ComponentZeebe:   {Name: types.ComponentZeebe, Skipped: true},
	}}

	if report.AllSourcesReachable() {
		t.Error("a source with an error must make the report partial")
	}
	unreachable := report.UnreachableSources()
	if len(unreachable) != 1 || unreachable[0] != types.ComponentOperate {
		t.Errorf("unreachable = %v, want [operate]; a skipped source is not a failure", unreachable)
	}
}

// A backup with a history record is a known backup that developed a problem; one
// without is an orphan. The UI renders them as different things, so the flag has
// to be right.
func TestRollupMarksTrackedBackups(t *testing.T) {
	now := time.Now()
	tracked := map[string]bool{"20260320080000": true}

	report := Rollup("prod", []Finding{
		f("20260320080000", ReasonDanglingESSnapshot),
		f("20260320090000", ReasonUntrackedComponentBackup),
	}, tracked, nil, now, now)

	for _, issue := range report.BackupIssues {
		want := issue.BackupID == "20260320080000"
		if issue.Tracked != want {
			t.Errorf("backup %s: Tracked = %v, want %v", issue.BackupID, issue.Tracked, want)
		}
	}
}

// The UI builds DELETE commands from these, so they must survive rollup.
func TestRollupCollectsSnapshotNames(t *testing.T) {
	now := time.Now()
	a := f(oldBackupID, ReasonUntrackedAppESSnapshot)
	a.SnapshotName = "camunda_operate_" + oldBackupID + "_8.6.0_part_1_of_6"
	b := f(oldBackupID, ReasonUntrackedAppESSnapshot)
	b.SnapshotName = "camunda_operate_" + oldBackupID + "_8.6.0_part_2_of_6"

	issue := findIssue(t, Rollup("prod", []Finding{a, b}, nil, nil, now, now), oldBackupID)
	if len(issue.SnapshotNames) != 2 {
		t.Fatalf("got %d snapshot names, want 2: %v", len(issue.SnapshotNames), issue.SnapshotNames)
	}
}
