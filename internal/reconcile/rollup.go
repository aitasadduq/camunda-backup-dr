package reconcile

import (
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
)

// Rollup turns the classifier's flat finding list into a report the UI can show
// one row at a time. It is pure: no I/O, no clock, no ordering assumptions about
// its input.
//
// Two things happen here. Implication rules drop findings that a more specific
// finding already explains, so one underlying cause produces one row instead of
// several. Then backup-scoped findings are grouped by backup ID, and each group
// gets a primary reason.
//
// Suppressed codes are not discarded: they move to BackupIssue.Implied so the
// evidence stays visible when a row is expanded.
func Rollup(instanceID string, findings []Finding, trackedIDs map[string]bool, sources map[string]SourceStatus, started, finished time.Time) *Report {
	report := &Report{
		CamundaInstanceID: instanceID,
		StartedAt:         started,
		FinishedAt:        finished,
		SourcesChecked:    sources,
	}

	kept, implied := applyImplications(findings)

	byBackup := make(map[string][]Finding)
	for _, f := range kept {
		switch f.Scope {
		case ScopeInstance:
			report.InstanceFindings = append(report.InstanceFindings, f)
		case ScopeRepository:
			report.RepositoryFindings = append(report.RepositoryFindings, f)
		default:
			byBackup[f.BackupID] = append(byBackup[f.BackupID], f)
		}
	}

	for backupID, group := range byBackup {
		report.BackupIssues = append(report.BackupIssues,
			buildIssue(backupID, group, implied[backupID], trackedIDs[backupID]))
	}

	sortFindings(report.InstanceFindings)
	sortFindings(report.RepositoryFindings)
	sortIssues(report.BackupIssues)
	return report
}

// buildIssue collapses one backup's findings into a single row.
func buildIssue(backupID string, group []Finding, implied []ReasonCode, tracked bool) BackupIssue {
	sortFindings(group)

	issue := BackupIssue{
		BackupID:      backupID,
		Tracked:       tracked,
		PrimaryReason: group[0].Reason,
		Severity:      group[0].Severity,
		Reasons:       group,
		Implied:       dedupeReasons(implied),
	}

	// The backup ID is a timestamp, so every row can show a real date even when
	// no history record survives to supply one.
	if ts, err := camunda.ParseBackupIDTimestamp(backupID); err == nil {
		issue.BackupTime = &ts
	}

	var present, missing, unverified []string
	for _, f := range group {
		present = append(present, f.PresentIn...)
		missing = append(missing, f.MissingIn...)
		unverified = append(unverified, f.Unverified...)
	}
	issue.PresentIn = dedupeStrings(present)
	issue.MissingIn = dedupeStrings(missing)
	issue.Unverified = dedupeStrings(unverified)
	return issue
}

// applyImplications drops findings that a more specific co-occurring finding
// already accounts for. It returns the survivors plus, per backup ID, the codes
// that were suppressed.
//
// Each rule exists because reporting both findings would either double-count one
// cause or actively mislead. The F1/F2 rule is the sharpest example: reporting
// "snapshot missing" when the repository was merely re-pointed would send an
// operator hunting for data loss that never happened.
func applyImplications(findings []Finding) ([]Finding, map[string][]ReasonCode) {
	implied := make(map[string][]ReasonCode)

	// Instance-scoped causes suppress their per-backup symptoms everywhere.
	zeebeStoreRebound := false
	abandonedPrefix := false
	for _, f := range findings {
		switch f.Reason {
		case ReasonZeebeStoreRebound:
			zeebeStoreRebound = true
		case ReasonAbandonedInstancePrefix:
			abandonedPrefix = true
		}
	}

	present := make(map[string]map[ReasonCode]bool)
	for _, f := range findings {
		if f.Scope != ScopeBackup {
			continue
		}
		if present[f.BackupID] == nil {
			present[f.BackupID] = make(map[ReasonCode]bool)
		}
		present[f.BackupID][f.Reason] = true
	}

	kept := make([]Finding, 0, len(findings))
	for _, f := range findings {
		if f.Scope != ScopeBackup {
			kept = append(kept, f)
			continue
		}

		at := present[f.BackupID]
		suppress := false

		switch f.Reason {
		case ReasonDanglingComponentBackup:
			// A split pair or a partial set already names this absence, and a
			// re-pointed Zeebe store explains it for every backup at once.
			if at[ReasonSplitRestorePair] || at[ReasonPartialSet] {
				suppress = true
			}
			if zeebeStoreRebound && hasSource(f, SourceZeebe) {
				suppress = true
			}
			// Present-but-wrong and absent are mutually exclusive for a component.
			if at[ReasonStateDivergenceComponent] && sameComponent(f, findings) {
				suppress = true
			}
		case ReasonDanglingESSnapshot:
			if at[ReasonSplitRestorePair] {
				suppress = true
			}
			// The snapshot is unreachable by name, not proven absent.
			if at[ReasonRepoRebound] || at[ReasonNamePrefixDrift] {
				suppress = true
			}
		case ReasonPartialSet:
			// The split pair is the specific case of a partial set.
			if at[ReasonSplitRestorePair] {
				suppress = true
			}
		}

		// An abandoned prefix means nothing under it is managed at all; the
		// per-backup detail is noise next to that.
		if abandonedPrefix {
			suppress = true
		}

		if suppress {
			implied[f.BackupID] = append(implied[f.BackupID], f.Reason)
			continue
		}
		kept = append(kept, f)
	}

	return kept, implied
}

// hasSource reports whether a finding names the given source on either side.
func hasSource(f Finding, source string) bool {
	for _, s := range f.MissingIn {
		if s == source {
			return true
		}
	}
	for _, s := range f.PresentIn {
		if s == source {
			return true
		}
	}
	return false
}

// sameComponent reports whether a divergence finding for the same backup covers
// the same component as f, which is what makes the two mutually exclusive.
func sameComponent(f Finding, all []Finding) bool {
	for _, other := range all {
		if other.BackupID != f.BackupID || other.Reason != ReasonStateDivergenceComponent {
			continue
		}
		for _, s := range f.MissingIn {
			if hasSource(other, s) {
				return true
			}
		}
	}
	return false
}

func dedupeReasons(in []ReasonCode) []ReasonCode {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[ReasonCode]struct{}, len(in))
	out := make([]ReasonCode, 0, len(in))
	for _, c := range in {
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	sortReasonCodes(out)
	return out
}

func sortReasonCodes(c []ReasonCode) {
	for i := 1; i < len(c); i++ {
		for j := i; j > 0 && rankOf(c[j]) < rankOf(c[j-1]); j-- {
			c[j], c[j-1] = c[j-1], c[j]
		}
	}
}

// sortIssues orders rows worst-first, newest-first within a severity.
func sortIssues(issues []BackupIssue) {
	for i := 1; i < len(issues); i++ {
		for j := i; j > 0 && issueLess(issues[j], issues[j-1]); j-- {
			issues[j], issues[j-1] = issues[j-1], issues[j]
		}
	}
}

func issueLess(a, b BackupIssue) bool {
	if a.Severity.Rank() != b.Severity.Rank() {
		return a.Severity.Rank() > b.Severity.Rank()
	}
	if rankOf(a.PrimaryReason) != rankOf(b.PrimaryReason) {
		return rankOf(a.PrimaryReason) < rankOf(b.PrimaryReason)
	}
	return a.BackupID > b.BackupID
}
