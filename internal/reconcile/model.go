package reconcile

import (
	"sort"
	"time"
)

// Source names an evidence source the reconciler enumerates. They double as the
// keys of Report.SourcesChecked and as the values in a finding's PresentIn,
// MissingIn and Unverified lists.
const (
	SourceControllerS3  = "controller_s3"
	SourceZeebe         = "zeebe"
	SourceOperate       = "operate"
	SourceTasklist      = "tasklist"
	SourceOptimize      = "optimize"
	SourceElasticsearch = "elasticsearch"
	SourceLogs          = "logs"
)

// SourceStatus records whether a source could be enumerated at all. This is the
// backbone of the "absence of evidence is not evidence of absence" guard: when
// Reachable is false, no conclusion may be drawn from data missing there.
type SourceStatus struct {
	Name      string `json:"name"`
	Reachable bool   `json:"reachable"`
	Error     string `json:"error,omitempty"`
	Count     int    `json:"count"`
	Skipped   bool   `json:"skipped,omitempty"` // not configured for this instance
}

// Finding is a single detected inconsistency.
type Finding struct {
	BackupID   string     `json:"backup_id,omitempty"` // empty for instance/repository scope
	Scope      Scope      `json:"scope"`
	Reason     ReasonCode `json:"reason"`
	Severity   Severity   `json:"severity"`
	PresentIn  []string   `json:"present_in,omitempty"`
	MissingIn  []string   `json:"missing_in,omitempty"`
	Unverified []string   `json:"unverified,omitempty"`
	Detail     string     `json:"detail,omitempty"`
	DetectedAt time.Time  `json:"detected_at"`
}

// BackupIssue rolls every finding for one backup ID into a single row. The UI
// renders one of these per table row; Reasons holds the full detail behind it.
type BackupIssue struct {
	BackupID string `json:"backup_id"`
	// Tracked says whether the controller has a history record for this backup.
	// An untracked backup is an orphan in the strict sense: it exists in some
	// component or repository with nothing describing it. A tracked one is a
	// known backup that has developed a problem. The UI renders them
	// differently, so the distinction has to survive into the report.
	Tracked       bool         `json:"tracked"`
	BackupTime    *time.Time   `json:"backup_time,omitempty"` // parsed from the ID
	PrimaryReason ReasonCode   `json:"primary_reason"`
	Severity      Severity     `json:"severity"`
	Reasons       []Finding    `json:"reasons"`
	Implied       []ReasonCode `json:"implied,omitempty"`
	PresentIn     []string     `json:"present_in,omitempty"`
	MissingIn     []string     `json:"missing_in,omitempty"`
	Unverified    []string     `json:"unverified,omitempty"`
}

// Report is one reconciliation sweep over one Camunda instance.
type Report struct {
	CamundaInstanceID string    `json:"camunda_instance_id"`
	StartedAt         time.Time `json:"started_at"`
	FinishedAt        time.Time `json:"finished_at"`
	// SnapshotRepository and ComponentEndpoints let a client turn a finding's
	// generic remediation into a command that can actually be run, instead of
	// showing the operator a placeholder to fill in by hand.
	SnapshotRepository string                  `json:"snapshot_repository,omitempty"`
	ComponentEndpoints map[string]string       `json:"component_endpoints,omitempty"`
	SourcesChecked     map[string]SourceStatus `json:"sources_checked"`
	InstanceFindings   []Finding               `json:"instance_findings"`
	BackupIssues       []BackupIssue           `json:"backup_issues"`
	RepositoryFindings []Finding               `json:"repository_findings"`
}

// AllSourcesReachable reports whether every source the sweep needed could be
// enumerated. When false the report is partial, and the UI must say so rather
// than presenting an empty result as a clean bill of health.
func (r *Report) AllSourcesReachable() bool {
	for _, s := range r.SourcesChecked {
		if !s.Reachable && !s.Skipped {
			return false
		}
	}
	return true
}

// UnreachableSources lists the sources that could not be enumerated.
func (r *Report) UnreachableSources() []string {
	var out []string
	for name, s := range r.SourcesChecked {
		if !s.Reachable && !s.Skipped {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// TotalFindings counts every finding across all three scopes.
func (r *Report) TotalFindings() int {
	n := len(r.InstanceFindings) + len(r.RepositoryFindings)
	for _, issue := range r.BackupIssues {
		n += len(issue.Reasons)
	}
	return n
}

// reasonPrecedence orders backup-scoped codes from most to least specific. It
// breaks ties when several findings share the top severity, so the row's primary
// reason names the most precise diagnosis rather than an arbitrary one.
var reasonPrecedence = []ReasonCode{
	ReasonSplitRestorePair,
	ReasonPartialSet,
	ReasonStateDivergenceComponent,
	ReasonStateDivergenceES,
	ReasonDanglingAppESSnapshot,
	ReasonDanglingESSnapshot,
	ReasonDanglingComponentBackup,
	ReasonRepoRebound,
	ReasonNamePrefixDrift,
	ReasonRetentionResidue,
	ReasonOrphanedInFailedSet,
	ReasonUntrackedComponentBackup,
	ReasonUntrackedESSnapshot,
	ReasonUntrackedAppESSnapshot,
	ReasonStaleRunningRecord,
	ReasonStaleInProgressComponent,
	ReasonStaleInProgressES,
	ReasonDuplicateRecord,
	ReasonCrossInstanceRecord,
	ReasonDatePathMismatch,
	ReasonUnparseableRecord,
	ReasonMissingLogFile,
}

var precedenceRank = func() map[ReasonCode]int {
	m := make(map[ReasonCode]int, len(reasonPrecedence))
	for i, code := range reasonPrecedence {
		m[code] = i
	}
	return m
}()

// rankOf returns a code's precedence position. Uncatalogued codes sort last.
func rankOf(code ReasonCode) int {
	if r, ok := precedenceRank[code]; ok {
		return r
	}
	return len(reasonPrecedence)
}

// sortFindings orders findings worst-first: severity descending, then
// precedence, then backup ID for a stable result.
func sortFindings(f []Finding) {
	sort.SliceStable(f, func(i, j int) bool {
		if f[i].Severity.Rank() != f[j].Severity.Rank() {
			return f[i].Severity.Rank() > f[j].Severity.Rank()
		}
		if rankOf(f[i].Reason) != rankOf(f[j].Reason) {
			return rankOf(f[i].Reason) < rankOf(f[j].Reason)
		}
		return f[i].BackupID > f[j].BackupID
	})
}

// sortReasonInfos orders catalogue entries by code for a stable API response.
func sortReasonInfos(r []ReasonInfo) {
	sort.Slice(r, func(i, j int) bool { return r[i].Code < r[j].Code })
}

// dedupeStrings returns a sorted copy with duplicates and blanks removed.
func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil
	}
	sort.Strings(out)
	return out
}
