package reconcile

import (
	"strings"
	"testing"
)

// allReasonCodes is every code the classifier can emit. Keeping this list
// explicit is what lets the tests below catch a code that ships without a
// description or without a place in the precedence order.
var allReasonCodes = []ReasonCode{
	ReasonUntrackedComponentBackup, ReasonUntrackedESSnapshot, ReasonUntrackedAppESSnapshot,
	ReasonForeignSnapshot, ReasonUntrackedLogFile,
	ReasonDanglingComponentBackup, ReasonDanglingESSnapshot, ReasonDanglingAppESSnapshot,
	ReasonDanglingLatestPointer, ReasonMissingLogFile,
	ReasonStateDivergenceComponent, ReasonStateDivergenceES,
	ReasonStaleInProgressComponent, ReasonStaleInProgressES, ReasonStaleRunningRecord,
	ReasonPartialSet, ReasonSplitRestorePair, ReasonOrphanedInFailedSet, ReasonRetentionResidue,
	ReasonDuplicateRecord, ReasonDatePathMismatch, ReasonUnparseableRecord,
	ReasonCrossInstanceRecord, ReasonAbandonedInstancePrefix,
	ReasonRepoRebound, ReasonNamePrefixDrift, ReasonZeebeStoreRebound, ReasonExporterLeftPaused,
}

// A code with no description would render in the UI as a bare identifier, which
// is exactly the outcome the catalogue exists to prevent.
func TestReasonCatalogIsComplete(t *testing.T) {
	for _, code := range allReasonCodes {
		info, ok := Describe(code)
		if !ok {
			t.Errorf("%s has no catalogue entry", code)
			continue
		}
		for field, value := range map[string]string{
			"Label":       info.Label,
			"Explanation": info.Explanation,
			"Impact":      info.Impact,
			"Remediation": info.Remediation,
		} {
			if strings.TrimSpace(value) == "" {
				t.Errorf("%s: %s is empty", code, field)
			}
		}
		if info.Code != code {
			t.Errorf("%s: entry reports code %s", code, info.Code)
		}
		if info.Severity == "" || info.Scope == "" {
			t.Errorf("%s: severity or scope is unset", code)
		}
	}
}

func TestCatalogCoversEveryCode(t *testing.T) {
	if got, want := len(Catalog()), len(allReasonCodes); got != want {
		t.Errorf("catalogue has %d entries, %d codes are declared", got, want)
	}
}

// Backup-scoped codes are the ones that compete to be a row's primary reason, so
// each needs a defined position rather than falling into the shared bucket at
// the end of the order.
func TestBackupScopedCodesHavePrecedence(t *testing.T) {
	for _, code := range allReasonCodes {
		if scopeOf(code) != ScopeBackup {
			continue
		}
		if rankOf(code) >= len(reasonPrecedence) {
			t.Errorf("%s is backup-scoped but has no precedence entry", code)
		}
	}
}

func TestSeverityRankOrdering(t *testing.T) {
	ordered := []Severity{SeverityInfo, SeverityWarn, SeverityBlocksRestore, SeverityCritical}
	for i := 1; i < len(ordered); i++ {
		if ordered[i].Rank() <= ordered[i-1].Rank() {
			t.Errorf("%s does not outrank %s", ordered[i], ordered[i-1])
		}
	}
}

func TestCatalogIsSortedByCode(t *testing.T) {
	catalog := Catalog()
	for i := 1; i < len(catalog); i++ {
		if catalog[i].Code < catalog[i-1].Code {
			t.Fatalf("catalogue not sorted: %s before %s", catalog[i-1].Code, catalog[i].Code)
		}
	}
}
