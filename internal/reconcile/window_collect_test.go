package reconcile

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/elasticsearch"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// ---- applyWindow --------------------------------------------------------

func TestApplyWindowRestrictsEveryList(t *testing.T) {
	ev := newEvidence().
		record("20261109080000", types.BackupStatusCompleted, types.ComponentZeebe). // before
		record("20261110080000", types.BackupStatusCompleted, types.ComponentZeebe). // in
		record("20261113080000", types.BackupStatusCompleted, types.ComponentZeebe). // after
		holds(types.ComponentZeebe, "20261109080000", camunda.BackupStateCompleted).
		holds(types.ComponentZeebe, "20261111080000", camunda.BackupStateCompleted).
		holds(types.ComponentOperate, "20261114080000", camunda.BackupStateCompleted).
		snapshot("20261109080000", elasticsearch.SnapshotStateSuccess).
		snapshot("20261111080000", elasticsearch.SnapshotStateSuccess).
		snapshot("nightly-slm-2026.11.11", elasticsearch.SnapshotStateSuccess). // no ID: kept
		ev
	ev.logBackupIDs = map[string]bool{"20261108000000": true, "20261112000000": true}
	ev.latestPointer = "20261113080000"
	ev.window = &Window{From: "2026-11-10", To: "2026-11-12"}
	ev.sources[SourceControllerS3] = SourceStatus{Name: SourceControllerS3, Reachable: true, Count: 3}
	ev.sources[types.ComponentZeebe] = SourceStatus{Name: types.ComponentZeebe, Reachable: true, Count: 2}

	ev.applyWindow()

	if len(ev.records) != 1 || ev.records[0].BackupID != "20261110080000" {
		t.Errorf("records = %v, want only 20261110080000", backupIDsOf(ev.records))
	}
	if _, ok := ev.componentBackups[types.ComponentZeebe]["20261109080000"]; ok {
		t.Error("a Zeebe backup before the window was kept")
	}
	if _, ok := ev.componentBackups[types.ComponentZeebe]["20261111080000"]; !ok {
		t.Error("a Zeebe backup inside the window was dropped")
	}
	if len(ev.componentBackups[types.ComponentOperate]) != 0 {
		t.Error("an Operate backup after the window was kept")
	}
	var names []string
	for _, s := range ev.snapshots {
		names = append(names, s.Name)
	}
	if strings.Join(names, ",") != "20261111080000,nightly-slm-2026.11.11" {
		t.Errorf("snapshots = %v, want the in-window one and the unplaceable one", names)
	}
	if len(ev.logBackupIDs) != 1 || !ev.logBackupIDs["20261112000000"] {
		t.Errorf("logBackupIDs = %v", ev.logBackupIDs)
	}
	if ev.latestPointer != "" {
		t.Errorf("a pointer outside the window must not be judged, got %q", ev.latestPointer)
	}
	if got := ev.sources[SourceControllerS3].Count; got != 1 {
		t.Errorf("controller_s3 count = %d, want 1 (in the window)", got)
	}
	if got := ev.sources[types.ComponentZeebe].Count; got != 1 {
		t.Errorf("zeebe count = %d, want 1 (in the window)", got)
	}
}

func backupIDsOf(recs []*models.BackupHistory) []string {
	var ids []string
	for _, r := range recs {
		ids = append(ids, r.BackupID)
	}
	return ids
}

// A backup outside the window must produce no finding at all: not "untracked",
// not "missing". It was never looked at.
func TestWindowedSweepSaysNothingAboutBackupsOutsideIt(t *testing.T) {
	ev := newEvidence().
		holds(types.ComponentZeebe, "20260301080000", camunda.BackupStateCompleted).
		snapshot("camunda_zeebe_records_backup_20260301080000", elasticsearch.SnapshotStateSuccess).
		holds(types.ComponentZeebe, "20260311080000", camunda.BackupStateCompleted).
		snapshot("camunda_zeebe_records_backup_20260311080000", elasticsearch.SnapshotStateSuccess).
		ev
	ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}
	ev.window = &Window{From: "2026-03-10", To: "2026-03-12"}
	ev.applyWindow()

	for _, f := range classify(ev, DefaultOptions()) {
		if f.BackupID == "20260301080000" {
			t.Errorf("a finding about a backup outside the window: %s", f.Reason)
		}
	}
	var inside bool
	for _, f := range classify(ev, DefaultOptions()) {
		inside = inside || f.BackupID == "20260311080000"
	}
	if !inside {
		t.Error("the in-window backup must still be reported")
	}
}

// ---- listComponent / listSnapshots with fakes --------------------------------

type fakeLister struct {
	full      func() ([]camunda.ComponentBackupRecord, error)
	byDays    func(days []string) ([]camunda.ComponentBackupRecord, error)
	fullCalls atomic.Int32
	dayCalls  atomic.Int32
}

func (f *fakeLister) ListBackups(context.Context, string) ([]camunda.ComponentBackupRecord, error) {
	f.fullCalls.Add(1)
	return f.full()
}

type fakeDayLister struct{ *fakeLister }

func (f fakeDayLister) ListBackupsByDays(_ context.Context, _ string, days []string) ([]camunda.ComponentBackupRecord, error) {
	f.dayCalls.Add(1)
	return f.byDays(days)
}

func TestListComponentUsesDaysOnlyForZeebeWithAWindow(t *testing.T) {
	base := &fakeLister{
		full:   func() ([]camunda.ComponentBackupRecord, error) { return nil, nil },
		byDays: func([]string) ([]camunda.ComponentBackupRecord, error) { return nil, nil },
	}
	r := &Reconciler{backupLister: fakeDayLister{base}, logger: utils.NewLogger("error")}
	w := &Window{From: "2026-11-10", To: "2026-11-11"}

	_, _ = r.listComponent(context.Background(), types.ComponentZeebe, "http://z", w)
	if base.dayCalls.Load() != 1 || base.fullCalls.Load() != 0 {
		t.Errorf("zeebe with a window: day calls %d, full calls %d; want 1 and 0", base.dayCalls.Load(), base.fullCalls.Load())
	}

	// Operate does not accept a prefix: it is always listed in full.
	_, _ = r.listComponent(context.Background(), types.ComponentOperate, "http://o", w)
	if base.dayCalls.Load() != 1 || base.fullCalls.Load() != 1 {
		t.Errorf("operate with a window: day calls %d, full calls %d; want 1 and 1", base.dayCalls.Load(), base.fullCalls.Load())
	}

	// All time: the full listing, with a hint when it fails.
	base.full = func() ([]camunda.ComponentBackupRecord, error) { return nil, fmt.Errorf("504") }
	_, err := r.listComponent(context.Background(), types.ComponentZeebe, "http://z", nil)
	if err == nil || !strings.Contains(err.Error(), "choose a date range") {
		t.Errorf("a failed all-time listing should suggest a range, got %v", err)
	}
	_, err = r.listComponent(context.Background(), types.ComponentOperate, "http://o", nil)
	if err == nil || strings.Contains(err.Error(), "choose a date range") {
		t.Errorf("operate has no per-day form, so it must not suggest one, got %v", err)
	}
}

type fakeSnapshots struct {
	full     func() ([]elasticsearch.SnapshotInfo, error)
	byDays   func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error)
	fullHits atomic.Int32
	dayHits  atomic.Int32
	deadline atomic.Int64 // time left on the full attempt's context, in ms
}

func (f *fakeSnapshots) ListSnapshots(ctx context.Context, _ string) ([]elasticsearch.SnapshotInfo, error) {
	f.fullHits.Add(1)
	if d, ok := ctx.Deadline(); ok {
		f.deadline.Store(time.Until(d).Milliseconds())
	}
	return f.full()
}

type fakeDaySnapshots struct{ *fakeSnapshots }

func (f fakeDaySnapshots) ListSnapshotsByDays(_ context.Context, _ string, days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) {
	f.dayHits.Add(1)
	return f.byDays(days, prefixes)
}

func snapshotEvidence(window *Window) *evidence {
	ev := newEvidence().ev
	ev.window = window
	ev.namePrefix = "hajj"
	ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}
	return ev
}

func TestListSnapshotsFallsBackToDaysWhenTheFullListingFails(t *testing.T) {
	fs := &fakeSnapshots{
		full: func() ([]elasticsearch.SnapshotInfo, error) { return nil, fmt.Errorf("timed out") },
		byDays: func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) {
			if len(days) != 2 {
				t.Errorf("days = %v", days)
			}
			joined := strings.Join(prefixes, "|")
			for _, want := range []string{"hajj-", "camunda_operate_", "camunda_zeebe_records_backup_"} {
				if !strings.Contains(joined, want) {
					t.Errorf("prefix %q missing from %v", want, prefixes)
				}
			}
			return []elasticsearch.SnapshotInfo{{Name: "hajj-20261110080000"}}, nil
		},
	}
	r := &Reconciler{logger: utils.NewLogger("error")}
	ev := snapshotEvidence(&Window{From: "2026-11-10", To: "2026-11-11"})

	// The scan has a deadline, as a real one does.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snaps, err := r.listSnapshots(ctx, ev, fakeDaySnapshots{fs})
	if err != nil || len(snaps) != 1 {
		t.Fatalf("got %v, %v; want the fallback's snapshot", snaps, err)
	}
	if fs.fullHits.Load() != 1 || fs.dayHits.Load() != 1 {
		t.Errorf("full %d, days %d; want 1 and 1", fs.fullHits.Load(), fs.dayHits.Load())
	}
	// The first attempt may not use the whole budget, or there is nothing left
	// for the fallback.
	if ms := fs.deadline.Load(); ms <= 0 || ms > 5200 {
		t.Errorf("the full attempt had %dms of a 10s budget; want at most about half", ms)
	}
}

func TestListSnapshotsDoesNotFallBackWithoutAWindow(t *testing.T) {
	fs := &fakeSnapshots{
		full:   func() ([]elasticsearch.SnapshotInfo, error) { return nil, fmt.Errorf("timed out") },
		byDays: func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) { return nil, nil },
	}
	r := &Reconciler{logger: utils.NewLogger("error")}
	_, err := r.listSnapshots(context.Background(), snapshotEvidence(nil), fakeDaySnapshots{fs})
	if err == nil || !strings.Contains(err.Error(), "choose a date range") {
		t.Errorf("expected the failure with a hint, got %v", err)
	}
	if fs.dayHits.Load() != 0 {
		t.Error("with no window there are no days to enumerate")
	}
}

func TestListSnapshotsReportsBothFailures(t *testing.T) {
	fs := &fakeSnapshots{
		full: func() ([]elasticsearch.SnapshotInfo, error) { return nil, fmt.Errorf("full listing timed out") },
		byDays: func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) {
			return nil, fmt.Errorf("day 20261110 failed")
		},
	}
	r := &Reconciler{logger: utils.NewLogger("error")}
	_, err := r.listSnapshots(context.Background(), snapshotEvidence(&Window{From: "2026-11-10", To: "2026-11-10"}), fakeDaySnapshots{fs})
	if err == nil || !strings.Contains(err.Error(), "full listing timed out") || !strings.Contains(err.Error(), "day 20261110 failed") {
		t.Errorf("both failures should be in the error, got %v", err)
	}
}

// ---- a whole windowed sweep, against real HTTP servers -------------------------

type scanS3 struct{ storage.S3Storage }

func (scanS3) ListAllBackups(string) ([]*models.BackupHistory, error) { return nil, nil }
func (scanS3) GetLatestBackupID(string) (string, error)               { return "", utils.ErrBackupNotFound }
func (scanS3) StoreReconcileReport(string, []byte) error              { return nil }

type scanFS struct{ storage.FileStorage }

func (scanFS) ListLogFiles(string) ([]string, error) { return nil, nil }

// The shape of the production failure: Zeebe's full listing hits its gateway's
// 15-second limit, Elasticsearch's _cat listing times out, and only listings by
// day succeed. A windowed sweep must come back complete, with only the window's
// backups in it.
func TestWindowedSweepCompletesWhereTheFullListingsFail(t *testing.T) {
	var zeebeFull, zeebeDays, esCat, esDays atomic.Int32
	zeebe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/actuator/backups":
			zeebeFull.Add(1)
			w.WriteHeader(http.StatusGatewayTimeout)
			_, _ = w.Write([]byte(`{"message":"Request from gateway to broker timed out"}`))
		case "/actuator/backups/20261111*":
			zeebeDays.Add(1)
			_, _ = w.Write([]byte(`[{"backupId":20261111080000,"state":"COMPLETED"}]`))
		default:
			zeebeDays.Add(1)
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer zeebe.Close()

	operate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A full listing: one backup in the window and one long before it.
		_, _ = w.Write([]byte(`[{"backupId":20261111080000,"state":"COMPLETED"},{"backupId":20250101080000,"state":"COMPLETED"}]`))
	}))
	defer operate.Close()

	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_cat/snapshots/") {
			esCat.Add(1)
			time.Sleep(2 * time.Second) // slower than the listing timeout below
			return
		}
		esDays.Add(1)
		if strings.Contains(r.URL.Path, "20261111") {
			_, _ = w.Write([]byte(`{"snapshots":[{"snapshot":"camunda_zeebe_records_backup_20261111080000","state":"SUCCESS"}],"total":1,"remaining":0}`))
			return
		}
		_, _ = w.Write([]byte(`{"snapshots":[],"total":0,"remaining":0}`))
	}))
	defer es.Close()

	t.Setenv("ELASTICSEARCH_EXTERNAL_SNAPSHOT_PREFIXES_UMRAH", "camunda_zeebe_records_backup_")
	logger := utils.NewLogger("error")
	client := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout: 10 * time.Second, ListTimeout: 300 * time.Millisecond, MaxRetries: 2, RetryDelay: time.Millisecond,
	}, logger)
	rec := NewReconciler(scanS3{}, scanFS{}, client, &config.Config{DefaultElasticsearchSnapshotRepository: "zeebe-umrah-oci"}, logger)
	inst := &models.CamundaInstance{ID: "umrah",
		ZeebeBackupEndpoint: zeebe.URL + "/actuator/backups", OperateBackupEndpoint: operate.URL + "/actuator/backups",
		ElasticsearchEndpoint: es.URL}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	report, err := rec.Reconcile(ctx, inst, &Window{From: "2026-11-10", To: "2026-11-12"})
	if err != nil {
		t.Fatal(err)
	}

	if !report.AllSourcesReachable() {
		t.Fatalf("the sweep should be complete, got %+v", report.SourcesChecked)
	}
	if zeebeFull.Load() != 0 || zeebeDays.Load() != 3 {
		t.Errorf("zeebe: %d full listings, %d day queries; want 0 and 3", zeebeFull.Load(), zeebeDays.Load())
	}
	if esCat.Load() != 1 || esDays.Load() != 3 {
		t.Errorf("elasticsearch: %d full listings, %d day queries; want 1 (failed) and 3", esCat.Load(), esDays.Load())
	}
	if report.Window == nil || report.Window.From != "2026-11-10" {
		t.Errorf("the report must record its window, got %+v", report.Window)
	}
	if len(report.BackupIssues) != 1 || report.BackupIssues[0].BackupID != "20261111080000" {
		t.Fatalf("issues = %+v, want only the in-window backup (the 2025 one is outside)", report.BackupIssues)
	}
	issue := report.BackupIssues[0]
	if issue.PrimaryReason != ReasonExternalBackup {
		t.Errorf("primary reason = %s, want %s", issue.PrimaryReason, ReasonExternalBackup)
	}
	for _, src := range []string{SourceZeebe, SourceOperate, SourceElasticsearch} {
		if !containsString(issue.PresentIn, src) {
			t.Errorf("present_in %v is missing %s", issue.PresentIn, src)
		}
	}
	if got := report.SourcesChecked[SourceOperate].Count; got != 1 {
		t.Errorf("operate count = %d, want 1 (the 2025 backup is outside the window)", got)
	}
}

// The same servers, no window: the sweep cannot be complete, and says what to do.
func TestUnwindowedSweepSuggestsARangeWhenListingsFail(t *testing.T) {
	zeebe := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout)
	}))
	defer zeebe.Close()

	logger := utils.NewLogger("error")
	client := camunda.NewHTTPClient(camunda.HTTPClientConfig{Timeout: time.Second, MaxRetries: 0}, logger)
	rec := NewReconciler(scanS3{}, scanFS{}, client, &config.Config{}, logger)
	report, err := rec.Reconcile(context.Background(), &models.CamundaInstance{ID: "umrah", ZeebeBackupEndpoint: zeebe.URL + "/actuator/backups"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.AllSourcesReachable() || !strings.Contains(report.SourcesChecked[SourceZeebe].Error, "choose a date range") {
		t.Errorf("expected an incomplete sweep that suggests a range, got %+v", report.SourcesChecked[SourceZeebe])
	}
	if report.Window != nil {
		t.Errorf("an all-time sweep has no window, got %+v", report.Window)
	}
}
