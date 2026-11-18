package reconcile

import (
	"context"
	"errors"
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

	ev.finalize()

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
	ev.finalize()

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
	if err == nil || !strings.Contains(err.Error(), "scan a date range") {
		t.Errorf("a failed all-time listing should suggest a range, got %v", err)
	}
	_, err = r.listComponent(context.Background(), types.ComponentOperate, "http://o", nil)
	if err == nil || strings.Contains(err.Error(), "scan a date range") {
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

// A long window: the full listing is tried first, with only part of the scan's
// time, and the repository is listed by day if it fails.
func TestListSnapshotsFallsBackToDaysWhenTheFullListingFailsOnALongWindow(t *testing.T) {
	fs := &fakeSnapshots{
		full: func() ([]elasticsearch.SnapshotInfo, error) { return nil, fmt.Errorf("timed out") },
		byDays: func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) {
			if len(days) != 72 {
				t.Errorf("days = %d, want 72", len(days))
			}
			joined := strings.Join(prefixes, "|")
			for _, want := range []string{"hajj-", "camunda_operate_", "camunda_zeebe_records_backup_"} {
				if !strings.Contains(joined, want) {
					t.Errorf("prefix %q missing from %v", want, prefixes)
				}
			}
			return []elasticsearch.SnapshotInfo{{Name: "hajj-20261010080000"}}, nil
		},
	}
	r := &Reconciler{logger: utils.NewLogger("error")}
	ev := snapshotEvidence(&Window{From: "2026-09-01", To: "2026-11-11"})

	// The scan has a deadline, as a real one does.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	snaps, byDay, err := r.listSnapshots(ctx, ev, fakeDaySnapshots{fs})
	if err != nil || len(snaps) != 1 || !byDay {
		t.Fatalf("got %v, byDay=%v, %v; want the fallback's snapshot, listed by day", snaps, byDay, err)
	}
	if fs.fullHits.Load() != 1 || fs.dayHits.Load() != 1 {
		t.Errorf("full %d, days %d; want 1 and 1", fs.fullHits.Load(), fs.dayHits.Load())
	}
	// The first attempt may not use the whole budget, or there is nothing left
	// for the fallback: two thirds of 10s at most.
	if ms := fs.deadline.Load(); ms <= 0 || ms > 6800 {
		t.Errorf("the full attempt had %dms of a 10s budget; want at most about two thirds", ms)
	}
}

// A short window is listed by day straight away: a few small requests, and no
// full listing of a large repository to cut off.
func TestListSnapshotsListsAShortWindowByDayWithoutAFullListing(t *testing.T) {
	fs := &fakeSnapshots{
		full: func() ([]elasticsearch.SnapshotInfo, error) {
			t.Error("the full listing must not run")
			return nil, nil
		},
		byDays: func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) {
			return []elasticsearch.SnapshotInfo{{Name: "hajj-20261110080000"}}, nil
		},
	}
	r := &Reconciler{logger: utils.NewLogger("error")}
	// Exactly the longest window that is listed by day first.
	w, err := NewWindow("2026-10-12", "2026-11-11")
	if err != nil || len(w.Days()) != esDayFirstMaxDays {
		t.Fatalf("test setup: %v, %d days", err, len(w.Days()))
	}
	snaps, byDay, err := r.listSnapshots(context.Background(), snapshotEvidence(w), fakeDaySnapshots{fs})
	if err != nil || len(snaps) != 1 || !byDay {
		t.Fatalf("got %v, byDay=%v, %v", snaps, byDay, err)
	}
	if fs.fullHits.Load() != 0 || fs.dayHits.Load() != 1 {
		t.Errorf("full %d, days %d; want 0 and 1", fs.fullHits.Load(), fs.dayHits.Load())
	}

	// One day more, and the full listing is tried first.
	long, _ := NewWindow("2026-10-11", "2026-11-11")
	fs2 := &fakeSnapshots{
		full: func() ([]elasticsearch.SnapshotInfo, error) { return []elasticsearch.SnapshotInfo{{Name: "x"}}, nil },
		byDays: func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) {
			t.Error("no fallback needed")
			return nil, nil
		},
	}
	snaps, byDay, err = r.listSnapshots(context.Background(), snapshotEvidence(long), fakeDaySnapshots{fs2})
	if err != nil || byDay || len(snaps) != 1 || fs2.fullHits.Load() != 1 {
		t.Errorf("a %d-day window should be listed in full first: %v, byDay=%v, %v", len(long.Days()), snaps, byDay, err)
	}
}

func TestListSnapshotsDoesNotFallBackWithoutAWindow(t *testing.T) {
	fs := &fakeSnapshots{
		full:   func() ([]elasticsearch.SnapshotInfo, error) { return nil, fmt.Errorf("timed out") },
		byDays: func(days, prefixes []string) ([]elasticsearch.SnapshotInfo, error) { return nil, nil },
	}
	r := &Reconciler{logger: utils.NewLogger("error")}
	_, byDay, err := r.listSnapshots(context.Background(), snapshotEvidence(nil), fakeDaySnapshots{fs})
	if err == nil || !strings.Contains(err.Error(), "RECONCILE_WINDOW_DAYS") || byDay {
		t.Errorf("expected the failure with a hint that covers both ways to scan a range, got %v (byDay=%v)", err, byDay)
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
	_, _, err := r.listSnapshots(context.Background(), snapshotEvidence(&Window{From: "2026-09-01", To: "2026-11-11"}), fakeDaySnapshots{fs})
	if err == nil || !strings.Contains(err.Error(), "full listing timed out") || !strings.Contains(err.Error(), "day 20261110 failed") {
		t.Errorf("both failures should be in the error, got %v", err)
	}
}

// A repository listed by day holds only snapshots a prefix claims. "No one
// claims these" cannot be said of it, so nothing is reported as foreign.
func TestListedByDayRepositoryReportsNoForeignSnapshots(t *testing.T) {
	build := func(byDay bool) *evidence {
		ev := newEvidence().
			snapshot("nightly-slm-2026.03.01", elasticsearch.SnapshotStateSuccess).
			snapshot("camunda_zeebe_records_backup_", elasticsearch.SnapshotStateSuccess).ev
		ev.snapshotsByDay = byDay
		return ev
	}
	has := func(findings []Finding) bool {
		for _, f := range findings {
			if f.Reason == ReasonForeignSnapshot {
				return true
			}
		}
		return false
	}
	if !has(classify(build(false), DefaultOptions())) {
		t.Fatal("a full listing must report foreign snapshots")
	}
	if has(classify(build(true), DefaultOptions())) {
		t.Error("a listing by day must not report foreign snapshots: it never looked for them")
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
		if strings.Contains(r.URL.Path, "camunda_zeebe_records_backup_20261111") {
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
	// A 3-day window is listed by day straight away: one request per day and
	// prefix (controller's, three components', one declared external), and no
	// full listing to wait for.
	if esCat.Load() != 0 || esDays.Load() != 15 {
		t.Errorf("elasticsearch: %d full listings, %d requests; want 0 and 15 (3 days x 5 prefixes)", esCat.Load(), esDays.Load())
	}
	if note := report.SourcesChecked[SourceElasticsearch].Note; !strings.Contains(note, "listed by day") {
		t.Errorf("the report should say the repository was listed by day, got note %q", note)
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
	if report.AllSourcesReachable() || !strings.Contains(report.SourcesChecked[SourceZeebe].Error, "scan a date range") {
		t.Errorf("expected an incomplete sweep that suggests a range, got %+v", report.SourcesChecked[SourceZeebe])
	}
	if report.Window != nil {
		t.Errorf("an all-time sweep has no window, got %+v", report.Window)
	}
}

// A long window: the full listing is tried first and fails, then each day and
// prefix is asked for, and the sweep still completes.
func TestLongWindowSweepFallsBackToDaysWhenTheFullListingFails(t *testing.T) {
	var esCat, esDays atomic.Int32
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/_cat/snapshots/") {
			esCat.Add(1)
			time.Sleep(2 * time.Second)
			return
		}
		esDays.Add(1)
		if strings.Contains(r.URL.Path, "camunda_zeebe_records_backup_20261011") {
			_, _ = w.Write([]byte(`{"snapshots":[{"snapshot":"camunda_zeebe_records_backup_20261011080000","state":"SUCCESS"}],"total":1,"remaining":0}`))
			return
		}
		_, _ = w.Write([]byte(`{"snapshots":[],"total":0,"remaining":0}`))
	}))
	defer es.Close()

	t.Setenv("ELASTICSEARCH_EXTERNAL_SNAPSHOT_PREFIXES_UMRAH", "camunda_zeebe_records_backup_")
	logger := utils.NewLogger("error")
	client := camunda.NewHTTPClient(camunda.HTTPClientConfig{Timeout: 10 * time.Second, ListTimeout: 300 * time.Millisecond}, logger)
	rec := NewReconciler(scanS3{}, scanFS{}, client, &config.Config{DefaultElasticsearchSnapshotRepository: "r"}, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	report, err := rec.Reconcile(ctx, &models.CamundaInstance{ID: "umrah", ElasticsearchEndpoint: es.URL}, &Window{From: "2026-09-01", To: "2026-11-11"})
	if err != nil {
		t.Fatal(err)
	}
	if !report.AllSourcesReachable() {
		t.Fatalf("expected a complete sweep, got %+v", report.SourcesChecked)
	}
	if esCat.Load() != 1 || esDays.Load() != 72*5 {
		t.Errorf("%d full listings, %d day requests; want 1 and %d (72 days x 5 prefixes)", esCat.Load(), esDays.Load(), 72*5)
	}
	if len(report.BackupIssues) != 1 || report.BackupIssues[0].BackupID != "20261011080000" {
		t.Errorf("issues = %+v", report.BackupIssues)
	}
}

// ---- cancelled sweeps are not stored; stored windows are validated -------------

type storeCountingS3 struct {
	scanS3
	stored atomic.Int32
	report []byte
}

func (s *storeCountingS3) StoreReconcileReport(string, []byte) error {
	s.stored.Add(1)
	return nil
}
func (s *storeCountingS3) GetLatestReconcileReport(string) ([]byte, error) {
	if s.report == nil {
		return nil, utils.ErrBackupNotFound
	}
	return s.report, nil
}

// A sweep cancelled part-way, as a shutdown does, has recorded every source in
// flight as "context canceled". Storing that would replace the last real report.
func TestCancelledSweepIsNotStored(t *testing.T) {
	s3 := &storeCountingS3{}
	rec := NewReconciler(s3, scanFS{}, camunda.NewHTTPClient(camunda.HTTPClientConfig{Timeout: time.Second}, nil), &config.Config{}, utils.NewLogger("error"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := rec.Reconcile(ctx, &models.CamundaInstance{ID: "umrah"}, nil)
	if !errors.Is(err, context.Canceled) || report != nil {
		t.Errorf("got %v, %v; want context.Canceled and no report", report, err)
	}
	if s3.stored.Load() != 0 {
		t.Error("a cancelled sweep stored its report")
	}

	// Running out of time is different: the partial report says what could not
	// be checked, and is worth keeping.
	expired, cancel2 := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel2()
	time.Sleep(5 * time.Millisecond)
	if _, err := rec.Reconcile(expired, &models.CamundaInstance{ID: "umrah"}, nil); err != nil {
		t.Fatalf("a deadline is not a cancellation: %v", err)
	}
	if s3.stored.Load() != 1 {
		t.Errorf("a sweep that ran out of time stored %d reports, want 1", s3.stored.Load())
	}
}

// A window that cannot be read must fail where the report is loaded. Treated as
// data, it would say every backup is "outside the scan window".
func TestLatestReportRejectsAnUnreadableWindow(t *testing.T) {
	tests := map[string]string{
		"garbage dates":  `{"camunda_instance_id":"u","window":{"from":"2026-13-45","to":"nope"},"sources_checked":{}}`,
		"missing to":     `{"camunda_instance_id":"u","window":{"from":"2026-11-01"},"sources_checked":{}}`,
		"reversed range": `{"camunda_instance_id":"u","window":{"from":"2026-11-17","to":"2026-11-01"},"sources_checked":{}}`,
	}
	for name, stored := range tests {
		t.Run(name, func(t *testing.T) {
			rec := NewReconciler(&storeCountingS3{report: []byte(stored)}, scanFS{}, nil, &config.Config{}, utils.NewLogger("error"))
			if _, err := rec.LatestReport("u"); !errors.Is(err, ErrInvalidWindow) {
				t.Errorf("expected ErrInvalidWindow, got %v", err)
			}
		})
	}

	ok := `{"camunda_instance_id":"u","window":{"from":"2026-11-01","to":"2026-11-17"},"sources_checked":{}}`
	rec := NewReconciler(&storeCountingS3{report: []byte(ok)}, scanFS{}, nil, &config.Config{}, utils.NewLogger("error"))
	if r, err := rec.LatestReport("u"); err != nil || r.Window == nil {
		t.Errorf("a valid window should load: %v, %v", r, err)
	}
	noWindow := `{"camunda_instance_id":"u","sources_checked":{}}`
	rec = NewReconciler(&storeCountingS3{report: []byte(noWindow)}, scanFS{}, nil, &config.Config{}, utils.NewLogger("error"))
	if _, err := rec.LatestReport("u"); err != nil {
		t.Errorf("a report of all time has no window and must load: %v", err)
	}
}

// Classification happens before windowing, because the window places a snapshot
// by the backup ID that classification resolves.
func TestFinalizeClassifiesBeforeWindowing(t *testing.T) {
	ev := newEvidence().
		snapshot("camunda_zeebe_records_backup_20260301080000", elasticsearch.SnapshotStateSuccess).
		snapshot("camunda_zeebe_records_backup_20260101080000", elasticsearch.SnapshotStateSuccess).ev
	ev.externalPrefixes = []string{"camunda_zeebe_records_backup_"}
	ev.window = &Window{From: "2026-03-01", To: "2026-03-31"}

	ev.finalize()
	if len(ev.snapshots) != 1 || ev.snapshots[0].BackupID != "20260301080000" {
		t.Errorf("snapshots = %+v, want only the March one", ev.snapshots)
	}
}
