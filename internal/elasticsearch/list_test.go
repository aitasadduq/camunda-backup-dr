package elasticsearch

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// A single repository holds the controller's own snapshots alongside every
// component's, so name partitioning decides whether a snapshot is recognised at
// all. On the verified 8.6 stack one repository held 31 controller snapshots and
// 84 Operate parts; without this classification every Operate part would read as
// a foreign snapshot.
func TestClassifySnapshot(t *testing.T) {
	tests := []struct {
		name         string
		snapshot     string
		namePrefix   string
		wantOwner    SnapshotOwner
		wantBackupID string
		wantComp     string
	}{
		{
			name:         "bare backup id is the controller's own",
			snapshot:     "20260713102006",
			wantOwner:    OwnerController,
			wantBackupID: "20260713102006",
		},
		{
			name:         "controller snapshot behind a configured prefix",
			snapshot:     "prod-20260713102006",
			namePrefix:   "prod",
			wantOwner:    OwnerController,
			wantBackupID: "20260713102006",
		},
		{
			name:         "operate part, verified naming",
			snapshot:     "camunda_operate_20260713102006_8.6.0_part_1_of_6",
			wantOwner:    OwnerComponent,
			wantBackupID: "20260713102006",
			wantComp:     "operate",
		},
		{
			name:         "tasklist part",
			snapshot:     "camunda_tasklist_20260713102006_8.6.0_part_2_of_6",
			wantOwner:    OwnerComponent,
			wantBackupID: "20260713102006",
			wantComp:     "tasklist",
		},
		{
			name:         "optimize part",
			snapshot:     "camunda_optimize_20260713102006_8.6.0_part_1_of_2",
			wantOwner:    OwnerComponent,
			wantBackupID: "20260713102006",
			wantComp:     "optimize",
		},
		{
			// Component snapshots must be matched before the controller pattern:
			// with no prefix configured the controller pattern would otherwise
			// have to reject them by shape alone.
			name:         "component snapshot wins over the controller pattern",
			snapshot:     "camunda_operate_20260713102006_8.6.0_part_1_of_6",
			namePrefix:   "",
			wantOwner:    OwnerComponent,
			wantBackupID: "20260713102006",
			wantComp:     "operate",
		},
		{name: "an SLM snapshot is foreign", snapshot: "slm-daily-2026.07.13", wantOwner: OwnerForeign},
		{name: "arbitrary name is foreign", snapshot: "manual-pre-upgrade", wantOwner: OwnerForeign},
		{name: "wrong length digits are foreign", snapshot: "202607131", wantOwner: OwnerForeign},
		{
			name:       "correct shape but wrong prefix is foreign",
			snapshot:   "staging-20260713102006",
			namePrefix: "prod",
			wantOwner:  OwnerForeign,
		},
		{
			name:       "unprefixed snapshot when a prefix is configured is foreign",
			snapshot:   "20260713102006",
			namePrefix: "prod",
			wantOwner:  OwnerForeign,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, backupID, component := ClassifySnapshot(tt.snapshot, tt.namePrefix)
			if owner != tt.wantOwner {
				t.Errorf("owner = %s, want %s", owner, tt.wantOwner)
			}
			if backupID != tt.wantBackupID {
				t.Errorf("backup ID = %q, want %q", backupID, tt.wantBackupID)
			}
			if component != tt.wantComp {
				t.Errorf("component = %q, want %q", component, tt.wantComp)
			}
		})
	}
}

func newListTestClient(server *httptest.Server) *Client {
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:    2 * time.Second,
		MaxRetries: 0,
	}, nil)
	return NewClient(server.URL, "elastic", "secret", httpClient, utils.NewLogger("error"))
}

// _cat returns every column as a string regardless of its underlying type.
const catSnapshotsResponse = `[
  {"id":"20260713102006","status":"SUCCESS","start_epoch":"1784029935","failed_shards":"0"},
  {"id":"camunda_operate_20260713102006_8.6.0_part_1_of_6","status":"SUCCESS","start_epoch":"1784029936","failed_shards":"0"},
  {"id":"20260713101756","status":"PARTIAL","start_epoch":"1784029800","failed_shards":"3"}
]`

func TestListSnapshots(t *testing.T) {
	var gotPath, gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(catSnapshotsResponse))
	}))
	defer server.Close()

	got, err := newListTestClient(server).ListSnapshots(context.Background(), "camunda-backup")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotPath != "/_cat/snapshots/camunda-backup" {
		t.Errorf("path = %q", gotPath)
	}
	if gotQuery == "" {
		t.Error("expected query parameters selecting format and columns")
	}
	if len(got) != 3 {
		t.Fatalf("got %d snapshots, want 3", len(got))
	}
	if got[0].State != SnapshotStateSuccess {
		t.Errorf("state = %s, want SUCCESS", got[0].State)
	}
	if got[2].State != SnapshotStatePartial || got[2].FailedShards != 3 {
		t.Errorf("partial snapshot decoded as state=%s failed_shards=%d", got[2].State, got[2].FailedShards)
	}
	if got[0].StartEpoch != 1784029935 {
		t.Errorf("start epoch = %d", got[0].StartEpoch)
	}
}

func TestListSnapshotsErrors(t *testing.T) {
	tests := []struct {
		name       string
		repository string
		status     int
		body       string
	}{
		{name: "missing repository", repository: ""},
		{name: "repository not found", repository: "gone", status: http.StatusNotFound, body: `{"error":"repository_missing_exception"}`},
		{name: "unauthorized", repository: "camunda-backup", status: http.StatusUnauthorized, body: `unauthorized`},
		{name: "malformed body", repository: "camunda-backup", status: http.StatusOK, body: `{oops`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			if _, err := newListTestClient(server).ListSnapshots(context.Background(), tt.repository); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestNormalizeSnapshotState(t *testing.T) {
	cases := map[string]SnapshotState{
		"SUCCESS":       SnapshotStateSuccess,
		"success":       SnapshotStateSuccess,
		" IN_PROGRESS ": SnapshotStateInProgress,
		"FAILED":        SnapshotStateFailed,
		"PARTIAL":       SnapshotStatePartial,
		"something":     SnapshotStateUnknown,
		"":              SnapshotStateUnknown,
	}
	for in, want := range cases {
		if got := normalizeSnapshotState(in); got != want {
			t.Errorf("normalizeSnapshotState(%q) = %s, want %s", in, got, want)
		}
	}
}

// The controller must claim exactly the names it writes. If the two drifted
// apart it would either miss its own snapshots or claim another tool's.
func TestBuildSnapshotNameRoundTripsThroughClassify(t *testing.T) {
	for _, prefix := range []string{"", "prod", "hajj-camunda"} {
		name := BuildSnapshotName(prefix, "20260713102006")
		owner, id, _ := ClassifySnapshot(name, prefix)
		if owner != OwnerController || id != "20260713102006" {
			t.Errorf("prefix %q: %q classified as (%v, %q), want controller-owned 20260713102006", prefix, name, owner, id)
		}
	}
}

// Snapshots following Camunda's manual backup procedure belong to whatever job
// runs that procedure, not to this controller. Undeclared, they must stay
// foreign: reported for information and never offered for deletion.
func TestClassifySnapshot_ZeebeRecordsSnapshotsFromAnotherToolStayForeign(t *testing.T) {
	for _, prefix := range []string{"", "hajj"} {
		for _, name := range []string{
			"camunda_zeebe_records_backup_20260111154511",
			"camunda_zeebe_records_backup_",
		} {
			if owner, _, _ := ClassifySnapshot(name, prefix); owner != OwnerForeign {
				t.Errorf("prefix %q: %q classified as %v, want foreign", prefix, name, owner)
			}
		}
	}
}

func TestSnapshotNamingClassify(t *testing.T) {
	naming := SnapshotNaming{Prefix: "hajj", ExternalPrefixes: []string{"camunda_zeebe_records_backup_", ""}}
	tests := []struct {
		name      string
		snapshot  string
		wantOwner SnapshotOwner
		wantID    string
	}{
		{"declared external job", "camunda_zeebe_records_backup_20260111154511", OwnerExternal, "20260111154511"},
		{"external prefix with no backup ID stays foreign", "camunda_zeebe_records_backup_", OwnerForeign, ""},
		{"external prefix with a non-ID remainder stays foreign", "camunda_zeebe_records_backup_latest", OwnerForeign, ""},
		{"external prefix is matched literally, no separator added", "camunda_zeebe_records_backup-20260111154511", OwnerForeign, ""},
		{"controller's own name still wins", "hajj-20260111154511", OwnerController, "20260111154511"},
		{"component snapshot still wins", "camunda_operate_20260111154511_8.6.0_part_1_of_6", OwnerComponent, "20260111154511"},
		// An empty declared prefix would claim every bare backup ID.
		{"empty external prefix is ignored", "20260111154511", OwnerForeign, ""},
		{"undeclared naming is foreign", "nightly-slm-2026.01.11", OwnerForeign, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, id, _ := naming.Classify(tt.snapshot)
			if owner != tt.wantOwner || id != tt.wantID {
				t.Errorf("Classify(%q) = (%v, %q), want (%v, %q)", tt.snapshot, owner, id, tt.wantOwner, tt.wantID)
			}
		})
	}
}

// Declaring an external prefix must never take a snapshot away from the
// controller, even when the two conventions overlap.
func TestSnapshotNamingControllerBeatsOverlappingExternalPrefix(t *testing.T) {
	naming := SnapshotNaming{Prefix: "backup", ExternalPrefixes: []string{"backup-"}}
	if owner, id, _ := naming.Classify("backup-20260111154511"); owner != OwnerController || id != "20260111154511" {
		t.Errorf("got (%v, %q), want the controller's snapshot", owner, id)
	}
}

// The repository listing is the source declared external snapshots depend on,
// so it gets the listing timeout, not the general one, and is not retried
// once it runs that out.
func TestListSnapshotsIsAListingRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(catSnapshotsResponse))
	}))
	defer server.Close()

	slowOK := camunda.NewHTTPClient(camunda.HTTPClientConfig{Timeout: 100 * time.Millisecond, ListTimeout: 2 * time.Second}, nil)
	if _, err := NewClient(server.URL, "", "", slowOK, nil).ListSnapshots(context.Background(), "camunda-backup"); err != nil {
		t.Fatalf("a listing within ListTimeout must succeed: %v", err)
	}

	calls.Store(0)
	tooSlow := camunda.NewHTTPClient(camunda.HTTPClientConfig{Timeout: 5 * time.Second, ListTimeout: 50 * time.Millisecond, MaxRetries: 3, RetryDelay: time.Millisecond}, nil)
	_, err := NewClient(server.URL, "", "", tooSlow, nil).ListSnapshots(context.Background(), "camunda-backup")
	if !errors.Is(err, camunda.ErrListTimeout) {
		t.Errorf("expected ErrListTimeout, got %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("a timed-out listing was attempted %d times, want 1", n)
	}
}

func TestListSnapshotsByDays(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.EscapedPath())
		mu.Unlock()
		if strings.Contains(r.URL.Path, "20261111") {
			_, _ = w.Write([]byte(`{"snapshots":[
				{"snapshot":"camunda_zeebe_records_backup_20261111080000","state":"IN_PROGRESS","start_time_in_millis":1763000000000,"shards":{"failed":0}},
				{"snapshot":"camunda_operate_20261111080000_8.6.0_part_1_of_2","state":"PARTIAL","start_time_in_millis":1763000001000,"shards":{"failed":2}}],
				"total":2,"remaining":0}`))
			return
		}
		_, _ = w.Write([]byte(`{"snapshots":[],"total":0,"remaining":0}`))
	}))
	defer server.Close()

	prefixes := SnapshotNamePrefixes(SnapshotNaming{Prefix: "hajj", ExternalPrefixes: []string{"camunda_zeebe_records_backup_", ""}})
	got, err := newListTestClient(server).ListSnapshotsByDays(context.Background(), "zeebe-hajj-oci", []string{"20261110", "20261111"}, prefixes)
	if err != nil {
		t.Fatalf("ListSnapshotsByDays: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %+v, want 2 snapshots", got)
	}
	byName := map[string]SnapshotInfo{}
	for _, s := range got {
		byName[s.Name] = s
	}
	if s := byName["camunda_zeebe_records_backup_20261111080000"]; s.State != SnapshotStateInProgress || s.StartEpoch != 1763000000 {
		t.Errorf("state/start not decoded: %+v", s)
	}
	if s := byName["camunda_operate_20261111080000_8.6.0_part_1_of_2"]; s.State != SnapshotStatePartial || s.FailedShards != 2 {
		t.Errorf("partial state/failed shards not decoded: %+v", s)
	}

	// One request per day, naming every prefix for that day. The empty external
	// prefix must not become a pattern that matches every snapshot of the day.
	if len(paths) != 2 {
		t.Fatalf("expected one request per day, got %v", paths)
	}
	sort.Strings(paths)
	for _, want := range []string{"hajj-20261110*", "camunda_operate_20261110*", "camunda_tasklist_20261110*", "camunda_optimize_20261110*", "camunda_zeebe_records_backup_20261110*"} {
		if !strings.Contains(paths[0], want) {
			t.Errorf("request %q is missing pattern %q", paths[0], want)
		}
	}
	if strings.Contains(paths[0], ",20261110*") || strings.Contains(paths[0], "/20261110*") {
		t.Errorf("a bare date pattern would match every snapshot of the day: %q", paths[0])
	}
}

// A truncated answer would read as snapshots that do not exist.
func TestListSnapshotsByDaysRefusesTruncatedAnswer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"snapshots":[{"snapshot":"hajj-20261110080000","state":"SUCCESS"}],"total":900,"remaining":899}`))
	}))
	defer server.Close()

	_, err := newListTestClient(server).ListSnapshotsByDays(context.Background(), "repo", []string{"20261110"}, []string{"hajj-"})
	if err == nil || !strings.Contains(err.Error(), "truncated") {
		t.Errorf("expected a truncation error, got %v", err)
	}
}

func TestListSnapshotsByDaysNamesFailedDays(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "20261111") {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"snapshots":[],"total":0,"remaining":0}`))
	}))
	defer server.Close()

	_, err := newListTestClient(server).ListSnapshotsByDays(context.Background(), "repo", []string{"20261110", "20261111"}, []string{"x-"})
	if err == nil || !strings.Contains(err.Error(), "1 of 2 days") || !strings.Contains(err.Error(), "20261111") {
		t.Errorf("expected the failed day to be named, got %v", err)
	}
}
