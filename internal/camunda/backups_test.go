package camunda

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// These payloads are verbatim (trimmed) responses captured from Zeebe 8.6.0 and
// Operate 8.6.0. The important detail is that backupId arrives as a JSON number
// in both: decoding it into a string field fails against a real server.
const (
	zeebeListResponse = `[
  {"backupId":20260713102006,"state":"COMPLETED",
   "details":[{"partitionId":1,"state":"COMPLETED","createdAt":"2026-07-13T10:20:06.914892595Z","brokerVersion":"8.6.0"}]},
  {"backupId":20260322020000,"state":"COMPLETED",
   "details":[{"partitionId":1,"state":"COMPLETED","createdAt":"2026-03-22T02:00:00.290574968Z","brokerVersion":"8.6.0"}]}
]`

	operateListResponse = `[
  {"backupId":20260713102006,"state":"COMPLETED","failureReason":null,
   "details":[
     {"snapshotName":"camunda_operate_20260713102006_8.6.0_part_1_of_6","state":"SUCCESS","startTime":"2026-07-13T10:20:06.905+0000","failures":[]},
     {"snapshotName":"camunda_operate_20260713102006_8.6.0_part_2_of_6","state":"SUCCESS","startTime":"2026-07-13T10:20:07.307+0000","failures":[]}
   ]}
]`
)

func TestParseBackupList(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		wantIDs     []string
		wantState   string
		wantSnaps   int
		wantErr     bool
		wantEmptyOK bool
	}{
		{
			name:      "zeebe 8.6 numeric backup ids",
			body:      zeebeListResponse,
			wantIDs:   []string{"20260713102006", "20260322020000"},
			wantState: BackupStateCompleted,
		},
		{
			name:      "operate 8.6 with snapshot details",
			body:      operateListResponse,
			wantIDs:   []string{"20260713102006"},
			wantState: BackupStateCompleted,
			wantSnaps: 2,
		},
		{
			name:        "empty array",
			body:        `[]`,
			wantEmptyOK: true,
		},
		{
			name:        "null body",
			body:        `null`,
			wantEmptyOK: true,
		},
		{
			name:      "object wrapping the array is tolerated",
			body:      `{"backups":[{"backupId":20260713102006,"state":"COMPLETED"}]}`,
			wantIDs:   []string{"20260713102006"},
			wantState: BackupStateCompleted,
		},
		{
			// The shape of details[].failures is not verified across versions.
			// One object where a string was expected used to fail the decode of
			// the whole listing and mark the component unreachable.
			name:      "failure entries that are objects do not fail the listing",
			body:      `[{"backupId":20260713102006,"state":"FAILED","failureReason":"shard failed","details":[{"snapshotName":"camunda_operate_20260713102006_8.6.0_part_1_of_6","state":"FAILED","failures":[{"index":"operate-list-view","shardId":0,"reason":"IndexShardSnapshotFailedException"}]}]}]`,
			wantIDs:   []string{"20260713102006"},
			wantState: BackupStateFailed,
			wantSnaps: 1,
		},
		{
			name:      "a failureReason that is not a string does not fail the listing",
			body:      `[{"backupId":20260713102006,"state":"FAILED","failureReason":{"message":"boom"}}]`,
			wantIDs:   []string{"20260713102006"},
			wantState: BackupStateFailed,
		},
		{
			name:    "malformed json is an error",
			body:    `{not json`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBackupList([]byte(tt.body))
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantEmptyOK {
				if len(got) != 0 {
					t.Fatalf("expected no records, got %d", len(got))
				}
				return
			}
			if len(got) != len(tt.wantIDs) {
				t.Fatalf("got %d records, want %d", len(got), len(tt.wantIDs))
			}
			for i, wantID := range tt.wantIDs {
				if got[i].BackupID != wantID {
					t.Errorf("record %d: backup ID = %q, want %q", i, got[i].BackupID, wantID)
				}
				if got[i].State != tt.wantState {
					t.Errorf("record %d: state = %q, want %q", i, got[i].State, tt.wantState)
				}
			}
			if tt.wantSnaps > 0 {
				if n := len(got[0].SnapshotNames()); n != tt.wantSnaps {
					t.Errorf("got %d snapshot names, want %d", n, tt.wantSnaps)
				}
			}
		})
	}
}

func newTestClient(t *testing.T) *HTTPClient {
	t.Helper()
	return NewHTTPClient(HTTPClientConfig{
		Timeout:    2 * time.Second,
		MaxRetries: 0,
	}, nil)
}

func TestListBackups(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		body      string
		wantCount int
		wantErr   bool
	}{
		{name: "success", status: http.StatusOK, body: operateListResponse, wantCount: 1},
		// A component with no backups answers 404. Treating that as an error
		// would mark the source unreachable and silently suppress real findings.
		{name: "404 means no backups, not a failure", status: http.StatusNotFound, body: `{"message":"No backups found"}`, wantCount: 0},
		{name: "401 is a genuine error", status: http.StatusUnauthorized, body: `unauthorized`, wantErr: true},
		{name: "malformed body is an error", status: http.StatusOK, body: `{oops`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet {
					t.Errorf("method = %s, want GET", r.Method)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer server.Close()

			got, err := newTestClient(t).ListBackups(context.Background(), server.URL)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != tt.wantCount {
				t.Errorf("got %d records, want %d", len(got), tt.wantCount)
			}
		})
	}
}

func TestListBackupsRequiresEndpoint(t *testing.T) {
	if _, err := newTestClient(t).ListBackups(context.Background(), ""); err == nil {
		t.Error("expected an error for an empty endpoint")
	}
}

func TestListBackupsTrimsTrailingSlash(t *testing.T) {
	var gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	if _, err := newTestClient(t).ListBackups(context.Background(), server.URL+"/actuator/backups/"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotPath != "/actuator/backups" {
		t.Errorf("path = %q, want %q", gotPath, "/actuator/backups")
	}
}

func TestComponentBackupRecordHelpers(t *testing.T) {
	rec := ComponentBackupRecord{
		State: "completed", // components are not consistent about case
		Details: []ComponentBackupDetail{
			{SnapshotName: "camunda_operate_20260713102006_8.6.0_part_1_of_6"},
			{PartitionID: 1}, // a Zeebe-style detail carries no snapshot name
		},
	}
	if !rec.IsCompleted() {
		t.Error("state matching should be case-insensitive")
	}
	if n := len(rec.SnapshotNames()); n != 1 {
		t.Errorf("got %d snapshot names, want 1", n)
	}
}

func TestDecodeFailureReason(t *testing.T) {
	tests := map[string]string{
		``:                   "",
		`null`:               "",
		`"disk full"`:        "disk full",
		`{"message":"boom"}`: `{"message":"boom"}`,
		`["a","b"]`:          `["a","b"]`,
	}
	for raw, want := range tests {
		if got := decodeFailureReason([]byte(raw)); got != want {
			t.Errorf("decodeFailureReason(%q) = %q, want %q", raw, got, want)
		}
	}
}

// A listing can legitimately take longer than the general request timeout, so
// it gets ListTimeout instead.
func TestListBackupsUsesListTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(zeebeListResponse))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Timeout:     100 * time.Millisecond,
		ListTimeout: 2 * time.Second,
	}, nil)

	got, err := client.ListBackups(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("a listing slower than the general timeout but within ListTimeout must succeed: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("got %d records, want 2", len(got))
	}
}

// A listing that times out is slow, not flaky: retrying it burns another full
// timeout each time and outlasts the sweep. The error must name the setting.
func TestListBackupsDoesNotRetryTimeoutAndNamesTheSetting(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Timeout:     5 * time.Second,
		ListTimeout: 50 * time.Millisecond,
		MaxRetries:  3,
		RetryDelay:  time.Millisecond,
	}, nil)

	_, err := client.ListBackups(context.Background(), server.URL)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("a timed-out listing was attempted %d times, want 1", n)
	}
	if !strings.Contains(err.Error(), "COMPONENT_LIST_TIMEOUT_SECONDS") {
		t.Errorf("error should name the setting to raise, got: %v", err)
	}
}

// When the sweep's own deadline is what ends the listing, raising the listing
// timeout would not help, and the error must say which budget ran out.
func TestListBackupsNamesTheSweepBudgetWhenItRunsOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{Timeout: 5 * time.Second, ListTimeout: 5 * time.Second}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := client.ListBackups(ctx, server.URL)
	if err == nil || !strings.Contains(err.Error(), "RECONCILE_TIMEOUT_SECONDS") {
		t.Errorf("error should name the scan budget, got: %v", err)
	}
}

// Server errors are transient as often as not, so they keep their retries.
func TestListBackupsStillRetriesServerErrors(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(zeebeListResponse))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{
		Timeout:    time.Second,
		MaxRetries: 2,
		RetryDelay: time.Millisecond,
	}, nil)

	got, err := client.ListBackups(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("a 503 followed by success should succeed: %v", err)
	}
	if len(got) != 2 || calls.Load() != 2 {
		t.Errorf("got %d records after %d calls, want 2 after 2", len(got), calls.Load())
	}
}

// A decode failure must show what the component actually sent; otherwise the
// only evidence is a Go type error with no payload behind it.
func TestListBackupsParseErrorIncludesBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"unexpected":"shape"`))
	}))
	defer server.Close()

	_, err := newTestClient(t).ListBackups(context.Background(), server.URL)
	if err == nil || !strings.Contains(err.Error(), `{"unexpected":"shape"`) {
		t.Errorf("error should include the start of the body, got: %v", err)
	}
}
