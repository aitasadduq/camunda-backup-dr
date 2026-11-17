package camunda

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
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

// A gateway that times a listing out is as slow as the listing: retrying it
// four times at up to a minute each outlasts the scan.
func TestListBackupsDoesNotRetryGatewayTimeout(t *testing.T) {
	for _, status := range []int{http.StatusGatewayTimeout, http.StatusRequestTimeout} {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.WriteHeader(status)
			_, _ = w.Write([]byte("upstream timed out"))
		}))

		client := NewHTTPClient(HTTPClientConfig{Timeout: time.Second, MaxRetries: 3, RetryDelay: time.Millisecond}, nil)
		_, err := client.ListBackups(context.Background(), server.URL)
		server.Close()

		if err == nil || !strings.Contains(err.Error(), "upstream timed out") {
			t.Errorf("status %d: error should carry the gateway's answer, got %v", status, err)
		}
		if n := calls.Load(); n != 1 {
			t.Errorf("status %d: attempted %d times, want 1", status, n)
		}
	}
}

// timeoutTransport fails every request at once with a timeout, as a dial or
// TLS-handshake timeout does long before the listing timeout is reached.
type timeoutTransport struct{ calls atomic.Int32 }

type dialTimeout struct{}

func (dialTimeout) Error() string   { return "dial tcp 10.0.0.1:9600: i/o timeout" }
func (dialTimeout) Timeout() bool   { return true }
func (dialTimeout) Temporary() bool { return true }

func (tt *timeoutTransport) RoundTrip(*http.Request) (*http.Response, error) {
	tt.calls.Add(1)
	return nil, dialTimeout{}
}

// A connection that never opens is not a slow listing: it keeps its retries,
// and the error must not send the operator to raise the listing timeout.
func TestListBackupsConnectTimeoutIsRetriedAndNotBlamedOnListTimeout(t *testing.T) {
	client := NewHTTPClient(HTTPClientConfig{
		Timeout:     time.Second,
		ListTimeout: 5 * time.Second,
		MaxRetries:  2,
		RetryDelay:  time.Millisecond,
	}, nil)
	transport := &timeoutTransport{}
	client.client.Transport = transport

	_, err := client.ListBackups(context.Background(), "http://zeebe:9600/actuator/backups")
	if err == nil {
		t.Fatal("expected an error")
	}
	if n := transport.calls.Load(); n != 3 {
		t.Errorf("a connect timeout was attempted %d times, want 3", n)
	}
	if errors.Is(err, ErrListTimeout) || !strings.Contains(err.Error(), "does not govern") {
		t.Errorf("error must not blame the listing timeout, got: %v", err)
	}
}

// http.Client.Timeout also covers reading the body, so a listing that times out
// while streaming must name the same setting as one that never answered.
func TestListBackupsBodyReadTimeoutNamesTheSetting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"backupId":1`))
		w.(http.Flusher).Flush()
		time.Sleep(400 * time.Millisecond)
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{Timeout: 5 * time.Second, ListTimeout: 100 * time.Millisecond}, nil)
	_, err := client.ListBackups(context.Background(), server.URL)
	if !errors.Is(err, ErrListTimeout) {
		t.Errorf("expected ErrListTimeout for a body-read timeout, got: %v", err)
	}
}

// When the scan's deadline ends a retry loop, the reason it was retrying must
// survive into the error; otherwise only the WARN log knows it was a 503.
func TestListBackupsKeepsLastAttemptWhenScanBudgetEndsRetries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("broker unavailable"))
	}))
	defer server.Close()

	client := NewHTTPClient(HTTPClientConfig{Timeout: time.Second, MaxRetries: 3, RetryDelay: 200 * time.Millisecond}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := client.ListBackups(ctx, server.URL)
	if err == nil || !strings.Contains(err.Error(), "RECONCILE_TIMEOUT_SECONDS") || !strings.Contains(err.Error(), "broker unavailable") {
		t.Errorf("error should name the scan budget and keep the 503, got: %v", err)
	}
}

// One prefix query per day, merged by backup ID.
func TestListBackupsByDays(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.EscapedPath()) // as sent: "*" must not be %2A
		mu.Unlock()
		switch r.URL.Path {
		case "/actuator/backups/20261110*":
			_, _ = w.Write([]byte(`[{"backupId":20261110080000,"state":"COMPLETED"},{"backupId":20261110083000,"state":"COMPLETED"}]`))
		case "/actuator/backups/20261111*":
			_, _ = w.Write([]byte(`[{"backupId":20261111080000,"state":"COMPLETED"}]`))
		default:
			w.WriteHeader(http.StatusNotFound) // a day with no backups
		}
	}))
	defer server.Close()

	got, err := newTestClient(t).ListBackupsByDays(context.Background(), server.URL+"/actuator/backups/",
		[]string{"20261110", "20261111", "20261112"})
	if err != nil {
		t.Fatalf("ListBackupsByDays: %v", err)
	}
	if len(got) != 3 || got[0].BackupID != "20261110080000" || got[2].BackupID != "20261111080000" {
		t.Errorf("got %+v, want the 3 backups, sorted by ID", got)
	}
	sort.Strings(paths)
	want := []string{"/actuator/backups/20261110*", "/actuator/backups/20261111*", "/actuator/backups/20261112*"}
	if !reflect.DeepEqual(paths, want) {
		t.Errorf("requested %v, want %v", paths, want)
	}
}

// One failed day must fail the whole listing and say which day. Returning what
// the other days held would read as "no backups on that day".
func TestListBackupsByDaysFailsNamingTheDay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "20261111*") {
			w.WriteHeader(http.StatusGatewayTimeout)
			_, _ = w.Write([]byte("broker timed out"))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	got, err := newTestClient(t).ListBackupsByDays(context.Background(), server.URL,
		[]string{"20261110", "20261111", "20261112", "20261113"})
	if err == nil {
		t.Fatalf("expected an error, got %+v", got)
	}
	for _, want := range []string{"20261111", "of 4 days", "broker timed out"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should mention %q, got: %v", want, err)
		}
	}
}

// After the first failure the listing is unusable, so the remaining days are
// not asked for: a service that is down or slow is not queried another few
// hundred times until the scan's deadline.
func TestListBackupsByDaysStopsAfterTheFirstFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	days := make([]string, 200)
	for i := range days {
		days[i] = fmt.Sprintf("2026%02d%02d", i/28+1, i%28+1)
	}
	_, err := newTestClient(t).ListBackupsByDays(context.Background(), server.URL, days)
	if err == nil {
		t.Fatal("expected an error")
	}
	if n := calls.Load(); n > int32(dayQueryWorkers)*2 {
		t.Errorf("%d of 200 days were requested after the first failure; want about %d at most", n, dayQueryWorkers)
	}
	if !strings.Contains(err.Error(), "of 200 days") {
		t.Errorf("the error should say how far the run got, got: %v", err)
	}
}

func TestListBackupsByDaysStopsWhenTheScanEnds(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	days := make([]string, 300)
	for i := range days {
		days[i] = fmt.Sprintf("2026%02d%02d", i/28+1, i%28+1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := newTestClient(t).ListBackupsByDays(ctx, server.URL, days); err == nil {
		t.Fatal("a listing the scan ran out of time on must fail")
	}
	if n := calls.Load(); n > 60 {
		t.Errorf("%d requests were made after the scan's deadline; dispatch must stop", n)
	}
}

func TestForEachBounded(t *testing.T) {
	items := []string{"a", "b", "c", "d", "e"}
	label := func(s string) string { return s }

	t.Run("runs every item and reports none failed", func(t *testing.T) {
		var n atomic.Int32
		started, failures := ForEachBounded(context.Background(), items, 2, label,
			func(context.Context, string) error { n.Add(1); return nil })
		if started != 5 || len(failures) != 0 || n.Load() != 5 {
			t.Errorf("started %d, failures %v, ran %d", started, failures, n.Load())
		}
	})

	t.Run("stops handing out items after a failure, and labels it", func(t *testing.T) {
		started, failures := ForEachBounded(context.Background(), items, 1, label,
			func(_ context.Context, s string) error {
				if s == "b" {
					return fmt.Errorf("boom")
				}
				return nil
			})
		if started != 2 {
			t.Errorf("started %d, want 2 (a, then the failing b)", started)
		}
		if len(failures) != 1 || failures[0].Label != "b" || failures[0].Err.Error() != "boom" {
			t.Errorf("failures = %+v", failures)
		}
	})

	t.Run("a cancelled context is a failure, never a clean run", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		started, failures := ForEachBounded(ctx, items, 2, label, func(context.Context, string) error { return nil })
		if started != 0 || len(failures) != 1 || failures[0].Label != "a" || !errors.Is(failures[0].Err, context.Canceled) {
			t.Errorf("started %d, failures %+v; want none started and the first item named", started, failures)
		}
	})

	t.Run("no items", func(t *testing.T) {
		if started, failures := ForEachBounded(context.Background(), nil, 4, label, func(context.Context, string) error { return nil }); started != 0 || failures != nil {
			t.Errorf("started %d, failures %v", started, failures)
		}
	})
}

// Each day's query hits the component's backup store, so they are bounded.
func TestListBackupsByDaysBoundsConcurrency(t *testing.T) {
	var inFlight, peak atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		inFlight.Add(-1)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	days := make([]string, 20)
	for i := range days {
		days[i] = fmt.Sprintf("202611%02d", i+1)
	}
	if _, err := newTestClient(t).ListBackupsByDays(context.Background(), server.URL, days); err != nil {
		t.Fatal(err)
	}
	if p := peak.Load(); p > dayQueryWorkers {
		t.Errorf("peak concurrency %d exceeds the limit of %d", p, dayQueryWorkers)
	}
}
