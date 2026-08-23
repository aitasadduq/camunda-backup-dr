package camunda

import (
	"context"
	"net/http"
	"net/http/httptest"
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
