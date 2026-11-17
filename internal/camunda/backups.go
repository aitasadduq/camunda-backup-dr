package camunda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Backup states reported by the Camunda component backup APIs.
const (
	BackupStateCompleted  = "COMPLETED"
	BackupStateFailed     = "FAILED"
	BackupStateIncomplete = "INCOMPLETE"
	BackupStateInProgress = "IN_PROGRESS"
)

// ComponentBackupDetail is one part of a component backup: a Zeebe partition or
// an Elasticsearch snapshot belonging to an Operate/Tasklist/Optimize backup.
//
// The two families report different fields, so everything here is optional.
// Zeebe fills PartitionID and CreatedAt; the web components fill SnapshotName
// and StartTime.
//
// Failures is kept as raw JSON. Its shape is not verified across versions, and
// one entry that is an object rather than a string would otherwise fail the
// decode of the whole listing and make the component look unreachable.
type ComponentBackupDetail struct {
	SnapshotName string          `json:"snapshotName,omitempty"`
	PartitionID  int             `json:"partitionId,omitempty"`
	State        string          `json:"state,omitempty"`
	CreatedAt    string          `json:"createdAt,omitempty"`
	StartTime    string          `json:"startTime,omitempty"`
	Failures     json.RawMessage `json:"failures,omitempty"`
}

// ComponentBackupRecord is one backup as a Camunda component reports it.
type ComponentBackupRecord struct {
	BackupID      string
	State         string
	FailureReason string
	Details       []ComponentBackupDetail
}

// SnapshotNames returns the Elasticsearch snapshot names this backup is made of.
// Zeebe backups have none.
func (r ComponentBackupRecord) SnapshotNames() []string {
	var out []string
	for _, d := range r.Details {
		if d.SnapshotName != "" {
			out = append(out, d.SnapshotName)
		}
	}
	return out
}

// IsCompleted reports whether the component considers this backup usable.
func (r ComponentBackupRecord) IsCompleted() bool {
	return strings.EqualFold(r.State, BackupStateCompleted)
}

// BackupLister lists the backups a Camunda component currently holds. It is the
// read side the reconciler needs: the orchestrator only ever asks about a backup
// ID it already knows.
type BackupLister interface {
	ListBackups(ctx context.Context, endpoint string) ([]ComponentBackupRecord, error)
}

// wireBackupRecord mirrors the on-the-wire shape of a single backup entry.
//
// backupId is decoded as json.Number because both Zeebe and Operate emit it as a
// JSON *number* (verified against 8.6.0), while the controller handles backup IDs
// as YYYYMMDDHHMMSS strings everywhere else. Decoding into a string field fails
// outright against a real server.
type wireBackupRecord struct {
	BackupID      json.Number             `json:"backupId"`
	State         string                  `json:"state"`
	FailureReason json.RawMessage         `json:"failureReason"`
	Details       []ComponentBackupDetail `json:"details"`
}

// ListBackups fetches every backup a component currently holds.
//
// It issues GET against the bare backup endpoint - the same URL the orchestrator
// POSTs to when creating a backup, with no ID appended. It is a listing request
// (see GetListing): it gets COMPONENT_LIST_TIMEOUT_SECONDS, and a request that
// runs that out, or a gateway that times it out, is not retried.
func (c *HTTPClient) ListBackups(ctx context.Context, endpoint string) ([]ComponentBackupRecord, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("backup endpoint is required")
	}

	return c.listBackupsAt(ctx, strings.TrimRight(endpoint, "/"))
}

// DayBackupLister lists the backups a component holds for specific dates, by
// asking for each date's backup-ID prefix.
//
// Only a component that answers a prefix query can offer this (Zeebe does,
// verified against the deployed version; Operate does not). Asking for one day
// at a time keeps each request small enough to finish inside the limits that
// make a full listing of a large backup store fail.
type DayBackupLister interface {
	ListBackupsByDays(ctx context.Context, endpoint string, days []string) ([]ComponentBackupRecord, error)
}

// dayQueryWorkers bounds how many day queries run at once against one
// component. They each make the component read its backup store.
const dayQueryWorkers = 4

// ListBackupsByDays fetches the backups for each given date (YYYYMMDD) with one
// prefix query per day: GET <endpoint>/<YYYYMMDD>*.
//
// It succeeds only if every day's query did. A missing day would otherwise read
// as "no backups that day" and turn into false findings, so it returns an error
// instead of what was gathered. The queries stop at the first failure (see
// ForEachBounded), so the error names the day that failed and how far the run
// got, not every day that would have failed.
func (c *HTTPClient) ListBackupsByDays(ctx context.Context, endpoint string, days []string) ([]ComponentBackupRecord, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("backup endpoint is required")
	}
	base := strings.TrimRight(endpoint, "/")

	var mu sync.Mutex
	byID := make(map[string]ComponentBackupRecord)
	started, failures := ForEachBounded(ctx, days, dayQueryWorkers,
		func(day string) string { return day },
		func(ctx context.Context, day string) error {
			recs, err := c.listBackupsAt(ctx, base+"/"+day+"*")
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for _, rec := range recs {
				byID[rec.BackupID] = rec
			}
			return nil
		})
	if len(failures) > 0 {
		return nil, fmt.Errorf("listing by day failed on %s after starting %d of %d days: %w",
			summarizeFailures(failures), started, len(days), failures[0].Err)
	}

	out := make([]ComponentBackupRecord, 0, len(byID))
	for _, rec := range byID {
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].BackupID < out[j].BackupID })
	return out, nil
}

// SummarizeFailures names the failed items of a ForEachBounded run, the first
// few in full.
func SummarizeFailures(failures []ItemFailure) string { return summarizeFailures(failures) }

func summarizeFailures(failures []ItemFailure) string {
	const shown = 5
	labels := make([]string, 0, len(failures))
	for _, f := range failures {
		labels = append(labels, f.Label)
	}
	if len(labels) <= shown {
		return strings.Join(labels, ", ")
	}
	return fmt.Sprintf("%s and %d more", strings.Join(labels[:shown], ", "), len(labels)-shown)
}

// listBackupsAt performs one listing request and decodes it. Both the full
// listing and each day's query go through it, so they fail with the same
// diagnostics.
func (c *HTTPClient) listBackupsAt(ctx context.Context, listURL string) ([]ComponentBackupRecord, error) {
	endpoint := listURL
	target := utils.RedactURL(endpoint)
	started := time.Now()

	resp, err := c.GetListing(ctx, listURL, nil)
	if err != nil {
		return nil, c.DescribeListFailure(ctx, "list backups", err, started)
	}
	defer resp.Body.Close()

	// Components answer 404 when they hold no backups at all, which means "none"
	// rather than "this request failed". Treating it as an error would make every
	// empty component look unreachable and suppress real findings.
	if resp.StatusCode == http.StatusNotFound {
		c.logger.Debug("Backup listing at %s answered 404 in %v; treating it as no backups",
			target, time.Since(started).Round(time.Millisecond))
		return nil, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("list backups failed with status %d after %v: %s",
			resp.StatusCode, time.Since(started).Round(time.Millisecond), strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, c.DescribeListFailure(ctx, "list backups",
			fmt.Errorf("reading the response (%d bytes received): %w", len(body), err), started)
	}

	records, err := parseBackupList(body)
	if err != nil {
		// The decoder names the field and offset; the start of the body shows
		// which shape the component actually sent.
		return nil, fmt.Errorf("%w (%d-byte response, starting: %s)", err, len(body), snippet(body, 300))
	}
	c.logger.Debug("Backup listing at %s: status %d, %d bytes, %d backups in %v",
		target, resp.StatusCode, len(body), len(records), time.Since(started).Round(time.Millisecond))
	return records, nil
}

// snippet returns at most n bytes of body for an error message, on one line.
func snippet(body []byte, n int) string {
	s := strings.TrimSpace(string(body))
	if len(s) > n {
		s = s[:n] + "..."
	}
	return strings.Join(strings.Fields(s), " ")
}

// parseBackupList decodes a component backup listing, tolerating both a bare
// array and an object wrapping one, since the components are not guaranteed to
// agree and only Zeebe and Operate have been verified directly.
func parseBackupList(body []byte) ([]ComponentBackupRecord, error) {
	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}

	var wire []wireBackupRecord
	if err := json.Unmarshal(body, &wire); err != nil {
		wrapped := struct {
			Backups []wireBackupRecord `json:"backups"`
		}{}
		if wrapErr := json.Unmarshal(body, &wrapped); wrapErr != nil {
			return nil, fmt.Errorf("failed to parse backup list: %w", err)
		}
		wire = wrapped.Backups
	}

	out := make([]ComponentBackupRecord, 0, len(wire))
	for _, w := range wire {
		id := w.BackupID.String()
		if id == "" {
			continue
		}
		out = append(out, ComponentBackupRecord{
			BackupID:      id,
			State:         w.State,
			FailureReason: decodeFailureReason(w.FailureReason),
			Details:       w.Details,
		})
	}
	return out, nil
}

// decodeFailureReason reads failureReason as a string when it is one, and keeps
// any other JSON value verbatim rather than failing the whole listing over it.
func decodeFailureReason(raw json.RawMessage) string {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return trimmed
}
