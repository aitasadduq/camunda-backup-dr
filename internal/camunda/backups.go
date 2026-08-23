package camunda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
type ComponentBackupDetail struct {
	SnapshotName string   `json:"snapshotName,omitempty"`
	PartitionID  int      `json:"partitionId,omitempty"`
	State        string   `json:"state,omitempty"`
	CreatedAt    string   `json:"createdAt,omitempty"`
	StartTime    string   `json:"startTime,omitempty"`
	Failures     []string `json:"failures,omitempty"`
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
	FailureReason *string                 `json:"failureReason"`
	Details       []ComponentBackupDetail `json:"details"`
}

// ListBackups fetches every backup a component currently holds.
//
// It issues GET against the bare backup endpoint - the same URL the orchestrator
// POSTs to when creating a backup, with no ID appended.
func (c *HTTPClient) ListBackups(ctx context.Context, endpoint string) ([]ComponentBackupRecord, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("backup endpoint is required")
	}

	resp, err := c.Get(ctx, strings.TrimRight(endpoint, "/"), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list backups: %w", err)
	}
	defer resp.Body.Close()

	// Components answer 404 when they hold no backups at all, which means "none"
	// rather than "this request failed". Treating it as an error would make every
	// empty component look unreachable and suppress real findings.
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("list backups failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup list response: %w", err)
	}

	return parseBackupList(body)
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
		rec := ComponentBackupRecord{
			BackupID: id,
			State:    w.State,
			Details:  w.Details,
		}
		if w.FailureReason != nil {
			rec.FailureReason = *w.FailureReason
		}
		out = append(out, rec)
	}
	return out, nil
}
