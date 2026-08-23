package elasticsearch

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
)

// SnapshotOwner says which system created a snapshot, derived from its name.
type SnapshotOwner string

const (
	// OwnerController is a snapshot this controller created: the backup ID,
	// optionally behind a configured name prefix.
	OwnerController SnapshotOwner = "controller"
	// OwnerComponent is a snapshot Operate, Tasklist or Optimize created as part
	// of its own backup.
	OwnerComponent SnapshotOwner = "component"
	// OwnerForeign is a snapshot matching neither convention: an SLM policy,
	// another cluster, or a different backup tool sharing the repository.
	OwnerForeign SnapshotOwner = "foreign"
)

// SnapshotInfo is one snapshot as listed from a repository.
type SnapshotInfo struct {
	Name         string
	State        SnapshotState
	Owner        SnapshotOwner
	BackupID     string // resolved for controller and component snapshots
	Component    string // operate/tasklist/optimize, for component snapshots
	StartEpoch   int64
	FailedShards int
}

// componentSnapshotPattern matches the snapshot names the Camunda web components
// generate, verified against Operate 8.6.0:
//
//	camunda_operate_20260713102006_8.6.0_part_1_of_6
//
// The backup ID is the third segment; the version and part counters vary.
var componentSnapshotPattern = regexp.MustCompile(`^camunda_(operate|tasklist|optimize)_([0-9]+)_.+$`)

// ListSnapshots returns every snapshot in a repository.
//
// It uses the _cat API rather than GET /_snapshot/{repo}/_all: _all reads full
// metadata for every snapshot, which is expensive on a large repository, while
// _cat returns exactly the fields needed here.
func (c *Client) ListSnapshots(ctx context.Context, repository string) ([]SnapshotInfo, error) {
	if repository == "" {
		return nil, fmt.Errorf("snapshot repository is required")
	}
	if c.httpClient == nil {
		return nil, fmt.Errorf("http client is not configured")
	}

	urlPath := fmt.Sprintf("/_cat/snapshots/%s", repository)
	fullURL, err := c.buildURL(urlPath, map[string]string{
		"format":             "json",
		"h":                  "id,status,start_epoch,failed_shards",
		"ignore_unavailable": "true",
	})
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Get(ctx, fullURL, c.authHeaders())
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshots: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("list snapshots failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	// _cat returns every field as a string regardless of its underlying type.
	var rows []struct {
		ID           string `json:"id"`
		Status       string `json:"status"`
		StartEpoch   string `json:"start_epoch"`
		FailedShards string `json:"failed_shards"`
	}
	if err := camunda.ReadJSONResponse(resp, &rows); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot list: %w", err)
	}

	out := make([]SnapshotInfo, 0, len(rows))
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		info := SnapshotInfo{
			Name:  row.ID,
			State: normalizeSnapshotState(row.Status),
		}
		info.StartEpoch, _ = strconv.ParseInt(row.StartEpoch, 10, 64)
		info.FailedShards, _ = strconv.Atoi(row.FailedShards)
		out = append(out, info)
	}
	return out, nil
}

// ClassifySnapshot resolves a snapshot name to its owner and backup ID.
//
// Order matters. The component pattern is checked first because a component
// snapshot name contains a backup ID and would otherwise be mistaken for a
// foreign name; the controller pattern is checked second because with an empty
// name prefix it accepts any bare digit string.
func ClassifySnapshot(name, namePrefix string) (SnapshotOwner, string, string) {
	if m := componentSnapshotPattern.FindStringSubmatch(name); m != nil {
		return OwnerComponent, m[2], m[1]
	}

	candidate := name
	if namePrefix != "" {
		prefix := namePrefix + "-"
		if !strings.HasPrefix(name, prefix) {
			return OwnerForeign, "", ""
		}
		candidate = strings.TrimPrefix(name, prefix)
	}

	if isBackupIDShaped(candidate) {
		return OwnerController, candidate, ""
	}
	return OwnerForeign, "", ""
}

// isBackupIDShaped reports whether a string looks like a YYYYMMDDHHMMSS backup
// ID. It is a shape check only; callers that need a real timestamp parse it with
// camunda.ParseBackupIDTimestamp.
func isBackupIDShaped(s string) bool {
	if len(s) != 14 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// normalizeSnapshotState maps a _cat status onto the SnapshotState vocabulary.
func normalizeSnapshotState(status string) SnapshotState {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCESS":
		return SnapshotStateSuccess
	case "IN_PROGRESS":
		return SnapshotStateInProgress
	case "FAILED":
		return SnapshotStateFailed
	case "PARTIAL":
		return SnapshotStatePartial
	default:
		return SnapshotStateUnknown
	}
}
