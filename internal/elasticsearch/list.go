package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
	// OwnerExternal is a snapshot written by another backup job that the
	// operator has declared for this instance by its name prefix (for example
	// "camunda_zeebe_records_backup_" from Camunda's manual procedure). The
	// controller did not create it, but has been told it may manage it.
	OwnerExternal SnapshotOwner = "external"
	// OwnerForeign is a snapshot matching no convention: an SLM policy,
	// another cluster, or a backup tool nobody has declared.
	OwnerForeign SnapshotOwner = "foreign"
)

// SnapshotInfo is one snapshot as listed from a repository.
type SnapshotInfo struct {
	Name         string
	State        SnapshotState
	Owner        SnapshotOwner
	BackupID     string // resolved for controller, component and external snapshots
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

	// Escaped because config.GetElasticsearchSnapshotRepository can source this
	// from an env var, which bypasses the instance-level name validation.
	urlPath := fmt.Sprintf("/_cat/snapshots/%s", url.PathEscape(repository))
	fullURL, err := c.buildURL(urlPath, map[string]string{
		"format":             "json",
		"h":                  "id,status,start_epoch,failed_shards",
		"ignore_unavailable": "true",
	})
	if err != nil {
		return nil, err
	}

	// A listing request, like a component's: a large repository can take far
	// longer than an ordinary call, and retrying a slow listing only spends the
	// scan's budget again.
	started := time.Now()
	resp, err := c.httpClient.GetListing(ctx, fullURL, c.authHeaders())
	if err != nil {
		return nil, c.httpClient.DescribeListFailure(ctx, "list snapshots", err, started)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("list snapshots failed with status %d after %v: %s",
			resp.StatusCode, time.Since(started).Round(time.Millisecond), strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, c.httpClient.DescribeListFailure(ctx, "list snapshots",
			fmt.Errorf("reading the response (%d bytes received): %w", len(body), err), started)
	}

	// _cat returns every field as a string regardless of its underlying type.
	var rows []struct {
		ID           string `json:"id"`
		Status       string `json:"status"`
		StartEpoch   string `json:"start_epoch"`
		FailedShards string `json:"failed_shards"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot list (%d-byte response): %w", len(body), err)
	}
	c.logger.Debug("Snapshot listing of %q: %d bytes, %d snapshots in %v",
		repository, len(body), len(rows), time.Since(started).Round(time.Millisecond))

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

// DaySnapshotLister lists the snapshots of specific dates by name pattern. It is
// how a large repository is read when a full listing is too slow, or fails.
//
// What it returns is every snapshot named prefix + date + "*" for the prefixes
// it is given, and nothing else. A name matching none of them (another tool's,
// or one with no backup ID) is not returned, so a listing made this way is not
// the whole repository: callers must not draw repository-wide conclusions from
// it, such as counting the snapshots nobody has claimed.
type DaySnapshotLister interface {
	ListSnapshotsByDays(ctx context.Context, repository string, days, prefixes []string) ([]SnapshotInfo, error)
}

// daySnapshotWorkers bounds how many requests run at once.
const daySnapshotWorkers = 4

// SnapshotNamePrefixes is every fixed name prefix a snapshot of interest can
// start with, in front of its backup ID: the controller's own, each component's,
// and every declared external job's. A name pattern is prefix + date + "*".
func SnapshotNamePrefixes(naming SnapshotNaming) []string {
	prefixes := []string{
		BuildSnapshotName(naming.Prefix, ""),
		"camunda_operate_", "camunda_tasklist_", "camunda_optimize_",
	}
	seen := map[string]bool{}
	for _, p := range prefixes {
		seen[p] = true
	}
	for _, p := range naming.ExternalPrefixes {
		if p != "" && !seen[p] {
			seen[p] = true
			prefixes = append(prefixes, p)
		}
	}
	return prefixes
}

// dayPrefix is one request of ListSnapshotsByDays.
type dayPrefix struct{ day, prefix string }

// ListSnapshotsByDays lists the snapshots named prefix + date + "*", one Get
// snapshot request for each date and each prefix:
// GET /_snapshot/{repo}/{prefix}{date}*
//
// Every prefix is asked for on its own, so each declared external job is checked
// separately and a failure names the prefix and day it happened on. It succeeds
// only if every request did, and fails if Elasticsearch says an answer was
// truncated: a short list would read as snapshots that do not exist. Requests
// stop at the first failure.
func (c *Client) ListSnapshotsByDays(ctx context.Context, repository string, days, prefixes []string) ([]SnapshotInfo, error) {
	if repository == "" {
		return nil, fmt.Errorf("snapshot repository is required")
	}
	if c.httpClient == nil {
		return nil, fmt.Errorf("http client is not configured")
	}

	jobs := make([]dayPrefix, 0, len(days)*len(prefixes))
	for _, day := range days {
		for _, prefix := range prefixes {
			jobs = append(jobs, dayPrefix{day: day, prefix: prefix})
		}
	}

	var mu sync.Mutex
	byName := make(map[string]SnapshotInfo)
	started, failures := camunda.ForEachBounded(ctx, jobs, daySnapshotWorkers,
		func(j dayPrefix) string { return j.prefix + j.day + "*" },
		func(ctx context.Context, j dayPrefix) error {
			snaps, err := c.listSnapshotsMatching(ctx, repository, j.prefix, j.day)
			if err != nil {
				return err
			}
			mu.Lock()
			defer mu.Unlock()
			for _, snap := range snaps {
				byName[snap.Name] = snap
			}
			return nil
		})
	if len(failures) > 0 {
		return nil, fmt.Errorf("listing snapshots by day failed on %s after starting %d of %d requests: %w",
			camunda.SummarizeFailures(failures), started, len(jobs), failures[0].Err)
	}

	out := make([]SnapshotInfo, 0, len(byName))
	for _, snap := range byName {
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (c *Client) listSnapshotsMatching(ctx context.Context, repository, prefix, day string) ([]SnapshotInfo, error) {
	// "*" must reach Elasticsearch as itself: PathEscape would send %2A, which
	// only works if everything between here and Elasticsearch decodes it.
	pattern := strings.ReplaceAll(url.PathEscape(prefix+day), "%2A", "*") + "*"
	fullURL, err := c.buildURL(fmt.Sprintf("/_snapshot/%s/%s", url.PathEscape(repository), pattern), nil)
	if err != nil {
		return nil, err
	}

	started := time.Now()
	resp, err := c.httpClient.GetListing(ctx, fullURL, c.authHeaders())
	if err != nil {
		return nil, c.httpClient.DescribeListFailure(ctx, "list snapshots", err, started)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("list snapshots failed with status %d after %v: %s",
			resp.StatusCode, time.Since(started).Round(time.Millisecond), strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, c.httpClient.DescribeListFailure(ctx, "list snapshots",
			fmt.Errorf("reading the response (%d bytes received): %w", len(body), err), started)
	}

	var parsed struct {
		Snapshots []struct {
			Snapshot string `json:"snapshot"`
			State    string `json:"state"`
			Start    int64  `json:"start_time_in_millis"`
			Shards   struct {
				Failed int `json:"failed"`
			} `json:"shards"`
		} `json:"snapshots"`
		Remaining int `json:"remaining"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("failed to parse snapshot list for %s%s* (%d-byte response): %w", prefix, day, len(body), err)
	}
	if parsed.Remaining > 0 {
		return nil, fmt.Errorf("snapshot list for %s%s* was truncated by Elasticsearch (%d more not returned)", prefix, day, parsed.Remaining)
	}

	out := make([]SnapshotInfo, 0, len(parsed.Snapshots))
	for _, snap := range parsed.Snapshots {
		if snap.Snapshot == "" {
			continue
		}
		out = append(out, SnapshotInfo{
			Name:         snap.Snapshot,
			State:        normalizeSnapshotState(snap.State),
			StartEpoch:   snap.Start / 1000,
			FailedShards: snap.Shards.Failed,
		})
	}
	return out, nil
}

// BuildSnapshotName is the name the controller gives the snapshot it takes for
// a backup: "<prefix>-<backupID>", or the bare backup ID with no prefix.
//
// It is the single definition of that convention. ClassifySnapshot recognises
// controller snapshots by the same function, so the names the controller
// writes and the names it claims as its own cannot drift apart.
func BuildSnapshotName(namePrefix, backupID string) string {
	if namePrefix == "" {
		return backupID
	}
	return namePrefix + "-" + backupID
}

// SnapshotNaming is everything that decides who owns a snapshot name for one
// instance: the controller's own name prefix, and the prefixes of any external
// backup jobs the operator has declared.
type SnapshotNaming struct {
	// Prefix is the controller's configured name prefix (see BuildSnapshotName).
	Prefix string
	// ExternalPrefixes are matched literally: a name is external only when it
	// is exactly one of these followed by a backup-ID-shaped string. Nothing is
	// inserted between them, so the prefix must include any separator.
	ExternalPrefixes []string
}

// ClassifySnapshot resolves a snapshot name to its owner and backup ID, for an
// instance with no external backup jobs declared.
func ClassifySnapshot(name, namePrefix string) (SnapshotOwner, string, string) {
	return SnapshotNaming{Prefix: namePrefix}.Classify(name)
}

// Classify resolves a snapshot name to its owner, backup ID and (for component
// snapshots) component.
//
// Order matters. The component pattern is checked first because a component
// snapshot name contains a backup ID and would otherwise be mistaken for
// another kind. The controller's own convention is checked before external
// prefixes, so declaring an external prefix can never take a snapshot away
// from the controller that wrote it. External prefixes are matched exactly and
// only with a backup-ID-shaped remainder, so "camunda_zeebe_records_backup_"
// with nothing after it stays foreign.
func (n SnapshotNaming) Classify(name string) (SnapshotOwner, string, string) {
	if m := componentSnapshotPattern.FindStringSubmatch(name); m != nil {
		return OwnerComponent, m[2], m[1]
	}

	// The fixed part BuildSnapshotName puts in front of every backup ID.
	if candidate, ok := strings.CutPrefix(name, BuildSnapshotName(n.Prefix, "")); ok && camunda.IsBackupIDShaped(candidate) {
		return OwnerController, candidate, ""
	}

	for _, prefix := range n.ExternalPrefixes {
		if prefix == "" {
			continue // would claim every bare backup ID; never meaningful
		}
		if candidate, ok := strings.CutPrefix(name, prefix); ok && camunda.IsBackupIDShaped(candidate) {
			return OwnerExternal, candidate, ""
		}
	}

	return OwnerForeign, "", ""
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
