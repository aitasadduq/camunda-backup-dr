package camunda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Backup states reported by the Camunda component backup APIs.
const (
	BackupStateCompleted    = "COMPLETED"
	BackupStateFailed       = "FAILED"
	BackupStateIncomplete   = "INCOMPLETE"
	BackupStateInProgress   = "IN_PROGRESS"
	BackupStateDoesNotExist = "DOES_NOT_EXIST"
)

// ComponentBackupDetail is one part of a component backup: a Zeebe partition or
// an Elasticsearch snapshot belonging to an Operate/Tasklist/Optimize backup.
//
// The two families report different fields, so everything here is optional.
// Zeebe fills PartitionID and CreatedAt; the web components fill SnapshotName
// and StartTime. Failures is kept as raw JSON because Operate/Tasklist emit
// Elasticsearch SnapshotShardFailure objects there when a shard fails.
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

// CandidateBackupLister is an optional extension of BackupLister for components
// (such as Zeebe backed by S3/OCI with hundreds or thousands of backups, or
// older Camunda versions) where an unfiltered GET /actuator/backups fails or
// times out, but prefix queries (GET /actuator/backups/{prefix}*) or per-ID
// status queries (GET /actuator/backups/{backupId}) succeed.
type CandidateBackupLister interface {
	ListBackupsByCandidates(ctx context.Context, endpoint string, candidateIDs []string) ([]ComponentBackupRecord, error)
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
// POSTs to when creating a backup, with no ID appended.
func (c *HTTPClient) ListBackups(ctx context.Context, endpoint string) ([]ComponentBackupRecord, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("backup endpoint is required")
	}

	return c.fetchBackupsAtURL(ctx, strings.TrimRight(endpoint, "/"))
}

func (c *HTTPClient) fetchBackupsAtURL(ctx context.Context, targetURL string) ([]ComponentBackupRecord, error) {
	resp, err := c.doListGet(ctx, targetURL)
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

func (c *HTTPClient) doListGet(ctx context.Context, rawURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	client := &http.Client{}
	if c.client != nil {
		cp := *c.client
		// When ctx carries a deadline, let it govern the request rather than
		// cutting large repository listings off at the 30s per-request default.
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			cp.Timeout = 0
		}
		client = &cp
	}
	return client.Do(req)
}

const (
	prefixQueryTimeout   = 15 * time.Second
	singleBackupTimeout  = 10 * time.Second
	prefixQueryWorkers   = 4
	candidateProbeWorkers = 16
)

// ListBackupsByCandidates queries a component backup endpoint using prefix
// wildcards (GET <endpoint>/<YYYYMM>* or GET <endpoint>?prefix=<YYYYMM>*) and
// falls back to per-backup-ID status queries (GET <endpoint>/<backupID>) when
// an unfiltered GET <endpoint> fails or times out on a large backup store.
func (c *HTTPClient) ListBackupsByCandidates(ctx context.Context, endpoint string, candidateIDs []string) ([]ComponentBackupRecord, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("backup endpoint is required")
	}
	base := strings.TrimRight(endpoint, "/")
	candidates := uniqueNonEmpty(candidateIDs)

	byID := make(map[string]ComponentBackupRecord)
	var mu sync.Mutex
	addRecords := func(recs []ComponentBackupRecord) {
		mu.Lock()
		defer mu.Unlock()
		for _, r := range recs {
			if r.BackupID != "" {
				byID[r.BackupID] = r
			}
		}
	}

	remainingIDs := candidates

	// Tier 1: try month-prefix wildcard queries (supported by Zeebe 8.5/8.6,
	// where S3BackupStore filters keys by prefix before downloading manifests).
	months := distinctPrefixes(candidates, 6)
	if len(months) > 0 {
		buildPrefixURL := c.detectPrefixFormat(ctx, base, months[0], addRecords)
		if buildPrefixURL != nil {
			var failedMonths []string
			var failedMu sync.Mutex

			runBounded(ctx, months[1:], prefixQueryWorkers, func(month string) {
				reqCtx, cancel := context.WithTimeout(ctx, prefixQueryTimeout)
				defer cancel()
				recs, err := c.fetchBackupsAtURL(reqCtx, buildPrefixURL(month))
				if err != nil {
					failedMu.Lock()
					failedMonths = append(failedMonths, month)
					failedMu.Unlock()
					return
				}
				addRecords(recs)
			})

			if len(failedMonths) == 0 {
				remainingIDs = nil
			} else {
				// Subdivide only the months that failed into YYYYMMDD day prefixes.
				monthCandidates := filterByPrefixes(candidates, failedMonths)
				days := distinctPrefixes(monthCandidates, 8)
				var failedDays []string
				runBounded(ctx, days, prefixQueryWorkers, func(day string) {
					reqCtx, cancel := context.WithTimeout(ctx, prefixQueryTimeout)
					defer cancel()
					recs, err := c.fetchBackupsAtURL(reqCtx, buildPrefixURL(day))
					if err != nil {
						failedMu.Lock()
						failedDays = append(failedDays, day)
						failedMu.Unlock()
						return
					}
					addRecords(recs)
				})
				remainingIDs = filterByPrefixes(monthCandidates, failedDays)
			}
		}
	}

	// Tier 2: per-backup-ID status queries (GET <endpoint>/<backupId>).
	if len(remainingIDs) > 0 {
		// Probe the first candidate ID synchronously so a genuinely unreachable
		// component fails fast rather than fanning out.
		probeCtx, cancel := context.WithTimeout(ctx, singleBackupTimeout)
		firstRecs, err := c.fetchBackupsAtURL(probeCtx, base+"/"+url.PathEscape(remainingIDs[0]))
		cancel()
		if err != nil {
			return nil, err
		}
		addRecords(firstRecs)

		var probeErrors int
		var errMu sync.Mutex
		var firstErr error
		runBounded(ctx, remainingIDs[1:], candidateProbeWorkers, func(id string) {
			reqCtx, reqCancel := context.WithTimeout(ctx, singleBackupTimeout)
			defer reqCancel()
			recs, reqErr := c.fetchBackupsAtURL(reqCtx, base+"/"+url.PathEscape(id))
			if reqErr != nil {
				errMu.Lock()
				probeErrors++
				if firstErr == nil {
					firstErr = reqErr
				}
				errMu.Unlock()
				return
			}
			addRecords(recs)
		})

		// If every follow-up probe failed and we found nothing, report the error.
		if probeErrors == len(remainingIDs)-1 && len(remainingIDs) > 1 && len(byID) == 0 {
			return nil, firstErr
		}
	} else if len(months) == 0 {
		// No candidate IDs at all: probe a synthetic backup ID to check whether
		// the component's per-ID status endpoint is reachable.
		probeCtx, cancel := context.WithTimeout(ctx, singleBackupTimeout)
		_, err := c.fetchBackupsAtURL(probeCtx, base+"/20000101000000")
		cancel()
		if err != nil {
			return nil, err
		}
	}

	out := make([]ComponentBackupRecord, 0, len(byID))
	for _, r := range byID {
		out = append(out, r)
	}
	return out, nil
}

func (c *HTTPClient) detectPrefixFormat(ctx context.Context, base, samplePrefix string, addRecords func([]ComponentBackupRecord)) func(string) string {
	pathBuilder := func(p string) string { return base + "/" + url.PathEscape(p+"*") }
	reqCtx, cancel := context.WithTimeout(ctx, prefixQueryTimeout)
	recs, err := c.fetchBackupsAtURL(reqCtx, pathBuilder(samplePrefix))
	cancel()
	if err == nil {
		addRecords(recs)
		return pathBuilder
	}

	queryBuilder := func(p string) string { return base + "?prefix=" + url.QueryEscape(p+"*") }
	reqCtx2, cancel2 := context.WithTimeout(ctx, prefixQueryTimeout)
	recs2, err2 := c.fetchBackupsAtURL(reqCtx2, queryBuilder(samplePrefix))
	cancel2()
	if err2 == nil {
		addRecords(recs2)
		return queryBuilder
	}

	return nil
}

func runBounded(ctx context.Context, items []string, workers int, fn func(string)) {
	if len(items) == 0 {
		return
	}
	if workers <= 0 {
		workers = 1
	}
	if workers > len(items) {
		workers = len(items)
	}
	ch := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range ch {
				if ctx.Err() != nil {
					return
				}
				fn(item)
			}
		}()
	}
	for _, item := range items {
		if ctx.Err() != nil {
			break
		}
		ch <- item
	}
	close(ch)
	wg.Wait()
}

func uniqueNonEmpty(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func distinctPrefixes(ids []string, length int) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, id := range ids {
		if len(id) < length {
			continue
		}
		p := id[:length]
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}
	return out
}

func filterByPrefixes(ids, prefixes []string) []string {
	if len(prefixes) == 0 {
		return nil
	}
	var out []string
	for _, id := range ids {
		for _, p := range prefixes {
			if strings.HasPrefix(id, p) {
				out = append(out, id)
				break
			}
		}
	}
	return out
}

// parseBackupList decodes a component backup listing, tolerating a bare array,
// an object wrapping one, or a single backup status object (returned by
// GET /actuator/backups/{backupId}).
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
		if wrapErr := json.Unmarshal(body, &wrapped); wrapErr == nil && wrapped.Backups != nil {
			wire = wrapped.Backups
		} else {
			var single wireBackupRecord
			if singleErr := json.Unmarshal(body, &single); singleErr == nil && single.BackupID.String() != "" {
				wire = []wireBackupRecord{single}
			} else {
				return nil, fmt.Errorf("failed to parse backup list: %w", err)
			}
		}
	}

	out := make([]ComponentBackupRecord, 0, len(wire))
	for _, w := range wire {
		id := w.BackupID.String()
		if id == "" || strings.EqualFold(w.State, BackupStateDoesNotExist) {
			continue
		}
		rec := ComponentBackupRecord{
			BackupID:      id,
			State:         w.State,
			FailureReason: decodeFailureReason(w.FailureReason),
			Details:       w.Details,
		}
		out = append(out, rec)
	}
	return out, nil
}

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
