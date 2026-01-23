package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// SnapshotState represents Elasticsearch snapshot state.
type SnapshotState string

const (
	SnapshotStateInProgress SnapshotState = "IN_PROGRESS"
	SnapshotStateSuccess    SnapshotState = "SUCCESS"
	SnapshotStateFailed     SnapshotState = "FAILED"
	SnapshotStatePartial    SnapshotState = "PARTIAL"
	SnapshotStateUnknown    SnapshotState = "UNKNOWN"
)

// SnapshotClient defines Elasticsearch snapshot operations.
type SnapshotClient interface {
	CreateSnapshot(ctx context.Context, repository, snapshot string) error
	GetSnapshotStatus(ctx context.Context, repository, snapshot string) (SnapshotState, error)
	DeleteSnapshot(ctx context.Context, repository, snapshot string) error
}

// CreateSnapshot creates an Elasticsearch snapshot in the specified repository.
// This triggers the snapshot asynchronously (wait_for_completion=false).
func (c *Client) CreateSnapshot(ctx context.Context, repository, snapshot string) error {
	if repository == "" {
		return fmt.Errorf("snapshot repository is required")
	}
	if snapshot == "" {
		return fmt.Errorf("snapshot name is required")
	}
	if c.httpClient == nil {
		return fmt.Errorf("http client is not configured")
	}

	urlPath := fmt.Sprintf("/_snapshot/%s/%s", repository, snapshot)
	fullURL, err := c.buildURL(urlPath, map[string]string{
		"wait_for_completion": "false",
	})
	if err != nil {
		return err
	}

	c.logger.Debug("Creating ES snapshot: PUT %s", fullURL)

	resp, err := c.httpClient.Put(ctx, fullURL, c.authHeaders(), map[string]interface{}{
		"ignore_unavailable":   true,
		"include_global_state": false,
	})
	if err != nil {
		return fmt.Errorf("failed to create snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("snapshot creation failed with status %d: %s", resp.StatusCode, string(body))
	}

	c.logger.Debug("ES snapshot creation initiated successfully")
	return nil
}

// GetSnapshotStatus retrieves the snapshot status from Elasticsearch.
func (c *Client) GetSnapshotStatus(ctx context.Context, repository, snapshot string) (SnapshotState, error) {
	if repository == "" {
		return SnapshotStateUnknown, fmt.Errorf("snapshot repository is required")
	}
	if snapshot == "" {
		return SnapshotStateUnknown, fmt.Errorf("snapshot name is required")
	}
	if c.httpClient == nil {
		return SnapshotStateUnknown, fmt.Errorf("http client is not configured")
	}

	urlPath := fmt.Sprintf("/_snapshot/%s/%s", repository, snapshot)
	fullURL, err := c.buildURL(urlPath, nil)
	if err != nil {
		return SnapshotStateUnknown, err
	}

	c.logger.Debug("Getting ES snapshot status: GET %s", fullURL)

	resp, err := c.httpClient.Get(ctx, fullURL, c.authHeaders())
	if err != nil {
		return SnapshotStateUnknown, fmt.Errorf("failed to get snapshot status: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return SnapshotStateUnknown, fmt.Errorf("failed to read snapshot status response: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return SnapshotStateUnknown, fmt.Errorf("snapshot not found: %s/%s", repository, snapshot)
	}

	if resp.StatusCode != http.StatusOK {
		return SnapshotStateUnknown, fmt.Errorf("snapshot status failed with status %d: %s", resp.StatusCode, string(body))
	}

	var payload struct {
		Snapshots []struct {
			Snapshot string `json:"snapshot"`
			State    string `json:"state"`
		} `json:"snapshots"`
	}

	if err := json.Unmarshal(body, &payload); err != nil {
		return SnapshotStateUnknown, fmt.Errorf("failed to parse snapshot status response: %w", err)
	}

	if len(payload.Snapshots) == 0 {
		return SnapshotStateUnknown, fmt.Errorf("snapshot status response missing snapshot data")
	}

	state := payload.Snapshots[0].State
	c.logger.Debug("ES snapshot state: %s", state)

	switch state {
	case "SUCCESS":
		return SnapshotStateSuccess, nil
	case "FAILED":
		return SnapshotStateFailed, nil
	case "PARTIAL":
		return SnapshotStatePartial, nil
	case "IN_PROGRESS", "STARTED":
		return SnapshotStateInProgress, nil
	default:
		return SnapshotStateUnknown, nil
	}
}

// DeleteSnapshot deletes an Elasticsearch snapshot.
func (c *Client) DeleteSnapshot(ctx context.Context, repository, snapshot string) error {
	if repository == "" {
		return fmt.Errorf("snapshot repository is required")
	}
	if snapshot == "" {
		return fmt.Errorf("snapshot name is required")
	}
	if c.httpClient == nil {
		return fmt.Errorf("http client is not configured")
	}

	urlPath := fmt.Sprintf("/_snapshot/%s/%s", repository, snapshot)
	fullURL, err := c.buildURL(urlPath, nil)
	if err != nil {
		return err
	}

	c.logger.Debug("Deleting ES snapshot: DELETE %s", fullURL)

	resp, err := c.httpClient.Delete(ctx, fullURL, c.authHeaders())
	if err != nil {
		return fmt.Errorf("failed to delete snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("snapshot deletion failed with status %d: %s", resp.StatusCode, string(body))
	}

	c.logger.Debug("ES snapshot deleted successfully")
	return nil
}
