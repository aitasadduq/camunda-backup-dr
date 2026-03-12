package elasticsearch

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func TestNewClient(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)

	client := NewClient("http://localhost:9200", "elastic", "password", httpClient, logger)

	if client == nil {
		t.Fatal("Expected client to be created")
	}
	if client.endpoint != "http://localhost:9200" {
		t.Errorf("Expected endpoint 'http://localhost:9200', got '%s'", client.endpoint)
	}
	if client.username != "elastic" {
		t.Errorf("Expected username 'elastic', got '%s'", client.username)
	}
}

func TestClient_authHeaders_WithCredentials(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)

	client := NewClient("http://localhost:9200", "elastic", "password", httpClient, logger)
	headers := client.authHeaders()

	expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte("elastic:password"))
	if headers["Authorization"] != expectedAuth {
		t.Errorf("Expected auth header '%s', got '%s'", expectedAuth, headers["Authorization"])
	}
}

func TestClient_authHeaders_WithoutCredentials(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)

	client := NewClient("http://localhost:9200", "", "", httpClient, logger)
	headers := client.authHeaders()

	if _, ok := headers["Authorization"]; ok {
		t.Error("Expected no Authorization header when credentials are empty")
	}
}

func TestCreateSnapshot_Success(t *testing.T) {
	repo := "backup-repo"
	snapshot := "snapshot-20240101-120000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("expected PUT, got %s", r.Method)
		}
		expectedPath := "/_snapshot/" + repo + "/" + snapshot
		if r.URL.Path != expectedPath {
			t.Errorf("unexpected path: expected %s, got %s", expectedPath, r.URL.Path)
		}
		if r.URL.Query().Get("wait_for_completion") != "false" {
			t.Errorf("expected wait_for_completion=false")
		}

		// Verify auth header
		user := "elastic"
		pass := "secret"
		expectedAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
		if r.Header.Get("Authorization") != expectedAuth {
			t.Errorf("missing or invalid auth header")
		}

		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"accepted": true}`))
	}))
	defer server.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)

	client := NewClient(server.URL, "elastic", "secret", httpClient, logger)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.CreateSnapshot(ctx, repo, snapshot); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestCreateSnapshot_ValidationErrors(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient("http://localhost:9200", "", "", httpClient, logger)

	ctx := context.Background()

	// Empty repository
	if err := client.CreateSnapshot(ctx, "", "snapshot"); err == nil {
		t.Error("expected error for empty repository")
	}

	// Empty snapshot name
	if err := client.CreateSnapshot(ctx, "repo", ""); err == nil {
		t.Error("expected error for empty snapshot name")
	}
}

func TestGetSnapshotStatus_Success(t *testing.T) {
	repo := "backup-repo"
	snapshot := "snapshot-20240101-120000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		expectedPath := "/_snapshot/" + repo + "/" + snapshot
		if r.URL.Path != expectedPath {
			t.Errorf("unexpected path: expected %s, got %s", expectedPath, r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"snapshots":[{"snapshot":"snapshot-20240101-120000","state":"SUCCESS"}]}`))
	}))
	defer server.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient(server.URL, "", "", httpClient, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	state, err := client.GetSnapshotStatus(ctx, repo, snapshot)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if state != SnapshotStateSuccess {
		t.Errorf("expected SUCCESS, got %s", state)
	}
}

func TestGetSnapshotStatus_InProgress(t *testing.T) {
	repo := "backup-repo"
	snapshot := "snapshot-20240101-120000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"snapshots":[{"snapshot":"snapshot-20240101-120000","state":"IN_PROGRESS"}]}`))
	}))
	defer server.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient(server.URL, "", "", httpClient, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	state, err := client.GetSnapshotStatus(ctx, repo, snapshot)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if state != SnapshotStateInProgress {
		t.Errorf("expected IN_PROGRESS, got %s", state)
	}
}

func TestGetSnapshotStatus_Failed(t *testing.T) {
	repo := "backup-repo"
	snapshot := "snapshot-20240101-120000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"snapshots":[{"snapshot":"snapshot-20240101-120000","state":"FAILED"}]}`))
	}))
	defer server.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient(server.URL, "", "", httpClient, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	state, err := client.GetSnapshotStatus(ctx, repo, snapshot)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if state != SnapshotStateFailed {
		t.Errorf("expected FAILED, got %s", state)
	}
}

func TestGetSnapshotStatus_NotFound(t *testing.T) {
	repo := "backup-repo"
	snapshot := "snapshot-20240101-120000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error":"snapshot not found"}`))
	}))
	defer server.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient(server.URL, "", "", httpClient, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := client.GetSnapshotStatus(ctx, repo, snapshot)
	if err == nil {
		t.Error("expected error for not found snapshot")
	}
}

func TestDeleteSnapshot_Success(t *testing.T) {
	repo := "backup-repo"
	snapshot := "snapshot-20240101-120000"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("expected DELETE, got %s", r.Method)
		}
		expectedPath := "/_snapshot/" + repo + "/" + snapshot
		if r.URL.Path != expectedPath {
			t.Errorf("unexpected path: expected %s, got %s", expectedPath, r.URL.Path)
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"acknowledged": true}`))
	}))
	defer server.Close()

	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient(server.URL, "", "", httpClient, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.DeleteSnapshot(ctx, repo, snapshot); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestClient_buildURL(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient("http://localhost:9200", "", "", httpClient, logger)

	// Test simple path
	url, err := client.buildURL("/_snapshot/repo/snap", nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if url != "http://localhost:9200/_snapshot/repo/snap" {
		t.Errorf("unexpected URL: %s", url)
	}

	// Test with query params
	url, err = client.buildURL("/_snapshot/repo/snap", map[string]string{"wait_for_completion": "false"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if url != "http://localhost:9200/_snapshot/repo/snap?wait_for_completion=false" {
		t.Errorf("unexpected URL: %s", url)
	}

	// Test with empty endpoint
	emptyClient := NewClient("", "", "", httpClient, logger)
	_, err = emptyClient.buildURL("/path", nil)
	if err == nil {
		t.Error("expected error for empty endpoint")
	}
}

// newTestClient creates a Client with zero retries for fast error-path tests.
func newTestClient(t *testing.T, serverURL string) *Client {
	t.Helper()
	logger := utils.NewLogger("debug")
	config := camunda.HTTPClientConfig{
		Timeout:       2 * time.Second,
		MaxRetries:    0,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 100 * time.Millisecond,
	}
	httpClient := camunda.NewHTTPClient(config, logger)
	return NewClient(serverURL, "", "", httpClient, logger)
}

// --- buildURL edge cases ---

func TestClient_buildURL_InvalidEndpoint(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient("://invalid-url", "", "", httpClient, logger)

	_, err := client.buildURL("/path", nil)
	if err == nil {
		t.Error("expected error for invalid endpoint URL")
	}
}

func TestClient_buildURL_MultipleQueryParams(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient("http://localhost:9200", "", "", httpClient, logger)

	u, err := client.buildURL("/_snapshot/repo/snap", map[string]string{
		"wait_for_completion": "true",
		"master_timeout":     "30s",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !strings.Contains(u, "wait_for_completion=true") {
		t.Errorf("missing wait_for_completion param in URL: %s", u)
	}
	if !strings.Contains(u, "master_timeout=30s") {
		t.Errorf("missing master_timeout param in URL: %s", u)
	}
}

func TestClient_buildURL_TrailingSlash(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient("http://localhost:9200/", "", "", httpClient, logger)

	u, err := client.buildURL("/_snapshot/repo/snap", nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if u != "http://localhost:9200/_snapshot/repo/snap" {
		t.Errorf("unexpected URL: %s", u)
	}
}

// --- CreateSnapshot error paths ---

func TestCreateSnapshot_NilHTTPClient(t *testing.T) {
	logger := utils.NewLogger("debug")
	client := NewClient("http://localhost:9200", "", "", nil, logger)

	err := client.CreateSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for nil HTTP client")
	}
	if !strings.Contains(err.Error(), "http client is not configured") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestCreateSnapshot_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	err := client.CreateSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for HTTP 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status code 500: %v", err)
	}
}

func TestCreateSnapshot_ConnectionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	serverURL := server.URL
	server.Close()

	client := newTestClient(t, serverURL)
	err := client.CreateSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for connection failure")
	}
	if !strings.Contains(err.Error(), "failed to create snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// --- GetSnapshotStatus error paths ---

func TestGetSnapshotStatus_ValidationErrors(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient("http://localhost:9200", "", "", httpClient, logger)
	ctx := context.Background()

	// Empty repository
	state, err := client.GetSnapshotStatus(ctx, "", "snapshot")
	if err == nil {
		t.Error("expected error for empty repository")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}

	// Empty snapshot name
	state, err = client.GetSnapshotStatus(ctx, "repo", "")
	if err == nil {
		t.Error("expected error for empty snapshot name")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
}

func TestGetSnapshotStatus_NilHTTPClient(t *testing.T) {
	logger := utils.NewLogger("debug")
	client := NewClient("http://localhost:9200", "", "", nil, logger)

	state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for nil HTTP client")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
}

func TestGetSnapshotStatus_HTTPError500(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for HTTP 500 response")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status code 500: %v", err)
	}
}

func TestGetSnapshotStatus_MalformedJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{invalid json`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for malformed JSON")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
	if !strings.Contains(err.Error(), "failed to parse snapshot status response") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestGetSnapshotStatus_EmptySnapshots(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"snapshots":[]}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for empty snapshots array")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
	if !strings.Contains(err.Error(), "missing snapshot data") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestGetSnapshotStatus_VariousStates(t *testing.T) {
	tests := []struct {
		name          string
		responseState string
		expectedState SnapshotState
	}{
		{"PARTIAL", "PARTIAL", SnapshotStatePartial},
		{"STARTED maps to IN_PROGRESS", "STARTED", SnapshotStateInProgress},
		{"unknown state", "SOMETHING_ELSE", SnapshotStateUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(fmt.Sprintf(`{"snapshots":[{"snapshot":"snap","state":"%s"}]}`, tt.responseState)))
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if state != tt.expectedState {
				t.Errorf("expected %s, got %s", tt.expectedState, state)
			}
		})
	}
}

func TestGetSnapshotStatus_ConnectionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	serverURL := server.URL
	server.Close()

	client := newTestClient(t, serverURL)
	state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for connection failure")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
}

// --- DeleteSnapshot error paths ---

func TestDeleteSnapshot_ValidationErrors(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient("http://localhost:9200", "", "", httpClient, logger)
	ctx := context.Background()

	if err := client.DeleteSnapshot(ctx, "", "snapshot"); err == nil {
		t.Error("expected error for empty repository")
	}

	if err := client.DeleteSnapshot(ctx, "repo", ""); err == nil {
		t.Error("expected error for empty snapshot name")
	}
}

func TestDeleteSnapshot_NilHTTPClient(t *testing.T) {
	logger := utils.NewLogger("debug")
	client := NewClient("http://localhost:9200", "", "", nil, logger)

	err := client.DeleteSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for nil HTTP client")
	}
	if !strings.Contains(err.Error(), "http client is not configured") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDeleteSnapshot_HTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal server error"}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	err := client.DeleteSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for HTTP 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status code 500: %v", err)
	}
}

func TestDeleteSnapshot_ConnectionError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	serverURL := server.URL
	server.Close()

	client := newTestClient(t, serverURL)
	err := client.DeleteSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for connection failure")
	}
	if !strings.Contains(err.Error(), "failed to delete snapshot") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestDeleteSnapshot_Accepted(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"acknowledged": true}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	err := client.DeleteSnapshot(context.Background(), "repo", "snap")
	if err != nil {
		t.Fatalf("expected no error for HTTP 202, got %v", err)
	}
}

func TestCreateSnapshot_OKStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"snapshot":{"snapshot":"snap","state":"SUCCESS"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	err := client.CreateSnapshot(context.Background(), "repo", "snap")
	if err != nil {
		t.Fatalf("expected no error for HTTP 200, got %v", err)
	}
}

func TestCreateSnapshot_CreatedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"snapshot":{"snapshot":"snap","state":"SUCCESS"}}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	err := client.CreateSnapshot(context.Background(), "repo", "snap")
	if err != nil {
		t.Fatalf("expected no error for HTTP 201, got %v", err)
	}
}

// --- buildURL error paths within snapshot functions ---

func TestCreateSnapshot_BuildURLError(t *testing.T) {
	logger := utils.NewLogger("debug")
	config := camunda.HTTPClientConfig{
		Timeout:       2 * time.Second,
		MaxRetries:    0,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 100 * time.Millisecond,
	}
	httpClient := camunda.NewHTTPClient(config, logger)
	client := NewClient("", "", "", httpClient, logger) // empty endpoint → buildURL fails

	err := client.CreateSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error when buildURL fails")
	}
	if !strings.Contains(err.Error(), "endpoint is empty") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestGetSnapshotStatus_BuildURLError(t *testing.T) {
	logger := utils.NewLogger("debug")
	config := camunda.HTTPClientConfig{
		Timeout:       2 * time.Second,
		MaxRetries:    0,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 100 * time.Millisecond,
	}
	httpClient := camunda.NewHTTPClient(config, logger)
	client := NewClient("", "", "", httpClient, logger)

	state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error when buildURL fails")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
}

func TestDeleteSnapshot_BuildURLError(t *testing.T) {
	logger := utils.NewLogger("debug")
	config := camunda.HTTPClientConfig{
		Timeout:       2 * time.Second,
		MaxRetries:    0,
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 100 * time.Millisecond,
	}
	httpClient := camunda.NewHTTPClient(config, logger)
	client := NewClient("", "", "", httpClient, logger)

	err := client.DeleteSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error when buildURL fails")
	}
	if !strings.Contains(err.Error(), "endpoint is empty") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// --- Non-retryable HTTP error paths (400/403 pass through HTTPClient) ---

func TestCreateSnapshot_BadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	err := client.CreateSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for HTTP 400 response")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status code 400: %v", err)
	}
}

func TestGetSnapshotStatus_Forbidden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":"forbidden"}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	state, err := client.GetSnapshotStatus(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for HTTP 403 response")
	}
	if state != SnapshotStateUnknown {
		t.Errorf("expected UNKNOWN state, got %s", state)
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("error should mention status code 403: %v", err)
	}
}

func TestDeleteSnapshot_BadRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"bad request"}`))
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	err := client.DeleteSnapshot(context.Background(), "repo", "snap")
	if err == nil {
		t.Error("expected error for HTTP 400 response")
	}
	if !strings.Contains(err.Error(), "400") {
		t.Errorf("error should mention status code 400: %v", err)
	}
}
