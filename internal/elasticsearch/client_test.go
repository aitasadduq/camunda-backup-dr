package elasticsearch

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
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
