package camunda

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func setupTestHTTPClient() (*HTTPClient, *utils.Logger) {
	logger := utils.NewLogger("debug")
	config := DefaultHTTPClientConfig()
	config.Timeout = 5 * time.Second
	config.MaxRetries = 2
	config.RetryDelay = 100 * time.Millisecond
	config.MaxRetryDelay = 500 * time.Millisecond
	return NewHTTPClient(config, logger), logger
}

func TestHTTPClient_Get_Success(t *testing.T) {
	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status": "ok"}`))
	}))
	defer server.Close()

	client, _ := setupTestHTTPClient()
	ctx := context.Background()

	resp, err := client.Get(ctx, server.URL, nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestHTTPClient_Get_RetryOnServerError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 2 {
			w.WriteHeader(http.StatusInternalServerError)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	client, _ := setupTestHTTPClient()
	ctx := context.Background()

	resp, err := client.Get(ctx, server.URL, nil)
	if err != nil {
		t.Fatalf("Unexpected error after retry: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200 after retry, got %d", resp.StatusCode)
	}

	if attempts < 2 {
		t.Errorf("Expected at least 2 attempts, got %d", attempts)
	}
}

func TestHTTPClient_Get_Timeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create a client with a very short timeout so this request will time out
	logger := utils.NewLogger("debug")
	config := DefaultHTTPClientConfig()
	config.Timeout = 100 * time.Millisecond
	client := NewHTTPClient(config, logger)

	ctx := context.Background()

	_, err := client.Get(ctx, server.URL, nil)
	if err == nil {
		t.Error("Expected timeout error")
	}
}

func TestHTTPClient_Post_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id": "123"}`))
	}))
	defer server.Close()

	client, _ := setupTestHTTPClient()
	ctx := context.Background()

	body := map[string]string{"name": "test"}
	resp, err := client.Post(ctx, server.URL, nil, body)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}
}

func TestHTTPClient_ShouldRetry(t *testing.T) {
	testCases := []struct {
		statusCode int
		shouldRetry bool
	}{
		{http.StatusOK, false},
		{http.StatusCreated, false},
		{http.StatusBadRequest, false},
		{http.StatusNotFound, false},
		{http.StatusRequestTimeout, true},
		{http.StatusTooManyRequests, true},
		{http.StatusInternalServerError, true},
		{http.StatusBadGateway, true},
		{http.StatusServiceUnavailable, true},
	}

	for _, tc := range testCases {
		result := shouldRetry(tc.statusCode)
		if result != tc.shouldRetry {
			t.Errorf("Status %d: expected shouldRetry=%v, got %v", tc.statusCode, tc.shouldRetry, result)
		}
	}
}

func TestReadJSONResponse_Success(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"id": "123", "name": "test"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}

	var result map[string]interface{}
	if err := ReadJSONResponse(resp, &result); err != nil {
		t.Fatalf("Failed to read JSON response: %v", err)
	}

	if result["id"] != "123" {
		t.Errorf("Expected id '123', got %v", result["id"])
	}
}

func TestReadJSONResponse_ErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "not found"}`))
	}))
	defer server.Close()

	client := &http.Client{}
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Failed to make request: %v", err)
	}

	var result map[string]interface{}
	err = ReadJSONResponse(resp, &result)
	if err == nil {
		t.Error("Expected error for non-2xx status code")
	}
}
