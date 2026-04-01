package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func setupCheckEndpointHandlers(t *testing.T) *Handlers {
	t.Helper()
	logger := utils.NewLogger("debug")
	// Disable SSRF protection in tests since httptest servers bind to 127.0.0.1
	origBlockedHost := isBlockedHost
	isBlockedHost = func(string) bool { return false }
	t.Cleanup(func() { isBlockedHost = origBlockedHost })
	// Also disable DialContext-level SSRF check for local test servers
	t.Setenv("PROBE_ALLOW_PRIVATE_IPS", "true")
	return NewHandlers(nil, nil, nil, nil, nil, nil, logger, &config.Config{})
}

func doCheckEndpoint(t *testing.T, h *Handlers, body EndpointCheckRequest) (*httptest.ResponseRecorder, EndpointCheckResponse) {
	t.Helper()
	jsonBody, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/check-endpoint", bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CheckEndpointHandler(rr, req)

	var resp EndpointCheckResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	return rr, resp
}

func TestCheckEndpointHandler_InvalidJSON(t *testing.T) {
	h := setupCheckEndpointHandlers(t)

	req := httptest.NewRequest(http.MethodPost, "/api/check-endpoint", bytes.NewReader([]byte("not json")))
	rr := httptest.NewRecorder()
	h.CheckEndpointHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rr.Code)
	}
}

func TestCheckEndpointHandler_EmptyURL(t *testing.T) {
	h := setupCheckEndpointHandlers(t)

	jsonBody, _ := json.Marshal(EndpointCheckRequest{URL: "", Type: "camunda"})
	req := httptest.NewRequest(http.MethodPost, "/api/check-endpoint", bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CheckEndpointHandler(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", rr.Code)
	}

	var errResp ErrorResponse
	if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
		t.Fatalf("Failed to decode error response: %v", err)
	}
	if errResp.Error != "validation_error" {
		t.Errorf("Expected error type %q, got %q", "validation_error", errResp.Error)
	}
	if errResp.Message == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestCheckEndpointHandler_InvalidURL(t *testing.T) {
	h := setupCheckEndpointHandlers(t)

	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: "://bad-url", Type: "camunda"})

	if resp.Status != EndpointStatusUnreachable {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnreachable, resp.Status)
	}
}

func TestCheckEndpointHandler_SSRFBlocksPrivateIPs(t *testing.T) {
	logger := utils.NewLogger("debug")
	h := NewHandlers(nil, nil, nil, nil, nil, nil, logger, nil)
	// Ensure SSRF protection is active (env var unset)
	t.Setenv("PROBE_ALLOW_PRIVATE_IPS", "")
	// Re-enable SSRF protection for this test
	origBlockedHost := isBlockedHost
	t.Cleanup(func() { isBlockedHost = origBlockedHost })

	tests := []struct {
		name string
		url  string
	}{
		{"loopback", "http://127.0.0.1:8080"},
		{"private-10", "http://10.0.0.1:8080"},
		{"private-172", "http://172.16.0.1:8080"},
		{"private-192", "http://192.168.1.1:8080"},
		{"link-local", "http://169.254.1.1:8080"},
		{"ipv6-loopback", "http://[::1]:8080"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jsonBody, _ := json.Marshal(EndpointCheckRequest{URL: tc.url, Type: "camunda"})
			req := httptest.NewRequest(http.MethodPost, "/api/check-endpoint", bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			h.CheckEndpointHandler(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("Expected status 400 for %s, got %d", tc.url, rr.Code)
			}
		})
	}
}

func TestCheckEndpointHandler_BlocksNonHTTPSchemes(t *testing.T) {
	h := setupCheckEndpointHandlers(t)

	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: "ftp://example.com/file", Type: "camunda"})

	if resp.Status != EndpointStatusUnreachable {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnreachable, resp.Status)
	}
}

func TestCheckEndpointHandler_SSRFBypassWithEnvVar(t *testing.T) {
	t.Setenv("PROBE_ALLOW_PRIVATE_IPS", "true")
	// Use real isBlockedHost (not the disabled one from setupCheckEndpointHandlers)
	logger := utils.NewLogger("debug")
	h := NewHandlers(nil, nil, nil, nil, nil, nil, logger, nil)

	// Start a local server on loopback — this would normally be blocked
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	jsonBody, _ := json.Marshal(EndpointCheckRequest{URL: srv.URL, Type: "camunda"})
	req := httptest.NewRequest(http.MethodPost, "/api/check-endpoint", bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CheckEndpointHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200 with PROBE_ALLOW_PRIVATE_IPS=true, got %d", rr.Code)
	}

	var resp EndpointCheckResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected %q with PROBE_ALLOW_PRIVATE_IPS=true, got %q", EndpointStatusConnected, resp.Status)
	}
}

// ---- Camunda tests ----

func TestCheckEndpointHandler_CamundaConnected(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"version":"8.3.0"}`))
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: srv.URL, Type: "camunda"})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200, got %d", resp.StatusCode)
	}
}

func TestCheckEndpointHandler_CamundaUnauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: srv.URL, Type: "camunda"})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
}

func TestCheckEndpointHandler_CamundaForbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: srv.URL, Type: "camunda"})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
}

func TestCheckEndpointHandler_CamundaUnreachable(t *testing.T) {
	h := setupCheckEndpointHandlers(t)
	// Start and immediately close a server to get a guaranteed-unreachable URL
	ts := httptest.NewServer(http.NotFoundHandler())
	closedURL := ts.URL
	ts.Close()

	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: closedURL, Type: "camunda"})

	if resp.Status != EndpointStatusUnreachable {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnreachable, resp.Status)
	}
}

// ---- Elasticsearch tests ----

func TestCheckEndpointHandler_ESConnectedWithAuth(t *testing.T) {
	// Server requires auth: returns 401 without creds, 200 with correct creds
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"cluster_name":"test","status":"green"}`))
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:      srv.URL,
		Type:     "elasticsearch",
		Username: "elastic",
		Password: "password",
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
	if resp.StatusCode != 200 {
		t.Errorf("Expected status code 200, got %d", resp.StatusCode)
	}
}

func TestCheckEndpointHandler_ESNoAuthRequired(t *testing.T) {
	// Server doesn't require auth at all
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"cluster_name":"test","status":"green"}`))
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:  srv.URL,
		Type: "elasticsearch",
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
}

func TestCheckEndpointHandler_ESUnauthenticatedNoCreds(t *testing.T) {
	// Server requires auth, no credentials provided
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"error":"security_exception"}`))
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:  srv.URL,
		Type: "elasticsearch",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
}

func TestCheckEndpointHandler_ESUsernameNoPassword(t *testing.T) {
	// Username provided but password env var not set
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:      srv.URL,
		Type:     "elasticsearch",
		Username: "elastic",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
	if resp.Message != "Reachable but not authenticated (password env var not set)" {
		t.Errorf("Expected password env var message, got %q", resp.Message)
	}
}

func TestCheckEndpointHandler_ESPasswordFromEnvVar(t *testing.T) {
	// Server requires auth, password is in env var
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "Basic "+basicAuth("elastic", "secret123") {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"cluster_name":"test"}`))
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	os.Setenv("ELASTICSEARCH_PASSWORD_MY_INSTANCE", "secret123")
	defer os.Unsetenv("ELASTICSEARCH_PASSWORD_MY_INSTANCE")

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        srv.URL,
		Type:       "elasticsearch",
		InstanceID: "my-instance",
		Username:   "elastic",
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
	if resp.Message != "Connected and authenticated" {
		t.Errorf("Expected 'Connected and authenticated', got %q", resp.Message)
	}
}

func TestCheckEndpointHandler_ESWrongCredentials(t *testing.T) {
	// Server always returns 401 — wrong credentials
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:      srv.URL,
		Type:     "elasticsearch",
		Username: "wrong",
		Password: "wrong",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
	if resp.Message != "Reachable but not authenticated (invalid credentials)" {
		t.Errorf("Expected invalid credentials message, got %q", resp.Message)
	}
}

func TestCheckEndpointHandler_ESNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:  srv.URL,
		Type: "elasticsearch",
	})

	// 503 with no credentials — reachable with HTTP status
	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
}

// ---- S3 tests ----

func TestCheckEndpointHandler_S3NoCredentials(t *testing.T) {
	// MinIO-style: returns 200 at root (web console) even without auth
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: srv.URL, Type: "s3"})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q (S3 returning 200 without credentials should be unauthenticated)", EndpointStatusUnauthenticated, resp.Status)
	}
}

func TestCheckEndpointHandler_S3AccessKeyNoSecretKey(t *testing.T) {
	// Access key provided but no secret key env var
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:       srv.URL,
		Type:      "s3",
		AccessKey: "AKIAIOSFODNN7EXAMPLE",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q (access key without secret key should be unauthenticated)", EndpointStatusUnauthenticated, resp.Status)
	}
	if resp.Message != "Reachable but not authenticated (secret key env var not set)" {
		t.Errorf("Expected secret key message, got %q", resp.Message)
	}
}

func TestCheckEndpointHandler_S3AuthenticatedViaSDK(t *testing.T) {
	// Mock the SDK-based probe to simulate successful authentication
	origProbe := probeS3WithSDK
	defer func() { probeS3WithSDK = origProbe }()

	probeS3WithSDK = func(endpoint, accessKey, secretKey string) EndpointCheckResponse {
		return EndpointCheckResponse{
			Status:     EndpointStatusConnected,
			StatusCode: http.StatusOK,
			Message:    "Connected and authenticated",
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	os.Setenv("S3_SECRETKEY_MY_INSTANCE", "secret123")
	defer os.Unsetenv("S3_SECRETKEY_MY_INSTANCE")

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        srv.URL,
		Type:       "s3",
		InstanceID: "my-instance",
		AccessKey:  "AKIAIOSFODNN7EXAMPLE",
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
	if resp.Message != "Connected and authenticated" {
		t.Errorf("Expected 'Connected and authenticated', got %q", resp.Message)
	}
}

func TestCheckEndpointHandler_S3AuthFailedViaSDK(t *testing.T) {
	// Mock the SDK-based probe to simulate authentication failure
	origProbe := probeS3WithSDK
	defer func() { probeS3WithSDK = origProbe }()

	probeS3WithSDK = func(endpoint, accessKey, secretKey string) EndpointCheckResponse {
		return EndpointCheckResponse{
			Status:     EndpointStatusUnauthenticated,
			StatusCode: http.StatusForbidden,
			Message:    "Reachable but not authenticated (invalid credentials)",
		}
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	os.Setenv("S3_SECRETKEY_MY_INSTANCE", "wrongsecret")
	defer os.Unsetenv("S3_SECRETKEY_MY_INSTANCE")

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        srv.URL,
		Type:       "s3",
		InstanceID: "my-instance",
		AccessKey:  "AKIAIOSFODNN7EXAMPLE",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
}

func TestCheckEndpointHandler_S3Forbidden(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`<Error><Code>AccessDenied</Code></Error>`))
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: srv.URL, Type: "s3"})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
}

func TestCheckEndpointHandler_S3Unreachable(t *testing.T) {
	h := setupCheckEndpointHandlers(t)
	// Start and immediately close a server to get a guaranteed-unreachable URL
	ts := httptest.NewServer(http.NotFoundHandler())
	closedURL := ts.URL
	ts.Close()

	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: closedURL, Type: "s3"})

	if resp.Status != EndpointStatusUnreachable {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnreachable, resp.Status)
	}
}

// ---- S3 SDK probe unit tests ----

func TestProbeS3WithSDK_Success(t *testing.T) {
	origProbe := probeS3WithSDK
	defer func() { probeS3WithSDK = origProbe }()

	probeS3WithSDK = func(endpoint, accessKey, secretKey string) EndpointCheckResponse {
		if accessKey == "validKey" && secretKey == "validSecret" {
			return EndpointCheckResponse{
				Status:     EndpointStatusConnected,
				StatusCode: http.StatusOK,
				Message:    "Connected and authenticated",
			}
		}
		return EndpointCheckResponse{
			Status:     EndpointStatusUnauthenticated,
			StatusCode: http.StatusForbidden,
			Message:    "Reachable but not authenticated (invalid credentials)",
		}
	}

	// Valid credentials
	resp := probeS3WithSDK("http://localhost:9000", "validKey", "validSecret")
	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected %q, got %q", EndpointStatusConnected, resp.Status)
	}

	// Invalid credentials
	resp = probeS3WithSDK("http://localhost:9000", "bad", "bad")
	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
}

func TestProbeS3_CredentialChecks(t *testing.T) {
	// Allow connections to local httptest server
	t.Setenv("PROBE_ALLOW_PRIVATE_IPS", "true")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	origProbe := probeS3WithSDK
	defer func() { probeS3WithSDK = origProbe }()
	probeS3WithSDK = func(endpoint, accessKey, secretKey string) EndpointCheckResponse {
		return EndpointCheckResponse{
			Status:     EndpointStatusConnected,
			StatusCode: http.StatusOK,
			Message:    "Connected and authenticated",
		}
	}

	tests := []struct {
		name           string
		accessKey      string
		secretKey      string
		expectedStatus string
	}{
		{"no credentials at all", "", "", EndpointStatusUnauthenticated},
		{"access key only", "AKIA123", "", EndpointStatusUnauthenticated},
		{"secret key only", "", "secret", EndpointStatusUnauthenticated},
		{"both credentials", "AKIA123", "secret", EndpointStatusConnected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := probeS3(srv.URL, tt.accessKey, tt.secretKey)
			if resp.Status != tt.expectedStatus {
				t.Errorf("Expected %q, got %q (message: %s)", tt.expectedStatus, resp.Status, resp.Message)
			}
		})
	}
}

// ---- Generic / misc tests ----

func TestCheckEndpointHandler_GenericType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: srv.URL, Type: "unknown"})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
}

func TestCheckEndpointHandler_EmptyType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{URL: srv.URL, Type: ""})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q", EndpointStatusConnected, resp.Status)
	}
}

func TestCheckEndpointHandler_InstanceIDEnvVarLookup(t *testing.T) {
	// Verify that instance_id triggers env var lookup for ES password
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth == "Basic "+basicAuth("elastic", "from-env") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	os.Setenv("ELASTICSEARCH_PASSWORD_TEST_CLUSTER", "from-env")
	defer os.Unsetenv("ELASTICSEARCH_PASSWORD_TEST_CLUSTER")

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        srv.URL,
		Type:       "elasticsearch",
		InstanceID: "test-cluster",
		Username:   "elastic",
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q (message: %s)", EndpointStatusConnected, resp.Status, resp.Message)
	}
}

func TestCheckEndpointHandler_InstanceIDEnvVarNotSet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	// Ensure env var is NOT set
	os.Unsetenv("ELASTICSEARCH_PASSWORD_NO_ENV")

	h := setupCheckEndpointHandlers(t)
	_, resp := doCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        srv.URL,
		Type:       "elasticsearch",
		InstanceID: "no-env",
		Username:   "elastic",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q", EndpointStatusUnauthenticated, resp.Status)
	}
	if resp.Message != "Reachable but not authenticated (password env var not set)" {
		t.Errorf("Expected password env var not set message, got %q", resp.Message)
	}
}

func TestSummarizeError(t *testing.T) {
	tests := []struct {
		name     string
		errMsg   string
		expected string
	}{
		{"nil error", "", ""},
		{"dns failure", "dial tcp: lookup bad.host: no such host", "DNS resolution failed"},
		{"connection refused", "dial tcp 127.0.0.1:9999: connection refused", "Connection refused"},
		{"timeout", "context deadline exceeded", "Connection timed out"},
		{"io timeout", "i/o timeout", "Connection timed out"},
		{"tls error", "x509: certificate signed by unknown authority", "TLS/SSL certificate error"},
		{"generic error", "something weird happened", "Network error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			if tt.errMsg != "" {
				err = &testError{msg: tt.errMsg}
			}
			result := summarizeError(err)
			if result != tt.expected {
				t.Errorf("summarizeError(%q) = %q, want %q", tt.errMsg, result, tt.expected)
			}
		})
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestBasicAuth(t *testing.T) {
	result := basicAuth("elastic", "password")
	expected := "ZWxhc3RpYzpwYXNzd29yZA=="
	if result != expected {
		t.Errorf("basicAuth(\"elastic\", \"password\") = %q, want %q", result, expected)
	}
}

func TestCheckEndpointHandler_RouteRegistered(t *testing.T) {
	// Disable SSRF protection since httptest servers bind to 127.0.0.1
	origBlockedHost := isBlockedHost
	isBlockedHost = func(string) bool { return false }
	t.Cleanup(func() { isBlockedHost = origBlockedHost })
	t.Setenv("PROBE_ALLOW_PRIVATE_IPS", "true")

	logger := utils.NewLogger("debug")
	handlers := NewHandlers(&mockCamundaManager{}, nil, nil, nil, nil, nil, logger, nil)
	router := NewRouter(handlers, nil, "/")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	body, _ := json.Marshal(EndpointCheckRequest{URL: srv.URL, Type: "camunda"})
	req := httptest.NewRequest(http.MethodPost, "/api/check-endpoint", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status 200 from routed request, got %d", rr.Code)
	}

	var resp EndpointCheckResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected connected status from routed request, got %q", resp.Status)
	}
}
