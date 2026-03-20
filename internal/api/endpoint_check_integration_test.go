//go:build integration
// +build integration

package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Integration tests for the endpoint check handler.
// These tests probe real Elasticsearch and S3 (MinIO) instances.
//
// Required infrastructure:
//   - Elasticsearch at ES_ENDPOINT (default: http://localhost:9200)
//   - S3/MinIO at S3_ENDPOINT (default: http://localhost:9000)
//
// Environment variables:
//   ES_ENDPOINT, ES_USERNAME, ES_PASSWORD
//   S3_ENDPOINT, S3_ACCESS_KEY, S3_SECRET_KEY

// ---- helpers ----

func getESConfig() (endpoint, username, password string) {
	endpoint = os.Getenv("ES_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9200"
	}
	username = os.Getenv("ES_USERNAME")
	if username == "" {
		username = "elastic"
	}
	password = os.Getenv("ES_PASSWORD")
	if password == "" {
		password = "localelastic12345"
	}
	return
}

func getS3Config() (endpoint, accessKey, secretKey string) {
	endpoint = os.Getenv("S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	accessKey = os.Getenv("S3_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "localminio"
	}
	secretKey = os.Getenv("S3_SECRET_KEY")
	if secretKey == "" {
		secretKey = "localminio12345"
	}
	return
}

func setupIntegrationHandlers(t *testing.T) *Handlers {
	t.Helper()
	logger := utils.NewLogger("debug")
	return NewHandlers(nil, nil, nil, nil, nil, nil, logger, nil)
}

func doIntegrationCheckEndpoint(t *testing.T, h *Handlers, body EndpointCheckRequest) EndpointCheckResponse {
	t.Helper()
	jsonBody, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/api/check-endpoint", bytes.NewReader(jsonBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.CheckEndpointHandler(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected HTTP 200 from handler, got %d", rr.Code)
	}

	var resp EndpointCheckResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	return resp
}

func skipIfUnreachable(t *testing.T, endpoint, service string) {
	t.Helper()
	resp, err := probeHTTPClient.Get(endpoint)
	if err != nil {
		t.Skipf("Skipping: %s at %s is not reachable: %v", service, endpoint, err)
	}
	resp.Body.Close()
}

// ============================================================
// Elasticsearch integration tests
// ============================================================

func TestIntegration_ESProbe_Connected(t *testing.T) {
	esEndpoint, esUsername, esPassword := getESConfig()
	skipIfUnreachable(t, esEndpoint, "Elasticsearch")

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:      esEndpoint,
		Type:     "elasticsearch",
		Username: esUsername,
		Password: esPassword,
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q (message: %s)", EndpointStatusConnected, resp.Status, resp.Message)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", resp.StatusCode)
	}
	if resp.Message != "Connected and authenticated" {
		t.Errorf("Expected 'Connected and authenticated', got %q", resp.Message)
	}
}

func TestIntegration_ESProbe_WrongPassword(t *testing.T) {
	esEndpoint, esUsername, _ := getESConfig()
	skipIfUnreachable(t, esEndpoint, "Elasticsearch")

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:      esEndpoint,
		Type:     "elasticsearch",
		Username: esUsername,
		Password: "definitely-wrong-password",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q (message: %s)", EndpointStatusUnauthenticated, resp.Status, resp.Message)
	}
	if !strings.Contains(resp.Message, "invalid credentials") {
		t.Errorf("Expected 'invalid credentials' in message, got %q", resp.Message)
	}
}

func TestIntegration_ESProbe_NoCredentials(t *testing.T) {
	esEndpoint, _, _ := getESConfig()
	skipIfUnreachable(t, esEndpoint, "Elasticsearch")

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:  esEndpoint,
		Type: "elasticsearch",
	})

	// Depending on ES configuration, this is either "connected (no auth)" or "unauthenticated"
	// Both are valid — just ensure the handler doesn't panic or return unreachable
	if resp.Status == EndpointStatusUnreachable {
		t.Errorf("ES is reachable but got unreachable status (message: %s)", resp.Message)
	}
	t.Logf("ES with no credentials: status=%s message=%q", resp.Status, resp.Message)
}

func TestIntegration_ESProbe_UsernameButNoPassword(t *testing.T) {
	esEndpoint, esUsername, _ := getESConfig()
	skipIfUnreachable(t, esEndpoint, "Elasticsearch")

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:      esEndpoint,
		Type:     "elasticsearch",
		Username: esUsername,
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q (username without password), got %q (message: %s)",
			EndpointStatusUnauthenticated, resp.Status, resp.Message)
	}
	if !strings.Contains(resp.Message, "password env var not set") {
		t.Errorf("Expected 'password env var not set' in message, got %q", resp.Message)
	}
}

func TestIntegration_ESProbe_PasswordFromEnvVar(t *testing.T) {
	esEndpoint, esUsername, esPassword := getESConfig()
	skipIfUnreachable(t, esEndpoint, "Elasticsearch")

	// Set the env var the handler will look up
	instanceID := "integration-test"
	envKey := "ELASTICSEARCH_PASSWORD_INTEGRATION_TEST"
	os.Setenv(envKey, esPassword)
	defer os.Unsetenv(envKey)

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        esEndpoint,
		Type:       "elasticsearch",
		InstanceID: instanceID,
		Username:   esUsername,
		// No Password field — handler should resolve from env var
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q (password from env var), got %q (message: %s)",
			EndpointStatusConnected, resp.Status, resp.Message)
	}
	if resp.Message != "Connected and authenticated" {
		t.Errorf("Expected 'Connected and authenticated', got %q", resp.Message)
	}
}

func TestIntegration_ESProbe_WrongPasswordInEnvVar(t *testing.T) {
	esEndpoint, esUsername, _ := getESConfig()
	skipIfUnreachable(t, esEndpoint, "Elasticsearch")

	instanceID := "integration-wrong"
	envKey := "ELASTICSEARCH_PASSWORD_INTEGRATION_WRONG"
	os.Setenv(envKey, "totally-wrong")
	defer os.Unsetenv(envKey)

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        esEndpoint,
		Type:       "elasticsearch",
		InstanceID: instanceID,
		Username:   esUsername,
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q (message: %s)", EndpointStatusUnauthenticated, resp.Status, resp.Message)
	}
	if !strings.Contains(resp.Message, "invalid credentials") {
		t.Errorf("Expected 'invalid credentials' in message, got %q", resp.Message)
	}
}

// ============================================================
// S3 / MinIO integration tests
// ============================================================

func TestIntegration_S3Probe_Connected(t *testing.T) {
	s3Endpoint, s3AccessKey, s3SecretKey := getS3Config()
	skipIfUnreachable(t, s3Endpoint, "S3/MinIO")

	// Set the secret key env var so the handler can resolve it
	instanceID := "integration-s3"
	envKey := "S3_SECRETKEY_INTEGRATION_S3"
	os.Setenv(envKey, s3SecretKey)
	defer os.Unsetenv(envKey)

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        s3Endpoint,
		Type:       "s3",
		InstanceID: instanceID,
		AccessKey:  s3AccessKey,
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q, got %q (message: %s)", EndpointStatusConnected, resp.Status, resp.Message)
	}
	if resp.Message != "Connected and authenticated" {
		t.Errorf("Expected 'Connected and authenticated', got %q", resp.Message)
	}
}

func TestIntegration_S3Probe_WrongCredentials(t *testing.T) {
	s3Endpoint, _, _ := getS3Config()
	skipIfUnreachable(t, s3Endpoint, "S3/MinIO")

	instanceID := "integration-s3-wrong"
	envKey := "S3_SECRETKEY_INTEGRATION_S3_WRONG"
	os.Setenv(envKey, "totally-wrong-secret")
	defer os.Unsetenv(envKey)

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        s3Endpoint,
		Type:       "s3",
		InstanceID: instanceID,
		AccessKey:  "totally-wrong-key",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q, got %q (message: %s)", EndpointStatusUnauthenticated, resp.Status, resp.Message)
	}
	if !strings.Contains(resp.Message, "invalid credentials") {
		t.Errorf("Expected 'invalid credentials' in message, got %q", resp.Message)
	}
}

func TestIntegration_S3Probe_NoCredentials(t *testing.T) {
	s3Endpoint, _, _ := getS3Config()
	skipIfUnreachable(t, s3Endpoint, "S3/MinIO")

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:  s3Endpoint,
		Type: "s3",
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q (no creds), got %q (message: %s)",
			EndpointStatusUnauthenticated, resp.Status, resp.Message)
	}
	if !strings.Contains(resp.Message, "credentials not provided") {
		t.Errorf("Expected 'credentials not provided' in message, got %q", resp.Message)
	}
}

func TestIntegration_S3Probe_AccessKeyNoSecretKey(t *testing.T) {
	s3Endpoint, s3AccessKey, _ := getS3Config()
	skipIfUnreachable(t, s3Endpoint, "S3/MinIO")

	// No env var set for secret key
	os.Unsetenv("S3_SECRETKEY_INTEGRATION_S3_NOSECRET")

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        s3Endpoint,
		Type:       "s3",
		InstanceID: "integration-s3-nosecret",
		AccessKey:  s3AccessKey,
	})

	if resp.Status != EndpointStatusUnauthenticated {
		t.Errorf("Expected status %q (access key but no secret), got %q (message: %s)",
			EndpointStatusUnauthenticated, resp.Status, resp.Message)
	}
	if !strings.Contains(resp.Message, "secret key env var not set") {
		t.Errorf("Expected 'secret key env var not set' in message, got %q", resp.Message)
	}
}

func TestIntegration_S3Probe_SecretKeyFromEnvVar(t *testing.T) {
	s3Endpoint, s3AccessKey, s3SecretKey := getS3Config()
	skipIfUnreachable(t, s3Endpoint, "S3/MinIO")

	instanceID := "integration-s3-env"
	envKey := "S3_SECRETKEY_INTEGRATION_S3_ENV"
	os.Setenv(envKey, s3SecretKey)
	defer os.Unsetenv(envKey)

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:        s3Endpoint,
		Type:       "s3",
		InstanceID: instanceID,
		AccessKey:  s3AccessKey,
	})

	if resp.Status != EndpointStatusConnected {
		t.Errorf("Expected status %q (secret from env var), got %q (message: %s)",
			EndpointStatusConnected, resp.Status, resp.Message)
	}
	if resp.Message != "Connected and authenticated" {
		t.Errorf("Expected 'Connected and authenticated', got %q", resp.Message)
	}
}

// ============================================================
// Camunda probe integration test
// ============================================================

func TestIntegration_CamundaProbe_Reachable(t *testing.T) {
	// Use ES endpoint as a generic reachable HTTP endpoint if no Camunda is running
	camundaURL := os.Getenv("CAMUNDA_URL")
	if camundaURL == "" {
		camundaURL = "http://localhost:8080"
	}
	skipIfUnreachable(t, camundaURL, "Camunda")

	h := setupIntegrationHandlers(t)
	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:  camundaURL,
		Type: "camunda",
	})

	// Camunda may or may not require auth — just verify it's not unreachable
	if resp.Status == EndpointStatusUnreachable {
		t.Errorf("Camunda is reachable but got unreachable status (message: %s)", resp.Message)
	}
	t.Logf("Camunda probe: status=%s code=%d message=%q", resp.Status, resp.StatusCode, resp.Message)
}

// ============================================================
// Unreachable endpoint tests (always runnable)
// ============================================================

func TestIntegration_UnreachableEndpoint(t *testing.T) {
	h := setupIntegrationHandlers(t)

	tests := []struct {
		name string
		req  EndpointCheckRequest
	}{
		{"camunda unreachable", EndpointCheckRequest{URL: "http://192.0.2.1:9999", Type: "camunda"}},
		{"elasticsearch unreachable", EndpointCheckRequest{URL: "http://192.0.2.1:9999", Type: "elasticsearch"}},
		{"s3 unreachable", EndpointCheckRequest{URL: "http://192.0.2.1:9999", Type: "s3"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := doIntegrationCheckEndpoint(t, h, tt.req)
			if resp.Status != EndpointStatusUnreachable {
				t.Errorf("Expected %q for unreachable endpoint, got %q (message: %s)",
					EndpointStatusUnreachable, resp.Status, resp.Message)
			}
		})
	}
}

func TestIntegration_InvalidDNS(t *testing.T) {
	h := setupIntegrationHandlers(t)

	resp := doIntegrationCheckEndpoint(t, h, EndpointCheckRequest{
		URL:  "http://this-host-does-not-exist.invalid:9200",
		Type: "elasticsearch",
	})

	if resp.Status != EndpointStatusUnreachable {
		t.Errorf("Expected %q for invalid DNS, got %q (message: %s)",
			EndpointStatusUnreachable, resp.Status, resp.Message)
	}
	if !strings.Contains(resp.Message, "DNS") {
		t.Logf("Note: expected DNS error in message, got %q", resp.Message)
	}
}
