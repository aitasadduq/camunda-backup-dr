package api

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awshttp "github.com/aws/smithy-go/transport/http"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// EndpointCheckRequest represents a request to check endpoint connectivity
type EndpointCheckRequest struct {
	URL        string `json:"url"`
	Type       string `json:"type"` // "camunda", "elasticsearch", "s3"
	InstanceID string `json:"instance_id,omitempty"`
	Username   string `json:"username,omitempty"`
	Password   string `json:"password,omitempty"`
	AccessKey  string `json:"access_key,omitempty"`
}

// EndpointCheckResponse represents the result of an endpoint connectivity check
type EndpointCheckResponse struct {
	Status     string `json:"status"` // "connected", "unauthenticated", "unreachable"
	StatusCode int    `json:"status_code,omitempty"`
	Message    string `json:"message"`
}

const (
	EndpointStatusConnected       = "connected"
	EndpointStatusUnauthenticated = "unauthenticated"
	EndpointStatusUnreachable     = "unreachable"
)

// probeHTTPClient is a short-timeout HTTP client used for endpoint probing.
var probeHTTPClient = newProbeHTTPClient()

func newProbeHTTPClient() *http.Client {
	insecureSkip := false
	if v := os.Getenv("PROBE_INSECURE_SKIP_VERIFY"); strings.EqualFold(v, "true") || v == "1" {
		insecureSkip = true
	}

	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
		ResponseHeaderTimeout: 4 * time.Second,
	}

	if insecureSkip {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	return &http.Client{
		Timeout:   5 * time.Second,
		Transport: transport,
		// Don't follow redirects — we just want the raw response
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

// CheckEndpointHandler handles endpoint connectivity check requests
func (h *Handlers) CheckEndpointHandler(w http.ResponseWriter, r *http.Request) {
	var req EndpointCheckRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Debug("Endpoint check: invalid JSON body")
		writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body")
		return
	}

	h.logger.Info("Checking endpoint: type=%s url=%s instance_id=%s", req.Type, req.URL, req.InstanceID)

	if req.URL == "" {
		h.logger.Debug("Endpoint check rejected: empty URL")
		writeError(w, http.StatusBadRequest, "validation_error", "URL is required")
		return
	}

	// Validate the URL
	parsedURL, err := url.ParseRequestURI(req.URL)
	if err != nil {
		h.logger.Debug("Endpoint check rejected: invalid URL format: %s", req.URL)
		writeJSON(w, http.StatusOK, EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: "Invalid URL format",
		})
		return
	}

	// Only allow http and https schemes
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		h.logger.Debug("Endpoint check rejected: unsupported scheme %q in URL %s", parsedURL.Scheme, req.URL)
		writeJSON(w, http.StatusOK, EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: "Only http and https URLs are supported",
		})
		return
	}

	// SSRF protection: block private, loopback, and link-local addresses
	if isBlockedHost(parsedURL.Hostname()) {
		h.logger.Warn("Endpoint check blocked (SSRF): %s — set PROBE_ALLOW_PRIVATE_IPS=true to allow private/loopback targets", req.URL)
		writeError(w, http.StatusBadRequest, "validation_error", "URLs targeting private or loopback addresses are not allowed (set PROBE_ALLOW_PRIVATE_IPS=true to allow)")
		return
	}

	var result EndpointCheckResponse
	switch req.Type {
	case "elasticsearch":
		// Look up password from env var if instance_id is provided
		password := req.Password
		if password == "" && req.InstanceID != "" {
			envKey := "ELASTICSEARCH_PASSWORD_" + config.NormalizeForEnvVar(req.InstanceID)
			password = os.Getenv(envKey)
		}
		result = probeElasticsearch(req.URL, req.Username, password)
	case "s3":
		// Look up secret key from env var if instance_id is provided
		secretKey := ""
		if req.InstanceID != "" {
			envKey := "S3_SECRETKEY_" + config.NormalizeForEnvVar(req.InstanceID)
			secretKey = os.Getenv(envKey)
		}
		result = probeS3(req.URL, req.AccessKey, secretKey)
	case "camunda":
		result = probeCamunda(req.URL)
	default:
		result = probeGeneric(req.URL)
	}

	h.logger.Info("Endpoint check result: type=%s url=%s status=%s message=%s", req.Type, req.URL, result.Status, result.Message)
	writeJSON(w, http.StatusOK, result)
}

// probeCamunda probes a Camunda endpoint by sending a GET request to its base URL.
func probeCamunda(endpoint string) EndpointCheckResponse {
	endpoint = strings.TrimRight(endpoint, "/")

	resp, err := doProbeRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: fmt.Sprintf("Connection failed: %s", summarizeError(err)),
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return EndpointCheckResponse{
			Status:     EndpointStatusUnauthenticated,
			StatusCode: resp.StatusCode,
			Message:    "Reachable but not authenticated",
		}
	}

	return EndpointCheckResponse{
		Status:     EndpointStatusConnected,
		StatusCode: resp.StatusCode,
		Message:    "Connected successfully",
	}
}

// probeElasticsearch probes an Elasticsearch endpoint.
// If username is provided but password is empty, returns unauthenticated without probing.
// If both are provided, sends Basic auth and verifies the server accepts it.
// If neither is provided, probes without auth (for clusters without security).
func probeElasticsearch(endpoint, username, password string) EndpointCheckResponse {
	endpoint = strings.TrimRight(endpoint, "/")

	// First, check basic reachability with an unauthenticated request
	resp, err := doProbeRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: fmt.Sprintf("Connection failed: %s", summarizeError(err)),
		}
	}
	resp.Body.Close()

	// No credentials provided at all
	if username == "" && password == "" {
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return EndpointCheckResponse{
				Status:     EndpointStatusConnected,
				StatusCode: resp.StatusCode,
				Message:    "Connected successfully (no auth required)",
			}
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			return EndpointCheckResponse{
				Status:     EndpointStatusUnauthenticated,
				StatusCode: resp.StatusCode,
				Message:    "Reachable but not authenticated",
			}
		}
		return EndpointCheckResponse{
			Status:     EndpointStatusConnected,
			StatusCode: resp.StatusCode,
			Message:    fmt.Sprintf("Reachable (HTTP %d)", resp.StatusCode),
		}
	}

	// Username provided but password missing (env var not set)
	if password == "" {
		return EndpointCheckResponse{
			Status:     EndpointStatusUnauthenticated,
			StatusCode: resp.StatusCode,
			Message:    "Reachable but not authenticated (password env var not set)",
		}
	}

	// Both username and password available — verify with authenticated request
	headers := map[string]string{
		"Authorization": "Basic " + basicAuth(username, password),
	}

	authResp, err := doProbeRequest(http.MethodGet, endpoint, headers)
	if err != nil {
		return EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: fmt.Sprintf("Connection failed during auth check: %s", summarizeError(err)),
		}
	}
	defer authResp.Body.Close()

	if authResp.StatusCode == http.StatusUnauthorized || authResp.StatusCode == http.StatusForbidden {
		return EndpointCheckResponse{
			Status:     EndpointStatusUnauthenticated,
			StatusCode: authResp.StatusCode,
			Message:    "Reachable but not authenticated (invalid credentials)",
		}
	}

	if authResp.StatusCode >= 200 && authResp.StatusCode < 300 {
		return EndpointCheckResponse{
			Status:     EndpointStatusConnected,
			StatusCode: authResp.StatusCode,
			Message:    "Connected and authenticated",
		}
	}

	return EndpointCheckResponse{
		Status:     EndpointStatusConnected,
		StatusCode: authResp.StatusCode,
		Message:    fmt.Sprintf("Reachable (HTTP %d)", authResp.StatusCode),
	}
}

// probeS3 probes an S3-compatible endpoint.
// First checks reachability, then if credentials are available, verifies
// authentication by performing a real ListBuckets call via the AWS SDK.
func probeS3(endpoint, accessKey, secretKey string) EndpointCheckResponse {
	endpoint = strings.TrimRight(endpoint, "/")

	// Step 1: check basic reachability
	resp, err := doProbeRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: fmt.Sprintf("Connection failed: %s", summarizeError(err)),
		}
	}
	resp.Body.Close()

	// Step 2: if missing credentials, report unauthenticated
	if accessKey == "" || secretKey == "" {
		msg := "Reachable but not authenticated"
		if accessKey == "" && secretKey == "" {
			msg = "Reachable but not authenticated (credentials not provided)"
		} else if secretKey == "" {
			msg = "Reachable but not authenticated (secret key env var not set)"
		}
		return EndpointCheckResponse{
			Status:     EndpointStatusUnauthenticated,
			StatusCode: resp.StatusCode,
			Message:    msg,
		}
	}

	// Step 3: verify credentials with a real ListBuckets call via AWS SDK
	return probeS3WithSDK(endpoint, accessKey, secretKey)
}

// probeS3WithSDK performs an actual authenticated ListBuckets call against an
// S3-compatible endpoint to verify the provided credentials are valid.
var probeS3WithSDK = func(endpoint, accessKey, secretKey string) EndpointCheckResponse {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               endpoint,
			HostnameImmutable: true,
			SigningRegion:     "us-east-1",
		}, nil
	})

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
		awsconfig.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: fmt.Sprintf("Failed to configure S3 client: %s", err.Error()),
		}
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	_, err = client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		// Check if this is an auth error (403/401)
		var respErr *awshttp.ResponseError
		if errors.As(err, &respErr) {
			sc := respErr.HTTPStatusCode()
			if sc == http.StatusForbidden || sc == http.StatusUnauthorized {
				return EndpointCheckResponse{
					Status:     EndpointStatusUnauthenticated,
					StatusCode: sc,
					Message:    "Reachable but not authenticated (invalid credentials)",
				}
			}
			// Other HTTP error — server is reachable but returned an error
			return EndpointCheckResponse{
				Status:     EndpointStatusConnected,
				StatusCode: sc,
				Message:    fmt.Sprintf("Reachable (HTTP %d)", sc),
			}
		}
		// Network-level error
		return EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: fmt.Sprintf("Connection failed: %s", summarizeError(err)),
		}
	}

	return EndpointCheckResponse{
		Status:     EndpointStatusConnected,
		StatusCode: http.StatusOK,
		Message:    "Connected and authenticated",
	}
}

// probeGeneric sends a basic GET and reports whether the host is reachable.
func probeGeneric(endpoint string) EndpointCheckResponse {
	resp, err := doProbeRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return EndpointCheckResponse{
			Status:  EndpointStatusUnreachable,
			Message: fmt.Sprintf("Connection failed: %s", summarizeError(err)),
		}
	}
	defer resp.Body.Close()

	return EndpointCheckResponse{
		Status:     EndpointStatusConnected,
		StatusCode: resp.StatusCode,
		Message:    "Reachable",
	}
}

// doProbeRequest performs a lightweight HTTP request for probing.
func doProbeRequest(method, url string, headers map[string]string) (*http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	return probeHTTPClient.Do(req)
}

// basicAuth encodes username:password for HTTP Basic auth.
func basicAuth(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}

// summarizeError extracts a user-friendly message from a network error.
func summarizeError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	// Trim verbose wrapped error chains to keep it user-friendly
	if strings.Contains(msg, "no such host") {
		return "DNS resolution failed"
	}
	if strings.Contains(msg, "connection refused") {
		return "Connection refused"
	}
	if strings.Contains(msg, "i/o timeout") || strings.Contains(msg, "context deadline exceeded") {
		return "Connection timed out"
	}
	if strings.Contains(msg, "certificate") {
		return "TLS/SSL certificate error"
	}
	return "Network error"
}

// privateIPNets defines CIDR ranges that should be blocked for SSRF protection.
var privateIPNets = func() []*net.IPNet {
	cidrs := []string{
		"127.0.0.0/8",    // loopback
		"10.0.0.0/8",     // RFC 1918
		"172.16.0.0/12",  // RFC 1918
		"192.168.0.0/16", // RFC 1918
		"169.254.0.0/16", // link-local
		"::1/128",        // IPv6 loopback
		"fc00::/7",       // IPv6 unique local
		"fe80::/10",      // IPv6 link-local
	}
	nets := make([]*net.IPNet, 0, len(cidrs))
	for _, c := range cidrs {
		_, n, _ := net.ParseCIDR(c)
		nets = append(nets, n)
	}
	return nets
}()

// isBlockedHost returns true if the hostname resolves to a private, loopback, or link-local address.
// Exposed as a var so tests can disable SSRF protection when using httptest servers.
// Set PROBE_ALLOW_PRIVATE_IPS=true to disable this check (e.g., for local/self-hosted services).
var isBlockedHost = func(hostname string) bool {
	if v := os.Getenv("PROBE_ALLOW_PRIVATE_IPS"); strings.EqualFold(v, "true") || v == "1" {
		return false
	}

	// Try parsing as a literal IP first
	if ip := net.ParseIP(hostname); ip != nil {
		return isPrivateIP(ip)
	}
	// Resolve the hostname
	addrs, err := net.LookupHost(hostname)
	if err != nil {
		return false // DNS failure is handled later by the probe itself
	}
	for _, addr := range addrs {
		if ip := net.ParseIP(addr); ip != nil && isPrivateIP(ip) {
			return true
		}
	}
	return false
}

// isPrivateIP checks whether an IP falls in any blocked CIDR range.
func isPrivateIP(ip net.IP) bool {
	for _, n := range privateIPNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}
