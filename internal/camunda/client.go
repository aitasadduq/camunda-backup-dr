package camunda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"errors"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// HTTPClientConfig holds configuration for the HTTP client
type HTTPClientConfig struct {
	Timeout       time.Duration // Request timeout
	MaxRetries    int           // Maximum number of retries
	RetryDelay    time.Duration // Initial retry delay
	MaxRetryDelay time.Duration // Maximum retry delay

	// ListTimeout bounds one GET of a component's full backup listing. Zero
	// means Timeout. Listing every backup in a large S3/OCI store is much slower
	// than any other component call, so it gets its own budget.
	ListTimeout time.Duration
}

// DefaultHTTPClientConfig returns a default HTTP client configuration
func DefaultHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		Timeout:       30 * time.Second,
		MaxRetries:    3,
		RetryDelay:    1 * time.Second,
		MaxRetryDelay: 30 * time.Second,
	}
}

// HTTPClient wraps http.Client with retry logic and timeout handling
type HTTPClient struct {
	client *http.Client
	config HTTPClientConfig
	logger *utils.Logger
}

// NewHTTPClient creates a new HTTP client with retry logic
func NewHTTPClient(config HTTPClientConfig, logger *utils.Logger) *HTTPClient {
	return &HTTPClient{
		client: &http.Client{
			Timeout: config.Timeout,
		},
		config: config,
		logger: logger,
	}
}

// RequestOptions holds options for HTTP requests
type RequestOptions struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    interface{}

	// Timeout overrides the client's per-attempt timeout when positive.
	Timeout time.Duration

	// Listing marks a request for a complete, possibly very slow listing. A
	// listing is slow rather than flaky, so retrying it only spends another full
	// timeout and the sum outlasts the caller's own deadline. When set, an
	// attempt that ran out its whole Timeout is not retried, and neither is a
	// 408 or 504 (a gateway in front of the server timing out the same way).
	// Connect and TLS-handshake timeouts end well before Timeout and are still
	// retried.
	Listing bool
}

// Do performs an HTTP request with retry logic and exponential backoff
func (c *HTTPClient) Do(ctx context.Context, opts RequestOptions) (*http.Response, error) {
	var lastErr error
	delay := c.config.RetryDelay

	client := c.client
	if opts.Timeout > 0 && opts.Timeout != c.client.Timeout {
		perRequest := *c.client
		perRequest.Timeout = opts.Timeout
		client = &perRequest
	}
	// Scheme and host only: paths and query strings can carry tokens.
	target := opts.Method + " " + utils.RedactURL(opts.URL)

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry with exponential backoff + jitter
			var jitter time.Duration
			if int64(delay) > 2 {
				jitter = time.Duration(rand.Int63n(int64(delay) / 2))
			}
			waitTime := delay + jitter
			c.logger.Warn("Retrying %s (retry %d/%d) in %v after: %v", target, attempt, c.config.MaxRetries, waitTime, lastErr)
			
			select {
			case <-ctx.Done():
				return nil, withLastAttempt(ctx.Err(), lastErr)
			case <-time.After(waitTime):
				// Continue with retry
			}
			
			// Exponential backoff: double the delay, but cap at MaxRetryDelay
			delay = time.Duration(float64(delay) * 2)
			if delay > c.config.MaxRetryDelay {
				delay = c.config.MaxRetryDelay
			}
		}

		// Create request
		var bodyReader io.Reader
		if opts.Body != nil {
			bodyBytes, err := json.Marshal(opts.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal request body: %w", err)
			}
			bodyReader = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(ctx, opts.Method, opts.URL, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Set headers
		if opts.Body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		for key, value := range opts.Headers {
			req.Header.Set(key, value)
		}

		// Execute request
		started := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			elapsed := time.Since(started)
			c.logger.Debug("%s failed after %v (attempt %d/%d): %v", target, elapsed.Round(time.Millisecond), attempt+1, c.config.MaxRetries+1, err)

			// Check if context was cancelled. The previous attempt's failure is
			// kept: when the caller's deadline ends a retry loop, it is the only
			// record of why the loop was retrying in the first place.
			if ctx.Err() != nil {
				return nil, withLastAttempt(ctx.Err(), lastErr)
			}
			lastErr = err

			if opts.Listing && ranOutTimeout(err, elapsed, client.Timeout) {
				return nil, fmt.Errorf("%w after %v: %v", ErrListTimeout, elapsed.Round(time.Millisecond), err)
			}

			// Retry on network errors
			continue
		}

		// A gateway timing out a listing is as slow as the listing itself;
		// hand the response to the caller instead of waiting for it again.
		if opts.Listing && (resp.StatusCode == http.StatusGatewayTimeout || resp.StatusCode == http.StatusRequestTimeout) {
			return resp, nil
		}

		// Check if status code indicates retryable error
		if shouldRetry(resp.StatusCode) {
			// Keep the start of the body: a 500 with an empty reason is
			// undiagnosable, and the component usually says what went wrong.
			snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			lastErr = fmt.Errorf("received retryable status code %d after %v: %s",
				resp.StatusCode, time.Since(started).Round(time.Millisecond), strings.TrimSpace(string(snippet)))
			c.logger.Debug("%s: %v (attempt %d/%d)", target, lastErr, attempt+1, c.config.MaxRetries+1)
			continue
		}

		// Success or non-retryable error
		return resp, nil
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// Get performs a GET request
func (c *HTTPClient) Get(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	return c.Do(ctx, RequestOptions{
		Method:  http.MethodGet,
		URL:     url,
		Headers: headers,
	})
}

// Post performs a POST request
func (c *HTTPClient) Post(ctx context.Context, url string, headers map[string]string, body interface{}) (*http.Response, error) {
	return c.Do(ctx, RequestOptions{
		Method:  http.MethodPost,
		URL:     url,
		Headers: headers,
		Body:    body,
	})
}

// Put performs a PUT request
func (c *HTTPClient) Put(ctx context.Context, url string, headers map[string]string, body interface{}) (*http.Response, error) {
	return c.Do(ctx, RequestOptions{
		Method:  http.MethodPut,
		URL:     url,
		Headers: headers,
		Body:    body,
	})
}

// Delete performs a DELETE request
func (c *HTTPClient) Delete(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	return c.Do(ctx, RequestOptions{
		Method:  http.MethodDelete,
		URL:     url,
		Headers: headers,
	})
}

// ErrListTimeout marks a listing that ran out its whole listing timeout.
var ErrListTimeout = errors.New("no response within the listing timeout (COMPONENT_LIST_TIMEOUT_SECONDS)")

// isTimeout reports whether err is a timeout of any kind: the whole-request
// http.Client.Timeout, or a dial, TLS-handshake or read deadline.
func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// ranOutTimeout reports whether a timeout is the request's own Timeout running
// out, rather than an earlier dial or TLS-handshake timeout. The transport's
// dialer gives up after 30s and its handshake after 10s, so only a failure at
// (nearly) the configured limit can be blamed on that limit.
func ranOutTimeout(err error, elapsed, limit time.Duration) bool {
	return isTimeout(err) && limit > 0 && elapsed >= limit*9/10
}

// withLastAttempt keeps the reason a retry loop was retrying when the caller's
// context ends it. errors.Is still sees err.
func withLastAttempt(err, last error) error {
	if last == nil {
		return err
	}
	return fmt.Errorf("%w (previous attempt: %v)", err, last)
}

// ListTimeout is the per-attempt limit for listing requests.
func (c *HTTPClient) ListTimeout() time.Duration {
	if c.config.ListTimeout > 0 {
		return c.config.ListTimeout
	}
	return c.config.Timeout
}

// GetListing performs a GET for a complete, possibly very slow listing — a
// component's backups or a snapshot repository. It uses ListTimeout and the
// Listing retry rules. Errors from it, and from reading its body, should go
// through DescribeListFailure so they name the limit that actually ran out.
func (c *HTTPClient) GetListing(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	return c.Do(ctx, RequestOptions{
		Method:  http.MethodGet,
		URL:     url,
		Headers: headers,
		Timeout: c.ListTimeout(),
		Listing: true,
	})
}

// DescribeListFailure says which limit ended a listing, because the fix
// differs: COMPONENT_LIST_TIMEOUT_SECONDS for one listing, RECONCILE_TIMEOUT_SECONDS
// for the whole scan, and neither for a connection that never opened. what
// names the listing ("list backups", "list snapshots"); started is when the
// request began, so a timeout while reading the body is judged the same way.
func (c *HTTPClient) DescribeListFailure(ctx context.Context, what string, err error, started time.Time) error {
	elapsed := time.Since(started).Round(time.Millisecond)
	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		return fmt.Errorf("failed to %s: the scan's time budget (RECONCILE_TIMEOUT_SECONDS) ran out after %v: %w", what, elapsed, err)
	case errors.Is(err, ErrListTimeout):
		return fmt.Errorf("failed to %s: %w", what, err)
	case ranOutTimeout(err, elapsed, c.ListTimeout()):
		// Reading the body counts against the same limit.
		return fmt.Errorf("failed to %s: %w after %v: %v", what, ErrListTimeout, elapsed, err)
	case isTimeout(err):
		return fmt.Errorf("failed to %s: timed out after %v, before the %v listing timeout, so the connection or TLS handshake failed rather than the listing being slow (COMPONENT_LIST_TIMEOUT_SECONDS does not govern this): %w",
			what, elapsed, c.ListTimeout(), err)
	default:
		return fmt.Errorf("failed to %s after %v: %w", what, elapsed, err)
	}
}

// shouldRetry determines if a status code should trigger a retry
func shouldRetry(statusCode int) bool {
	// Retry on server errors (5xx) and some client errors (429, 408)
	return statusCode == http.StatusRequestTimeout ||
		statusCode == http.StatusTooManyRequests ||
		(statusCode >= 500 && statusCode < 600)
}

// ReadJSONResponse reads and unmarshals a JSON response
func ReadJSONResponse(resp *http.Response, target interface{}) error {
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if len(bodyBytes) == 0 {
		return nil // Empty response is valid
	}

	if err := json.Unmarshal(bodyBytes, target); err != nil {
		return fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return nil
}
