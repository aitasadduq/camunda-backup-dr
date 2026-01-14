package camunda

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// HTTPClientConfig holds configuration for the HTTP client
type HTTPClientConfig struct {
	Timeout       time.Duration // Request timeout
	MaxRetries    int           // Maximum number of retries
	RetryDelay    time.Duration // Initial retry delay
	MaxRetryDelay time.Duration // Maximum retry delay
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
}

// Do performs an HTTP request with retry logic and exponential backoff
func (c *HTTPClient) Do(ctx context.Context, opts RequestOptions) (*http.Response, error) {
	var lastErr error
	delay := c.config.RetryDelay

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry with exponential backoff
			c.logger.Debug("Retrying request (attempt %d/%d) after %v", attempt, c.config.MaxRetries, delay)
			
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
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
		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			c.logger.Debug("Request failed (attempt %d/%d): %v", attempt+1, c.config.MaxRetries+1, err)
			
			// Check if context was cancelled
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			
			// Retry on network errors
			continue
		}

		// Check if status code indicates retryable error
		if shouldRetry(resp.StatusCode) {
			resp.Body.Close()
			lastErr = fmt.Errorf("received retryable status code: %d", resp.StatusCode)
			c.logger.Debug("Received retryable status code %d (attempt %d/%d)", resp.StatusCode, attempt+1, c.config.MaxRetries+1)
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
