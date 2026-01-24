package elasticsearch

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"strings"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Client provides Elasticsearch API access for snapshot management.
type Client struct {
	endpoint   string
	username   string
	password   string
	httpClient *camunda.HTTPClient
	logger     *utils.Logger
}

// NewClient creates a new Elasticsearch client.
func NewClient(endpoint, username, password string, httpClient *camunda.HTTPClient, logger *utils.Logger) *Client {
	return &Client{
		endpoint:   strings.TrimRight(endpoint, "/"),
		username:   username,
		password:   password,
		httpClient: httpClient,
		logger:     logger,
	}
}

// buildURL constructs a full URL for the Elasticsearch API.
func (c *Client) buildURL(path string, query map[string]string) (string, error) {
	if c.endpoint == "" {
		return "", fmt.Errorf("elasticsearch endpoint is empty")
	}

	baseURL, err := url.Parse(c.endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid elasticsearch endpoint: %w", err)
	}

	fullPath, err := url.JoinPath(baseURL.String(), path)
	if err != nil {
		return "", fmt.Errorf("failed to join path: %w", err)
	}

	fullURL, err := url.Parse(fullPath)
	if err != nil {
		return "", fmt.Errorf("failed to parse url: %w", err)
	}

	if len(query) > 0 {
		q := fullURL.Query()
		for key, value := range query {
			q.Set(key, value)
		}
		fullURL.RawQuery = q.Encode()
	}

	return fullURL.String(), nil
}

// authHeaders returns headers with Basic authentication if credentials are configured.
func (c *Client) authHeaders() map[string]string {
	headers := map[string]string{}
	if c.username == "" && c.password == "" {
		return headers
	}

	credentials := fmt.Sprintf("%s:%s", c.username, c.password)
	encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
	headers["Authorization"] = "Basic " + encoded
	return headers
}
