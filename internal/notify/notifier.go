package notify

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// DefaultTimeout bounds a single notification request.
const DefaultTimeout = 10 * time.Second

// Notifier sends the user-configured HTTP request that carries the
// controller's message about a finished backup.
type Notifier struct {
	client *http.Client
	logger *utils.Logger
}

// NewNotifier creates a Notifier with the default request timeout.
func NewNotifier(logger *utils.Logger) *Notifier {
	return &Notifier{
		client: &http.Client{Timeout: DefaultTimeout},
		logger: logger,
	}
}

// Send delivers message to the configured endpoint, writing it into the body
// field the user named. A disabled request is a no-op.
//
// The request is validated before it is sent: a notification is a side effect
// of a backup that has already finished, so a misconfigured one is reported to
// the caller rather than retried.
func (n *Notifier) Send(ctx context.Context, cfg models.NotificationRequest, message string) error {
	if !cfg.Enabled {
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return err
	}

	body, err := cfg.RenderBody(message)
	if err != nil {
		return err
	}

	// A request with neither a body nor a message field carries no body at all:
	// its arrival is the whole signal.
	var payload io.Reader = http.NoBody
	if len(body) > 0 {
		payload = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, cfg.NormalizedMethod(), cfg.URL, payload)
	if err != nil {
		return fmt.Errorf("failed to build notification request: %w", err)
	}
	if contentType := cfg.ContentType(); contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 300 {
		return fmt.Errorf("notification endpoint %s returned status %d", cfg.RedactedURL(), resp.StatusCode)
	}

	if n.logger != nil {
		n.logger.Debug("Notification delivered to %s (%s)", cfg.RedactedURL(), cfg.NormalizedMethod())
	}
	return nil
}
