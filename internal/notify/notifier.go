package notify

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
//
// Redirects are not followed. Go's client turns a redirected POST into a GET
// and drops the body on 301, 302 and 303, so following one would report a
// delivery whose message never arrived; a 3xx is reported as a failure
// instead, with its status code, so the user can fix the URL.
func NewNotifier(logger *utils.Logger) *Notifier {
	return &Notifier{
		client: &http.Client{
			Timeout: DefaultTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
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

	// A nil body yields a zero-length reader, which NewRequestWithContext turns
	// into http.NoBody: a request with nothing to say carries no body at all.
	req, err := http.NewRequestWithContext(ctx, cfg.NormalizedMethod(), cfg.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to build notification request to %s: %w", cfg.RedactedURL(), redactedErr(err))
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send notification to %s: %w", cfg.RedactedURL(), redactedErr(err))
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

// redactedErr strips the *url.Error wrapper net/http puts around transport
// failures. That wrapper prints the full URL, path and query included, which
// would undo the redaction every caller applies before logging.
func redactedErr(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return urlErr.Err
	}
	return err
}
