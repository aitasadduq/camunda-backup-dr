package models

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// notificationMethods are the HTTP methods a notification request may use.
var notificationMethods = map[string]bool{
	http.MethodGet:    true,
	http.MethodPost:   true,
	http.MethodPut:    true,
	http.MethodPatch:  true,
	http.MethodDelete: true,
}

// NotificationRequest describes an HTTP request the controller sends when a
// backup reaches a terminal state. The user supplies the method, the endpoint,
// the body, and the field inside that body where the controller writes its
// message.
type NotificationRequest struct {
	Enabled bool   `json:"enabled"`
	Method  string `json:"method,omitempty"`
	URL     string `json:"url,omitempty"`
	// Body is the request body. Optional — an empty body with no MessageField
	// sends no body at all — but when given it must be a JSON object, whether
	// or not MessageField names a place inside it. One rule either way: a body
	// that is accepted with the field blank must not be rejected once it is
	// filled in.
	Body string `json:"body,omitempty"`
	// MessageField is a dotted path naming where in Body the controller writes
	// its message, e.g. "text" or "payload.message". Missing intermediate
	// objects are created. Optional: without it the body is sent unchanged and
	// the request itself is the whole signal.
	MessageField string `json:"message_field,omitempty"`
}

// NotificationConfig holds the requests sent for each backup outcome.
type NotificationConfig struct {
	OnSuccess NotificationRequest `json:"on_success"`
	OnFailure NotificationRequest `json:"on_failure"`
}

// For returns the request configured for a backup status. A backup that has
// not reached a terminal state notifies nothing, and INCOMPLETE is a failure:
// artifacts were left in a state nobody asked for.
func (nc NotificationConfig) For(status types.BackupStatus) (NotificationRequest, bool) {
	switch status {
	case types.BackupStatusCompleted:
		return nc.OnSuccess, true
	case types.BackupStatusFailed, types.BackupStatusIncomplete:
		return nc.OnFailure, true
	default:
		return NotificationRequest{}, false
	}
}

// NormalizedMethod returns the HTTP method in the canonical upper case form,
// defaulting to POST when none was given.
func (nr NotificationRequest) NormalizedMethod() string {
	if nr.Method == "" {
		return http.MethodPost
	}
	return strings.ToUpper(nr.Method)
}

// RedactedURL returns the endpoint reduced to scheme and host. Notification
// URLs routinely carry tokens in the path or query, so only this form is safe
// to log.
func (nr NotificationRequest) RedactedURL() string {
	u, err := url.Parse(nr.URL)
	if err != nil {
		return "<invalid-url>"
	}
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
}

// Validate checks a notification request. A disabled request is never sent, so
// it is never validated — half-filled forms stay saveable.
func (nr NotificationRequest) Validate() error {
	if !nr.Enabled {
		return nil
	}

	if !notificationMethods[nr.NormalizedMethod()] {
		return fmt.Errorf("notification method %q is not supported", nr.Method)
	}

	u, err := url.Parse(nr.URL)
	if err != nil {
		return fmt.Errorf("notification url is not a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("notification url must use http or https")
	}
	if u.Host == "" {
		return fmt.Errorf("notification url must include a host")
	}

	if _, err := nr.decodeBody(); err != nil {
		return err
	}

	if nr.MessageField == "" {
		return nil
	}
	if err := validateMessageField(nr.MessageField); err != nil {
		return err
	}

	// Rendering proves the message path is writable. A path running through a
	// string could never deliver, and finding that out at save time beats
	// finding out when a backup finishes.
	_, err = nr.RenderBody("")
	return err
}

// ContentType returns the media type of the rendered body, or "" when the
// request has no body to send. Every body this package accepts is JSON.
func (nr NotificationRequest) ContentType() string {
	if nr.MessageField == "" && strings.TrimSpace(nr.Body) == "" {
		return ""
	}
	return "application/json"
}

// RenderBody produces the request body with message written to MessageField.
// Without a message field the body is sent byte for byte as the user wrote it,
// keeping their key order and formatting, and a nil result means the request
// carries no body at all.
func (nr NotificationRequest) RenderBody(message string) ([]byte, error) {
	if nr.MessageField == "" {
		if strings.TrimSpace(nr.Body) == "" {
			return nil, nil
		}
		return []byte(nr.Body), nil
	}

	body, err := nr.decodeBody()
	if err != nil {
		return nil, err
	}

	if err := validateMessageField(nr.MessageField); err != nil {
		return nil, err
	}

	segments := strings.Split(nr.MessageField, ".")
	target := body
	for _, segment := range segments[:len(segments)-1] {
		next, ok := target[segment]
		if !ok || next == nil {
			child := map[string]interface{}{}
			target[segment] = child
			target = child
			continue
		}
		child, ok := next.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("notification message_field %q traverses %q, which is not an object", nr.MessageField, segment)
		}
		target = child
	}
	target[segments[len(segments)-1]] = message

	return json.Marshal(body)
}

// decodeBody parses Body as a JSON object. An empty body is an empty object.
func (nr NotificationRequest) decodeBody() (map[string]interface{}, error) {
	if strings.TrimSpace(nr.Body) == "" {
		return map[string]interface{}{}, nil
	}
	var body map[string]interface{}
	if err := json.Unmarshal([]byte(nr.Body), &body); err != nil {
		return nil, fmt.Errorf("notification body must be a JSON object: %w", err)
	}
	if body == nil {
		return map[string]interface{}{}, nil
	}
	return body, nil
}

// validateMessageField checks that a non-empty field path names a reachable key.
func validateMessageField(field string) error {
	for _, segment := range strings.Split(field, ".") {
		if segment == "" {
			return fmt.Errorf("notification message_field %q has an empty path segment", field)
		}
	}
	return nil
}

// Validate checks both configured requests.
func (nc NotificationConfig) Validate() error {
	if err := nc.OnSuccess.Validate(); err != nil {
		return fmt.Errorf("on_success: %w", err)
	}
	if err := nc.OnFailure.Validate(); err != nil {
		return fmt.Errorf("on_failure: %w", err)
	}
	return nil
}
