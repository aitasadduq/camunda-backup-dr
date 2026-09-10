package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

type capturedRequest struct {
	method      string
	path        string
	contentType string
	raw         string
	body        map[string]interface{}
	count       int
}

// newCapturingServer returns a server recording what it was sent, replying with status.
func newCapturingServer(t *testing.T, status int) (*httptest.Server, *capturedRequest) {
	t.Helper()
	captured := &capturedRequest{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.count++
		captured.method = r.Method
		captured.path = r.URL.Path
		captured.contentType = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		captured.raw = string(raw)
		if len(raw) > 0 && json.Valid(raw) {
			if err := json.Unmarshal(raw, &captured.body); err != nil {
				t.Errorf("Request body is not valid JSON: %v", err)
			}
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	return server, captured
}

func TestNotifierSend(t *testing.T) {
	server, captured := newCapturingServer(t, http.StatusOK)
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "POST",
		URL:          server.URL + "/backup-events",
		Body:         `{"channel":"backups"}`,
		MessageField: "payload.text",
	}

	if err := notifier.Send(context.Background(), cfg, "backup finished"); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if captured.method != http.MethodPost {
		t.Errorf("Expected POST, got %s", captured.method)
	}
	if captured.path != "/backup-events" {
		t.Errorf("Expected path /backup-events, got %s", captured.path)
	}
	if captured.contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %s", captured.contentType)
	}
	if captured.body["channel"] != "backups" {
		t.Errorf("Expected the user's body to survive, got %v", captured.body)
	}
	payload, ok := captured.body["payload"].(map[string]interface{})
	if !ok {
		t.Fatalf("Expected a nested payload object, got %v", captured.body)
	}
	if payload["text"] != "backup finished" {
		t.Errorf("Expected the message in payload.text, got %v", payload["text"])
	}
}

func TestNotifierSendUsesConfiguredMethod(t *testing.T) {
	server, captured := newCapturingServer(t, http.StatusAccepted)
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "put",
		URL:          server.URL,
		MessageField: "text",
	}

	if err := notifier.Send(context.Background(), cfg, "backup finished"); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if captured.method != http.MethodPut {
		t.Errorf("Expected PUT, got %s", captured.method)
	}
	if captured.body["text"] != "backup finished" {
		t.Errorf("Expected the message in text, got %v", captured.body)
	}
}

func TestNotifierSendWithoutMessageField(t *testing.T) {
	server, captured := newCapturingServer(t, http.StatusOK)
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled: true,
		Method:  "POST",
		URL:     server.URL,
		Body:    `{"event":"backup"}`,
	}

	if err := notifier.Send(context.Background(), cfg, "backup finished"); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if captured.raw != `{"event":"backup"}` {
		t.Errorf("Expected the body to travel unchanged, got %q", captured.raw)
	}
	if captured.contentType != "application/json" {
		t.Errorf("Expected Content-Type application/json, got %q", captured.contentType)
	}
}

func TestNotifierSendWithoutBodyOrMessageField(t *testing.T) {
	server, captured := newCapturingServer(t, http.StatusOK)
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled: true,
		Method:  "GET",
		URL:     server.URL + "/ping",
	}

	if err := notifier.Send(context.Background(), cfg, "backup finished"); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if captured.count != 1 {
		t.Fatalf("Expected exactly 1 request, got %d", captured.count)
	}
	if captured.method != http.MethodGet {
		t.Errorf("Expected GET, got %s", captured.method)
	}
	if captured.raw != "" {
		t.Errorf("Expected no body, got %q", captured.raw)
	}
	if captured.contentType != "" {
		t.Errorf("Expected no Content-Type, got %q", captured.contentType)
	}
}

func TestNotifierSendRefusesNonJSONBody(t *testing.T) {
	server, captured := newCapturingServer(t, http.StatusOK)
	notifier := NewNotifier(utils.NewLogger("test"))

	// A body with no message field is never written into, but it is still
	// checked: a trailing comma that only failed once a message field was
	// filled in would be a trap.
	cfg := models.NotificationRequest{
		Enabled: true,
		Method:  "POST",
		URL:     server.URL,
		Body:    "{\n  \"sync_status\": \"Success\",\n}",
	}

	if err := notifier.Send(context.Background(), cfg, "ignored message"); err == nil {
		t.Fatal("Expected an error for a body that is not valid JSON")
	}
	if captured.count != 0 {
		t.Errorf("Expected nothing to be sent, got %d request(s)", captured.count)
	}
}

func TestNotifierSendDisabled(t *testing.T) {
	server, captured := newCapturingServer(t, http.StatusOK)
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled:      false,
		URL:          server.URL,
		MessageField: "text",
	}

	if err := notifier.Send(context.Background(), cfg, "backup finished"); err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if captured.count != 0 {
		t.Errorf("Expected a disabled notification to send nothing, got %d request(s)", captured.count)
	}
}

func TestNotifierSendErrorStatus(t *testing.T) {
	server, _ := newCapturingServer(t, http.StatusInternalServerError)
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "POST",
		URL:          server.URL,
		MessageField: "text",
	}

	err := notifier.Send(context.Background(), cfg, "backup finished")
	if err == nil {
		t.Fatal("Expected an error for a 500 response")
	}
}

func TestNotifierSendInvalidConfig(t *testing.T) {
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "POST",
		URL:          "not-a-url",
		MessageField: "text",
	}

	if err := notifier.Send(context.Background(), cfg, "backup finished"); err == nil {
		t.Fatal("Expected an error for an invalid endpoint")
	}
}

func TestNotifierSendUnreachable(t *testing.T) {
	server, _ := newCapturingServer(t, http.StatusOK)
	url := server.URL
	server.Close()

	notifier := NewNotifier(utils.NewLogger("test"))
	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "POST",
		URL:          url,
		MessageField: "text",
	}

	if err := notifier.Send(context.Background(), cfg, "backup finished"); err == nil {
		t.Fatal("Expected an error when the endpoint is unreachable")
	}
}
