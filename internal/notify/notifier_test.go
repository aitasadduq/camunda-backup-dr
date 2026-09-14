package notify

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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

func TestNotifierSendStatusBoundary(t *testing.T) {
	tests := []struct {
		status  int
		wantErr bool
	}{
		{http.StatusOK, false},
		{http.StatusNoContent, false},
		{299, false},
		{http.StatusMultipleChoices, true},
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, true},
		{http.StatusNotFound, true},
		{http.StatusInternalServerError, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.status), func(t *testing.T) {
			server, _ := newCapturingServer(t, tt.status)
			notifier := NewNotifier(utils.NewLogger("test"))
			cfg := models.NotificationRequest{
				Enabled:      true,
				Method:       "POST",
				URL:          server.URL + "/hook?token=secret",
				MessageField: "text",
			}

			err := notifier.Send(context.Background(), cfg, "backup finished")
			if (err != nil) != tt.wantErr {
				t.Fatalf("status %d: wantErr=%v, got %v", tt.status, tt.wantErr, err)
			}
			if err != nil && !strings.Contains(err.Error(), fmt.Sprint(tt.status)) {
				t.Errorf("Expected the error to name status %d, got: %v", tt.status, err)
			}
			if err != nil && strings.Contains(err.Error(), "token=secret") {
				t.Errorf("Error leaked the URL query: %v", err)
			}
		})
	}
}

func TestNotifierSendDoesNotFollowRedirects(t *testing.T) {
	var seen []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Method+" "+r.URL.Path)
		if r.URL.Path == "/hook" {
			http.Redirect(w, r, "/moved", http.StatusFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "POST",
		URL:          server.URL + "/hook",
		MessageField: "text",
	}

	// Following the redirect would turn the POST into a bodiless GET and then
	// report success for a message that never arrived.
	err := notifier.Send(context.Background(), cfg, "backup finished")
	if err == nil {
		t.Fatal("Expected a 302 to be reported as a failure")
	}
	if !strings.Contains(err.Error(), "302") {
		t.Errorf("Expected the error to name the status, got: %v", err)
	}
	if len(seen) != 1 || seen[0] != "POST /hook" {
		t.Errorf("Expected exactly the original POST and no follow-up, got %v", seen)
	}
}

func TestNotifierSendRedactsURLInTransportErrors(t *testing.T) {
	server, _ := newCapturingServer(t, http.StatusOK)
	dead := server.URL
	server.Close()

	notifier := NewNotifier(utils.NewLogger("test"))
	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "POST",
		URL:          dead + "/services/T0/B0/SECRET-TOKEN?key=abc",
		MessageField: "text",
	}

	err := notifier.Send(context.Background(), cfg, "backup finished")
	if err == nil {
		t.Fatal("Expected an error when the endpoint is unreachable")
	}
	for _, leak := range []string{"SECRET-TOKEN", "key=abc", "/services/"} {
		if strings.Contains(err.Error(), leak) {
			t.Errorf("Error leaked %q from the URL: %v", leak, err)
		}
	}
	if !strings.Contains(err.Error(), cfg.RedactedURL()) {
		t.Errorf("Expected the error to name the redacted endpoint %s, got: %v", cfg.RedactedURL(), err)
	}
}

func TestNotifierSendHonoursContextDeadline(t *testing.T) {
	// The handler drains the body first: net/http only notices a client
	// hanging up once the request body has been read, and server.Close waits
	// for the handler. The release channel keeps cleanup from ever blocking.
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})
	notifier := NewNotifier(utils.NewLogger("test"))

	cfg := models.NotificationRequest{
		Enabled:      true,
		Method:       "POST",
		URL:          server.URL,
		MessageField: "text",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := notifier.Send(ctx, cfg, "backup finished")
	if err == nil {
		t.Fatal("Expected an error when the endpoint never answers")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Send ignored the context deadline and waited %s", elapsed)
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
