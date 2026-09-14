package models

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func TestNotificationRequestValidate(t *testing.T) {
	tests := []struct {
		name    string
		request NotificationRequest
		wantErr bool
	}{
		{
			name:    "disabled request is never validated",
			request: NotificationRequest{Enabled: false, URL: "not-a-url", Method: "TELEPORT"},
		},
		{
			name:    "minimal enabled request",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com:8443/events", MessageField: "text"},
		},
		{
			name:    "method defaults to POST when empty",
			request: NotificationRequest{Enabled: true, URL: "https://hooks.example.com/events", MessageField: "text"},
		},
		{
			name:    "lowercase method is accepted",
			request: NotificationRequest{Enabled: true, Method: "put", URL: "https://hooks.example.com/events", MessageField: "text"},
		},
		{
			name:    "unsupported method",
			request: NotificationRequest{Enabled: true, Method: "TRACE", URL: "https://hooks.example.com/events", MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "non-http scheme",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "ftp://hooks.example.com/events", MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "url without host",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "/events", MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "url with a port but no host dials this machine",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "http://:8080/events", MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "body at the size cap",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `{"pad":"` + strings.Repeat("x", MaxNotificationBodyBytes-10) + `"}`},
		},
		{
			name:    "body over the size cap",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `{"pad":"` + strings.Repeat("x", MaxNotificationBodyBytes) + `"}`},
			wantErr: true,
		},
		{
			name:    "body that is not JSON",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: "not json", MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "a non-JSON body is refused even with no message field",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: "<event kind=\"backup\"/>"},
			wantErr: true,
		},
		{
			name:    "a trailing comma is refused with no message field",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: "{\n  \"env\": \"prod\",\n}"},
			wantErr: true,
		},
		{
			name:    "a trailing comma is refused with a message field",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: "{\n  \"env\": \"prod\",\n}", MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "a JSON array body is refused with no message field",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `[{"env":"prod"}]`},
			wantErr: true,
		},
		{
			name:    "a JSON null body is refused with no message field",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: "null"},
			wantErr: true,
		},
		{
			name:    "a JSON null body is refused with a message field",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: "null", MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "trailing content after the object is refused",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `{"env":"prod"} {"again":true}`},
			wantErr: true,
		},
		{
			name:    "a stray closing bracket after the object is refused",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `{"env":"prod"}]`},
			wantErr: true,
		},
		{
			name:    "neither a body nor a message field",
			request: NotificationRequest{Enabled: true, Method: "GET", URL: "https://hooks.example.com/events"},
		},
		{
			name:    "a body with no message field",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `{"event":"backup"}`},
		},
		{
			name:    "a message field with no body",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", MessageField: "text"},
		},
		{
			name:    "body that is a JSON array",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `["a"]`, MessageField: "text"},
			wantErr: true,
		},
		{
			name:    "message field with an empty segment",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", MessageField: "payload..text"},
			wantErr: true,
		},
		{
			name:    "message field traversing a non-object",
			request: NotificationRequest{Enabled: true, Method: "POST", URL: "https://hooks.example.com/events", Body: `{"payload":"a string"}`, MessageField: "payload.text"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.request.Validate()
			if tt.wantErr && err == nil {
				t.Fatal("Expected an error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}
		})
	}
}

func TestNotificationRequestRenderBody(t *testing.T) {
	tests := []struct {
		name    string
		request NotificationRequest
		message string
		want    map[string]interface{}
		wantErr bool
	}{
		{
			name:    "empty body becomes an object holding only the message",
			request: NotificationRequest{MessageField: "text"},
			message: "backup done",
			want:    map[string]interface{}{"text": "backup done"},
		},
		{
			name:    "message is added alongside the user's fields",
			request: NotificationRequest{Body: `{"channel":"backups"}`, MessageField: "text"},
			message: "backup done",
			want:    map[string]interface{}{"channel": "backups", "text": "backup done"},
		},
		{
			name:    "message replaces a placeholder value",
			request: NotificationRequest{Body: `{"text":"placeholder"}`, MessageField: "text"},
			message: "backup done",
			want:    map[string]interface{}{"text": "backup done"},
		},
		{
			name:    "dotted path creates missing objects",
			request: NotificationRequest{Body: `{"channel":"backups"}`, MessageField: "payload.text"},
			message: "backup done",
			want: map[string]interface{}{
				"channel": "backups",
				"payload": map[string]interface{}{"text": "backup done"},
			},
		},
		{
			name:    "dotted path descends into an existing object",
			request: NotificationRequest{Body: `{"payload":{"kind":"backup"}}`, MessageField: "payload.text"},
			message: "backup done",
			want: map[string]interface{}{
				"payload": map[string]interface{}{"kind": "backup", "text": "backup done"},
			},
		},
		{
			name:    "large integers and float notation survive the round trip",
			request: NotificationRequest{Body: `{"id": 9007199254740993, "big": 12345678901234567890, "ratio": 1.0}`, MessageField: "text"},
			message: "backup done",
			want: map[string]interface{}{
				"id":    json.Number("9007199254740993"),
				"big":   json.Number("12345678901234567890"),
				"ratio": json.Number("1.0"),
				"text":  "backup done",
			},
		},
		{
			name:    "dotted path through a non-object is refused",
			request: NotificationRequest{Body: `{"payload":"a string"}`, MessageField: "payload.text"},
			message: "backup done",
			wantErr: true,
		},
		{
			name:    "invalid body is refused",
			request: NotificationRequest{Body: `{`, MessageField: "text"},
			message: "backup done",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.request.RenderBody(tt.message)
			if tt.wantErr {
				if err == nil {
					t.Fatal("Expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}

			if !json.Valid(raw) {
				t.Fatalf("Rendered body is not valid JSON: %s", raw)
			}

			// Marshal sorts map keys, so comparing the encoded forms compares
			// content without re-parsing the rendered numbers into floats.
			wantJSON, _ := json.Marshal(tt.want)
			if string(wantJSON) != string(raw) {
				t.Errorf("Expected body %s, got %s", wantJSON, raw)
			}
		})
	}
}

func TestNotificationRequestRenderBodyWithoutMessageField(t *testing.T) {
	tests := []struct {
		name    string
		request NotificationRequest
		want    string
	}{
		{
			name:    "no body and no message field sends nothing",
			request: NotificationRequest{},
			want:    "",
		},
		{
			name:    "a blank body sends nothing",
			request: NotificationRequest{Body: "   "},
			want:    "",
		},
		{
			name:    "a JSON body travels unchanged",
			request: NotificationRequest{Body: `{"event":"backup"}`},
			want:    `{"event":"backup"}`,
		},
		{
			name:    "key order and formatting are preserved byte for byte",
			request: NotificationRequest{Body: "{\n  \"z\": 1,\n  \"a\": 2\n}"},
			want:    "{\n  \"z\": 1,\n  \"a\": 2\n}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.request.RenderBody("ignored message")
			if err != nil {
				t.Fatalf("Expected no error, got: %v", err)
			}
			if string(got) != tt.want {
				t.Errorf("Expected body %q, got %q", tt.want, string(got))
			}
			if tt.want == "" && got != nil {
				t.Errorf("Expected a nil body, got %v", got)
			}
		})
	}
}

func TestNotificationRequestHostname(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"host without port", "https://hooks.example.com/events", "hooks.example.com"},
		{"port is stripped", "https://hooks.example.com:8443/events", "hooks.example.com"},
		{"literal IP", "http://10.0.0.5:9000/hook", "10.0.0.5"},
		{"unparseable url", "://nope", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := (NotificationRequest{URL: tt.url}).Hostname(); got != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestNotificationConfigFor(t *testing.T) {
	cfg := NotificationConfig{
		OnSuccess: NotificationRequest{Enabled: true, URL: "https://success.example.com"},
		OnFailure: NotificationRequest{Enabled: true, URL: "https://failure.example.com"},
	}

	tests := []struct {
		status       types.BackupStatus
		wantTerminal bool
		wantURL      string
	}{
		{types.BackupStatusCompleted, true, "https://success.example.com"},
		{types.BackupStatusFailed, true, "https://failure.example.com"},
		{types.BackupStatusIncomplete, true, "https://failure.example.com"},
		{types.BackupStatusRunning, false, ""},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			got, terminal := cfg.For(tt.status)
			if terminal != tt.wantTerminal {
				t.Fatalf("Expected terminal=%v for %s, got %v", tt.wantTerminal, tt.status, terminal)
			}
			if got.URL != tt.wantURL {
				t.Errorf("Expected URL %q, got %q", tt.wantURL, got.URL)
			}
		})
	}
}

func TestNotificationRequestNormalizedMethod(t *testing.T) {
	tests := []struct {
		method string
		want   string
	}{
		{"", http.MethodPost},
		{"get", http.MethodGet},
		{"PATCH", http.MethodPatch},
	}

	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			if got := (NotificationRequest{Method: tt.method}).NormalizedMethod(); got != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestNotificationRequestRedactedURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"path and query are dropped", "https://hooks.example.com:8443/services/T0/B0/secret?token=abc", "https://hooks.example.com:8443"},
		{"credentials are dropped", "https://user:pass@hooks.example.com/events", "https://hooks.example.com"},
		{"unparseable url", "://nope", "<invalid-url>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := (NotificationRequest{URL: tt.url}).RedactedURL(); got != tt.want {
				t.Errorf("Expected %q, got %q", tt.want, got)
			}
		})
	}
}

func TestCamundaInstanceValidateRejectsBadNotification(t *testing.T) {
	instance := NewCamundaInstance("test", "Test", "http://localhost:8080")
	instance.BackupIDS3Endpoint = "http://localhost:9000"
	instance.BackupIDS3AccessKey = "key"

	if err := instance.Validate(); err != nil {
		t.Fatalf("Expected the base instance to be valid, got: %v", err)
	}

	instance.Notifications.OnFailure = NotificationRequest{Enabled: true, Method: "POST", URL: "not-a-url", MessageField: "text"}
	if err := instance.Validate(); err == nil {
		t.Error("Expected an enabled notification with an invalid URL to fail validation")
	}
}
