package utils

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestAppError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *AppError
		want string
	}{
		{
			name: "basic",
			err:  NewAppError("test_code", "something broke", 500),
			want: "test_code: something broke",
		},
		{
			name: "with operation",
			err:  NewAppError("test_code", "something broke", 500).WithOperation("ExecuteBackup"),
			want: "[ExecuteBackup] test_code: something broke",
		},
		{
			name: "with component",
			err:  NewAppError("test_code", "something broke", 500).WithComponent("zeebe"),
			want: "test_code: something broke (component=zeebe)",
		},
		{
			name: "with instance",
			err:  NewAppError("test_code", "something broke", 500).WithInstance("prod-cluster"),
			want: "test_code: something broke (instance=prod-cluster)",
		},
		{
			name: "with cause",
			err:  NewAppError("test_code", "something broke", 500).Wrap(fmt.Errorf("connection refused")),
			want: "test_code: something broke: connection refused",
		},
		{
			name: "full context",
			err: NewAppError("backup_failed", "backup failed", 500).
				WithOperation("ExecuteBackup").
				WithComponent("zeebe").
				WithInstance("prod").
				Wrap(fmt.Errorf("timeout")),
			want: "[ExecuteBackup] backup_failed: backup failed (component=zeebe) (instance=prod): timeout",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAppError_Unwrap(t *testing.T) {
	cause := fmt.Errorf("root cause")
	appErr := NewAppError("test", "msg", 500).Wrap(cause)

	if !errors.Is(appErr, cause) {
		t.Error("errors.Is should find the wrapped cause")
	}
}

func TestAppError_ErrorsAs(t *testing.T) {
	appErr := NewAppError("test", "msg", 500)
	wrapped := fmt.Errorf("wrapper: %w", appErr)

	result := IsAppError(wrapped)
	if result == nil {
		t.Fatal("IsAppError should find the AppError in the chain")
	}
	if result.Code != "test" {
		t.Errorf("Code = %q, want %q", result.Code, "test")
	}
}

func TestAppError_IsAppError_Nil(t *testing.T) {
	if IsAppError(fmt.Errorf("plain error")) != nil {
		t.Error("IsAppError should return nil for non-AppError")
	}
}

func TestToHTTPError_AppError(t *testing.T) {
	appErr := NewAppError("test_code", "test msg", http.StatusBadRequest).
		WithComponent("operate").
		WithInstance("dev")

	status, body := ToHTTPError(appErr)
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", status, http.StatusBadRequest)
	}
	if body["error"] != "test_code" {
		t.Errorf("body[error] = %q, want %q", body["error"], "test_code")
	}
	if body["component"] != "operate" {
		t.Errorf("body[component] = %q, want %q", body["component"], "operate")
	}
	if body["instance_id"] != "dev" {
		t.Errorf("body[instance_id] = %q, want %q", body["instance_id"], "dev")
	}
}

func TestToHTTPError_PlainError(t *testing.T) {
	status, body := ToHTTPError(fmt.Errorf("something went wrong"))
	if status != http.StatusInternalServerError {
		t.Errorf("status = %d, want %d", status, http.StatusInternalServerError)
	}
	if body["error"] != "internal_error" {
		t.Errorf("body[error] = %q, want %q", body["error"], "internal_error")
	}
}

func TestToHTTPError_ZeroHTTPStatus(t *testing.T) {
	appErr := &AppError{Code: "test", Message: "msg"}
	status, _ := ToHTTPError(appErr)
	if status != http.StatusInternalServerError {
		t.Errorf("zero HTTPStatus should default to 500, got %d", status)
	}
}

func TestWrapError(t *testing.T) {
	cause := fmt.Errorf("network error")
	appErr := WrapError(cause, "external_call_failed", "ES unreachable", http.StatusBadGateway)

	if appErr.Code != "external_call_failed" {
		t.Errorf("Code = %q", appErr.Code)
	}
	if !errors.Is(appErr, cause) {
		t.Error("should unwrap to the cause")
	}
}

func TestConvenienceConstructors(t *testing.T) {
	tests := []struct {
		name     string
		err      *AppError
		wantCode string
	}{
		{"BackupFailed", NewBackupFailedError("fail", nil), ErrCodeBackupFailed},
		{"Timeout", NewTimeoutError("timeout", nil), ErrCodeTimeout},
		{"CircuitOpen", NewCircuitOpenError("es"), ErrCodeCircuitOpen},
		{"ExternalCall", NewExternalCallError("fail", nil), ErrCodeExternalCall},
		{"CleanupFailed", NewCleanupFailedError("fail", nil), ErrCodeCleanupFailed},
		{"Validation", NewValidationError("bad input"), ErrCodeValidation},
		{"NotFound", NewNotFoundError("missing"), ErrCodeNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.err.Code != tt.wantCode {
				t.Errorf("Code = %q, want %q", tt.err.Code, tt.wantCode)
			}
		})
	}
}

func TestAppError_CopySemantics(t *testing.T) {
	original := NewAppError("code", "msg", 500)
	withOp := original.WithOperation("op1")
	withComp := original.WithComponent("comp1")

	if original.Operation != "" {
		t.Error("WithOperation should not mutate the original")
	}
	if original.Component != "" {
		t.Error("WithComponent should not mutate the original")
	}
	if withOp.Operation != "op1" {
		t.Errorf("copy should have Operation = %q", withOp.Operation)
	}
	if withComp.Component != "comp1" {
		t.Errorf("copy should have Component = %q", withComp.Component)
	}
}
