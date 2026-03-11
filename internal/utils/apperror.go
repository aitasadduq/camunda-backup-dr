package utils

import (
	"errors"
	"fmt"
	"net/http"
)

// AppError is a structured error type that carries contextual information
// for logging, API responses, and error chain inspection.
type AppError struct {
	Code       string // Machine-readable error code (e.g. "backup_failed")
	Message    string // Human-readable message
	HTTPStatus int    // HTTP status code for API responses (0 = use default)
	Operation  string // The operation that failed (e.g. "ExecuteBackup")
	Component  string // The component involved (e.g. "zeebe", "elasticsearch")
	InstanceID string // The Camunda instance ID, if applicable
	Cause      error  // The underlying error
}

// Error implements the error interface with full context.
func (e *AppError) Error() string {
	msg := e.Code + ": " + e.Message
	if e.Operation != "" {
		msg = "[" + e.Operation + "] " + msg
	}
	if e.Component != "" {
		msg += " (component=" + e.Component + ")"
	}
	if e.InstanceID != "" {
		msg += " (instance=" + e.InstanceID + ")"
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

// Unwrap returns the underlying cause for errors.Is / errors.As chains.
func (e *AppError) Unwrap() error {
	return e.Cause
}

// NewAppError creates a new AppError.
func NewAppError(code, message string, httpStatus int) *AppError {
	return &AppError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
	}
}

// WithOperation returns a copy with the Operation field set.
func (e *AppError) WithOperation(op string) *AppError {
	cp := *e
	cp.Operation = op
	return &cp
}

// WithComponent returns a copy with the Component field set.
func (e *AppError) WithComponent(comp string) *AppError {
	cp := *e
	cp.Component = comp
	return &cp
}

// WithInstance returns a copy with the InstanceID field set.
func (e *AppError) WithInstance(id string) *AppError {
	cp := *e
	cp.InstanceID = id
	return &cp
}

// Wrap returns a copy that wraps the given cause error.
func (e *AppError) Wrap(cause error) *AppError {
	cp := *e
	cp.Cause = cause
	return &cp
}

// WrapError creates a new AppError that wraps an existing error with context.
func WrapError(cause error, code, message string, httpStatus int) *AppError {
	return &AppError{
		Code:       code,
		Message:    message,
		HTTPStatus: httpStatus,
		Cause:      cause,
	}
}

// IsAppError checks if an error (or any error in its chain) is an *AppError
// and returns it. Returns nil if not found.
func IsAppError(err error) *AppError {
	var appErr *AppError
	if errors.As(err, &appErr) {
		return appErr
	}
	return nil
}

// ToHTTPError extracts an HTTP status code and JSON-friendly error body
// from an error. If the error is an *AppError, its fields are used;
// otherwise a generic 500 is returned.
func ToHTTPError(err error) (int, map[string]string) {
	if appErr := IsAppError(err); appErr != nil {
		status := appErr.HTTPStatus
		if status == 0 {
			status = http.StatusInternalServerError
		}
		body := map[string]string{
			"error":   appErr.Code,
			"message": appErr.Message,
		}
		if appErr.Component != "" {
			body["component"] = appErr.Component
		}
		if appErr.InstanceID != "" {
			body["instance_id"] = appErr.InstanceID
		}
		return status, body
	}
	return http.StatusInternalServerError, map[string]string{
		"error":   "internal_error",
		"message": err.Error(),
	}
}

// Common AppError codes reusable across the application.
var (
	ErrCodeBackupFailed   = "backup_failed"
	ErrCodeBackupTimeout  = "backup_timeout"
	ErrCodeCircuitOpen    = "circuit_open"
	ErrCodeCleanupFailed  = "cleanup_failed"
	ErrCodeNotFound       = "not_found"
	ErrCodeValidation     = "validation_error"
	ErrCodeExternalCall   = "external_call_failed"
	ErrCodeRetryExhausted = "retry_exhausted"
	ErrCodeTimeout        = "timeout"
)

// Convenience constructors for common error patterns.

func NewBackupFailedError(message string, cause error) *AppError {
	return &AppError{Code: ErrCodeBackupFailed, Message: message, HTTPStatus: http.StatusInternalServerError, Cause: cause}
}

func NewTimeoutError(message string, cause error) *AppError {
	return &AppError{Code: ErrCodeTimeout, Message: message, HTTPStatus: http.StatusGatewayTimeout, Cause: cause}
}

func NewCircuitOpenError(service string) *AppError {
	return &AppError{
		Code:       ErrCodeCircuitOpen,
		Message:    fmt.Sprintf("circuit breaker is open for service: %s", service),
		HTTPStatus: http.StatusServiceUnavailable,
	}
}

func NewExternalCallError(message string, cause error) *AppError {
	return &AppError{Code: ErrCodeExternalCall, Message: message, HTTPStatus: http.StatusBadGateway, Cause: cause}
}

func NewCleanupFailedError(message string, cause error) *AppError {
	return &AppError{Code: ErrCodeCleanupFailed, Message: message, HTTPStatus: http.StatusInternalServerError, Cause: cause}
}

func NewValidationError(message string) *AppError {
	return &AppError{Code: ErrCodeValidation, Message: message, HTTPStatus: http.StatusBadRequest}
}

func NewNotFoundError(message string) *AppError {
	return &AppError{Code: ErrCodeNotFound, Message: message, HTTPStatus: http.StatusNotFound}
}
