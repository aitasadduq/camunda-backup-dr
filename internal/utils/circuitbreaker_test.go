package utils

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitBreaker_ClosedState(t *testing.T) {
	cb := NewCircuitBreaker("test", DefaultCircuitBreakerConfig())

	if cb.State() != CircuitClosed {
		t.Fatalf("initial state should be CLOSED, got %s", cb.State())
	}

	err := cb.Execute(func() error { return nil })
	if err != nil {
		t.Fatalf("successful call should not error: %v", err)
	}

	if cb.Failures() != 0 {
		t.Errorf("failures should be 0, got %d", cb.Failures())
	}
}

func TestCircuitBreaker_OpensAfterMaxFailures(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 3, ResetTimeout: time.Minute, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test", cfg)

	simulatedErr := fmt.Errorf("connection refused")

	for i := 0; i < 3; i++ {
		_ = cb.Execute(func() error { return simulatedErr })
	}

	if cb.State() != CircuitOpen {
		t.Fatalf("state should be OPEN after %d failures, got %s", cfg.MaxFailures, cb.State())
	}

	// Next call should be rejected immediately
	err := cb.Execute(func() error { return nil })
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("expected ErrCircuitBreakerOpen, got: %v", err)
	}
}

func TestCircuitBreaker_TransitionsToHalfOpen(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 2, ResetTimeout: 100 * time.Millisecond, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test", cfg)

	// Trip the breaker
	for i := 0; i < 2; i++ {
		_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN, got %s", cb.State())
	}

	// Wait for reset timeout
	time.Sleep(150 * time.Millisecond)

	// Should transition to HALF_OPEN and allow one call
	if cb.State() != CircuitHalfOpen {
		t.Fatalf("expected HALF_OPEN after timeout, got %s", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 2, ResetTimeout: 100 * time.Millisecond, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test", cfg)

	// Trip the breaker
	for i := 0; i < 2; i++ {
		_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	}

	// Wait for reset
	time.Sleep(150 * time.Millisecond)

	// Successful call should close the circuit
	err := cb.Execute(func() error { return nil })
	if err != nil {
		t.Fatalf("half-open probe should succeed: %v", err)
	}

	if cb.State() != CircuitClosed {
		t.Fatalf("expected CLOSED after successful probe, got %s", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 2, ResetTimeout: 100 * time.Millisecond, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test", cfg)

	// Trip the breaker
	for i := 0; i < 2; i++ {
		_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	}

	// Wait for reset
	time.Sleep(150 * time.Millisecond)

	// Failed probe should reopen
	_ = cb.Execute(func() error { return fmt.Errorf("still broken") })

	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN after failed probe, got %s", cb.State())
	}
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 3, ResetTimeout: time.Minute, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test", cfg)

	// Two failures
	_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	if cb.Failures() != 2 {
		t.Fatalf("expected 2 failures, got %d", cb.Failures())
	}

	// Success resets
	_ = cb.Execute(func() error { return nil })
	if cb.Failures() != 0 {
		t.Fatalf("expected 0 failures after success, got %d", cb.Failures())
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: time.Minute, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test", cfg)

	_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN, got %s", cb.State())
	}

	cb.Reset()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected CLOSED after reset, got %s", cb.State())
	}
}

func TestCircuitBreaker_OnStateChange(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: time.Minute, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test-svc", cfg)

	var called atomic.Int32
	cb.OnStateChange(func(name string, from, to CircuitState) {
		called.Add(1)
	})

	_ = cb.Execute(func() error { return fmt.Errorf("fail") })

	// Give async callback time to fire
	time.Sleep(20 * time.Millisecond)

	if called.Load() == 0 {
		t.Error("state change callback should have been called")
	}
}

func TestCircuitBreaker_Name(t *testing.T) {
	cb := NewCircuitBreaker("my-service", DefaultCircuitBreakerConfig())
	if cb.Name() != "my-service" {
		t.Errorf("Name() = %q, want %q", cb.Name(), "my-service")
	}
}

func TestCircuitState_String(t *testing.T) {
	tests := []struct {
		state CircuitState
		want  string
	}{
		{CircuitClosed, "CLOSED"},
		{CircuitOpen, "OPEN"},
		{CircuitHalfOpen, "HALF_OPEN"},
		{CircuitState(99), "UNKNOWN"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("CircuitState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}
