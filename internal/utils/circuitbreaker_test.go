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

func TestCircuitBreaker_HalfOpenProbeLimitRejectsExtraCalls(t *testing.T) {
	cfg := CircuitBreakerConfig{MaxFailures: 2, ResetTimeout: 100 * time.Millisecond, HalfOpenMaxCalls: 1}
	cb := NewCircuitBreaker("test", cfg)

	// Trip the breaker
	for i := 0; i < 2; i++ {
		_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected OPEN, got %s", cb.State())
	}

	// Wait for reset timeout so OPEN → HALF_OPEN transition is eligible
	time.Sleep(150 * time.Millisecond)

	// First beforeCall: transitions OPEN → HALF_OPEN and allows the call
	if err := cb.beforeCall(); err != nil {
		t.Fatalf("first half-open call should be allowed: %v", err)
	}

	// Second beforeCall: probe limit reached, should be rejected
	err := cb.beforeCall()
	if err == nil {
		t.Fatal("expected rejection when half-open probe limit is reached")
	}
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("expected ErrCircuitBreakerOpen, got: %v", err)
	}
	if got := err.Error(); !containsSubstring(got, "half-open probe limit reached") {
		t.Errorf("error message should mention probe limit, got: %q", got)
	}
}

func TestCircuitBreaker_HalfOpenMultipleProbes(t *testing.T) {
	// With HalfOpenMaxCalls=3, three probes should be allowed before rejection
	cfg := CircuitBreakerConfig{MaxFailures: 1, ResetTimeout: 50 * time.Millisecond, HalfOpenMaxCalls: 3}
	cb := NewCircuitBreaker("multi-probe", cfg)

	_ = cb.Execute(func() error { return fmt.Errorf("fail") })
	time.Sleep(80 * time.Millisecond)

	// First three beforeCall invocations should succeed
	for i := 0; i < 3; i++ {
		if err := cb.beforeCall(); err != nil {
			t.Fatalf("beforeCall #%d should be allowed, got: %v", i+1, err)
		}
	}

	// Fourth should be rejected
	if err := cb.beforeCall(); err == nil {
		t.Fatal("fourth call should be rejected")
	}
}

func TestCircuitBreaker_TransitionTo_NoOp(t *testing.T) {
	// Calling Reset on an already-closed circuit exercises the transitionTo
	// no-op path (state == newState → early return).
	var called atomic.Int32
	cfg := DefaultCircuitBreakerConfig()
	cb := NewCircuitBreaker("noop-test", cfg)

	cb.OnStateChange(func(name string, from, to CircuitState) {
		called.Add(1)
	})

	// Circuit is already CLOSED; Reset calls transitionTo(CircuitClosed) which
	// should be a no-op — the callback must NOT fire.
	cb.Reset()

	time.Sleep(20 * time.Millisecond)

	if called.Load() != 0 {
		t.Error("transitionTo should be a no-op when state is unchanged; callback should not fire")
	}
}

// containsSubstring is a small helper to keep assertions readable.
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && containsCheck(s, substr)
}

func containsCheck(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
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
