package utils

import (
	"fmt"
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // Normal operation — requests flow through
	CircuitOpen                         // Too many failures — requests are rejected
	CircuitHalfOpen                     // Probing — limited requests to test recovery
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "CLOSED"
	case CircuitOpen:
		return "OPEN"
	case CircuitHalfOpen:
		return "HALF_OPEN"
	default:
		return "UNKNOWN"
	}
}

// CircuitBreakerConfig holds configuration for a circuit breaker.
type CircuitBreakerConfig struct {
	MaxFailures     int           // Failures before opening the circuit
	ResetTimeout    time.Duration // How long to wait before moving to half-open
	HalfOpenMaxCalls int          // Number of probe calls allowed in half-open state
}

// DefaultCircuitBreakerConfig returns sensible defaults.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxFailures:      5,
		ResetTimeout:     60 * time.Second,
		HalfOpenMaxCalls: 1,
	}
}

// CircuitBreaker implements the circuit breaker pattern for external service calls.
type CircuitBreaker struct {
	name            string
	config          CircuitBreakerConfig
	mu              sync.Mutex
	state           CircuitState
	failures        int
	successes       int
	lastFailureTime time.Time
	halfOpenCalls   int
	onStateChange   func(name string, from, to CircuitState) // optional callback
}

// NewCircuitBreaker creates a new circuit breaker.
func NewCircuitBreaker(name string, config CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		name:   name,
		config: config,
		state:  CircuitClosed,
	}
}

// OnStateChange sets a callback invoked when the circuit state changes.
func (cb *CircuitBreaker) OnStateChange(fn func(name string, from, to CircuitState)) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.onStateChange = fn
}

// Execute runs the given function through the circuit breaker.
// Returns ErrCircuitBreakerOpen if the circuit is open.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	if err := cb.beforeCall(); err != nil {
		return err
	}

	err := fn()

	cb.afterCall(err)
	return err
}

// State returns the current state of the circuit breaker.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	// Check for auto-transition from OPEN → HALF_OPEN
	if cb.state == CircuitOpen && time.Since(cb.lastFailureTime) >= cb.config.ResetTimeout {
		cb.halfOpenCalls = 0
		cb.transitionTo(CircuitHalfOpen)
	}
	return cb.state
}

// Reset manually resets the circuit breaker to closed state.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.transitionTo(CircuitClosed)
	cb.failures = 0
	cb.successes = 0
	cb.halfOpenCalls = 0
}

// Name returns the circuit breaker name.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// Failures returns the current failure count.
func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}

func (cb *CircuitBreaker) beforeCall() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		// Check if enough time has passed to transition to half-open
		if time.Since(cb.lastFailureTime) >= cb.config.ResetTimeout {
			cb.transitionTo(CircuitHalfOpen)
			cb.halfOpenCalls = 1
			return nil
		}
		return fmt.Errorf("%w: %s", ErrCircuitBreakerOpen, cb.name)
	case CircuitHalfOpen:
		if cb.halfOpenCalls >= cb.config.HalfOpenMaxCalls {
			return fmt.Errorf("%w: %s (half-open probe limit reached)", ErrCircuitBreakerOpen, cb.name)
		}
		cb.halfOpenCalls++
		return nil
	}
	return nil
}

func (cb *CircuitBreaker) afterCall(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.recordFailure()
	} else {
		cb.recordSuccess()
	}
}

func (cb *CircuitBreaker) recordFailure() {
	cb.failures++
	cb.lastFailureTime = time.Now()
	cb.successes = 0

	switch cb.state {
	case CircuitClosed:
		if cb.failures >= cb.config.MaxFailures {
			cb.transitionTo(CircuitOpen)
		}
	case CircuitHalfOpen:
		// Any failure in half-open reverts to open
		cb.transitionTo(CircuitOpen)
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	switch cb.state {
	case CircuitClosed:
		// Reset failure count on success
		cb.failures = 0
	case CircuitHalfOpen:
		cb.successes++
		// A successful call in half-open transitions back to closed
		cb.transitionTo(CircuitClosed)
		cb.failures = 0
		cb.successes = 0
		cb.halfOpenCalls = 0
	}
}

func (cb *CircuitBreaker) transitionTo(newState CircuitState) {
	if cb.state == newState {
		return
	}
	old := cb.state
	cb.state = newState
	if cb.onStateChange != nil {
		// Fire callback asynchronously in a separate goroutine.
		fn := cb.onStateChange
		go fn(cb.name, old, newState)
	}
}
