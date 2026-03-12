package api

import (
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Middleware is a function that wraps an HTTP handler
type Middleware func(http.Handler) http.Handler

// LoggingMiddleware logs all HTTP requests
func LoggingMiddleware(logger *utils.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Create a response writer wrapper to capture the status code
			wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			// Process request
			next.ServeHTTP(wrappedWriter, r)

			// Calculate duration
			duration := time.Since(start)

			// Log the request
			logger.Info("%s %s %d %s", r.Method, r.URL.Path, wrappedWriter.statusCode, duration)
		})
	}
}

// RecoveryMiddleware recovers from panics and returns a 500 error
func RecoveryMiddleware(logger *utils.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					// Log the panic with full stack trace
					logger.Error("Panic recovered: %v\nStack trace:\n%s", err, debug.Stack())

					// If the panic value implements error, try to extract an AppError
					if errVal, ok := err.(error); ok {
						if appErr := utils.IsAppError(errVal); appErr != nil {
							writeAppError(w, appErr)
							return
						}
					}

					// Return a generic 500 error
					writeError(w, http.StatusInternalServerError, "internal_error", "Internal server error")
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// CORSMiddleware adds CORS headers for the web UI.
// Since the UI is served from the same origin, we restrict to same-origin
// rather than allowing all origins with "*".
func CORSMiddleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin != "" {
				// Only reflect the origin if it matches the Host header (same-origin)
				host := r.Host
				// Normalize: origin includes scheme, host does not
				if strings.HasSuffix(origin, "://"+host) {
					w.Header().Set("Access-Control-Allow-Origin", origin)
					w.Header().Set("Vary", "Origin")
				}
			}
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With")

			// Handle preflight requests
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// CSRFMiddleware protects state-changing API requests from Cross-Site Request
// Forgery by requiring the X-Requested-With header. Browsers block cross-origin
// requests with custom headers unless explicitly allowed by CORS preflight,
// making this an effective CSRF defense when combined with a restrictive CORS policy.
func CSRFMiddleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Only enforce on state-changing methods for API routes
			if r.Method != http.MethodGet && r.Method != http.MethodHead && r.Method != http.MethodOptions {
				if strings.HasPrefix(r.URL.Path, "/api/") {
					if r.Header.Get("X-Requested-With") != "XMLHttpRequest" {
						writeError(w, http.StatusForbidden, "csrf_rejected", "Missing or invalid X-Requested-With header")
						return
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ContentTypeMiddleware sets the default content type to application/json for API routes
func ContentTypeMiddleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Set default content type for API routes
			if len(r.URL.Path) >= 4 && r.URL.Path[:4] == "/api" {
				w.Header().Set("Content-Type", "application/json")
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ChainMiddleware chains multiple middleware functions together
func ChainMiddleware(middlewares ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

// responseWriter is a wrapper around http.ResponseWriter that captures the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code
func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}
