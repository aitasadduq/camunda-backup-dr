package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func TestLoggingMiddleware(t *testing.T) {
	logger := utils.NewLogger("error")

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	middleware := LoggingMiddleware(logger)
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
	}
}

func TestRecoveryMiddleware(t *testing.T) {
	logger := utils.NewLogger("error")

	// Handler that panics
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	middleware := RecoveryMiddleware(logger)
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	// Should not panic
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected status %d, got %d", http.StatusInternalServerError, w.Code)
	}
}

func TestCORSMiddleware(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CORSMiddleware()
	wrappedHandler := middleware(handler)

	// Test same-origin request — origin matches host
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Host = "localhost:8080"
	req.Header.Set("Origin", "http://localhost:8080")
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "http://localhost:8080" {
		t.Errorf("expected CORS origin to be reflected for same-origin, got: %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSMiddleware_CrossOriginBlocked(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CORSMiddleware()
	wrappedHandler := middleware(handler)

	// Test cross-origin request — origin does not match host
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Host = "localhost:8080"
	req.Header.Set("Origin", "http://evil.com")
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS origin header for cross-origin, got: %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
}

func TestCORSMiddleware_NoOriginHeader(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CORSMiddleware()
	wrappedHandler := middleware(handler)

	// Request without Origin header (same-origin browser requests omit it)
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Errorf("expected no CORS origin header when Origin is absent, got: %s", w.Header().Get("Access-Control-Allow-Origin"))
	}
	if w.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", w.Code)
	}
}

func TestCORSMiddleware_Preflight(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CORSMiddleware()
	wrappedHandler := middleware(handler)

	// Test preflight request
	req := httptest.NewRequest(http.MethodOptions, "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("expected status %d for preflight, got %d", http.StatusNoContent, w.Code)
	}
}

func TestChainMiddleware(t *testing.T) {
	callOrder := []string{}

	middleware1 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callOrder = append(callOrder, "m1-before")
			next.ServeHTTP(w, r)
			callOrder = append(callOrder, "m1-after")
		})
	}

	middleware2 := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			callOrder = append(callOrder, "m2-before")
			next.ServeHTTP(w, r)
			callOrder = append(callOrder, "m2-after")
		})
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callOrder = append(callOrder, "handler")
		w.WriteHeader(http.StatusOK)
	})

	chain := ChainMiddleware(middleware1, middleware2)
	wrappedHandler := chain(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	expected := []string{"m1-before", "m2-before", "handler", "m2-after", "m1-after"}
	if len(callOrder) != len(expected) {
		t.Errorf("expected %d calls, got %d", len(expected), len(callOrder))
	}
	for i, v := range expected {
		if callOrder[i] != v {
			t.Errorf("expected call order %v, got %v", expected, callOrder)
			break
		}
	}
}

func TestResponseWriter_CapturesStatusCode(t *testing.T) {
	w := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

	rw.WriteHeader(http.StatusNotFound)

	if rw.statusCode != http.StatusNotFound {
		t.Errorf("expected status code %d, got %d", http.StatusNotFound, rw.statusCode)
	}
}

func TestCSRFMiddleware_AllowsGET(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	// GET requests should pass without the header
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("GET should be allowed without X-Requested-With, got %d", w.Code)
	}
}

func TestCSRFMiddleware_BlocksPOSTWithoutHeader(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodPost, "/api/camundas", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("POST without X-Requested-With should be 403, got %d", w.Code)
	}
}

func TestCSRFMiddleware_AllowsPOSTWithHeader(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodPost, "/api/camundas", nil)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("POST with X-Requested-With should be 200, got %d", w.Code)
	}
}

func TestCSRFMiddleware_BlocksPUTWithoutHeader(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodPut, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("PUT without X-Requested-With should be 403, got %d", w.Code)
	}
}

func TestCSRFMiddleware_BlocksDELETEWithoutHeader(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/test-1", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("DELETE without X-Requested-With should be 403, got %d", w.Code)
	}
}

func TestCSRFMiddleware_AllowsNonAPIRoutes(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	// POST to a non-API path (e.g. webhook) should pass without the header
	req := httptest.NewRequest(http.MethodPost, "/healthz", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("POST to non-API path should be allowed without header, got %d", w.Code)
	}
}

func TestCSRFMiddleware_RejectsWrongHeaderValue(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodPost, "/api/camundas", nil)
	req.Header.Set("X-Requested-With", "SomethingElse")
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Errorf("POST with wrong X-Requested-With value should be 403, got %d", w.Code)
	}
}

func TestCSRFMiddleware_AllowsOPTIONS(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodOptions, "/api/camundas", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Errorf("OPTIONS should be allowed without header, got %d", w.Code)
	}
}

func TestContentTypeMiddleware_SetsJSONForAPIRoutes(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := ContentTypeMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type 'application/json' for /api route, got '%s'", ct)
	}
}

func TestContentTypeMiddleware_NoContentTypeForNonAPIRoutes(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := ContentTypeMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	ct := w.Header().Get("Content-Type")
	if ct == "application/json" {
		t.Errorf("expected no application/json for non-/api route, got '%s'", ct)
	}
}

func TestContentTypeMiddleware_ShortPath(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := ContentTypeMiddleware()
	wrappedHandler := middleware(handler)

	// Path shorter than 4 characters should not trigger content type setting
	req := httptest.NewRequest(http.MethodGet, "/ab", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	ct := w.Header().Get("Content-Type")
	if ct == "application/json" {
		t.Errorf("short path should not get application/json, got '%s'", ct)
	}
}

func TestRecoveryMiddleware_PanicWithAppError(t *testing.T) {
	logger := utils.NewLogger("error")

	appErr := utils.NewAppError("backup_failed", "Backup exploded", http.StatusBadGateway)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(appErr)
	})

	middleware := RecoveryMiddleware(logger)
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusBadGateway {
		t.Errorf("expected status %d for AppError panic, got %d", http.StatusBadGateway, w.Code)
	}
}

func TestCSRFMiddleware_AllowsHEAD(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	middleware := CSRFMiddleware()
	wrappedHandler := middleware(handler)

	req := httptest.NewRequest(http.MethodHead, "/api/status", nil)
	w := httptest.NewRecorder()
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("HEAD should be allowed without X-Requested-With, got %d", w.Code)
	}
}
