package api

import (
	"net/http"
	"strings"
)

// Router is a simple HTTP router with method-based routing
type Router struct {
	handlers *Handlers
	mux      *http.ServeMux
}

// NewRouter creates a new router with the given handlers
func NewRouter(handlers *Handlers) *Router {
	router := &Router{
		handlers: handlers,
		mux:      http.NewServeMux(),
	}
	router.registerRoutes()
	return router
}

// registerRoutes registers all API routes
func (r *Router) registerRoutes() {
	// Health check endpoints (outside /api)
	r.mux.HandleFunc("/healthz", r.handlers.HealthzHandler)
	r.mux.HandleFunc("/readyz", r.handlers.ReadyzHandler)

	// System status
	r.mux.HandleFunc("/api/status", r.methodHandler(map[string]http.HandlerFunc{
		http.MethodGet: r.handlers.SystemStatusHandler,
	}))

	// Camunda instances - collection endpoints
	r.mux.HandleFunc("/api/camundas", r.methodHandler(map[string]http.HandlerFunc{
		http.MethodGet:  r.handlers.ListCamundaInstancesHandler,
		http.MethodPost: r.handlers.CreateCamundaInstanceHandler,
	}))

	// Camunda instances - resource endpoints
	// Use a pattern that catches all /api/camundas/ routes and route based on path
	r.mux.HandleFunc("/api/camundas/", r.camundaResourceHandler())
}

// camundaResourceHandler handles all /api/camundas/{id}... routes
func (r *Router) camundaResourceHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		path := req.URL.Path

		// Remove trailing slash for consistent matching
		path = strings.TrimSuffix(path, "/")

		// Determine which handler to use based on path suffix and method
		switch {
		// POST /api/camundas/{id}/enable
		case strings.HasSuffix(path, "/enable"):
			if req.Method != http.MethodPost {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.EnableCamundaInstanceHandler(w, req)

		// POST /api/camundas/{id}/disable
		case strings.HasSuffix(path, "/disable"):
			if req.Method != http.MethodPost {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.DisableCamundaInstanceHandler(w, req)

		// POST /api/camundas/{id}/backup
		case strings.HasSuffix(path, "/backup"):
			if req.Method != http.MethodPost {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.TriggerBackupHandler(w, req)

		// GET /api/camundas/{id}/backups/orphaned
		case strings.HasSuffix(path, "/backups/orphaned"):
			if req.Method != http.MethodGet {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.ListOrphanedBackupsHandler(w, req)

		// GET /api/camundas/{id}/backups/incomplete
		case strings.HasSuffix(path, "/backups/incomplete"):
			if req.Method != http.MethodGet {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.ListIncompleteBackupsHandler(w, req)

		// GET /api/camundas/{id}/backups/failed
		case strings.HasSuffix(path, "/backups/failed"):
			if req.Method != http.MethodGet {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.ListFailedBackupsHandler(w, req)

		// GET/DELETE /api/camundas/{id}/backups/{backupId}
		case strings.Contains(path, "/backups/"):
			switch req.Method {
			case http.MethodGet:
				r.handlers.GetBackupDetailsHandler(w, req)
			case http.MethodDelete:
				r.handlers.DeleteBackupHandler(w, req)
			default:
				r.methodNotAllowed(w, req)
			}

		// GET /api/camundas/{id}/backups
		case strings.HasSuffix(path, "/backups"):
			if req.Method != http.MethodGet {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.ListBackupHistoryHandler(w, req)

		// GET/PUT/DELETE /api/camundas/{id}
		default:
			switch req.Method {
			case http.MethodGet:
				r.handlers.GetCamundaInstanceHandler(w, req)
			case http.MethodPut:
				r.handlers.UpdateCamundaInstanceHandler(w, req)
			case http.MethodDelete:
				r.handlers.DeleteCamundaInstanceHandler(w, req)
			default:
				r.methodNotAllowed(w, req)
			}
		}
	}
}

// methodHandler creates a handler that routes based on HTTP method
func (r *Router) methodHandler(methods map[string]http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		// Handle OPTIONS for CORS preflight
		if req.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		handler, ok := methods[req.Method]
		if !ok {
			r.methodNotAllowed(w, req)
			return
		}
		handler(w, req)
	}
}

// methodNotAllowed returns a 405 Method Not Allowed response
func (r *Router) methodNotAllowed(w http.ResponseWriter, req *http.Request) {
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method "+req.Method+" not allowed")
}

// ServeHTTP implements http.Handler
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}
