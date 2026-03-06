package api

import (
	"io/fs"
	"net/http"
	"strings"
)

// Router is a simple HTTP router with method-based routing
type Router struct {
	handlers *Handlers
	mux      *http.ServeMux
	webFS    fs.FS
}

// NewRouter creates a new router with the given handlers
func NewRouter(handlers *Handlers, webFS fs.FS) *Router {
	router := &Router{
		handlers: handlers,
		mux:      http.NewServeMux(),
		webFS:    webFS,
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

	// Endpoint connectivity check
	r.mux.HandleFunc("/api/check-endpoint", r.methodHandler(map[string]http.HandlerFunc{
		http.MethodPost: r.handlers.CheckEndpointHandler,
	}))

	// Camunda instances - collection endpoints
	r.mux.HandleFunc("/api/camundas", r.methodHandler(map[string]http.HandlerFunc{
		http.MethodGet:  r.handlers.ListCamundaInstancesHandler,
		http.MethodPost: r.handlers.CreateCamundaInstanceHandler,
	}))

	// Camunda instances - resource endpoints
	// Use a pattern that catches all /api/camundas/ routes and route based on path
	r.mux.HandleFunc("/api/camundas/", r.camundaResourceHandler())

	// Serve embedded web UI static files
	if r.webFS != nil {
		fileServer := http.FileServer(http.FS(r.webFS))
		r.mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
			urlPath := req.URL.Path

			// Reject paths containing directory traversal sequences
			if containsDotDot(urlPath) {
				http.NotFound(w, req)
				return
			}

			// Serve static assets directly.
			// Only paths under /css/ or /js/ are considered static assets.
			// Bare /css or /js (without trailing slash) are not valid asset paths.
			if strings.HasPrefix(urlPath, "/css/") || strings.HasPrefix(urlPath, "/js/") {
				fileServer.ServeHTTP(w, req)
				return
			}
			if urlPath == "/css" || urlPath == "/js" {
				http.NotFound(w, req)
				return
			}

			// For root and any other non-API path, serve index.html directly.
			// We read the file and write it ourselves to avoid http.FileServer's
			// redirect from /index.html -> / which causes a redirect loop.
			data, err := fs.ReadFile(r.webFS, "index.html")
			if err != nil {
				http.NotFound(w, req)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(data)
		})
	}
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
		case strings.HasSuffix(path, "/backups/orphaned"): // Reserved sub-paths only respond to GET for listing
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

		// GET /api/camundas/{id}/backups/{backupId}/logs
		case strings.HasSuffix(path, "/logs") && strings.Contains(path, "/backups/"):
			if req.Method != http.MethodGet {
				r.methodNotAllowed(w, req)
				return
			}
			r.handlers.GetBackupLogsHandler(w, req)

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

// containsDotDot checks whether the URL path contains ".." path segments
// that could be used for directory traversal.
func containsDotDot(urlPath string) bool {
	for _, seg := range strings.Split(urlPath, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}

// ServeHTTP implements http.Handler
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}
