package api

import (
	"context"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Server represents the HTTP server
type Server struct {
	server   *http.Server
	listener net.Listener
	handlers *Handlers
	router   *Router
	logger   *utils.Logger
	port     int
}

// SetSecretStore registers the store used to persist credentials entered in the UI.
func (s *Server) SetSecretStore(store SecretStore) {
	s.handlers.SetSecretStore(store)
}

// SetReconciler registers the reconciler backing the reconcile endpoints.
func (s *Server) SetReconciler(r Reconciler) {
	s.handlers.SetReconciler(r)
}

// NewServer creates a new HTTP server
func NewServer(
	port int,
	camundaManager CamundaManager,
	orchestrator BackupOrchestrator,
	historyProvider BackupHistoryProvider,
	scheduler SchedulerInterface,
	retentionManager RetentionManager,
	logFileReader LogFileReader,
	logger *utils.Logger,
	webFS fs.FS,
	cfg *config.Config,
) *Server {
	basePath := "/"
	if cfg != nil {
		basePath = cfg.BasePath
	}

	handlers := NewHandlers(camundaManager, orchestrator, historyProvider, scheduler, retentionManager, logFileReader, logger, cfg)
	router := NewRouter(handlers, webFS, basePath)

	// Apply middleware chain
	middlewareChain := ChainMiddleware(
		RecoveryMiddleware(logger),
		LoggingMiddleware(logger),
		CORSMiddleware(),
		CSRFMiddleware(),
		ContentTypeMiddleware(),
	)

	handler := middlewareChain(router)

	// When a base path is configured (e.g. "/backup"), strip the prefix
	// so that the router sees clean paths like "/api/..." and "/css/...".
	// Also redirect "/backup" -> "/backup/" so relative URLs resolve correctly.
	if basePath != "/" {
		stripped := http.StripPrefix(basePath, handler)
		handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == basePath {
				http.Redirect(w, r, basePath+"/", http.StatusTemporaryRedirect)
				return
			}
			if !strings.HasPrefix(r.URL.Path, basePath+"/") {
				http.NotFound(w, r)
				return
			}
			stripped.ServeHTTP(w, r)
		})
	}

	return &Server{
		handlers: handlers,
		router:   router,
		logger:   logger,
		port:     port,
		server: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      handler,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 120 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
	}
}

// Start starts the HTTP server
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP server on port %d", s.port)

	// Bind the socket synchronously to detect port conflicts immediately
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to bind to port %d: %w", s.port, err)
	}
	s.listener = listener

	// Start serving in background
	go func() {
		if err := s.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error: %v", err)
		}
	}()

	s.logger.Info("HTTP server started successfully on port %d", s.port)
	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP server...")

	// Create a timeout context if not provided
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
	}

	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	s.logger.Info("HTTP server shutdown complete")
	return nil
}

// GetPort returns the port the server is listening on
func (s *Server) GetPort() int {
	return s.port
}
