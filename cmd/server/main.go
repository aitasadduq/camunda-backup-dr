package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/api"
	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/orchestrator"
	"github.com/aitasadduq/camunda-backup-dr/internal/scheduler"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Initialize logger
	logger := utils.NewLogger(cfg.LogLevel)

	logger.Info("Starting Camunda Backup Controller...")
	logger.Info("Configuration loaded successfully")
	logger.Info("Data directory: %s", cfg.DataDir)
	logger.Info("Log level: %s", cfg.LogLevel)
	logger.Info("Port: %d", cfg.Port)

	// Initialize file storage
	fileStorage, err := storage.NewFileStorage(cfg.DataDir, cfg, logger)
	if err != nil {
		logger.Error("Failed to initialize file storage: %v", err)
		log.Fatalf("Failed to initialize file storage: %v", err)
	}
	logger.Info("File storage initialized successfully")

	// Initialize S3 storage (using mock/in-memory for now if no credentials)
	var s3Storage storage.S3Storage
	s3Endpoint := cfg.DefaultS3Endpoint
	s3AccessKey := cfg.DefaultS3AccessKey
	s3SecretKey := os.Getenv("DEFAULT_S3_SECRETKEY")

	if s3Endpoint != "" && s3AccessKey != "" && s3SecretKey != "" {
		s3StorageImpl, err := storage.NewS3Storage(s3Endpoint, s3AccessKey, s3SecretKey, "camunda-backups", "", logger)
		if err != nil {
			logger.Warn("Failed to initialize S3 storage, using mock storage: %v", err)
			s3Storage = newMockS3Storage(logger)
		} else {
			s3Storage = s3StorageImpl
			logger.Info("S3 storage initialized successfully")
		}
	} else {
		logger.Info("S3 credentials not configured, using mock storage")
		s3Storage = newMockS3Storage(logger)
	}

	// Initialize Camunda manager
	camundaManager := camunda.NewManager(fileStorage, logger)
	logger.Info("Camunda manager initialized successfully")

	// Initialize HTTP client for Camunda API calls
	httpClientCfg := camunda.HTTPClientConfig{
		Timeout:       30 * time.Second,
		MaxRetries:    3,
		RetryDelay:    time.Second,
		MaxRetryDelay: 30 * time.Second,
	}
	httpClient := camunda.NewHTTPClient(httpClientCfg, logger)

	// Initialize backup orchestrator
	pollInterval := time.Duration(cfg.DefaultBackupPollInterval) * time.Second
	backupOrchestrator := orchestrator.NewOrchestrator(
		fileStorage,
		s3Storage,
		httpClient,
		cfg,
		logger,
		pollInterval,
		cfg.DefaultBackupMaxAttempts,
	)
	logger.Info("Backup orchestrator initialized successfully")

	// Create backup executor adapter for scheduler
	backupExecutor := &backupExecutorAdapter{orchestrator: backupOrchestrator}

	// Initialize scheduler
	schedulerCfg := scheduler.DefaultConfig()
	sched := scheduler.NewScheduler(backupExecutor, camundaManager, logger, schedulerCfg)
	logger.Info("Scheduler initialized successfully")

	// Start scheduler
	ctx := context.Background()
	if err := sched.Start(ctx); err != nil {
		logger.Error("Failed to start scheduler: %v", err)
		log.Fatalf("Failed to start scheduler: %v", err)
	}
	logger.Info("Scheduler started successfully")

	// Initialize HTTP server
	server := api.NewServer(
		cfg.Port,
		camundaManager,
		backupOrchestrator,
		s3Storage,
		sched,
		logger,
	)

	// Start HTTP server
	if err := server.Start(); err != nil {
		logger.Error("Failed to start HTTP server: %v", err)
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
	logger.Info("HTTP server started on port %d", cfg.Port)

	// Setup graceful shutdown
	setupGracefulShutdown(logger, server, sched)

	logger.Info("Camunda Backup Controller started successfully")

	// Keep the main goroutine running
	select {}
}

// backupExecutorAdapter adapts the orchestrator to the BackupExecutor interface
type backupExecutorAdapter struct {
	orchestrator *orchestrator.Orchestrator
}

func (a *backupExecutorAdapter) ExecuteScheduledBackup(ctx context.Context, instance *models.CamundaInstance) error {
	req := orchestrator.BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeScheduled,
		BackupReason:    "Scheduled backup",
	}
	_, err := a.orchestrator.ExecuteBackup(ctx, req)
	return err
}

func setupGracefulShutdown(logger *utils.Logger, server *api.Server, sched *scheduler.Scheduler) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal: %v", sig)
		logger.Info("Shutting down gracefully...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Stop accepting new requests
		logger.Info("Stopping HTTP server...")
		if err := server.Shutdown(ctx); err != nil {
			logger.Error("Failed to shutdown HTTP server: %v", err)
		}

		// Shutdown scheduler gracefully (waits for in-progress backups)
		logger.Info("Stopping scheduler...")
		if err := sched.Stop(ctx); err != nil {
			logger.Error("Failed to stop scheduler: %v", err)
		}

		logger.Info("Shutdown complete")
		os.Exit(0)
	}()
}

// mockS3Storage is a mock S3 storage for when credentials are not configured
type mockS3Storage struct {
	*storage.S3StorageImpl
}

func newMockS3Storage(logger *utils.Logger) storage.S3Storage {
	// Create a mock S3 storage that works without real credentials
	s3, _ := storage.NewS3Storage("http://mock", "mock", "mock", "mock-bucket", "", logger)
	return s3
}
