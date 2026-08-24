package main

import (
	"context"
	"errors"
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
	"github.com/aitasadduq/camunda-backup-dr/internal/reconcile"
	"github.com/aitasadduq/camunda-backup-dr/internal/retention"
	"github.com/aitasadduq/camunda-backup-dr/internal/scheduler"
	"github.com/aitasadduq/camunda-backup-dr/internal/secrets"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
	"github.com/aitasadduq/camunda-backup-dr/web"
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

	// Initialize the secret store for credentials entered through the UI
	secretStore, err := secrets.NewStore(cfg.DataDir, logger)
	if err != nil {
		logger.Error("Failed to initialize secret store: %v", err)
		log.Fatalf("Failed to initialize secret store: %v", err)
	}
	cfg.SetSecretProvider(secretStore)
	logger.Info("Secret store initialized successfully")

	// Initialize S3 storage
	var s3Storage storage.S3Storage
	s3Endpoint := cfg.DefaultS3Endpoint
	s3AccessKey := cfg.DefaultS3AccessKey
	s3SecretKey := cfg.DefaultS3SecretKey

	if s3Endpoint != "" && s3AccessKey != "" && s3SecretKey != "" {
		s3Client, err := storage.NewS3Client(storage.S3Config{
			Endpoint:     s3Endpoint,
			AccessKey:    s3AccessKey,
			SecretKey:    s3SecretKey,
			Bucket:       cfg.DefaultS3Bucket,
			Prefix:       cfg.DefaultS3Prefix,
			Region:       cfg.DefaultS3Region,
			UsePathStyle: cfg.DefaultS3UsePathStyle,
		}, logger)
		if err != nil {
			logger.Error("Failed to initialize S3 storage: %v", err)
			log.Fatalf("Failed to initialize S3 storage: %v", err)
		}
		s3Storage = s3Client
		logger.Info("S3 storage initialized successfully (endpoint: %s, bucket: %s)", s3Endpoint, cfg.DefaultS3Bucket)
	} else {
		logger.Error("S3 credentials not configured — DEFAULT_S3_ENDPOINT, DEFAULT_S3_ACCESSKEY, and DEFAULT_S3_SECRETKEY are required")
		log.Fatalf("S3 credentials not configured — set DEFAULT_S3_ENDPOINT, DEFAULT_S3_ACCESSKEY, and DEFAULT_S3_SECRETKEY")
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

	// Wire up retention manager to orchestrator
	retentionManager := retention.NewManager(s3Storage, fileStorage, httpClient, cfg, logger)
	backupOrchestrator.SetRetentionFunc(func(instance *models.CamundaInstance) {
		retentionManager.ApplyRetention(instance)
	})
	logger.Info("Retention manager initialized and wired to orchestrator")

	// Reconciler: cross-references controller metadata against the artifacts that
	// actually exist. Report-only, so it is safe to run automatically.
	reconciler := reconcile.NewReconciler(s3Storage, fileStorage, httpClient, cfg, logger)
	// Lets the sweep tell a backup that is pausing exporting right now from one
	// that failed to resume it.
	reconciler.SetBackupRunningFunc(backupOrchestrator.IsBackupRunning)
	reconciler.SetOptions(reconcile.Options{
		GracePeriod: time.Duration(cfg.ReconcileGracePeriodMinutes) * time.Minute,
		StaleAfter:  time.Duration(cfg.ReconcileStaleAfterMinutes) * time.Minute,
	})
	if cfg.ReconcileEnabled {
		// Hooked to the orchestrator rather than to ApplyRetention: retention runs
		// only for COMPLETED and FAILED backups, and an INCOMPLETE backup is
		// exactly the case where a sweep matters most.
		backupOrchestrator.SetReconcileFunc(func(instance *models.CamundaInstance) {
			sweepCtx, cancel := context.WithTimeout(context.Background(),
				time.Duration(cfg.ReconcileTimeoutSeconds)*time.Second)
			defer cancel()
			if _, err := reconciler.Reconcile(sweepCtx, instance); err != nil {
				if errors.Is(err, reconcile.ErrSweepInProgress) {
					logger.Info("Skipping post-backup reconciliation for %s: a sweep is already running", instance.ID)
					return
				}
				logger.Error("Post-backup reconciliation failed for %s: %v", instance.ID, err)
			}
		})
		logger.Info("Reconciler initialized and wired to orchestrator")
	} else {
		logger.Info("Reconciler disabled (RECONCILE_ENABLED=false); on-demand sweeps remain available")
	}

	// Persist last-backup time/status to the instance config after each backup
	// so the UI reflects the most recent backup result.
	backupOrchestrator.SetLastBackupFunc(func(instanceID string, backupTime time.Time, status string) {
		if err := camundaManager.UpdateLastBackup(instanceID, backupTime, status); err != nil {
			logger.Error("Failed to update last backup for instance %s: %v", instanceID, err)
		}
	})

	// Initialize alerter for critical failure notifications
	alerter := utils.NewAlerter(cfg.AlertWebhookURL, logger)
	alerter.SetFilter(utils.AlertFilter{
		BackupFailed:         cfg.AlertEnableBackupFailed,
		CleanupFailed:        cfg.AlertEnableCleanupFailed,
		StuckBackup:          cfg.AlertEnableStuckBackup,
		CircuitOpen:          cfg.AlertEnableCircuitOpen,
		SchedulerError:       cfg.AlertEnableSchedulerError,
		ExporterResumeFailed: cfg.AlertEnableExporterResumeFailed,
	})
	if alerter.IsEnabled() {
		logger.Info("Alert webhook configured: notifications enabled")
	} else {
		logger.Info("Alert webhook not configured: notifications disabled")
	}
	backupOrchestrator.SetAlerter(alerter)
	retentionManager.SetAlerter(alerter)

	// Create backup executor adapter for scheduler
	backupExecutor := &backupExecutorAdapter{orchestrator: backupOrchestrator}

	// Initialize scheduler
	schedulerCfg := scheduler.DefaultConfig()
	if cfg.BackupStuckTimeoutMinutes > 0 {
		schedulerCfg.StuckTimeout = time.Duration(cfg.BackupStuckTimeoutMinutes) * time.Minute
	} else {
		schedulerCfg.StuckTimeout = 0 // disabled
	}
	sched := scheduler.NewScheduler(backupExecutor, camundaManager, logger, schedulerCfg)
	sched.SetAlerter(alerter)
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
		retentionManager,
		fileStorage,
		logger,
		web.FS,
		cfg,
	)
	server.SetSecretStore(secretStore)
	server.SetReconciler(reconciler)

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
