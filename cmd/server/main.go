package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
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

	// TODO: Initialize storage layer (Phase 2)
	// TODO: Initialize Camunda manager (Phase 3)
	// TODO: Initialize backup orchestrator (Phase 4)
	// TODO: Initialize scheduler (Phase 7)
	// TODO: Initialize HTTP API server (Phase 8)

	// Setup graceful shutdown
	setupGracefulShutdown(logger)

	logger.Info("Camunda Backup Controller started successfully")
	
	// Keep the main goroutine running
	select {}
}

func setupGracefulShutdown(logger *utils.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		logger.Info("Received signal: %v", sig)
		logger.Info("Shutting down gracefully...")

		// TODO: Stop accepting new requests (Phase 8)
		// TODO: Wait for in-progress backups to complete (Phase 8)
		// TODO: Shutdown scheduler gracefully (Phase 7)
		// TODO: Close file storage connections (Phase 2)

		logger.Info("Shutdown complete")
		os.Exit(0)
	}()
}