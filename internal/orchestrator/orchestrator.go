package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Orchestrator manages the backup workflow for Camunda instances
type Orchestrator struct {
	fileStorage storage.FileStorage
	s3Storage   storage.S3Storage
	httpClient  *camunda.HTTPClient
	logger      *utils.Logger
	mutex       sync.Mutex
}

// NewOrchestrator creates a new backup orchestrator
func NewOrchestrator(
	fileStorage storage.FileStorage,
	s3Storage storage.S3Storage,
	httpClient *camunda.HTTPClient,
	logger *utils.Logger,
) *Orchestrator {
	return &Orchestrator{
		fileStorage: fileStorage,
		s3Storage:   s3Storage,
		httpClient:  httpClient,
		logger:      logger,
	}
}

// BackupRequest represents a backup request
type BackupRequest struct {
	CamundaInstance *models.CamundaInstance
	TriggerType     types.TriggerType
	BackupReason    string
}

// ExecuteBackup orchestrates the complete backup workflow for a Camunda instance
func (o *Orchestrator) ExecuteBackup(ctx context.Context, req BackupRequest) (*models.BackupExecution, error) {
	if req.CamundaInstance == nil {
		return nil, fmt.Errorf("camunda instance is nil")
	}
	// Generate backup ID
	backupID := camunda.GenerateBackupID()
	o.logger.Info("Starting backup for instance %s (ID: %s)", req.CamundaInstance.Name, backupID)

	// Create backup execution record
	execution := models.NewBackupExecution(req.CamundaInstance.ID, backupID)

	// Create log file
	if err := o.fileStorage.CreateLogFile(req.CamundaInstance.ID, backupID); err != nil {
		return nil, fmt.Errorf("failed to create log file: %w", err)
	}

	// Write initial log
	o.writeLog(req.CamundaInstance.ID, backupID, fmt.Sprintf("Backup started at %s", execution.StartTime.Format(time.RFC3339)))
	o.writeLog(req.CamundaInstance.ID, backupID, fmt.Sprintf("Trigger type: %s", req.TriggerType))
	o.writeLog(req.CamundaInstance.ID, backupID, fmt.Sprintf("Execution mode: %s", o.getExecutionMode(req.CamundaInstance)))

	o.mutex.Lock()
	defer o.mutex.Unlock()
	// Store backup ID in S3 before triggering components
	if err := o.s3Storage.StoreLatestBackupID(req.CamundaInstance.ID, backupID); err != nil {
		o.writeLog(req.CamundaInstance.ID, backupID, fmt.Sprintf("Failed to store backup ID in S3: %v", err))
		execution.Status = types.BackupStatusFailed
		execution.ErrorMessage = fmt.Sprintf("Failed to store backup ID in S3: %v", err)

		// Best-effort cleanup of the log file created for this backup to avoid orphaned logs.
		if cleanupErr := o.fileStorage.DeleteLogFile(req.CamundaInstance.ID, backupID); cleanupErr != nil {
			o.logger.Error("failed to delete log file for backup %s (instance %s): %v", backupID, req.CamundaInstance.ID, cleanupErr)
		}
		return execution, err
	}
	o.writeLog(req.CamundaInstance.ID, backupID, "Backup ID stored in S3")

	// Initialize component statuses
	enabledComponents := req.CamundaInstance.GetEnabledComponents()
	for _, comp := range enabledComponents {
		execution.UpdateComponentStatus(comp, types.ComponentStatusPending)
	}

	// Create initial backup history
	history := o.createBackupHistory(req, execution)
	if err := o.s3Storage.StoreBackupHistory(history); err != nil {
		o.writeLog(req.CamundaInstance.ID, backupID, fmt.Sprintf("Failed to store initial backup history: %v", err))
	}

	// Execute components based on execution mode
	if req.CamundaInstance.ParallelExecution {
		o.writeLog(req.CamundaInstance.ID, backupID, "Executing components in parallel mode")
		o.executeComponentsParallel(ctx, req.CamundaInstance, execution)
	} else {
		o.writeLog(req.CamundaInstance.ID, backupID, "Executing components in sequential mode")
		o.executeComponentsSequential(ctx, req.CamundaInstance, execution)
	}

	// Finalize backup execution
	o.finalizeBackup(req, execution)

	// Update backup history with final state
	history = o.createBackupHistory(req, execution)
	if err := o.s3Storage.StoreBackupHistory(history); err != nil {
		o.logger.Error("Failed to update backup history: %v", err)
	}

	o.writeLog(req.CamundaInstance.ID, backupID, fmt.Sprintf("Backup completed with status: %s", execution.Status))

	return execution, nil
}

// executeComponentsParallel executes all enabled components concurrently
func (o *Orchestrator) executeComponentsParallel(ctx context.Context, instance *models.CamundaInstance, execution *models.BackupExecution) {
	var wg sync.WaitGroup
	var mu sync.Mutex

	enabledComponents := instance.GetEnabledComponents()
	o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Starting parallel execution of %d components", len(enabledComponents)))

	for _, component := range enabledComponents {
		wg.Add(1)
		go func(comp string) {
			defer wg.Done()

			// Update status to running
			mu.Lock()
			execution.UpdateComponentStatus(comp, types.ComponentStatusRunning)
			mu.Unlock()

			// Execute component backup
			status, err := o.executeComponentBackup(ctx, instance, execution.BackupID, comp)

			// Update status
			mu.Lock()
			execution.UpdateComponentStatus(comp, status)
			if err != nil {
				o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Component %s failed: %v", comp, err))
			}
			mu.Unlock()
		}(component)
	}

	wg.Wait()

	// If the context was cancelled, reflect that in the final log message.
	// Note: in-flight component backups may have continued to completion even after cancellation.
	if err := ctx.Err(); err != nil {
		o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Parallel execution cancelled: %v. In-flight component backups may have completed under cancellation.", err))
		return
	}
	o.writeLog(instance.ID, execution.BackupID, "All components completed in parallel mode")
}

// executeComponentsSequential executes enabled components one after another
func (o *Orchestrator) executeComponentsSequential(ctx context.Context, instance *models.CamundaInstance, execution *models.BackupExecution) {
	enabledComponents := instance.GetEnabledComponents()
	o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Starting sequential execution of %d components", len(enabledComponents)))

	for _, component := range enabledComponents {
		// Update status to running
		execution.UpdateComponentStatus(component, types.ComponentStatusRunning)
		o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Starting backup for component: %s", component))

		// Execute component backup
		status, err := o.executeComponentBackup(ctx, instance, execution.BackupID, component)

		// Update status
		execution.UpdateComponentStatus(component, status)

		if err != nil {
			o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Component %s failed: %v", component, err))
			// On failure, log the error and continue to the next component to allow partial backups
		} else if status == types.ComponentStatusSkipped {
			o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Component %s was skipped", component))
		} else {
			o.writeLog(instance.ID, execution.BackupID, fmt.Sprintf("Component %s completed successfully", component))
		}
	}

	o.writeLog(instance.ID, execution.BackupID, "All components completed in sequential mode")
}

// executeComponentBackup executes backup for a specific component
func (o *Orchestrator) executeComponentBackup(ctx context.Context, instance *models.CamundaInstance, backupID, component string) (types.ComponentStatus, error) {
	o.writeLog(instance.ID, backupID, fmt.Sprintf("Executing backup for component: %s", component))

	switch component {
	case types.ComponentZeebe:
		return o.executeZeebeBackup(ctx, instance, backupID)
	case types.ComponentOperate:
		return o.executeOperateBackup(ctx, instance, backupID)
	case types.ComponentTasklist:
		return o.executeTasklistBackup(ctx, instance, backupID)
	case types.ComponentOptimize:
		return o.executeOptimizeBackup(ctx, instance, backupID)
	case types.ComponentElasticsearch:
		return o.executeElasticsearchBackup(ctx, instance, backupID)
	default:
		o.writeLog(instance.ID, backupID, fmt.Sprintf("Unknown component: %s", component))
		return types.ComponentStatusFailed, fmt.Errorf("unknown component: %s", component)
	}
}

// executeZeebeBackup executes Zeebe backup
func (o *Orchestrator) executeZeebeBackup(ctx context.Context, instance *models.CamundaInstance, backupID string) (types.ComponentStatus, error) {
	if instance.ZeebeBackupEndpoint == "" {
		o.writeLog(instance.ID, backupID, "Skipping Zeebe backup: backup endpoint not configured")
		return types.ComponentStatusSkipped, nil
	}

	// Trigger backup
	o.writeLog(instance.ID, backupID, "Triggering Zeebe backup")
	backupReq := map[string]interface{}{
		"backupId": backupID,
	}

	resp, err := o.httpClient.Post(ctx, instance.ZeebeBackupEndpoint, nil, backupReq)
	if err != nil {
		return types.ComponentStatusFailed, fmt.Errorf("failed to trigger Zeebe backup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			o.writeLog(instance.ID, backupID, fmt.Sprintf("failed to read Zeebe backup error response body: %v", readErr))
		}
		return types.ComponentStatusFailed, fmt.Errorf("Zeebe backup trigger failed with status %d: %s", resp.StatusCode, string(body))
	}

	o.writeLog(instance.ID, backupID, "Zeebe backup triggered successfully")

	// Poll for status
	if instance.ZeebeStatusEndpoint != "" {
		return o.pollBackupStatus(ctx, instance, backupID, instance.ZeebeStatusEndpoint, "Zeebe")
	}

	// If no status endpoint, assume success
	return types.ComponentStatusCompleted, nil
}

// executeOperateBackup executes Operate backup
func (o *Orchestrator) executeOperateBackup(ctx context.Context, instance *models.CamundaInstance, backupID string) (types.ComponentStatus, error) {
	if instance.OperateBackupEndpoint == "" {
		o.writeLog(instance.ID, backupID, "Skipping Operate backup: backup endpoint not configured")
		return types.ComponentStatusSkipped, nil
	}

	// Trigger backup
	o.writeLog(instance.ID, backupID, "Triggering Operate backup")
	backupReq := map[string]interface{}{
		"backupId": backupID,
	}

	resp, err := o.httpClient.Post(ctx, instance.OperateBackupEndpoint, nil, backupReq)
	if err != nil {
		return types.ComponentStatusFailed, fmt.Errorf("failed to trigger Operate backup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			o.writeLog(instance.ID, backupID, fmt.Sprintf("Failed to read Operate backup error response body: %v", readErr))
		}
		return types.ComponentStatusFailed, fmt.Errorf("Operate backup trigger failed with status %d: %s", resp.StatusCode, string(body))
	}

	o.writeLog(instance.ID, backupID, "Operate backup triggered successfully")

	// Poll for status
	if instance.OperateStatusEndpoint != "" {
		return o.pollBackupStatus(ctx, instance, backupID, instance.OperateStatusEndpoint, "Operate")
	}

	// If no status endpoint, assume success
	return types.ComponentStatusCompleted, nil
}

// executeTasklistBackup executes Tasklist backup
func (o *Orchestrator) executeTasklistBackup(ctx context.Context, instance *models.CamundaInstance, backupID string) (types.ComponentStatus, error) {
	if instance.TasklistBackupEndpoint == "" {
		o.writeLog(instance.ID, backupID, "Skipping Tasklist backup: backup endpoint not configured")
		return types.ComponentStatusSkipped, nil
	}

	// Trigger backup
	o.writeLog(instance.ID, backupID, "Triggering Tasklist backup")
	backupReq := map[string]interface{}{
		"backupId": backupID,
	}

	resp, err := o.httpClient.Post(ctx, instance.TasklistBackupEndpoint, nil, backupReq)
	if err != nil {
		return types.ComponentStatusFailed, fmt.Errorf("failed to trigger Tasklist backup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return types.ComponentStatusFailed, fmt.Errorf("Tasklist backup trigger failed with status %d and could not read response body: %v", resp.StatusCode, readErr)
		}
		return types.ComponentStatusFailed, fmt.Errorf("Tasklist backup trigger failed with status %d: %s", resp.StatusCode, string(body))
	}

	o.writeLog(instance.ID, backupID, "Tasklist backup triggered successfully")

	// Poll for status
	if instance.TasklistStatusEndpoint != "" {
		return o.pollBackupStatus(ctx, instance, backupID, instance.TasklistStatusEndpoint, "Tasklist")
	}

	// If no status endpoint, assume success
	return types.ComponentStatusCompleted, nil
}

// executeOptimizeBackup executes Optimize backup
func (o *Orchestrator) executeOptimizeBackup(ctx context.Context, instance *models.CamundaInstance, backupID string) (types.ComponentStatus, error) {
	if instance.OptimizeBackupEndpoint == "" {
		o.writeLog(instance.ID, backupID, "Skipping Optimize backup: backup endpoint not configured")
		return types.ComponentStatusSkipped, nil
	}

	// Trigger backup
	o.writeLog(instance.ID, backupID, "Triggering Optimize backup")
	backupReq := map[string]interface{}{
		"backupId": backupID,
	}

	resp, err := o.httpClient.Post(ctx, instance.OptimizeBackupEndpoint, nil, backupReq)
	if err != nil {
		return types.ComponentStatusFailed, fmt.Errorf("failed to trigger Optimize backup: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			o.writeLog(instance.ID, backupID, fmt.Sprintf("Failed to read Optimize backup error response body: %v", readErr))
		}
		return types.ComponentStatusFailed, fmt.Errorf("Optimize backup trigger failed with status %d: %s", resp.StatusCode, string(body))
	}

	o.writeLog(instance.ID, backupID, "Optimize backup triggered successfully")

	// Poll for status
	if instance.OptimizeStatusEndpoint != "" {
		return o.pollBackupStatus(ctx, instance, backupID, instance.OptimizeStatusEndpoint, "Optimize")
	}

	// If no status endpoint, assume success
	return types.ComponentStatusCompleted, nil
}

// executeElasticsearchBackup executes Elasticsearch snapshot backup
func (o *Orchestrator) executeElasticsearchBackup(ctx context.Context, instance *models.CamundaInstance, backupID string) (types.ComponentStatus, error) {
	if instance.ElasticsearchEndpoint == "" {
		o.writeLog(instance.ID, backupID, "Skipping Elasticsearch backup: endpoint not configured")
		return types.ComponentStatusSkipped, nil
	}

	// For now, this is a placeholder implementation
	// In Phase 5, this will be fully implemented with ES snapshot API
	o.writeLog(instance.ID, backupID, "Elasticsearch backup not yet implemented (Phase 5)")
	return types.ComponentStatusSkipped, nil
}

// pollBackupStatus polls the status endpoint until backup is completed or fails
func (o *Orchestrator) pollBackupStatus(ctx context.Context, instance *models.CamundaInstance, backupID, statusEndpoint, componentName string) (types.ComponentStatus, error) {
	o.writeLog(instance.ID, backupID, fmt.Sprintf("Polling %s backup status", componentName))

	// Poll configuration
	maxAttempts := 120 // 10 minutes with 5 second intervals
	pollInterval := 5 * time.Second
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for attempt := 0; attempt < maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return types.ComponentStatusFailed, ctx.Err()
		case <-ticker.C:
			// Check status
			statusURL := fmt.Sprintf("%s?backupId=%s", statusEndpoint, backupID)
			resp, err := o.httpClient.Get(ctx, statusURL, nil)
			if err != nil {
				o.writeLog(instance.ID, backupID, fmt.Sprintf("%s status check failed: %v", componentName, err))
				continue
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				o.writeLog(instance.ID, backupID, fmt.Sprintf("Failed to read %s status response body: %v", componentName, err))
				resp.Body.Close()
				continue
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				// Parse status response
				var statusResp map[string]interface{}
				if err := json.Unmarshal(body, &statusResp); err != nil {
					o.writeLog(instance.ID, backupID, fmt.Sprintf("Failed to parse %s status response: %v", componentName, err))
					continue
				}

				// Check if backup is completed
				// This assumes the API returns a "state" or "status" field
				state, ok := statusResp["state"].(string)
				if !ok {
					state, ok = statusResp["status"].(string)
				}

				if ok {
					switch state {
					case "COMPLETED", "COMPLETE", "SUCCESS":
						o.writeLog(instance.ID, backupID, fmt.Sprintf("%s backup completed", componentName))
						return types.ComponentStatusCompleted, nil
					case "FAILED", "FAILURE":
						o.writeLog(instance.ID, backupID, fmt.Sprintf("%s backup failed", componentName))
						return types.ComponentStatusFailed, fmt.Errorf("%s backup failed", componentName)
					case "RUNNING", "IN_PROGRESS":
						// Continue polling
						o.writeLog(instance.ID, backupID, fmt.Sprintf("%s backup still running (attempt %d/%d)", componentName, attempt+1, maxAttempts))
					}
				} else {
					// Log unexpected response format to aid debugging
					o.writeLog(instance.ID, backupID, fmt.Sprintf("Unexpected %s status response: missing 'state'/'status' field. Raw body: %s", componentName, string(body)))
				}
			} else {
				// Log non-OK status codes and handle certain errors as terminal
				o.writeLog(instance.ID, backupID, fmt.Sprintf("%s status endpoint returned HTTP %d with body: %s", componentName, resp.StatusCode, string(body)))

				// Treat 404 as a non-retryable error: backup/status not found
				if resp.StatusCode == http.StatusNotFound {
					o.writeLog(instance.ID, backupID, fmt.Sprintf("%s status endpoint returned 404 Not Found; stopping polling", componentName))
					return types.ComponentStatusFailed, fmt.Errorf("%s status endpoint returned 404 Not Found", componentName)
				}

				// For other non-OK responses, continue polling (may be transient)
				continue
			}
		}
	}

	// Timeout
	o.writeLog(instance.ID, backupID, fmt.Sprintf("%s backup timeout after polling", componentName))
	return types.ComponentStatusFailed, fmt.Errorf("%s backup timeout", componentName)
}

// finalizeBackup determines the final status of the backup
func (o *Orchestrator) finalizeBackup(req BackupRequest, execution *models.BackupExecution) {
	now := time.Now()
	execution.EndTime = &now

	// Check component statuses to determine overall status
	hasFailures := false
	allCompleted := true

	for _, status := range execution.ComponentStatus {
		if status == types.ComponentStatusFailed {
			hasFailures = true
			allCompleted = false
		}
		if status != types.ComponentStatusCompleted && status != types.ComponentStatusSkipped {
			allCompleted = false
		}
	}

	if hasFailures {
		execution.Status = types.BackupStatusFailed
		o.writeLog(req.CamundaInstance.ID, execution.BackupID, "Backup failed: one or more components failed")
	} else if allCompleted {
		execution.Status = types.BackupStatusCompleted
		o.writeLog(req.CamundaInstance.ID, execution.BackupID, "Backup completed successfully")
	} else {
		execution.Status = types.BackupStatusIncomplete
		o.writeLog(req.CamundaInstance.ID, execution.BackupID, "Backup incomplete: not all components finished")
	}

	// Update backup status in S3
	if err := o.s3Storage.UpdateBackupStatus(req.CamundaInstance.ID, execution.BackupID, execution.Status); err != nil {
		o.logger.Error("Failed to update backup status in S3: %v", err)
	}
}

// createBackupHistory creates a backup history record from execution
func (o *Orchestrator) createBackupHistory(req BackupRequest, execution *models.BackupExecution) *models.BackupHistory {
	history := &models.BackupHistory{
		BackupID:            execution.BackupID,
		CamundaInstanceID:   execution.CamundaInstanceID,
		CamundaInstanceName: req.CamundaInstance.Name,
		StartTime:           execution.StartTime,
		EndTime:             execution.EndTime,
		Status:              execution.Status,
		TriggerType:         req.TriggerType,
		Components:          make(map[string]models.ComponentBackupInfo),
		ErrorMessage:        execution.ErrorMessage,
		Metadata: models.BackupMetadata{
			ConfigVersion:     "1.0",
			ControllerVersion: "0.1.0",
			ExecutionMode:     o.getExecutionMode(req.CamundaInstance),
			LogFilePath:       fmt.Sprintf("%s/%s.log", req.CamundaInstance.ID, execution.BackupID),
			BackupReason:      req.BackupReason,
		},
	}

	// Calculate duration if completed
	if execution.EndTime != nil {
		duration := int(execution.EndTime.Sub(execution.StartTime).Seconds())
		history.DurationSeconds = &duration
	}

	// Add component information
	for component, status := range execution.ComponentStatus {
		history.Components[component] = models.ComponentBackupInfo{
			Enabled: true,
			Status:  status,
		}
	}

	// Calculate backup stats
	history.BackupStats = o.calculateBackupStats(execution)

	return history
}

// calculateBackupStats calculates statistics from component statuses
func (o *Orchestrator) calculateBackupStats(execution *models.BackupExecution) models.BackupStats {
	stats := models.BackupStats{
		TotalComponents: len(execution.ComponentStatus),
	}

	for _, status := range execution.ComponentStatus {
		switch status {
		case types.ComponentStatusCompleted:
			stats.SuccessfulComponents++
		case types.ComponentStatusFailed:
			stats.FailedComponents++
		case types.ComponentStatusSkipped:
			stats.SkippedComponents++
		case types.ComponentStatusRunning:
			stats.RunningComponents++
		case types.ComponentStatusPending:
			stats.PendingComponents++
		}
	}

	return stats
}

// getExecutionMode returns the execution mode as a string
func (o *Orchestrator) getExecutionMode(instance *models.CamundaInstance) string {
	if instance.ParallelExecution {
		return "parallel"
	}
	return "sequential"
}

// writeLog writes a log message to the backup log file
func (o *Orchestrator) writeLog(camundaInstanceID, backupID, message string) {
	timestamp := time.Now().Format(time.RFC3339)
	logMessage := fmt.Sprintf("[%s] %s\n", timestamp, message)

	if err := o.fileStorage.WriteToLogFile(camundaInstanceID, backupID, logMessage); err != nil {
		o.logger.Error("Failed to write to log file: %v", err)
	}

	// Also log to main logger
	o.logger.Info("[%s/%s] %s", camundaInstanceID, backupID, message)
}
