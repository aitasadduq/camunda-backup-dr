package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/orchestrator"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// CamundaManager defines the interface for Camunda instance management
type CamundaManager interface {
	CreateInstance(instance *models.CamundaInstance) error
	GetInstance(id string) (*models.CamundaInstance, error)
	ListInstances() ([]models.CamundaInstance, error)
	UpdateInstance(id string, updates *models.CamundaInstance) error
	DeleteInstance(id string) error
	EnableInstance(id string) error
	DisableInstance(id string) error
}

// BackupOrchestrator defines the interface for backup orchestration
type BackupOrchestrator interface {
	ExecuteBackup(ctx context.Context, req orchestrator.BackupRequest) (*models.BackupExecution, error)
	IsBackupRunning() bool
}

// BackupHistoryProvider defines the interface for backup history operations
type BackupHistoryProvider interface {
	GetBackupHistory(camundaInstanceID, backupID string) (*models.BackupHistory, error)
	ListBackupHistory(camundaInstanceID string, status types.BackupStatus) ([]*models.BackupHistory, error)
}

// SchedulerInterface defines the interface for scheduler operations
type SchedulerInterface interface {
	IsRunning() bool
	GetJobsCount() int
	GetEnabledJobsCount() int
	TryAcquireBackupLock(instanceID string) bool
	ReleaseBackupLock()
	RegisterJob(instanceID, schedule string, enabled bool) error
	DeregisterJob(instanceID string) error
	UpdateJob(instanceID, schedule string, enabled bool) error
}

// Handlers contains HTTP request handlers
type Handlers struct {
	camundaManager  CamundaManager
	orchestrator    BackupOrchestrator
	historyProvider BackupHistoryProvider
	scheduler       SchedulerInterface
	logger          *utils.Logger
}

// NewHandlers creates a new handlers instance
func NewHandlers(
	camundaManager CamundaManager,
	orchestrator BackupOrchestrator,
	historyProvider BackupHistoryProvider,
	scheduler SchedulerInterface,
	logger *utils.Logger,
) *Handlers {
	return &Handlers{
		camundaManager:  camundaManager,
		orchestrator:    orchestrator,
		historyProvider: historyProvider,
		scheduler:       scheduler,
		logger:          logger,
	}
}

// HealthzHandler handles health check requests
func (h *Handlers) HealthzHandler(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().Format(time.RFC3339),
		Checks:    make(map[string]string),
	}

	// Basic service health - if we can respond, we're healthy
	response.Checks["service"] = "ok"

	writeJSON(w, http.StatusOK, response)
}

// ReadyzHandler handles readiness check requests
func (h *Handlers) ReadyzHandler(w http.ResponseWriter, r *http.Request) {
	response := HealthResponse{
		Status:    "ready",
		Timestamp: time.Now().Format(time.RFC3339),
		Checks:    make(map[string]string),
	}

	// Check scheduler readiness
	if h.scheduler != nil && h.scheduler.IsRunning() {
		response.Checks["scheduler"] = "running"
	} else {
		response.Checks["scheduler"] = "not_running"
	}

	// Check if camunda manager is accessible
	if h.camundaManager != nil {
		_, err := h.camundaManager.ListInstances()
		if err != nil {
			response.Checks["camunda_manager"] = "error"
			response.Status = "not_ready"
			writeJSON(w, http.StatusServiceUnavailable, response)
			return
		}
		response.Checks["camunda_manager"] = "ok"
	}

	writeJSON(w, http.StatusOK, response)
}

// SystemStatusHandler handles system status requests
func (h *Handlers) SystemStatusHandler(w http.ResponseWriter, r *http.Request) {
	response := SystemStatusResponse{
		Status:    "ok",
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// Get scheduler status
	if h.scheduler != nil {
		response.Scheduler = SchedulerStatus{
			Running:     h.scheduler.IsRunning(),
			JobsCount:   h.scheduler.GetJobsCount(),
			EnabledJobs: h.scheduler.GetEnabledJobsCount(),
		}
	}

	// Get storage status
	response.Storage = StorageStatus{
		FileStorageHealthy: true, // Will be enhanced later
		S3StorageHealthy:   true, // Will be enhanced later
	}

	// Get Camunda instances status
	if h.camundaManager != nil {
		instances, err := h.camundaManager.ListInstances()
		if err == nil {
			response.CamundaInstances.Total = len(instances)
			for _, instance := range instances {
				if instance.Enabled {
					response.CamundaInstances.Enabled++
				} else {
					response.CamundaInstances.Disabled++
				}
			}
		}
	}

	// Check for active backups
	if h.orchestrator != nil && h.orchestrator.IsBackupRunning() {
		response.ActiveBackups = 1
	}

	writeJSON(w, http.StatusOK, response)
}

// ListCamundaInstancesHandler handles listing all Camunda instances
func (h *Handlers) ListCamundaInstancesHandler(w http.ResponseWriter, r *http.Request) {
	instances, err := h.camundaManager.ListInstances()
	if err != nil {
		h.logger.Error("Failed to list Camunda instances: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to list Camunda instances")
		return
	}

	writeJSON(w, http.StatusOK, instances)
}

// CreateCamundaInstanceHandler handles creating a new Camunda instance
func (h *Handlers) CreateCamundaInstanceHandler(w http.ResponseWriter, r *http.Request) {
	var instance models.CamundaInstance
	if err := json.NewDecoder(r.Body).Decode(&instance); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body: "+err.Error())
		return
	}

	// Validate required fields
	if instance.ID == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "ID is required")
		return
	}
	if instance.Name == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Name is required")
		return
	}
	if instance.BaseURL == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "BaseURL is required")
		return
	}

	// Apply defaults for optional fields
	if instance.Schedule == "" {
		instance.Schedule = "0 2 * * *" // Default: daily at 2 AM
	}
	if instance.RetentionCount == 0 {
		instance.RetentionCount = 7
	}
	if instance.SuccessHistoryCount == 0 {
		instance.SuccessHistoryCount = 30
	}
	if instance.FailureHistoryCount == 0 {
		instance.FailureHistoryCount = 30
	}
	if len(instance.Components) == 0 {
		instance.Components = []models.CamundaComponentConfig{
			{Name: types.ComponentZeebe, Enabled: true},
			{Name: types.ComponentOperate, Enabled: true},
			{Name: types.ComponentTasklist, Enabled: true},
			{Name: types.ComponentOptimize, Enabled: false},
			{Name: types.ComponentElasticsearch, Enabled: true},
		}
	}

	if err := h.camundaManager.CreateInstance(&instance); err != nil {
		if err == utils.ErrCamundaInstanceAlreadyExists {
			writeError(w, http.StatusConflict, "conflict", "Camunda instance already exists")
			return
		}
		if err == utils.ErrInvalidCamundaInstance || err == utils.ErrNoComponentsEnabled {
			writeError(w, http.StatusBadRequest, "validation_error", "Invalid Camunda instance configuration")
			return
		}
		h.logger.Error("Failed to create Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to create Camunda instance")
		return
	}

	// Register job with scheduler if enabled
	if h.scheduler != nil && instance.Enabled && instance.Schedule != "" {
		if err := h.scheduler.RegisterJob(instance.ID, instance.Schedule, instance.Enabled); err != nil {
			h.logger.Warn("Failed to register scheduler job for instance %s: %v", instance.ID, err)
		}
	}

	writeSuccess(w, http.StatusCreated, "Camunda instance created successfully", instance)
}

// GetCamundaInstanceHandler handles getting a specific Camunda instance
func (h *Handlers) GetCamundaInstanceHandler(w http.ResponseWriter, r *http.Request) {
	id := extractIDFromPath(r.URL.Path, "/api/camundas/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	// Check if there's a sub-path (like /backup, /backups, /enable, /disable)
	if strings.Contains(id, "/") {
		writeError(w, http.StatusBadRequest, "validation_error", "Invalid request")
		return
	}

	instance, err := h.camundaManager.GetInstance(id)
	if err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to get Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get Camunda instance")
		return
	}

	writeJSON(w, http.StatusOK, instance)
}

// UpdateCamundaInstanceHandler handles updating a Camunda instance
func (h *Handlers) UpdateCamundaInstanceHandler(w http.ResponseWriter, r *http.Request) {
	id := extractIDFromPath(r.URL.Path, "/api/camundas/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	var updates models.CamundaInstance
	if err := json.NewDecoder(r.Body).Decode(&updates); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "Invalid JSON body: "+err.Error())
		return
	}

	if err := h.camundaManager.UpdateInstance(id, &updates); err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		if err == utils.ErrInvalidCamundaInstance {
			writeError(w, http.StatusBadRequest, "validation_error", "Invalid Camunda instance configuration")
			return
		}
		h.logger.Error("Failed to update Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to update Camunda instance")
		return
	}

	// Update scheduler job
	if h.scheduler != nil && updates.Schedule != "" {
		if err := h.scheduler.UpdateJob(id, updates.Schedule, updates.Enabled); err != nil {
			h.logger.Warn("Failed to update scheduler job for instance %s: %v", id, err)
		}
	}

	writeSuccess(w, http.StatusOK, "Camunda instance updated successfully", nil)
}

// DeleteCamundaInstanceHandler handles deleting a Camunda instance
func (h *Handlers) DeleteCamundaInstanceHandler(w http.ResponseWriter, r *http.Request) {
	id := extractIDFromPath(r.URL.Path, "/api/camundas/")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	if err := h.camundaManager.DeleteInstance(id); err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to delete Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to delete Camunda instance")
		return
	}

	// Deregister scheduler job
	if h.scheduler != nil {
		if err := h.scheduler.DeregisterJob(id); err != nil {
			h.logger.Warn("Failed to deregister scheduler job for instance %s: %v", id, err)
		}
	}

	writeSuccess(w, http.StatusOK, "Camunda instance deleted successfully", nil)
}

// EnableCamundaInstanceHandler handles enabling a Camunda instance
func (h *Handlers) EnableCamundaInstanceHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := extractIDFromPath(path, "/api/camundas/")
	id = strings.TrimSuffix(id, "/enable")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	if err := h.camundaManager.EnableInstance(id); err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to enable Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to enable Camunda instance")
		return
	}

	// Update scheduler job
	if h.scheduler != nil {
		instance, err := h.camundaManager.GetInstance(id)
		if err == nil && instance.Schedule != "" {
			if err := h.scheduler.UpdateJob(id, instance.Schedule, true); err != nil {
				h.logger.Warn("Failed to update scheduler job for instance %s: %v", id, err)
			}
		}
	}

	writeSuccess(w, http.StatusOK, "Camunda instance enabled successfully", nil)
}

// DisableCamundaInstanceHandler handles disabling a Camunda instance
func (h *Handlers) DisableCamundaInstanceHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := extractIDFromPath(path, "/api/camundas/")
	id = strings.TrimSuffix(id, "/disable")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	if err := h.camundaManager.DisableInstance(id); err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to disable Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to disable Camunda instance")
		return
	}

	// Update scheduler job
	if h.scheduler != nil {
		instance, err := h.camundaManager.GetInstance(id)
		if err == nil && instance.Schedule != "" {
			if err := h.scheduler.UpdateJob(id, instance.Schedule, false); err != nil {
				h.logger.Warn("Failed to update scheduler job for instance %s: %v", id, err)
			}
		}
	}

	writeSuccess(w, http.StatusOK, "Camunda instance disabled successfully", nil)
}

// TriggerBackupHandler handles triggering a manual backup
func (h *Handlers) TriggerBackupHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := extractIDFromPath(path, "/api/camundas/")
	id = strings.TrimSuffix(id, "/backup")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	// Get the instance
	instance, err := h.camundaManager.GetInstance(id)
	if err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to get Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get Camunda instance")
		return
	}

	// Try to acquire backup lock via scheduler
	if h.scheduler != nil && !h.scheduler.TryAcquireBackupLock(id) {
		writeError(w, http.StatusConflict, "backup_in_progress", "A backup is already in progress")
		return
	}

	// Generate backup ID before starting async execution
	backupID := time.Now().Format("20060102-150405")

	// Execute backup asynchronously
	req := orchestrator.BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Manual backup triggered via API",
	}

	go func() {
		defer func() {
			if h.scheduler != nil {
				h.scheduler.ReleaseBackupLock()
			}
		}()

		// Use background context since the HTTP request context will be cancelled
		// when we return the response
		ctx := context.Background()
		if _, err := h.orchestrator.ExecuteBackup(ctx, req); err != nil {
			h.logger.Error("Backup execution failed for instance %s: %v", id, err)
		}
	}()

	response := BackupTriggerResponse{
		Message:  "Backup triggered successfully",
		BackupID: backupID,
		Status:   string(types.BackupStatusRunning),
	}

	writeJSON(w, http.StatusAccepted, response)
}

// ListBackupHistoryHandler handles listing backup history for a Camunda instance
func (h *Handlers) ListBackupHistoryHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := extractIDFromPath(path, "/api/camundas/")
	id = strings.TrimSuffix(id, "/backups")
	if id == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID is required")
		return
	}

	// Verify instance exists
	_, err := h.camundaManager.GetInstance(id)
	if err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to get Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get Camunda instance")
		return
	}

	// Get optional status filter
	statusFilter := r.URL.Query().Get("status")
	var status types.BackupStatus
	if statusFilter != "" {
		status = types.BackupStatus(strings.ToUpper(statusFilter))
	}

	history, err := h.historyProvider.ListBackupHistory(id, status)
	if err != nil {
		h.logger.Error("Failed to list backup history: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to list backup history")
		return
	}

	writeJSON(w, http.StatusOK, history)
}

// GetBackupDetailsHandler handles getting details of a specific backup
func (h *Handlers) GetBackupDetailsHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	// Extract instance ID and backup ID from path like /api/camundas/{id}/backups/{backupId}
	parts := strings.Split(strings.TrimPrefix(path, "/api/camundas/"), "/")
	if len(parts) < 3 || parts[1] != "backups" || parts[2] == "" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID and Backup ID are required")
		return
	}

	instanceID := parts[0]
	backupID := parts[2]

	// Verify instance exists
	_, err := h.camundaManager.GetInstance(instanceID)
	if err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Camunda instance not found")
			return
		}
		h.logger.Error("Failed to get Camunda instance: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get Camunda instance")
		return
	}

	history, err := h.historyProvider.GetBackupHistory(instanceID, backupID)
	if err != nil {
		if err == utils.ErrBackupNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Backup not found")
			return
		}
		h.logger.Error("Failed to get backup details: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to get backup details")
		return
	}

	writeJSON(w, http.StatusOK, history)
}

// extractIDFromPath extracts an ID from a URL path
func extractIDFromPath(path, prefix string) string {
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	remaining := strings.TrimPrefix(path, prefix)
	// Return everything up to the next slash or end of string
	if idx := strings.Index(remaining, "/"); idx != -1 {
		return remaining[:idx]
	}
	return remaining
}
