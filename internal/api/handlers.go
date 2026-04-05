package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
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

// RetentionManager defines the interface for retention operations
type RetentionManager interface {
	DeleteBackup(camundaInstanceID, backupID string) error
	ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error)
	ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error)
	ListFailedBackups(camundaInstanceID string) ([]*models.BackupHistory, error)
}

// LogFileReader defines the interface for reading backup log files
type LogFileReader interface {
	ReadLogFile(camundaInstanceID, backupID string) (string, error)
}

// Handlers contains HTTP request handlers
type Handlers struct {
	camundaManager   CamundaManager
	orchestrator     BackupOrchestrator
	historyProvider  BackupHistoryProvider
	scheduler        SchedulerInterface
	retentionManager RetentionManager
	logFileReader    LogFileReader
	logger           *utils.Logger
	cfg              *config.Config
}

// NewHandlers creates a new handlers instance
func NewHandlers(
	camundaManager CamundaManager,
	orchestrator BackupOrchestrator,
	historyProvider BackupHistoryProvider,
	scheduler SchedulerInterface,
	retentionManager RetentionManager,
	logFileReader LogFileReader,
	logger *utils.Logger,
	cfg *config.Config,
) *Handlers {
	return &Handlers{
		camundaManager:   camundaManager,
		orchestrator:     orchestrator,
		historyProvider:  historyProvider,
		scheduler:        scheduler,
		retentionManager: retentionManager,
		logFileReader:    logFileReader,
		logger:           logger,
		cfg:              cfg,
	}
}

// DefaultsResponse contains configuration defaults for the UI to pre-populate form fields.
// Sensitive values (passwords, secret keys) are never included.
type DefaultsResponse struct {
	Schedule                        string `json:"schedule"`
	SuccessRetention                int    `json:"success_retention"`
	FailureRetention                int    `json:"failure_retention"`
	ElasticsearchEndpoint           string `json:"elasticsearch_endpoint"`
	ElasticsearchUsername           string `json:"elasticsearch_username"`
	ElasticsearchSnapshotRepository string `json:"elasticsearch_snapshot_repository"`
	ElasticsearchSnapshotNamePrefix string `json:"elasticsearch_snapshot_name_prefix"`
	S3Endpoint                      string `json:"s3_endpoint"`
	S3AccessKey                     string `json:"s3_accesskey"`
}

// GetDefaultsHandler returns configuration defaults for the UI to pre-populate form fields.
func (h *Handlers) GetDefaultsHandler(w http.ResponseWriter, r *http.Request) {
	defaults := DefaultsResponse{
		Schedule:                        "0 2 * * *",
		SuccessRetention:                7,
		FailureRetention:                7,
		ElasticsearchSnapshotRepository: "camunda-backup",
	}

	if h.cfg != nil {
		defaults.ElasticsearchEndpoint = h.cfg.DefaultElasticsearchEndpoint
		defaults.ElasticsearchUsername = h.cfg.DefaultElasticsearchUsername
		defaults.ElasticsearchSnapshotRepository = h.cfg.DefaultElasticsearchSnapshotRepository
		defaults.ElasticsearchSnapshotNamePrefix = h.cfg.DefaultElasticsearchSnapshotNamePrefix
		defaults.S3Endpoint = h.cfg.DefaultS3Endpoint
		defaults.S3AccessKey = h.cfg.DefaultS3AccessKey
		if h.cfg.DefaultSchedule != "" {
			defaults.Schedule = h.cfg.DefaultSchedule
		}
		if h.cfg.DefaultSuccessRetention > 0 {
			defaults.SuccessRetention = h.cfg.DefaultSuccessRetention
		}
		if h.cfg.DefaultFailureRetention > 0 {
			defaults.FailureRetention = h.cfg.DefaultFailureRetention
		}
	}

	writeJSON(w, http.StatusOK, defaults)
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

// validateExportingEndpoint returns an error if the endpoint is non-empty but
// has an unsupported scheme or resolves to a private/loopback address (SSRF guard).
func validateExportingEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return utils.NewValidationError("exporting_endpoint is not a valid URL")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return utils.NewValidationError("exporting_endpoint must use http or https")
	}
	if isBlockedHost(u.Hostname()) {
		return utils.NewValidationError("exporting_endpoint must not target private or loopback addresses (set PROBE_ALLOW_PRIVATE_IPS=true to allow)")
	}
	return nil
}

// CreateCamundaInstanceHandler handles creating a new Camunda instance
func (h *Handlers) CreateCamundaInstanceHandler(w http.ResponseWriter, r *http.Request) {
	var instance models.CamundaInstance
	if err := json.NewDecoder(r.Body).Decode(&instance); err != nil {
		writeAppError(w, utils.NewValidationError("Invalid JSON body: "+err.Error()))
		return
	}

	// Validate required fields
	if instance.ID == "" {
		writeAppError(w, utils.NewValidationError("ID is required"))
		return
	}
	// Normalize: lowercase the ID automatically
	instance.ID = strings.ToLower(instance.ID)
	if instance.Name == "" {
		writeAppError(w, utils.NewValidationError("Name is required"))
		return
	}
	if instance.BaseURL == "" {
		writeAppError(w, utils.NewValidationError("BaseURL is required"))
		return
	}
	if err := validateExportingEndpoint(instance.ExportingEndpoint); err != nil {
		writeAppError(w, err)
		return
	}

	// Apply defaults for optional fields
	if instance.Schedule == "" {
		instance.Schedule = "0 2 * * *" // Default: daily at 2 AM
	}
	if instance.SuccessRetention == 0 {
		instance.SuccessRetention = 7
	}
	if instance.FailureRetention == 0 {
		instance.FailureRetention = 7
	}

	// Apply defaults from environment config
	if h.cfg != nil {
		if instance.ElasticsearchEndpoint == "" && h.cfg.DefaultElasticsearchEndpoint != "" {
			instance.ElasticsearchEndpoint = h.cfg.DefaultElasticsearchEndpoint
		}
		if instance.ElasticsearchUsername == "" && h.cfg.DefaultElasticsearchUsername != "" {
			instance.ElasticsearchUsername = h.cfg.DefaultElasticsearchUsername
		}
		if instance.BackupIDS3Endpoint == "" && h.cfg.DefaultS3Endpoint != "" {
			instance.BackupIDS3Endpoint = h.cfg.DefaultS3Endpoint
		}
		if instance.BackupIDS3AccessKey == "" && h.cfg.DefaultS3AccessKey != "" {
			instance.BackupIDS3AccessKey = h.cfg.DefaultS3AccessKey
		}
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
			writeAppError(w, utils.NewValidationError("Invalid Camunda instance configuration").WithInstance(instance.ID))
			return
		}
		h.logger.Error("Failed to create Camunda instance: %v", err)
		writeAppError(w, utils.WrapError(err, "internal_error", "Failed to create Camunda instance", http.StatusInternalServerError))
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
	if err := validateExportingEndpoint(updates.ExportingEndpoint); err != nil {
		writeAppError(w, err)
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
		writeAppError(w, utils.NewValidationError("Instance ID is required"))
		return
	}

	// Get the instance
	instance, err := h.camundaManager.GetInstance(id)
	if err != nil {
		if err == utils.ErrCamundaInstanceNotFound {
			writeAppError(w, utils.NewNotFoundError("Camunda instance not found").WithInstance(id))
			return
		}
		h.logger.Error("Failed to get Camunda instance: %v", err)
		writeAppError(w, utils.WrapError(err, "internal_error", "Failed to get Camunda instance", http.StatusInternalServerError).WithInstance(id))
		return
	}

	// Try to acquire backup lock via scheduler
	if h.scheduler != nil && !h.scheduler.TryAcquireBackupLock(id) {
		writeError(w, http.StatusConflict, "backup_in_progress", "A backup is already in progress")
		return
	}

	// Generate backup ID before starting async execution
	backupID := time.Now().Format("20060102150405")

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

// DeleteBackupHandler handles deleting a specific backup
func (h *Handlers) DeleteBackupHandler(w http.ResponseWriter, r *http.Request) {
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

	if h.retentionManager == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Retention manager not configured")
		return
	}

	if err := h.retentionManager.DeleteBackup(instanceID, backupID); err != nil {
		if err == utils.ErrBackupNotFound {
			writeError(w, http.StatusNotFound, "not_found", "Backup not found")
			return
		}
		if errors.Is(err, utils.ErrCannotDeleteMostRecentBackup) {
			writeError(w, http.StatusConflict, "safety_refusal", err.Error())
			return
		}
		h.logger.Error("Failed to delete backup: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to delete backup")
		return
	}

	writeSuccess(w, http.StatusOK, "Backup deleted successfully", nil)
}

// ListOrphanedBackupsHandler handles listing orphaned backups for a Camunda instance
func (h *Handlers) ListOrphanedBackupsHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := extractIDFromPath(path, "/api/camundas/")
	id = strings.TrimSuffix(id, "/backups/orphaned")
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

	if h.retentionManager == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Retention manager not configured")
		return
	}

	orphaned, err := h.retentionManager.ListOrphanedBackups(id)
	if err != nil {
		h.logger.Error("Failed to list orphaned backups: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to list orphaned backups")
		return
	}

	writeJSON(w, http.StatusOK, orphaned)
}

// ListIncompleteBackupsHandler handles listing incomplete backups for a Camunda instance
func (h *Handlers) ListIncompleteBackupsHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := extractIDFromPath(path, "/api/camundas/")
	id = strings.TrimSuffix(id, "/backups/incomplete")
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

	if h.retentionManager == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Retention manager not configured")
		return
	}

	incomplete, err := h.retentionManager.ListIncompleteBackups(id)
	if err != nil {
		h.logger.Error("Failed to list incomplete backups: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to list incomplete backups")
		return
	}

	writeJSON(w, http.StatusOK, incomplete)
}

// ListFailedBackupsHandler handles listing failed backups for a Camunda instance
func (h *Handlers) ListFailedBackupsHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	id := extractIDFromPath(path, "/api/camundas/")
	id = strings.TrimSuffix(id, "/backups/failed")
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

	if h.retentionManager == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Retention manager not configured")
		return
	}

	failed, err := h.retentionManager.ListFailedBackups(id)
	if err != nil {
		h.logger.Error("Failed to list failed backups: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to list failed backups")
		return
	}

	writeJSON(w, http.StatusOK, failed)
}

// GetBackupLogsHandler handles retrieving backup log file contents
func (h *Handlers) GetBackupLogsHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	// Extract instance ID and backup ID from path like /api/camundas/{id}/backups/{backupId}/logs
	parts := strings.Split(strings.TrimPrefix(path, "/api/camundas/"), "/")
	if len(parts) != 4 || parts[0] == "" || parts[1] != "backups" || parts[2] == "" || parts[3] != "logs" {
		writeError(w, http.StatusBadRequest, "validation_error", "Instance ID and Backup ID are required")
		return
	}

	instanceID := strings.ToLower(parts[0])
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

	if h.logFileReader == nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "Log file reader not configured")
		return
	}

	logContent, err := h.logFileReader.ReadLogFile(instanceID, backupID)
	if err != nil {
		if err == utils.ErrBackupNotFound || err == utils.ErrFileStorageFailed {
			writeError(w, http.StatusNotFound, "not_found", "Backup log file not found")
			return
		}
		h.logger.Error("Failed to read backup log file: %v", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "Failed to read backup log file")
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(logContent)); err != nil {
		h.logger.Error("Failed to write backup log response: %v", err)
	}
}

// extractIDFromPath extracts an ID from a URL path, normalizing to lowercase.
func extractIDFromPath(path, prefix string) string {
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	remaining := strings.TrimPrefix(path, prefix)
	var id string
	// Return everything up to the next slash or end of string
	if idx := strings.Index(remaining, "/"); idx != -1 {
		id = remaining[:idx]
	} else {
		id = remaining
	}
	return strings.ToLower(id)
}
