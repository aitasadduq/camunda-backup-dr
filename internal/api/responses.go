package api

import (
	"encoding/json"
	"log"
	"net/http"
)

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// SuccessResponse represents a generic success response
type SuccessResponse struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// HealthResponse represents a health check response
type HealthResponse struct {
	Status    string            `json:"status"`
	Checks    map[string]string `json:"checks,omitempty"`
	Timestamp string            `json:"timestamp"`
}

// SystemStatusResponse represents the system status response
type SystemStatusResponse struct {
	Status           string                 `json:"status"`
	Scheduler        SchedulerStatus        `json:"scheduler"`
	Storage          StorageStatus          `json:"storage"`
	CamundaInstances CamundaInstancesStatus `json:"camunda_instances"`
	ActiveBackups    int                    `json:"active_backups"`
	Timestamp        string                 `json:"timestamp"`
}

// SchedulerStatus represents scheduler status
type SchedulerStatus struct {
	Running     bool `json:"running"`
	JobsCount   int  `json:"jobs_count"`
	EnabledJobs int  `json:"enabled_jobs"`
}

// StorageStatus represents storage status
type StorageStatus struct {
	FileStorageHealthy bool `json:"file_storage_healthy"`
	S3StorageHealthy   bool `json:"s3_storage_healthy"`
}

// CamundaInstancesStatus represents Camunda instances status
type CamundaInstancesStatus struct {
	Total    int `json:"total"`
	Enabled  int `json:"enabled"`
	Disabled int `json:"disabled"`
}

// BackupTriggerResponse represents a backup trigger response
type BackupTriggerResponse struct {
	Message  string `json:"message"`
	BackupID string `json:"backup_id"`
	Status   string `json:"status"`
}

// writeJSON writes a JSON response with proper error handling.
// It pre-encodes to a buffer before writing headers to ensure
// we can return a 500 error if encoding fails.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	if data == nil {
		w.WriteHeader(status)
		return
	}

	// Pre-encode to buffer before writing headers
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("ERROR: Failed to encode JSON response: %v", err)
		// Write a fallback 500 error response
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal_error","message":"Failed to encode response","code":500}`))
		return
	}

	w.WriteHeader(status)
	if _, err := w.Write(jsonData); err != nil {
		// Headers already sent, can only log the error
		log.Printf("ERROR: Failed to write JSON response: %v", err)
	}
}

// writeError writes an error response
func writeError(w http.ResponseWriter, status int, errorType, message string) {
	writeJSON(w, status, ErrorResponse{
		Error:   errorType,
		Message: message,
		Code:    status,
	})
}

// writeSuccess writes a success response
func writeSuccess(w http.ResponseWriter, status int, message string, data interface{}) {
	writeJSON(w, status, SuccessResponse{
		Message: message,
		Data:    data,
	})
}
