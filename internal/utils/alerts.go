package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// AlertLevel represents the severity of an alert.
type AlertLevel string

const (
	AlertInfo     AlertLevel = "INFO"
	AlertWarning  AlertLevel = "WARNING"
	AlertCritical AlertLevel = "CRITICAL"
)

// Alert represents a single alert payload.
type Alert struct {
	Level     AlertLevel        `json:"level"`
	Title     string            `json:"title"`
	Message   string            `json:"message"`
	Timestamp string            `json:"timestamp"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// AlertFilter controls which alert types are enabled.
type AlertFilter struct {
	BackupFailed   bool
	CleanupFailed  bool
	StuckBackup    bool
	CircuitOpen    bool
	SchedulerError bool
}

// DefaultAlertFilter returns a filter with all alerts enabled.
func DefaultAlertFilter() AlertFilter {
	return AlertFilter{
		BackupFailed:   true,
		CleanupFailed:  true,
		StuckBackup:    true,
		CircuitOpen:    true,
		SchedulerError: true,
	}
}

// Alerter sends alerts to a configured webhook endpoint.
// If no webhook URL is configured, all operations are no-ops.
type Alerter struct {
	webhookURL string
	client     *http.Client
	logger     *Logger
	mu         sync.RWMutex
	filter     AlertFilter
}

// NewAlerter creates a new Alerter. Pass an empty webhookURL to disable alerting.
func NewAlerter(webhookURL string, logger *Logger) *Alerter {
	return &Alerter{
		webhookURL: webhookURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
		filter: DefaultAlertFilter(),
	}
}

// SetFilter configures which alert types are enabled. Safe for concurrent use.
func (a *Alerter) SetFilter(filter AlertFilter) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.filter = filter
}

// IsEnabled returns true if the alerter has a webhook configured.
func (a *Alerter) IsEnabled() bool {
	return a.webhookURL != ""
}

// SendAlert sends an alert to the webhook. It is fire-and-forget: errors are
// logged but never returned to the caller to avoid disrupting critical paths.
func (a *Alerter) SendAlert(level AlertLevel, title, message string, metadata map[string]string) {
	if !a.IsEnabled() {
		return
	}

	alert := Alert{
		Level:     level,
		Title:     title,
		Message:   message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Metadata:  metadata,
	}

	go a.sendAsync(alert)
}

func (a *Alerter) sendAsync(alert Alert) {
	body, err := json.Marshal(alert)
	if err != nil {
		a.logger.Error("Failed to marshal alert: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.webhookURL, bytes.NewReader(body))
	if err != nil {
		a.logger.Error("Failed to create alert request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to send alert: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		a.logger.Warn("Alert webhook returned status %d", resp.StatusCode)
	}
}

// Convenience methods for common alert patterns.

func (a *Alerter) AlertBackupFailed(instanceID, backupID, reason string) {
	a.mu.RLock()
	enabled := a.filter.BackupFailed
	a.mu.RUnlock()
	if !enabled {
		return
	}
	a.SendAlert(AlertCritical, "Backup Failed", fmt.Sprintf("Backup %s failed for instance %s: %s", backupID, instanceID, reason), map[string]string{
		"instance_id": instanceID,
		"backup_id":   backupID,
	})
}

func (a *Alerter) AlertCircuitOpen(serviceName string) {
	a.mu.RLock()
	enabled := a.filter.CircuitOpen
	a.mu.RUnlock()
	if !enabled {
		return
	}
	a.SendAlert(AlertWarning, "Circuit Breaker Open", fmt.Sprintf("Circuit breaker opened for service: %s", serviceName), map[string]string{
		"service": serviceName,
	})
}

func (a *Alerter) AlertCleanupFailed(instanceID, backupID, reason string) {
	a.mu.RLock()
	enabled := a.filter.CleanupFailed
	a.mu.RUnlock()
	if !enabled {
		return
	}
	a.SendAlert(AlertWarning, "Cleanup Failed", fmt.Sprintf("Cleanup failed for backup %s (instance %s): %s", backupID, instanceID, reason), map[string]string{
		"instance_id": instanceID,
		"backup_id":   backupID,
	})
}

func (a *Alerter) AlertStuckBackup(instanceID, jobID string, duration time.Duration) {
	a.mu.RLock()
	enabled := a.filter.StuckBackup
	a.mu.RUnlock()
	if !enabled {
		return
	}
	a.SendAlert(AlertCritical, "Stuck Backup Detected", fmt.Sprintf("Backup for instance %s (job %s) has been running for %s", instanceID, jobID, duration.Round(time.Second)), map[string]string{
		"instance_id": instanceID,
		"job_id":      jobID,
		"duration":    duration.String(),
	})
}

func (a *Alerter) AlertSchedulerError(message string) {
	a.mu.RLock()
	enabled := a.filter.SchedulerError
	a.mu.RUnlock()
	if !enabled {
		return
	}
	a.SendAlert(AlertCritical, "Scheduler Error", message, nil)
}
