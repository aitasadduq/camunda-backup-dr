package models

import (
	"encoding/json"
	"time"
\t"errors"

	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// CamundaComponentConfig represents a component configuration
type CamundaComponentConfig struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
}

// CamundaInstance represents a Camunda instance configuration
type CamundaInstance struct {
	ID                  string                    `json:"id"`
	Name                string                    `json:"name"`
	BaseURL             string                    `json:"base_url"`
	Enabled             bool                      `json:"enabled"`
	Schedule            string                    `json:"schedule"`
	RetentionCount      int                       `json:"retention_count"`
	SuccessHistoryCount int                       `json:"success_history_count"`
	FailureHistoryCount int                       `json:"failure_history_count"`

	// Backup Configuration
	ZeebeBackupEndpoint    string `json:"zeebe_backup_endpoint"`
	ZeebeStatusEndpoint    string `json:"zeebe_status_endpoint"`
	OperateBackupEndpoint  string `json:"operate_backup_endpoint"`
	OperateStatusEndpoint  string `json:"operate_status_endpoint"`
	TasklistBackupEndpoint string `json:"tasklist_backup_endpoint"`
	TasklistStatusEndpoint string `json:"tasklist_status_endpoint"`
	OptimizeBackupEndpoint string `json:"optimize_backup_endpoint"`
	OptimizeStatusEndpoint string `json:"optimize_status_endpoint"`

	// Component Settings
	Components        []CamundaComponentConfig `json:"components"`
	ParallelExecution bool                    `json:"parallel_execution"`

	// External Systems
	ElasticsearchEndpoint string `json:"elasticsearch_endpoint"`
	ElasticsearchUsername string `json:"elasticsearch_username"`
	BackupIDS3Endpoint    string `json:"s3_endpoint"`
	BackupIDS3AccessKey   string `json:"s3_accesskey"`

	// Metadata
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	LastBackupAt    *time.Time `json:"last_backup_at,omitempty"`
	LastBackupStatus string    `json:"last_backup_status"`
}

// Configuration represents the main configuration file structure
type Configuration struct {
	Version          string             `json:"version"`
	CamundaInstances []CamundaInstance  `json:"camunda_instances"`
}

// NewCamundaInstance creates a new Camunda instance with defaults
func NewCamundaInstance(id, name, baseURL string) *CamundaInstance {
	now := time.Now()
	return &CamundaInstance{
		ID:                  id,
		Name:                name,
		BaseURL:             baseURL,
		Enabled:             true,
		Schedule:            "0 2 * * *",
		RetentionCount:      7,
		SuccessHistoryCount: 30,
		FailureHistoryCount: 30,
		Components: []CamundaComponentConfig{
			{Name: types.ComponentZeebe, Enabled: true},
			{Name: types.ComponentOperate, Enabled: true},
			{Name: types.ComponentTasklist, Enabled: true},
			{Name: types.ComponentOptimize, Enabled: false},
			{Name: types.ComponentElasticsearch, Enabled: true},
		},
		ParallelExecution:   false,
		LastBackupStatus:   "NEVER_BACKED_UP",
		CreatedAt:           now,
		UpdatedAt:           now,
	}
}

// IsComponentEnabled checks if a component is enabled for backup
func (ci *CamundaInstance) IsComponentEnabled(componentName string) bool {
	for _, component := range ci.Components {
		if component.Name == componentName {
			return component.Enabled
		}
	}
	return false
}

// GetEnabledComponents returns a list of enabled components
func (ci *CamundaInstance) GetEnabledComponents() []string {
	var enabled []string
	for _, component := range ci.Components {
		if component.Enabled {
			enabled = append(enabled, component.Name)
		}
	}
	return enabled
}

// Validate validates the Camunda instance configuration
func (ci *CamundaInstance) Validate() error {
	if ci.ID == "" {
		return ErrInvalidCamundaInstance
	}
	if ci.Name == "" {
		return ErrInvalidCamundaInstance
	}
	if ci.BaseURL == "" {
		return ErrInvalidCamundaInstance
	}
	if ci.Schedule == "" {
		return ErrInvalidCamundaInstance
	}
	if ci.RetentionCount < 0 {
		return ErrInvalidCamundaInstance
	}
	if ci.SuccessHistoryCount < 0 {
		return ErrInvalidCamundaInstance
	}
	if ci.FailureHistoryCount < 0 {
		return ErrInvalidCamundaInstance
	}

	// Validate components
	if len(ci.Components) == 0 {
		return ErrNoComponentsEnabled
	}

	return nil
}

// UpdateLastBackup updates the last backup information
func (ci *CamundaInstance) UpdateLastBackup(backupTime time.Time, status string) {
	ci.LastBackupAt = &backupTime
	ci.LastBackupStatus = status
	ci.UpdatedAt = time.Now()
}

// ToJSON converts the Camunda instance to JSON
func (ci *CamundaInstance) ToJSON() ([]byte, error) {
	return json.Marshal(ci)
}

// FromJSON creates a Camunda instance from JSON
func (ci *CamundaInstance) FromJSON(data []byte) error {
	return json.Unmarshal(data, ci)
}
// ErrInvalidCamundaInstance is returned when Camunda instance configuration is invalid
var ErrInvalidCamundaInstance = errors.New("invalid camunda instance configuration")
