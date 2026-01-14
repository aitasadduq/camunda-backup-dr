package camunda

import (
	"fmt"
	"sync"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// Manager handles Camunda instance management operations
type Manager struct {
	fileStorage storage.FileStorage
	logger      *utils.Logger
	mutex       sync.RWMutex
}

// NewManager creates a new Camunda instance manager
func NewManager(fileStorage storage.FileStorage, logger *utils.Logger) *Manager {
	return &Manager{
		fileStorage: fileStorage,
		logger:      logger,
	}
}

// CreateInstance creates a new Camunda instance
func (m *Manager) CreateInstance(instance *models.CamundaInstance) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Validate instance
	if err := instance.Validate(); err != nil {
		return err
	}

	// Load current configuration
	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Check if instance already exists
	for _, existing := range config.CamundaInstances {
		if existing.ID == instance.ID {
			return utils.ErrCamundaInstanceAlreadyExists
		}
	}

	// Set timestamps
	now := time.Now()
	instance.CreatedAt = now
	instance.UpdatedAt = now
	if instance.LastBackupStatus == "" {
		instance.LastBackupStatus = "NEVER_BACKED_UP"
	}

	// Add instance to configuration
	config.CamundaInstances = append(config.CamundaInstances, *instance)

	// Save configuration
	if err := m.fileStorage.SaveConfiguration(config); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	m.logger.Info("Created Camunda instance: %s (ID: %s)", instance.Name, instance.ID)
	return nil
}

// GetInstance retrieves a Camunda instance by ID
func (m *Manager) GetInstance(id string) (*models.CamundaInstance, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	for i := range config.CamundaInstances {
		if config.CamundaInstances[i].ID == id {
			return &config.CamundaInstances[i], nil
		}
	}

	return nil, utils.ErrCamundaInstanceNotFound
}

// ListInstances returns all Camunda instances
func (m *Manager) ListInstances() ([]models.CamundaInstance, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	return config.CamundaInstances, nil
}

// UpdateInstance updates an existing Camunda instance
func (m *Manager) UpdateInstance(id string, updates *models.CamundaInstance) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Load current configuration
	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Find instance
	found := false
	for i := range config.CamundaInstances {
		if config.CamundaInstances[i].ID == id {
			// Preserve metadata
			updates.ID = id
			updates.CreatedAt = config.CamundaInstances[i].CreatedAt
			updates.UpdatedAt = time.Now()
			updates.LastBackupAt = config.CamundaInstances[i].LastBackupAt
			updates.LastBackupStatus = config.CamundaInstances[i].LastBackupStatus

			// Validate updated instance
			if err := updates.Validate(); err != nil {
				return err
			}

			// Update instance
			config.CamundaInstances[i] = *updates
			found = true
			break
		}
	}

	if !found {
		return utils.ErrCamundaInstanceNotFound
	}

	// Save configuration
	if err := m.fileStorage.SaveConfiguration(config); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	m.logger.Info("Updated Camunda instance: %s (ID: %s)", updates.Name, id)
	return nil
}

// DeleteInstance deletes a Camunda instance
func (m *Manager) DeleteInstance(id string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Load current configuration
	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	// Find and remove instance
	found := false
	for i, instance := range config.CamundaInstances {
		if instance.ID == id {
			config.CamundaInstances = append(config.CamundaInstances[:i], config.CamundaInstances[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return utils.ErrCamundaInstanceNotFound
	}

	// Save configuration
	if err := m.fileStorage.SaveConfiguration(config); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	m.logger.Info("Deleted Camunda instance: %s", id)
	return nil
}

// EnableInstance enables a Camunda instance
func (m *Manager) EnableInstance(id string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	found := false
	for i := range config.CamundaInstances {
		if config.CamundaInstances[i].ID == id {
			config.CamundaInstances[i].Enabled = true
			config.CamundaInstances[i].UpdatedAt = time.Now()
			found = true
			break
		}
	}

	if !found {
		return utils.ErrCamundaInstanceNotFound
	}

	if err := m.fileStorage.SaveConfiguration(config); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	m.logger.Info("Enabled Camunda instance: %s", id)
	return nil
}

// DisableInstance disables a Camunda instance
func (m *Manager) DisableInstance(id string) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	found := false
	for i := range config.CamundaInstances {
		if config.CamundaInstances[i].ID == id {
			config.CamundaInstances[i].Enabled = false
			config.CamundaInstances[i].UpdatedAt = time.Now()
			found = true
			break
		}
	}

	if !found {
		return utils.ErrCamundaInstanceNotFound
	}

	if err := m.fileStorage.SaveConfiguration(config); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	m.logger.Info("Disabled Camunda instance: %s", id)
	return nil
}

// UpdateComponentConfig updates component configuration for an instance
func (m *Manager) UpdateComponentConfig(instanceID string, componentName string, enabled bool) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// Validate component name
	valid := false
	for _, validComponent := range types.ValidComponents {
		if validComponent == componentName {
			valid = true
			break
		}
	}
	if !valid {
		return utils.ErrInvalidComponent
	}

	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	found := false
	for i := range config.CamundaInstances {
		if config.CamundaInstances[i].ID == instanceID {
			// Update or add component configuration
			componentFound := false
			for j := range config.CamundaInstances[i].Components {
				if config.CamundaInstances[i].Components[j].Name == componentName {
					config.CamundaInstances[i].Components[j].Enabled = enabled
					componentFound = true
					break
				}
			}

			if !componentFound {
				config.CamundaInstances[i].Components = append(config.CamundaInstances[i].Components, models.CamundaComponentConfig{
					Name:    componentName,
					Enabled: enabled,
				})
			}

			// Validate that at least one component is enabled
			hasEnabled := false
			for _, comp := range config.CamundaInstances[i].Components {
				if comp.Enabled {
					hasEnabled = true
					break
				}
			}
			if !hasEnabled {
				return utils.ErrNoComponentsEnabled
			}

			config.CamundaInstances[i].UpdatedAt = time.Now()
			found = true
			break
		}
	}

	if !found {
		return utils.ErrCamundaInstanceNotFound
	}

	if err := m.fileStorage.SaveConfiguration(config); err != nil {
		return fmt.Errorf("failed to save configuration: %w", err)
	}

	m.logger.Info("Updated component %s for instance %s: enabled=%v", componentName, instanceID, enabled)
	return nil
}

// GetEnabledInstances returns all enabled Camunda instances
func (m *Manager) GetEnabledInstances() ([]models.CamundaInstance, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	config, err := m.fileStorage.LoadConfiguration()
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration: %w", err)
	}

	var enabled []models.CamundaInstance
	for _, instance := range config.CamundaInstances {
		if instance.Enabled {
			enabled = append(enabled, instance)
		}
	}

	return enabled, nil
}
