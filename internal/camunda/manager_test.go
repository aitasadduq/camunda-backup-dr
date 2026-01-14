package camunda

import (
	"os"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func setupTestManager(t *testing.T) (*Manager, string, func()) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "camunda-manager-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	// Create file storage
	cfg := &config.Config{DataDir: tempDir}
	logger := utils.NewLogger("debug")
	fs, err := storage.NewFileStorage(tempDir, cfg, logger)
	if err != nil {
		t.Fatalf("Failed to create file storage: %v", err)
	}

	// Create manager
	manager := NewManager(fs, logger)

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return manager, tempDir, cleanup
}

func TestManager_CreateInstance(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")

	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Verify instance was created
	retrieved, err := manager.GetInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to retrieve instance: %v", err)
	}

	if retrieved.Name != "Test Camunda" {
		t.Errorf("Expected name 'Test Camunda', got '%s'", retrieved.Name)
	}
}

func TestManager_CreateInstance_Duplicate(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")

	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Try to create duplicate
	err = manager.CreateInstance(instance)
	if err != utils.ErrCamundaInstanceAlreadyExists {
		t.Errorf("Expected ErrCamundaInstanceAlreadyExists, got %v", err)
	}
}

func TestManager_CreateInstance_Invalid(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := &models.CamundaInstance{
		ID: "", // Invalid: empty ID
	}

	err := manager.CreateInstance(instance)
	if err == nil {
		t.Error("Expected validation error for invalid instance")
	}
}

func TestManager_GetInstance(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if retrieved.ID != "camunda1" {
		t.Errorf("Expected ID 'camunda1', got '%s'", retrieved.ID)
	}
}

func TestManager_GetInstance_NotFound(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	_, err := manager.GetInstance("nonexistent")
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

func TestManager_ListInstances(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	// Create multiple instances
	instance1 := models.NewCamundaInstance("camunda1", "Test 1", "https://test1.example.com")
	instance2 := models.NewCamundaInstance("camunda2", "Test 2", "https://test2.example.com")

	manager.CreateInstance(instance1)
	manager.CreateInstance(instance2)

	instances, err := manager.ListInstances()
	if err != nil {
		t.Fatalf("Failed to list instances: %v", err)
	}

	if len(instances) != 2 {
		t.Errorf("Expected 2 instances, got %d", len(instances))
	}
}

func TestManager_UpdateInstance(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Update instance
	updated := models.NewCamundaInstance("camunda1", "Updated Name", "https://updated.example.com")
	updated.Schedule = "0 3 * * *"
	err = manager.UpdateInstance("camunda1", updated)
	if err != nil {
		t.Fatalf("Failed to update instance: %v", err)
	}

	// Verify update
	retrieved, err := manager.GetInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if retrieved.Name != "Updated Name" {
		t.Errorf("Expected name 'Updated Name', got '%s'", retrieved.Name)
	}

	if retrieved.Schedule != "0 3 * * *" {
		t.Errorf("Expected schedule '0 3 * * *', got '%s'", retrieved.Schedule)
	}
}

func TestManager_UpdateInstance_NotFound(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test", "https://test.example.com")
	err := manager.UpdateInstance("nonexistent", instance)
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

func TestManager_DeleteInstance(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.DeleteInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to delete instance: %v", err)
	}

	// Verify deletion
	_, err = manager.GetInstance("camunda1")
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound after deletion, got %v", err)
	}
}

func TestManager_DeleteInstance_NotFound(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	err := manager.DeleteInstance("nonexistent")
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

func TestManager_EnableInstance(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	instance.Enabled = false
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.EnableInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to enable instance: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if !retrieved.Enabled {
		t.Error("Instance should be enabled")
	}
}

func TestManager_DisableInstance(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.DisableInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to disable instance: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if retrieved.Enabled {
		t.Error("Instance should be disabled")
	}
}

func TestManager_GetEnabledInstances(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance1 := models.NewCamundaInstance("camunda1", "Test 1", "https://test1.example.com")
	instance1.Enabled = true
	instance2 := models.NewCamundaInstance("camunda2", "Test 2", "https://test2.example.com")
	instance2.Enabled = false

	manager.CreateInstance(instance1)
	manager.CreateInstance(instance2)

	enabled, err := manager.GetEnabledInstances()
	if err != nil {
		t.Fatalf("Failed to get enabled instances: %v", err)
	}

	if len(enabled) != 1 {
		t.Errorf("Expected 1 enabled instance, got %d", len(enabled))
	}

	if enabled[0].ID != "camunda1" {
		t.Errorf("Expected enabled instance ID 'camunda1', got '%s'", enabled[0].ID)
	}
}

func TestManager_UpdateComponentConfig(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Disable a component
	err = manager.UpdateComponentConfig("camunda1", types.ComponentOptimize, false)
	if err != nil {
		t.Fatalf("Failed to update component config: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda1")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if retrieved.IsComponentEnabled(types.ComponentOptimize) {
		t.Error("Component should be disabled")
	}
}

func TestManager_UpdateComponentConfig_InvalidComponent(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.UpdateComponentConfig("camunda1", "invalid-component", true)
	if err != utils.ErrInvalidComponent {
		t.Errorf("Expected ErrInvalidComponent, got %v", err)
	}
}

func TestManager_UpdateComponentConfig_NoComponentsEnabled(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	// Disable all components
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: false},
		{Name: types.ComponentOperate, Enabled: false},
		{Name: types.ComponentTasklist, Enabled: false},
		{Name: types.ComponentOptimize, Enabled: false},
		{Name: types.ComponentElasticsearch, Enabled: false},
	}
	err := manager.CreateInstance(instance)
	if err == nil {
		t.Error("Expected validation error when no components are enabled")
	}
}

func TestManager_UpdateComponentConfig_DisableLastComponent(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda1", "Test Camunda", "https://test.example.com")
	// Keep only one component enabled
	instance.Components = []models.CamundaComponentConfig{
		{Name: types.ComponentZeebe, Enabled: true},
		{Name: types.ComponentOperate, Enabled: false},
		{Name: types.ComponentTasklist, Enabled: false},
		{Name: types.ComponentOptimize, Enabled: false},
		{Name: types.ComponentElasticsearch, Enabled: false},
	}
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Try to disable the last enabled component
	err = manager.UpdateComponentConfig("camunda1", types.ComponentZeebe, false)
	if err != utils.ErrNoComponentsEnabled {
		t.Errorf("Expected ErrNoComponentsEnabled, got %v", err)
	}
}
