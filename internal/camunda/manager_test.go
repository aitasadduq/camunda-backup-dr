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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"

	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Verify instance was created
	retrieved, err := manager.GetInstance("camunda-a")
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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"

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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda-a")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if retrieved.ID != "camunda-a" {
		t.Errorf("Expected ID 'camunda-a', got '%s'", retrieved.ID)
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
	instance1 := models.NewCamundaInstance("camunda-a", "Test 1", "https://test1.example.com")
	instance1.BackupIDS3Endpoint = "https://s3.example.com"
	instance1.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	instance2 := models.NewCamundaInstance("camunda-b", "Test 2", "https://test2.example.com")
	instance2.BackupIDS3Endpoint = "https://s3.example.com"
	instance2.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"

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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Update instance
	updated := models.NewCamundaInstance("camunda-a", "Updated Name", "https://updated.example.com")
	updated.BackupIDS3Endpoint = "https://s3.example.com"
	updated.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	updated.Schedule = "0 3 * * *"
	err = manager.UpdateInstance("camunda-a", updated)
	if err != nil {
		t.Fatalf("Failed to update instance: %v", err)
	}

	// Verify update
	retrieved, err := manager.GetInstance("camunda-a")
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

	instance := models.NewCamundaInstance("camunda-a", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.UpdateInstance("nonexistent", instance)
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

func TestManager_DeleteInstance(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.DeleteInstance("camunda-a")
	if err != nil {
		t.Fatalf("Failed to delete instance: %v", err)
	}

	// Verify deletion
	_, err = manager.GetInstance("camunda-a")
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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	instance.Enabled = false
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.EnableInstance("camunda-a")
	if err != nil {
		t.Fatalf("Failed to enable instance: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda-a")
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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.DisableInstance("camunda-a")
	if err != nil {
		t.Fatalf("Failed to disable instance: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda-a")
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

	instance1 := models.NewCamundaInstance("camunda-a", "Test 1", "https://test1.example.com")
	instance1.BackupIDS3Endpoint = "https://s3.example.com"
	instance1.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	instance1.Enabled = true
	instance2 := models.NewCamundaInstance("camunda-b", "Test 2", "https://test2.example.com")
	instance2.BackupIDS3Endpoint = "https://s3.example.com"
	instance2.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
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

	if enabled[0].ID != "camunda-a" {
		t.Errorf("Expected enabled instance ID 'camunda-a', got '%s'", enabled[0].ID)
	}
}

func TestManager_UpdateComponentConfig(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Disable a component
	err = manager.UpdateComponentConfig("camunda-a", types.ComponentOptimize, false)
	if err != nil {
		t.Fatalf("Failed to update component config: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda-a")
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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	err = manager.UpdateComponentConfig("camunda-a", "invalid-component", true)
	if err != utils.ErrInvalidComponent {
		t.Errorf("Expected ErrInvalidComponent, got %v", err)
	}
}

func TestManager_UpdateComponentConfig_NoComponentsEnabled(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
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

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
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
	err = manager.UpdateComponentConfig("camunda-a", types.ComponentZeebe, false)
	if err != utils.ErrNoComponentsEnabled {
		t.Errorf("Expected ErrNoComponentsEnabled, got %v", err)
	}
}

// --- Env Var Field Population Tests ---

func TestManager_GetInstance_PopulatesEnvVarFields(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("my-cluster", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.CreateInstance(instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	retrieved, err := manager.GetInstance("my-cluster")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	expectedES := "ELASTICSEARCH_PASSWORD_" + config.NormalizeForEnvVar("my-cluster")
	expectedS3 := "S3_SECRETKEY_" + config.NormalizeForEnvVar("my-cluster")

	if retrieved.ElasticsearchPasswordEnvVar != expectedES {
		t.Errorf("ElasticsearchPasswordEnvVar = %q, want %q", retrieved.ElasticsearchPasswordEnvVar, expectedES)
	}
	if retrieved.BackupIDS3SecretKeyEnvVar != expectedS3 {
		t.Errorf("BackupIDS3SecretKeyEnvVar = %q, want %q", retrieved.BackupIDS3SecretKeyEnvVar, expectedS3)
	}
}

func TestManager_GetInstance_EnvVarNormalization(t *testing.T) {
	tests := []struct {
		name         string
		instanceID   string
		expectedESEV string
		expectedS3EV string
	}{
		{
			name:         "simple id",
			instanceID:   "camunda-a",
			expectedESEV: "ELASTICSEARCH_PASSWORD_CAMUNDA_A",
			expectedS3EV: "S3_SECRETKEY_CAMUNDA_A",
		},
		{
			name:         "hyphenated id",
			instanceID:   "my-cluster",
			expectedESEV: "ELASTICSEARCH_PASSWORD_MY_CLUSTER",
			expectedS3EV: "S3_SECRETKEY_MY_CLUSTER",
		},
		{
			name:         "multi-hyphen id",
			instanceID:   "test-camunda-instance",
			expectedESEV: "ELASTICSEARCH_PASSWORD_TEST_CAMUNDA_INSTANCE",
			expectedS3EV: "S3_SECRETKEY_TEST_CAMUNDA_INSTANCE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, _, cleanup := setupTestManager(t)
			defer cleanup()

			instance := models.NewCamundaInstance(tt.instanceID, "Test", "https://test.example.com")
			instance.BackupIDS3Endpoint = "https://s3.example.com"
			instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
			if err := manager.CreateInstance(instance); err != nil {
				t.Fatalf("Failed to create instance: %v", err)
			}

			retrieved, err := manager.GetInstance(tt.instanceID)
			if err != nil {
				t.Fatalf("Failed to get instance: %v", err)
			}

			if retrieved.ElasticsearchPasswordEnvVar != tt.expectedESEV {
				t.Errorf("ElasticsearchPasswordEnvVar = %q, want %q", retrieved.ElasticsearchPasswordEnvVar, tt.expectedESEV)
			}
			if retrieved.BackupIDS3SecretKeyEnvVar != tt.expectedS3EV {
				t.Errorf("BackupIDS3SecretKeyEnvVar = %q, want %q", retrieved.BackupIDS3SecretKeyEnvVar, tt.expectedS3EV)
			}
		})
	}
}

func TestManager_ListInstances_PopulatesEnvVarFields(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance1 := models.NewCamundaInstance("cluster-a", "A", "https://a.example.com")
	instance1.BackupIDS3Endpoint = "https://s3.example.com"
	instance1.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	instance2 := models.NewCamundaInstance("cluster-b", "B", "https://b.example.com")
	instance2.BackupIDS3Endpoint = "https://s3.example.com"
	instance2.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	manager.CreateInstance(instance1)
	manager.CreateInstance(instance2)

	instances, err := manager.ListInstances()
	if err != nil {
		t.Fatalf("Failed to list instances: %v", err)
	}

	if len(instances) != 2 {
		t.Fatalf("Expected 2 instances, got %d", len(instances))
	}

	for _, inst := range instances {
		normalized := config.NormalizeForEnvVar(inst.ID)
		expectedES := "ELASTICSEARCH_PASSWORD_" + normalized
		expectedS3 := "S3_SECRETKEY_" + normalized

		if inst.ElasticsearchPasswordEnvVar != expectedES {
			t.Errorf("Instance %s: ElasticsearchPasswordEnvVar = %q, want %q", inst.ID, inst.ElasticsearchPasswordEnvVar, expectedES)
		}
		if inst.BackupIDS3SecretKeyEnvVar != expectedS3 {
			t.Errorf("Instance %s: BackupIDS3SecretKeyEnvVar = %q, want %q", inst.ID, inst.BackupIDS3SecretKeyEnvVar, expectedS3)
		}
	}
}

func TestManager_UpdateInstance_PopulatesEnvVarFields(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("my-cluster", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.CreateInstance(instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	updated := models.NewCamundaInstance("my-cluster", "Updated", "https://updated.example.com")
	updated.BackupIDS3Endpoint = "https://s3.example.com"
	updated.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.UpdateInstance("my-cluster", updated); err != nil {
		t.Fatalf("Failed to update instance: %v", err)
	}

	retrieved, err := manager.GetInstance("my-cluster")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	expectedES := "ELASTICSEARCH_PASSWORD_MY_CLUSTER"
	expectedS3 := "S3_SECRETKEY_MY_CLUSTER"

	if retrieved.ElasticsearchPasswordEnvVar != expectedES {
		t.Errorf("ElasticsearchPasswordEnvVar = %q, want %q", retrieved.ElasticsearchPasswordEnvVar, expectedES)
	}
	if retrieved.BackupIDS3SecretKeyEnvVar != expectedS3 {
		t.Errorf("BackupIDS3SecretKeyEnvVar = %q, want %q", retrieved.BackupIDS3SecretKeyEnvVar, expectedS3)
	}
}

func TestManager_GetEnabledInstances_PopulatesEnvVarFields(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance1 := models.NewCamundaInstance("prod-cluster", "Prod", "https://prod.example.com")
	instance1.BackupIDS3Endpoint = "https://s3.example.com"
	instance1.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	instance1.Enabled = true
	instance2 := models.NewCamundaInstance("staging-cluster", "Staging", "https://staging.example.com")
	instance2.BackupIDS3Endpoint = "https://s3.example.com"
	instance2.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	instance2.Enabled = false

	manager.CreateInstance(instance1)
	manager.CreateInstance(instance2)

	enabled, err := manager.GetEnabledInstances()
	if err != nil {
		t.Fatalf("Failed to get enabled instances: %v", err)
	}

	if len(enabled) != 1 {
		t.Fatalf("Expected 1 enabled instance, got %d", len(enabled))
	}

	expectedES := "ELASTICSEARCH_PASSWORD_PROD_CLUSTER"
	expectedS3 := "S3_SECRETKEY_PROD_CLUSTER"

	if enabled[0].ElasticsearchPasswordEnvVar != expectedES {
		t.Errorf("ElasticsearchPasswordEnvVar = %q, want %q", enabled[0].ElasticsearchPasswordEnvVar, expectedES)
	}
	if enabled[0].BackupIDS3SecretKeyEnvVar != expectedS3 {
		t.Errorf("BackupIDS3SecretKeyEnvVar = %q, want %q", enabled[0].BackupIDS3SecretKeyEnvVar, expectedS3)
	}
}

func TestManager_GetEnabledInstances_AllEnvVarsNormalized(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	ids := []string{"a-b", "c-d-e", "simple"}
	for _, id := range ids {
		inst := models.NewCamundaInstance(id, "Test "+id, "https://"+id+".example.com")
		inst.BackupIDS3Endpoint = "https://s3.example.com"
		inst.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
		inst.Enabled = true
		manager.CreateInstance(inst)
	}

	enabled, err := manager.GetEnabledInstances()
	if err != nil {
		t.Fatalf("Failed to get enabled instances: %v", err)
	}

	for _, inst := range enabled {
		normalized := config.NormalizeForEnvVar(inst.ID)

		if inst.ElasticsearchPasswordEnvVar != "ELASTICSEARCH_PASSWORD_"+normalized {
			t.Errorf("Instance %s: ES env var = %q, expected suffix %q", inst.ID, inst.ElasticsearchPasswordEnvVar, normalized)
		}
		if inst.BackupIDS3SecretKeyEnvVar != "S3_SECRETKEY_"+normalized {
			t.Errorf("Instance %s: S3 env var = %q, expected suffix %q", inst.ID, inst.BackupIDS3SecretKeyEnvVar, normalized)
		}
	}
}

// --- Error path tests for manager functions ---

func TestManager_EnableInstance_NotFound(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	err := manager.EnableInstance("nonexistent")
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

func TestManager_DisableInstance_NotFound(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	err := manager.DisableInstance("nonexistent")
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

func TestManager_UpdateComponentConfig_InstanceNotFound(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	err := manager.UpdateComponentConfig("nonexistent", types.ComponentZeebe, true)
	if err != utils.ErrCamundaInstanceNotFound {
		t.Errorf("Expected ErrCamundaInstanceNotFound, got %v", err)
	}
}

func TestManager_UpdateComponentConfig_AddNewComponent(t *testing.T) {
	manager, _, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test Camunda", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Add a new component that doesn't exist yet in the components list
	err = manager.UpdateComponentConfig("camunda-a", types.ComponentElasticsearch, true)
	if err != nil {
		t.Fatalf("Failed to update component config: %v", err)
	}

	retrieved, err := manager.GetInstance("camunda-a")
	if err != nil {
		t.Fatalf("Failed to get instance: %v", err)
	}

	if !retrieved.IsComponentEnabled(types.ComponentElasticsearch) {
		t.Error("Expected Elasticsearch component to be enabled")
	}
}

func TestManager_EnableInstance_SaveError(t *testing.T) {
	manager, tempDir, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.CreateInstance(instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Remove the data directory to cause SaveConfiguration to fail
	os.RemoveAll(tempDir)

	err := manager.EnableInstance("camunda-a")
	if err == nil {
		t.Error("Expected error when save fails")
	}
}

func TestManager_DisableInstance_SaveError(t *testing.T) {
	manager, tempDir, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.CreateInstance(instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Remove the data directory to cause SaveConfiguration to fail
	os.RemoveAll(tempDir)

	err := manager.DisableInstance("camunda-a")
	if err == nil {
		t.Error("Expected error when save fails")
	}
}

func TestManager_DeleteInstance_SaveError(t *testing.T) {
	manager, tempDir, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.CreateInstance(instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Remove the data directory to cause SaveConfiguration to fail
	os.RemoveAll(tempDir)

	err := manager.DeleteInstance("camunda-a")
	if err == nil {
		t.Error("Expected error when save fails")
	}
}

func TestManager_UpdateInstance_SaveError(t *testing.T) {
	manager, tempDir, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.CreateInstance(instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Remove the data directory to cause SaveConfiguration to fail
	os.RemoveAll(tempDir)

	updated := models.NewCamundaInstance("camunda-a", "Updated", "https://updated.example.com")
	updated.BackupIDS3Endpoint = "https://s3.example.com"
	updated.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.UpdateInstance("camunda-a", updated)
	if err == nil {
		t.Error("Expected error when save fails")
	}
}

func TestManager_CreateInstance_SaveError(t *testing.T) {
	manager, tempDir, cleanup := setupTestManager(t)
	defer cleanup()

	// Remove the data directory to cause SaveConfiguration to fail
	os.RemoveAll(tempDir)

	instance := models.NewCamundaInstance("camunda-a", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	err := manager.CreateInstance(instance)
	if err == nil {
		t.Error("Expected error when save fails")
	}
}

func TestManager_UpdateComponentConfig_SaveError(t *testing.T) {
	manager, tempDir, cleanup := setupTestManager(t)
	defer cleanup()

	instance := models.NewCamundaInstance("camunda-a", "Test", "https://test.example.com")
	instance.BackupIDS3Endpoint = "https://s3.example.com"
	instance.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"
	if err := manager.CreateInstance(instance); err != nil {
		t.Fatalf("Failed to create instance: %v", err)
	}

	// Remove the data directory to cause SaveConfiguration to fail
	os.RemoveAll(tempDir)

	err := manager.UpdateComponentConfig("camunda-a", types.ComponentOptimize, false)
	if err == nil {
		t.Error("Expected error when save fails")
	}
}
