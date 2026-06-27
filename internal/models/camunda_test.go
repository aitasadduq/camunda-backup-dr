package models

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

func TestNewCamundaInstance_EnvVarFields(t *testing.T) {
	tests := []struct {
		name         string
		instanceID   string
		expectedESEV string
		expectedS3EV string
	}{
		{
			name:         "simple id",
			instanceID:   "camunda",
			expectedESEV: "",
			expectedS3EV: "",
		},
		{
			name:         "hyphenated id",
			instanceID:   "my-cluster",
			expectedESEV: "",
			expectedS3EV: "",
		},
		{
			name:         "complex hyphenated id",
			instanceID:   "test-camunda-instance",
			expectedESEV: "",
			expectedS3EV: "",
		},
		{
			name:         "single-hyphen id",
			instanceID:   "prod-cluster",
			expectedESEV: "",
			expectedS3EV: "",
		},
		{
			name:         "long hyphenated id",
			instanceID:   "my-test-cluster",
			expectedESEV: "",
			expectedS3EV: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := NewCamundaInstance(tt.instanceID, "Test", "https://test.example.com")

			if instance.ElasticsearchPasswordEnvVar != tt.expectedESEV {
				t.Errorf("ElasticsearchPasswordEnvVar = %q, want %q (should not be set by constructor)", instance.ElasticsearchPasswordEnvVar, tt.expectedESEV)
			}
			if instance.BackupIDS3SecretKeyEnvVar != tt.expectedS3EV {
				t.Errorf("BackupIDS3SecretKeyEnvVar = %q, want %q (should not be set by constructor)", instance.BackupIDS3SecretKeyEnvVar, tt.expectedS3EV)
			}
		})
	}
}

func TestNewCamundaInstance_EnvVarFieldsNotSetByConstructor(t *testing.T) {
	ids := []string{"my-cluster", "test-camunda-instance", "a-b-c-d"}

	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			instance := NewCamundaInstance(id, "Test", "https://test.example.com")

			if instance.ElasticsearchPasswordEnvVar != "" {
				t.Errorf("ElasticsearchPasswordEnvVar should be empty, got %q", instance.ElasticsearchPasswordEnvVar)
			}
			if instance.BackupIDS3SecretKeyEnvVar != "" {
				t.Errorf("BackupIDS3SecretKeyEnvVar should be empty, got %q", instance.BackupIDS3SecretKeyEnvVar)
			}
		})
	}
}

func TestNewCamundaInstance_EnvVarFieldsOmittedInJSON(t *testing.T) {
	instance := NewCamundaInstance("my-cluster", "Test", "https://test.example.com")

	data, err := instance.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}

	if _, ok := parsed["elasticsearch_password_env_var"]; ok {
		t.Error("elasticsearch_password_env_var should be omitted from JSON when not set by constructor")
	}

	if _, ok := parsed["s3_secret_key_env_var"]; ok {
		t.Error("s3_secret_key_env_var should be omitted from JSON when not set by constructor")
	}
}

func TestEnvVarFieldsOmittedWhenEmpty(t *testing.T) {
	instance := &CamundaInstance{
		ID:      "test",
		Name:    "Test",
		BaseURL: "https://test.example.com",
	}

	data, err := json.Marshal(instance)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	if _, ok := parsed["elasticsearch_password_env_var"]; ok {
		t.Error("elasticsearch_password_env_var should be omitted when empty")
	}
	if _, ok := parsed["s3_secret_key_env_var"]; ok {
		t.Error("s3_secret_key_env_var should be omitted when empty")
	}
}

func TestCamundaInstance_Validate_RequiresS3Fields(t *testing.T) {
	base := &CamundaInstance{
		ID:                  "test",
		Name:                "Test",
		BaseURL:             "https://test.example.com",
		Schedule:            "0 2 * * *",
		BackupIDS3Endpoint:  "https://s3.example.com",
		BackupIDS3AccessKey: "AKIAIOSFODNN7EXAMPLE",
		Components: []CamundaComponentConfig{
			{Name: "zeebe", Enabled: true},
		},
	}

	// Valid with both S3 fields
	if err := base.Validate(); err != nil {
		t.Errorf("Expected no error with both S3 fields set, got %v", err)
	}

	// Missing S3 endpoint
	noEndpoint := *base
	noEndpoint.BackupIDS3Endpoint = ""
	if err := noEndpoint.Validate(); err == nil {
		t.Error("Expected validation error when S3 endpoint is empty")
	}

	// Missing S3 access key
	noKey := *base
	noKey.BackupIDS3AccessKey = ""
	if err := noKey.Validate(); err == nil {
		t.Error("Expected validation error when S3 access key is empty")
	}
}

// --- Tests for IsComponentEnabled ---

func TestCamundaInstance_IsComponentEnabled(t *testing.T) {
	instance := NewCamundaInstance("test", "Test", "https://test.example.com")

	tests := []struct {
		name      string
		component string
		want      bool
	}{
		{"zeebe is enabled by default", types.ComponentZeebe, true},
		{"operate is enabled by default", types.ComponentOperate, true},
		{"tasklist is enabled by default", types.ComponentTasklist, true},
		{"optimize is disabled by default", types.ComponentOptimize, false},
		{"elasticsearch is enabled by default", types.ComponentElasticsearch, true},
		{"nonexistent component returns false", "nonexistent", false},
		{"empty string returns false", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := instance.IsComponentEnabled(tt.component)
			if got != tt.want {
				t.Errorf("IsComponentEnabled(%q) = %v, want %v", tt.component, got, tt.want)
			}
		})
	}
}

func TestCamundaInstance_IsComponentEnabled_EmptyComponents(t *testing.T) {
	instance := &CamundaInstance{Components: []CamundaComponentConfig{}}
	if instance.IsComponentEnabled(types.ComponentZeebe) {
		t.Error("expected false for empty components list")
	}
}

// --- Tests for GetEnabledComponents ---

func TestCamundaInstance_GetEnabledComponents_Default(t *testing.T) {
	instance := NewCamundaInstance("test", "Test", "https://test.example.com")
	enabled := instance.GetEnabledComponents()

	// Default: zeebe, operate, tasklist, elasticsearch enabled; optimize disabled
	expected := map[string]bool{
		types.ComponentZeebe:         true,
		types.ComponentOperate:       true,
		types.ComponentTasklist:      true,
		types.ComponentElasticsearch: true,
	}

	if len(enabled) != len(expected) {
		t.Errorf("got %d enabled components, want %d", len(enabled), len(expected))
	}

	for _, comp := range enabled {
		if !expected[comp] {
			t.Errorf("unexpected enabled component: %q", comp)
		}
	}
}

func TestCamundaInstance_GetEnabledComponents_AllDisabled(t *testing.T) {
	instance := &CamundaInstance{
		Components: []CamundaComponentConfig{
			{Name: types.ComponentZeebe, Enabled: false},
			{Name: types.ComponentOperate, Enabled: false},
		},
	}

	enabled := instance.GetEnabledComponents()
	if len(enabled) != 0 {
		t.Errorf("expected 0 enabled components, got %d: %v", len(enabled), enabled)
	}
}

func TestCamundaInstance_GetEnabledComponents_AllEnabled(t *testing.T) {
	instance := &CamundaInstance{
		Components: []CamundaComponentConfig{
			{Name: types.ComponentZeebe, Enabled: true},
			{Name: types.ComponentOperate, Enabled: true},
			{Name: types.ComponentOptimize, Enabled: true},
		},
	}

	enabled := instance.GetEnabledComponents()
	if len(enabled) != 3 {
		t.Errorf("expected 3 enabled components, got %d", len(enabled))
	}
}

func TestCamundaInstance_GetEnabledComponents_Empty(t *testing.T) {
	instance := &CamundaInstance{Components: []CamundaComponentConfig{}}
	enabled := instance.GetEnabledComponents()
	if enabled != nil {
		t.Errorf("expected nil for empty components, got %v", enabled)
	}
}

// --- Tests for UpdateLastBackup ---

func TestCamundaInstance_UpdateLastBackup(t *testing.T) {
	instance := NewCamundaInstance("test", "Test", "https://test.example.com")

	if instance.LastBackupAt != nil {
		t.Fatal("LastBackupAt should be nil initially")
	}
	if instance.LastBackupStatus != "NEVER_BACKED_UP" {
		t.Errorf("initial LastBackupStatus = %q, want %q", instance.LastBackupStatus, "NEVER_BACKED_UP")
	}

	backupTime := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	beforeUpdate := time.Now()
	instance.UpdateLastBackup(backupTime, "COMPLETED")
	afterUpdate := time.Now()

	if instance.LastBackupAt == nil {
		t.Fatal("LastBackupAt should be set after UpdateLastBackup")
	}
	if !instance.LastBackupAt.Equal(backupTime) {
		t.Errorf("LastBackupAt = %v, want %v", *instance.LastBackupAt, backupTime)
	}
	if instance.LastBackupStatus != "COMPLETED" {
		t.Errorf("LastBackupStatus = %q, want %q", instance.LastBackupStatus, "COMPLETED")
	}
	if instance.UpdatedAt.Before(beforeUpdate) || instance.UpdatedAt.After(afterUpdate) {
		t.Errorf("UpdatedAt %v not in expected range [%v, %v]", instance.UpdatedAt, beforeUpdate, afterUpdate)
	}
}

func TestCamundaInstance_UpdateLastBackup_OverwritesPrevious(t *testing.T) {
	instance := NewCamundaInstance("test", "Test", "https://test.example.com")

	t1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	instance.UpdateLastBackup(t1, "COMPLETED")

	t2 := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	instance.UpdateLastBackup(t2, "FAILED")

	if !instance.LastBackupAt.Equal(t2) {
		t.Errorf("LastBackupAt = %v, want %v", *instance.LastBackupAt, t2)
	}
	if instance.LastBackupStatus != "FAILED" {
		t.Errorf("LastBackupStatus = %q, want %q", instance.LastBackupStatus, "FAILED")
	}
}

// --- Tests for FromJSON ---

func TestCamundaInstance_FromJSON_Valid(t *testing.T) {
	original := NewCamundaInstance("my-cluster", "My Cluster", "https://cluster.example.com")
	original.BackupIDS3Endpoint = "https://s3.example.com"
	original.BackupIDS3AccessKey = "AKIAIOSFODNN7EXAMPLE"

	data, err := original.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	restored := &CamundaInstance{}
	if err := restored.FromJSON(data); err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if restored.ID != original.ID {
		t.Errorf("ID = %q, want %q", restored.ID, original.ID)
	}
	if restored.Name != original.Name {
		t.Errorf("Name = %q, want %q", restored.Name, original.Name)
	}
	if restored.BaseURL != original.BaseURL {
		t.Errorf("BaseURL = %q, want %q", restored.BaseURL, original.BaseURL)
	}
	if restored.Enabled != original.Enabled {
		t.Errorf("Enabled = %v, want %v", restored.Enabled, original.Enabled)
	}
	if restored.Schedule != original.Schedule {
		t.Errorf("Schedule = %q, want %q", restored.Schedule, original.Schedule)
	}
	if len(restored.Components) != len(original.Components) {
		t.Errorf("Components count = %d, want %d", len(restored.Components), len(original.Components))
	}
}

func TestCamundaInstance_FromJSON_InvalidJSON(t *testing.T) {
	instance := &CamundaInstance{}
	err := instance.FromJSON([]byte("not valid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestCamundaInstance_FromJSON_EmptyJSON(t *testing.T) {
	instance := &CamundaInstance{}
	err := instance.FromJSON([]byte("{}"))
	if err != nil {
		t.Errorf("unexpected error for empty JSON object: %v", err)
	}
	if instance.ID != "" {
		t.Errorf("ID = %q, want empty", instance.ID)
	}
}

func TestCamundaInstance_FromJSON_PartialFields(t *testing.T) {
	jsonData := []byte(`{"id": "partial", "name": "Partial Instance", "enabled": true}`)

	instance := &CamundaInstance{}
	if err := instance.FromJSON(jsonData); err != nil {
		t.Fatalf("FromJSON failed: %v", err)
	}

	if instance.ID != "partial" {
		t.Errorf("ID = %q, want %q", instance.ID, "partial")
	}
	if instance.Name != "Partial Instance" {
		t.Errorf("Name = %q, want %q", instance.Name, "Partial Instance")
	}
	if !instance.Enabled {
		t.Error("Enabled should be true")
	}
	if instance.BaseURL != "" {
		t.Errorf("BaseURL = %q, want empty", instance.BaseURL)
	}
}

// --- Tests for Validate edge cases ---

func TestCamundaInstance_Validate_AllErrors(t *testing.T) {
	validBase := func() *CamundaInstance {
		return &CamundaInstance{
			ID:                  "test",
			Name:                "Test",
			BaseURL:             "https://test.example.com",
			Schedule:            "0 2 * * *",
			BackupIDS3Endpoint:  "https://s3.example.com",
			BackupIDS3AccessKey: "AKIAIOSFODNN7EXAMPLE",
			Components: []CamundaComponentConfig{
				{Name: types.ComponentZeebe, Enabled: true},
			},
		}
	}

	tests := []struct {
		name    string
		modify  func(*CamundaInstance)
		wantErr error
	}{
		{"empty ID", func(ci *CamundaInstance) { ci.ID = "" }, utils.ErrInvalidCamundaInstance},
		{"ID with uppercase", func(ci *CamundaInstance) { ci.ID = "Test" }, utils.ErrInvalidCamundaInstance},
		{"ID with leading hyphen", func(ci *CamundaInstance) { ci.ID = "-test" }, utils.ErrInvalidCamundaInstance},
		{"ID with trailing hyphen", func(ci *CamundaInstance) { ci.ID = "test-" }, utils.ErrInvalidCamundaInstance},
		{"ID with numbers", func(ci *CamundaInstance) { ci.ID = "test123" }, utils.ErrInvalidCamundaInstance},
		{"empty name", func(ci *CamundaInstance) { ci.Name = "" }, utils.ErrInvalidCamundaInstance},
		{"empty base URL", func(ci *CamundaInstance) { ci.BaseURL = "" }, utils.ErrInvalidCamundaInstance},
		{"empty schedule", func(ci *CamundaInstance) { ci.Schedule = "" }, utils.ErrInvalidCamundaInstance},
		{"negative success retention", func(ci *CamundaInstance) { ci.SuccessRetention = -1 }, utils.ErrInvalidCamundaInstance},
		{"negative failure retention", func(ci *CamundaInstance) { ci.FailureRetention = -1 }, utils.ErrInvalidCamundaInstance},
		{"empty components", func(ci *CamundaInstance) { ci.Components = []CamundaComponentConfig{} }, utils.ErrNoComponentsEnabled},
		{"all components disabled", func(ci *CamundaInstance) {
			ci.Components = []CamundaComponentConfig{{Name: types.ComponentZeebe, Enabled: false}}
		}, utils.ErrNoComponentsEnabled},
		{"single letter ID is valid", func(ci *CamundaInstance) { ci.ID = "a" }, nil},
		{"zero success retention is valid", func(ci *CamundaInstance) { ci.SuccessRetention = 0 }, nil},
		{"snapshot repo with slash is invalid", func(ci *CamundaInstance) { ci.ElasticsearchSnapshotRepository = "repo/traversal" }, utils.ErrInvalidCamundaInstance},
		{"snapshot repo with dotdot is invalid", func(ci *CamundaInstance) { ci.ElasticsearchSnapshotRepository = "../other" }, utils.ErrInvalidCamundaInstance},
		{"snapshot repo with percent is invalid", func(ci *CamundaInstance) { ci.ElasticsearchSnapshotRepository = "repo%2ftraversal" }, utils.ErrInvalidCamundaInstance},
		{"valid snapshot repo is accepted", func(ci *CamundaInstance) { ci.ElasticsearchSnapshotRepository = "camunda-backup" }, nil},
		{"valid snapshot repo with dots and underscores", func(ci *CamundaInstance) { ci.ElasticsearchSnapshotRepository = "my_repo.v2" }, nil},
		{"empty snapshot repo is accepted", func(ci *CamundaInstance) { ci.ElasticsearchSnapshotRepository = "" }, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ci := validBase()
			tt.modify(ci)
			err := ci.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			} else {
				if err != tt.wantErr {
					t.Errorf("error = %v, want %v", err, tt.wantErr)
				}
			}
		})
	}
}
