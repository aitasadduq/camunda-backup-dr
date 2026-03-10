package models

import (
	"encoding/json"
	"testing"
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
