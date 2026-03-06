package models

import (
	"encoding/json"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/config"
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
			instanceID:   "camunda1",
			expectedESEV: "ELASTICSEARCH_PASSWORD_CAMUNDA1",
			expectedS3EV: "S3_SECRETKEY_CAMUNDA1",
		},
		{
			name:         "hyphenated id",
			instanceID:   "my-cluster",
			expectedESEV: "ELASTICSEARCH_PASSWORD_MY_CLUSTER",
			expectedS3EV: "S3_SECRETKEY_MY_CLUSTER",
		},
		{
			name:         "complex hyphenated id",
			instanceID:   "test-camunda-instance",
			expectedESEV: "ELASTICSEARCH_PASSWORD_TEST_CAMUNDA_INSTANCE",
			expectedS3EV: "S3_SECRETKEY_TEST_CAMUNDA_INSTANCE",
		},
		{
			name:         "mixed case id",
			instanceID:   "Prod-Cluster-01",
			expectedESEV: "ELASTICSEARCH_PASSWORD_PROD_CLUSTER_01",
			expectedS3EV: "S3_SECRETKEY_PROD_CLUSTER_01",
		},
		{
			name:         "underscore id unchanged",
			instanceID:   "my_cluster",
			expectedESEV: "ELASTICSEARCH_PASSWORD_MY_CLUSTER",
			expectedS3EV: "S3_SECRETKEY_MY_CLUSTER",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := NewCamundaInstance(tt.instanceID, "Test", "https://test.example.com")

			if instance.ElasticsearchPasswordEnvVar != tt.expectedESEV {
				t.Errorf("ElasticsearchPasswordEnvVar = %q, want %q", instance.ElasticsearchPasswordEnvVar, tt.expectedESEV)
			}
			if instance.BackupIDS3SecretKeyEnvVar != tt.expectedS3EV {
				t.Errorf("BackupIDS3SecretKeyEnvVar = %q, want %q", instance.BackupIDS3SecretKeyEnvVar, tt.expectedS3EV)
			}
		})
	}
}

func TestNewCamundaInstance_EnvVarFieldsConsistentWithNormalize(t *testing.T) {
	ids := []string{"my-cluster", "test-camunda-instance", "prod01", "a-b-c-d"}

	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			instance := NewCamundaInstance(id, "Test", "https://test.example.com")
			normalized := config.NormalizeForEnvVar(id)

			expectedES := "ELASTICSEARCH_PASSWORD_" + normalized
			expectedS3 := "S3_SECRETKEY_" + normalized

			if instance.ElasticsearchPasswordEnvVar != expectedES {
				t.Errorf("ES env var mismatch: got %q, want %q", instance.ElasticsearchPasswordEnvVar, expectedES)
			}
			if instance.BackupIDS3SecretKeyEnvVar != expectedS3 {
				t.Errorf("S3 env var mismatch: got %q, want %q", instance.BackupIDS3SecretKeyEnvVar, expectedS3)
			}
		})
	}
}

func TestNewCamundaInstance_EnvVarFieldsInJSON(t *testing.T) {
	instance := NewCamundaInstance("my-cluster", "Test", "https://test.example.com")

	data, err := instance.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("JSON unmarshal failed: %v", err)
	}

	esEnvVar, ok := parsed["elasticsearch_password_env_var"]
	if !ok {
		t.Fatal("elasticsearch_password_env_var missing from JSON output")
	}
	if esEnvVar != "ELASTICSEARCH_PASSWORD_MY_CLUSTER" {
		t.Errorf("elasticsearch_password_env_var = %q, want %q", esEnvVar, "ELASTICSEARCH_PASSWORD_MY_CLUSTER")
	}

	s3EnvVar, ok := parsed["s3_secret_key_env_var"]
	if !ok {
		t.Fatal("s3_secret_key_env_var missing from JSON output")
	}
	if s3EnvVar != "S3_SECRETKEY_MY_CLUSTER" {
		t.Errorf("s3_secret_key_env_var = %q, want %q", s3EnvVar, "S3_SECRETKEY_MY_CLUSTER")
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
