package config

import (
	"os"
	"testing"
)

func TestNormalizeForEnvVar(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple lowercase",
			input:    "camunda",
			expected: "CAMUNDA",
		},
		{
			name:     "hyphenated lowercase",
			input:    "my-cluster",
			expected: "MY_CLUSTER",
		},
		{
			name:     "multiple hyphens",
			input:    "my-test-camunda-instance",
			expected: "MY_TEST_CAMUNDA_INSTANCE",
		},
		{
			name:     "already uppercase no hyphens",
			input:    "CAMUNDA",
			expected: "CAMUNDA",
		},
		{
			name:     "already uppercase with underscores",
			input:    "MY_CLUSTER",
			expected: "MY_CLUSTER",
		},
		{
			name:     "mixed case with hyphens",
			input:    "My-Cluster-01",
			expected: "MY_CLUSTER_01",
		},
		{
			name:     "numbers only",
			input:    "123",
			expected: "123",
		},
		{
			name:     "numbers with hyphens",
			input:    "cluster-1-prod",
			expected: "CLUSTER_1_PROD",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "single character",
			input:    "a",
			expected: "A",
		},
		{
			name:     "single hyphen",
			input:    "-",
			expected: "_",
		},
		{
			name:     "leading hyphen",
			input:    "-cluster",
			expected: "_CLUSTER",
		},
		{
			name:     "trailing hyphen",
			input:    "cluster-",
			expected: "CLUSTER_",
		},
		{
			name:     "consecutive hyphens",
			input:    "my--cluster",
			expected: "MY__CLUSTER",
		},
		{
			name:     "underscores preserved",
			input:    "my_cluster",
			expected: "MY_CLUSTER",
		},
		{
			name:     "mixed hyphens and underscores",
			input:    "my-cluster_prod",
			expected: "MY_CLUSTER_PROD",
		},
		{
			name:     "realistic instance id",
			input:    "test-camunda-instance",
			expected: "TEST_CAMUNDA_INSTANCE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeForEnvVar(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeForEnvVar(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// setEnvForTest sets an env var and returns a cleanup function.
func setEnvForTest(t *testing.T, key, value string) {
	t.Helper()
	old, existed := os.LookupEnv(key)
	os.Setenv(key, value)
	t.Cleanup(func() {
		if existed {
			os.Setenv(key, old)
		} else {
			os.Unsetenv(key)
		}
	})
}

func TestGetElasticsearchPassword(t *testing.T) {
	cfg := &Config{}

	tests := []struct {
		name       string
		instanceID string
		envKey     string
		envValue   string
		expected   string
	}{
		{
			name:       "simple id",
			instanceID: "camunda1",
			envKey:     "ELASTICSEARCH_PASSWORD_CAMUNDA1",
			envValue:   "secret123",
			expected:   "secret123",
		},
		{
			name:       "hyphenated id uses normalized env var",
			instanceID: "my-cluster",
			envKey:     "ELASTICSEARCH_PASSWORD_MY_CLUSTER",
			envValue:   "pass-word",
			expected:   "pass-word",
		},
		{
			name:       "complex hyphenated id",
			instanceID: "test-camunda-instance",
			envKey:     "ELASTICSEARCH_PASSWORD_TEST_CAMUNDA_INSTANCE",
			envValue:   "complex-secret!@#",
			expected:   "complex-secret!@#",
		},
		{
			name:       "missing env var returns empty",
			instanceID: "nonexistent-instance",
			envKey:     "",
			envValue:   "",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envKey != "" {
				setEnvForTest(t, tt.envKey, tt.envValue)
			}
			result := cfg.GetElasticsearchPassword(tt.instanceID)
			if result != tt.expected {
				t.Errorf("GetElasticsearchPassword(%q) = %q, want %q", tt.instanceID, result, tt.expected)
			}
		})
	}
}

func TestGetS3SecretKey(t *testing.T) {
	cfg := &Config{}

	tests := []struct {
		name       string
		instanceID string
		envKey     string
		envValue   string
		expected   string
	}{
		{
			name:       "simple id",
			instanceID: "camunda1",
			envKey:     "S3_SECRETKEY_CAMUNDA1",
			envValue:   "s3secret",
			expected:   "s3secret",
		},
		{
			name:       "hyphenated id uses normalized env var",
			instanceID: "my-cluster",
			envKey:     "S3_SECRETKEY_MY_CLUSTER",
			envValue:   "s3-key-123",
			expected:   "s3-key-123",
		},
		{
			name:       "complex hyphenated id",
			instanceID: "test-camunda-instance",
			envKey:     "S3_SECRETKEY_TEST_CAMUNDA_INSTANCE",
			envValue:   "long-secret-key",
			expected:   "long-secret-key",
		},
		{
			name:       "missing env var returns empty",
			instanceID: "nonexistent-instance",
			envKey:     "",
			envValue:   "",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envKey != "" {
				setEnvForTest(t, tt.envKey, tt.envValue)
			}
			result := cfg.GetS3SecretKey(tt.instanceID)
			if result != tt.expected {
				t.Errorf("GetS3SecretKey(%q) = %q, want %q", tt.instanceID, result, tt.expected)
			}
		})
	}
}

func TestGetElasticsearchSnapshotRepository(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		envKey     string
		envValue   string
		defaultVal string
		expected   string
	}{
		{
			name:       "instance-specific override",
			instanceID: "my-cluster",
			envKey:     "ELASTICSEARCH_SNAPSHOT_REPOSITORY_MY_CLUSTER",
			envValue:   "custom-repo",
			defaultVal: "camunda-backup",
			expected:   "custom-repo",
		},
		{
			name:       "falls back to default when no env var",
			instanceID: "other-cluster",
			envKey:     "",
			envValue:   "",
			defaultVal: "camunda-backup",
			expected:   "camunda-backup",
		},
		{
			name:       "simple id override",
			instanceID: "prod1",
			envKey:     "ELASTICSEARCH_SNAPSHOT_REPOSITORY_PROD1",
			envValue:   "prod-repo",
			defaultVal: "default-repo",
			expected:   "prod-repo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{DefaultElasticsearchSnapshotRepository: tt.defaultVal}
			if tt.envKey != "" {
				setEnvForTest(t, tt.envKey, tt.envValue)
			}
			result := cfg.GetElasticsearchSnapshotRepository(tt.instanceID)
			if result != tt.expected {
				t.Errorf("GetElasticsearchSnapshotRepository(%q) = %q, want %q", tt.instanceID, result, tt.expected)
			}
		})
	}
}

func TestGetElasticsearchSnapshotNamePrefix(t *testing.T) {
	tests := []struct {
		name       string
		instanceID string
		envKey     string
		envValue   string
		defaultVal string
		expected   string
	}{
		{
			name:       "instance-specific override",
			instanceID: "my-cluster",
			envKey:     "ELASTICSEARCH_SNAPSHOT_NAME_PREFIX_MY_CLUSTER",
			envValue:   "custom-prefix",
			defaultVal: "",
			expected:   "custom-prefix",
		},
		{
			name:       "falls back to default when no env var",
			instanceID: "other-cluster",
			envKey:     "",
			envValue:   "",
			defaultVal: "default-prefix",
			expected:   "default-prefix",
		},
		{
			name:       "falls back to empty default",
			instanceID: "another-cluster",
			envKey:     "",
			envValue:   "",
			defaultVal: "",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{DefaultElasticsearchSnapshotNamePrefix: tt.defaultVal}
			if tt.envKey != "" {
				setEnvForTest(t, tt.envKey, tt.envValue)
			}
			result := cfg.GetElasticsearchSnapshotNamePrefix(tt.instanceID)
			if result != tt.expected {
				t.Errorf("GetElasticsearchSnapshotNamePrefix(%q) = %q, want %q", tt.instanceID, result, tt.expected)
			}
		})
	}
}

// TestEnvVarNormalizationConsistency verifies that the env var name generated
// by NormalizeForEnvVar matches what Get* methods actually look up.
func TestEnvVarNormalizationConsistency(t *testing.T) {
	instanceID := "my-test-cluster"
	normalizedSuffix := NormalizeForEnvVar(instanceID)

	// Set env vars using the normalized suffix
	setEnvForTest(t, "ELASTICSEARCH_PASSWORD_"+normalizedSuffix, "es-pass")
	setEnvForTest(t, "S3_SECRETKEY_"+normalizedSuffix, "s3-key")
	setEnvForTest(t, "ELASTICSEARCH_SNAPSHOT_REPOSITORY_"+normalizedSuffix, "test-repo")
	setEnvForTest(t, "ELASTICSEARCH_SNAPSHOT_NAME_PREFIX_"+normalizedSuffix, "test-prefix")

	cfg := &Config{
		DefaultElasticsearchSnapshotRepository: "default-repo",
		DefaultElasticsearchSnapshotNamePrefix: "default-prefix",
	}

	if got := cfg.GetElasticsearchPassword(instanceID); got != "es-pass" {
		t.Errorf("GetElasticsearchPassword: got %q, want %q", got, "es-pass")
	}
	if got := cfg.GetS3SecretKey(instanceID); got != "s3-key" {
		t.Errorf("GetS3SecretKey: got %q, want %q", got, "s3-key")
	}
	if got := cfg.GetElasticsearchSnapshotRepository(instanceID); got != "test-repo" {
		t.Errorf("GetElasticsearchSnapshotRepository: got %q, want %q", got, "test-repo")
	}
	if got := cfg.GetElasticsearchSnapshotNamePrefix(instanceID); got != "test-prefix" {
		t.Errorf("GetElasticsearchSnapshotNamePrefix: got %q, want %q", got, "test-prefix")
	}
}
