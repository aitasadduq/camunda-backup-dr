package config

import (
	"os"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// ---------------------------------------------------------------------------
// getEnv / getEnvAsInt (unexported helpers)
// ---------------------------------------------------------------------------

func TestGetEnv(t *testing.T) {
	t.Run("returns env value when set", func(t *testing.T) {
		t.Setenv("TEST_GET_ENV_KEY", "custom-value")
		if got := getEnv("TEST_GET_ENV_KEY", "default"); got != "custom-value" {
			t.Errorf("getEnv() = %q, want %q", got, "custom-value")
		}
	})

	t.Run("returns default when env not set", func(t *testing.T) {
		// Ensure the key doesn't exist
		os.Unsetenv("TEST_GET_ENV_MISSING")
		if got := getEnv("TEST_GET_ENV_MISSING", "fallback"); got != "fallback" {
			t.Errorf("getEnv() = %q, want %q", got, "fallback")
		}
	})

	t.Run("returns default when env is empty string", func(t *testing.T) {
		t.Setenv("TEST_GET_ENV_EMPTY", "")
		if got := getEnv("TEST_GET_ENV_EMPTY", "default-val"); got != "default-val" {
			t.Errorf("getEnv() = %q, want %q", got, "default-val")
		}
	})
}

func TestGetEnvAsInt(t *testing.T) {
	t.Run("returns parsed int when valid", func(t *testing.T) {
		t.Setenv("TEST_INT_KEY", "42")
		if got := getEnvAsInt("TEST_INT_KEY", 0); got != 42 {
			t.Errorf("getEnvAsInt() = %d, want 42", got)
		}
	})

	t.Run("returns default when env not set", func(t *testing.T) {
		os.Unsetenv("TEST_INT_MISSING")
		if got := getEnvAsInt("TEST_INT_MISSING", 99); got != 99 {
			t.Errorf("getEnvAsInt() = %d, want 99", got)
		}
	})

	t.Run("returns default when env is not a valid int", func(t *testing.T) {
		t.Setenv("TEST_INT_BAD", "not-a-number")
		if got := getEnvAsInt("TEST_INT_BAD", 55); got != 55 {
			t.Errorf("getEnvAsInt() = %d, want 55", got)
		}
	})

	t.Run("returns default when env is empty string", func(t *testing.T) {
		t.Setenv("TEST_INT_EMPTY", "")
		if got := getEnvAsInt("TEST_INT_EMPTY", 10); got != 10 {
			t.Errorf("getEnvAsInt() = %d, want 10", got)
		}
	})

	t.Run("handles negative int", func(t *testing.T) {
		t.Setenv("TEST_INT_NEG", "-5")
		if got := getEnvAsInt("TEST_INT_NEG", 0); got != -5 {
			t.Errorf("getEnvAsInt() = %d, want -5", got)
		}
	})
}

// ---------------------------------------------------------------------------
// Load()
// ---------------------------------------------------------------------------

// clearLoadEnvVars unsets all env vars that Load() reads so tests get defaults.
func clearLoadEnvVars(t *testing.T) {
	t.Helper()
	keys := []string{
		"PORT", "LOG_LEVEL", "DATA_DIR",
		"DEFAULT_SCHEDULE", "DEFAULT_SUCCESS_RETENTION",
		"DEFAULT_FAILURE_RETENTION",
		"DEFAULT_BACKUP_POLL_INTERVAL", "DEFAULT_BACKUP_MAX_ATTEMPTS",
		"DEFAULT_ELASTICSEARCH_ENDPOINT", "DEFAULT_ELASTICSEARCH_USERNAME",
		"DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY", "DEFAULT_ELASTICSEARCH_SNAPSHOT_NAME_PREFIX",
		"DEFAULT_S3_ENDPOINT", "DEFAULT_S3_ACCESSKEY", "DEFAULT_S3_SECRETKEY",
		"DEFAULT_ELASTICSEARCH_PASSWORD",
		"ALERT_WEBHOOK_URL", "BACKUP_STUCK_TIMEOUT_MINUTES",
	}
	for _, k := range keys {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}
}

func TestLoad_Defaults(t *testing.T) {
	clearLoadEnvVars(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	// Service Configuration defaults
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080", cfg.Port)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, "info")
	}
	if cfg.DataDir != "/data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/data")
	}

	// Schedule & retention defaults
	if cfg.DefaultSchedule != "0 2 * * *" {
		t.Errorf("DefaultSchedule = %q, want %q", cfg.DefaultSchedule, "0 2 * * *")
	}
	if cfg.DefaultSuccessRetention != 7 {
		t.Errorf("DefaultSuccessRetention = %d, want 7", cfg.DefaultSuccessRetention)
	}
	if cfg.DefaultFailureRetention != 7 {
		t.Errorf("DefaultFailureRetention = %d, want 7", cfg.DefaultFailureRetention)
	}

	// Polling defaults
	if cfg.DefaultBackupPollInterval != 5 {
		t.Errorf("DefaultBackupPollInterval = %d, want 5", cfg.DefaultBackupPollInterval)
	}
	if cfg.DefaultBackupMaxAttempts != 120 {
		t.Errorf("DefaultBackupMaxAttempts = %d, want 120", cfg.DefaultBackupMaxAttempts)
	}

	// Elasticsearch defaults
	if cfg.DefaultElasticsearchEndpoint != "" {
		t.Errorf("DefaultElasticsearchEndpoint = %q, want empty", cfg.DefaultElasticsearchEndpoint)
	}
	if cfg.DefaultElasticsearchUsername != "" {
		t.Errorf("DefaultElasticsearchUsername = %q, want empty", cfg.DefaultElasticsearchUsername)
	}
	if cfg.DefaultElasticsearchSnapshotRepository != "camunda-backup" {
		t.Errorf("DefaultElasticsearchSnapshotRepository = %q, want %q", cfg.DefaultElasticsearchSnapshotRepository, "camunda-backup")
	}
	if cfg.DefaultElasticsearchSnapshotNamePrefix != "" {
		t.Errorf("DefaultElasticsearchSnapshotNamePrefix = %q, want empty", cfg.DefaultElasticsearchSnapshotNamePrefix)
	}

	// S3 defaults
	if cfg.DefaultS3Endpoint != "" {
		t.Errorf("DefaultS3Endpoint = %q, want empty", cfg.DefaultS3Endpoint)
	}
	if cfg.DefaultS3AccessKey != "" {
		t.Errorf("DefaultS3AccessKey = %q, want empty", cfg.DefaultS3AccessKey)
	}
	if cfg.DefaultS3SecretKey != "" {
		t.Errorf("DefaultS3SecretKey = %q, want empty", cfg.DefaultS3SecretKey)
	}
	if cfg.DefaultElasticsearchPassword != "" {
		t.Errorf("DefaultElasticsearchPassword = %q, want empty", cfg.DefaultElasticsearchPassword)
	}

	// Alert
	if cfg.AlertWebhookURL != "" {
		t.Errorf("AlertWebhookURL = %q, want empty", cfg.AlertWebhookURL)
	}

	// Stuck timeout
	if cfg.BackupStuckTimeoutMinutes != 120 {
		t.Errorf("BackupStuckTimeoutMinutes = %d, want 120", cfg.BackupStuckTimeoutMinutes)
	}
}

func TestLoad_EnvVarOverrides(t *testing.T) {
	clearLoadEnvVars(t)

	t.Setenv("PORT", "9090")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("DATA_DIR", "/custom/data")
	t.Setenv("DEFAULT_SCHEDULE", "0 0 * * *")
	t.Setenv("DEFAULT_SUCCESS_RETENTION", "14")
	t.Setenv("DEFAULT_FAILURE_RETENTION", "14")
	t.Setenv("DEFAULT_BACKUP_POLL_INTERVAL", "10")
	t.Setenv("DEFAULT_BACKUP_MAX_ATTEMPTS", "240")
	t.Setenv("DEFAULT_ELASTICSEARCH_ENDPOINT", "http://es:9200")
	t.Setenv("DEFAULT_ELASTICSEARCH_USERNAME", "admin")
	t.Setenv("DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY", "my-repo")
	t.Setenv("DEFAULT_ELASTICSEARCH_SNAPSHOT_NAME_PREFIX", "snap-")
	t.Setenv("DEFAULT_S3_ENDPOINT", "http://s3:9000")
	t.Setenv("DEFAULT_S3_ACCESSKEY", "AKID")
	t.Setenv("DEFAULT_S3_SECRETKEY", "s3-global-secret")
	t.Setenv("DEFAULT_ELASTICSEARCH_PASSWORD", "es-global-pass")
	t.Setenv("ALERT_WEBHOOK_URL", "https://hooks.example.com/alert")
	t.Setenv("BACKUP_STUCK_TIMEOUT_MINUTES", "60")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}

	checks := []struct {
		name string
		got  interface{}
		want interface{}
	}{
		{"Port", cfg.Port, 9090},
		{"LogLevel", cfg.LogLevel, "debug"},
		{"DataDir", cfg.DataDir, "/custom/data"},
		{"DefaultSchedule", cfg.DefaultSchedule, "0 0 * * *"},
		{"DefaultSuccessRetention", cfg.DefaultSuccessRetention, 14},
		{"DefaultFailureRetention", cfg.DefaultFailureRetention, 14},
		{"DefaultBackupPollInterval", cfg.DefaultBackupPollInterval, 10},
		{"DefaultBackupMaxAttempts", cfg.DefaultBackupMaxAttempts, 240},
		{"DefaultElasticsearchEndpoint", cfg.DefaultElasticsearchEndpoint, "http://es:9200"},
		{"DefaultElasticsearchUsername", cfg.DefaultElasticsearchUsername, "admin"},
		{"DefaultElasticsearchSnapshotRepository", cfg.DefaultElasticsearchSnapshotRepository, "my-repo"},
		{"DefaultElasticsearchSnapshotNamePrefix", cfg.DefaultElasticsearchSnapshotNamePrefix, "snap-"},
		{"DefaultS3Endpoint", cfg.DefaultS3Endpoint, "http://s3:9000"},
		{"DefaultS3AccessKey", cfg.DefaultS3AccessKey, "AKID"},
		{"DefaultS3SecretKey", cfg.DefaultS3SecretKey, "s3-global-secret"},
		{"DefaultElasticsearchPassword", cfg.DefaultElasticsearchPassword, "es-global-pass"},
		{"AlertWebhookURL", cfg.AlertWebhookURL, "https://hooks.example.com/alert"},
		{"BackupStuckTimeoutMinutes", cfg.BackupStuckTimeoutMinutes, 60},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoad_ValidationFailure(t *testing.T) {
	clearLoadEnvVars(t)
	// Invalid port triggers Validate() error inside Load()
	t.Setenv("PORT", "0")

	_, err := Load()
	if err == nil {
		t.Fatal("Load() expected error for invalid port, got nil")
	}
	if err != utils.ErrInvalidConfiguration {
		t.Errorf("Load() error = %v, want %v", err, utils.ErrInvalidConfiguration)
	}
}

func TestLoad_InvalidIntFallsBackToDefault(t *testing.T) {
	clearLoadEnvVars(t)
	// Non-numeric PORT falls back to default 8080 which is valid
	t.Setenv("PORT", "not-a-port")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080 (fallback default)", cfg.Port)
	}
}

// ---------------------------------------------------------------------------
// Validate()
// ---------------------------------------------------------------------------

func validConfig() *Config {
	return &Config{
		Port:                      8080,
		LogLevel:                  "info",
		DataDir:                   "/data",
		DefaultSchedule:           "0 2 * * *",
		DefaultSuccessRetention: 7,
		DefaultFailureRetention: 7,
		DefaultBackupPollInterval: 5,
		DefaultBackupMaxAttempts:  120,
		BackupStuckTimeoutMinutes: 120,
	}
}

func TestValidate(t *testing.T) {
	t.Run("valid config passes", func(t *testing.T) {
		if err := validConfig().Validate(); err != nil {
			t.Errorf("Validate() unexpected error: %v", err)
		}
	})

	// Port boundary checks
	t.Run("port 0 invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Port = 0
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
	t.Run("port negative invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Port = -1
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
	t.Run("port 65536 invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Port = 65536
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
	t.Run("port 65535 valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Port = 65535
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error: %v", err)
		}
	})
	t.Run("port 1 valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.Port = 1
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error: %v", err)
		}
	})

	// Log level checks
	for _, lvl := range []string{"debug", "info", "warn", "error"} {
		t.Run("log level "+lvl+" valid", func(t *testing.T) {
			cfg := validConfig()
			cfg.LogLevel = lvl
			if err := cfg.Validate(); err != nil {
				t.Errorf("Validate() unexpected error for log level %q: %v", lvl, err)
			}
		})
	}
	t.Run("log level invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.LogLevel = "trace"
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
	t.Run("log level empty invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.LogLevel = ""
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})

	// Negative success retention
	t.Run("negative success retention invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DefaultSuccessRetention = -1
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
	t.Run("zero success retention valid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DefaultSuccessRetention = 0
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() unexpected error: %v", err)
		}
	})

	// Negative failure retention
	t.Run("negative failure retention invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DefaultFailureRetention = -1
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})

	// Poll interval
	t.Run("zero poll interval invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DefaultBackupPollInterval = 0
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
	t.Run("negative poll interval invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DefaultBackupPollInterval = -1
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})

	// Max attempts
	t.Run("zero max attempts invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DefaultBackupMaxAttempts = 0
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
	t.Run("negative max attempts invalid", func(t *testing.T) {
		cfg := validConfig()
		cfg.DefaultBackupMaxAttempts = -1
		if err := cfg.Validate(); err != utils.ErrInvalidConfiguration {
			t.Errorf("Validate() error = %v, want %v", err, utils.ErrInvalidConfiguration)
		}
	})
}

// ---------------------------------------------------------------------------
// GetDefaultComponents()
// ---------------------------------------------------------------------------

func TestGetDefaultComponents(t *testing.T) {
	components := GetDefaultComponents()

	if len(components) != 5 {
		t.Fatalf("GetDefaultComponents() returned %d components, want 5", len(components))
	}

	expected := []struct {
		name    string
		enabled bool
	}{
		{types.ComponentZeebe, true},
		{types.ComponentOperate, true},
		{types.ComponentTasklist, true},
		{types.ComponentOptimize, false},
		{types.ComponentElasticsearch, true},
	}

	for i, exp := range expected {
		name, ok := components[i]["name"].(string)
		if !ok {
			t.Fatalf("components[%d][\"name\"] is not a string", i)
		}
		if name != exp.name {
			t.Errorf("components[%d] name = %q, want %q", i, name, exp.name)
		}

		enabled, ok := components[i]["enabled"].(bool)
		if !ok {
			t.Fatalf("components[%d][\"enabled\"] is not a bool", i)
		}
		if enabled != exp.enabled {
			t.Errorf("components[%d] enabled = %v, want %v", i, enabled, exp.enabled)
		}
	}
}

// ---------------------------------------------------------------------------
// ValidateComponent()
// ---------------------------------------------------------------------------

func TestValidateComponent(t *testing.T) {
	// All valid component names should pass
	for _, name := range types.ValidComponents {
		t.Run("valid component "+name, func(t *testing.T) {
			comp := map[string]interface{}{"name": name, "enabled": true}
			if err := ValidateComponent(comp); err != nil {
				t.Errorf("ValidateComponent(%q) unexpected error: %v", name, err)
			}
		})
	}

	t.Run("invalid component name", func(t *testing.T) {
		comp := map[string]interface{}{"name": "unknown-component", "enabled": true}
		if err := ValidateComponent(comp); err != utils.ErrInvalidComponent {
			t.Errorf("ValidateComponent() error = %v, want %v", err, utils.ErrInvalidComponent)
		}
	})

	t.Run("missing name key", func(t *testing.T) {
		comp := map[string]interface{}{"enabled": true}
		if err := ValidateComponent(comp); err != utils.ErrInvalidComponent {
			t.Errorf("ValidateComponent() error = %v, want %v", err, utils.ErrInvalidComponent)
		}
	})

	t.Run("name is not a string", func(t *testing.T) {
		comp := map[string]interface{}{"name": 123, "enabled": true}
		if err := ValidateComponent(comp); err != utils.ErrInvalidComponent {
			t.Errorf("ValidateComponent() error = %v, want %v", err, utils.ErrInvalidComponent)
		}
	})

	t.Run("empty name string", func(t *testing.T) {
		comp := map[string]interface{}{"name": "", "enabled": true}
		if err := ValidateComponent(comp); err != utils.ErrInvalidComponent {
			t.Errorf("ValidateComponent() error = %v, want %v", err, utils.ErrInvalidComponent)
		}
	})

	t.Run("empty map", func(t *testing.T) {
		comp := map[string]interface{}{}
		if err := ValidateComponent(comp); err != utils.ErrInvalidComponent {
			t.Errorf("ValidateComponent() error = %v, want %v", err, utils.ErrInvalidComponent)
		}
	})
}

// ---------------------------------------------------------------------------
// NormalizeForEnvVar (existing)
// ---------------------------------------------------------------------------

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
			name:       "missing env var falls back to default password",
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

	// Test fallback to DefaultElasticsearchPassword
	t.Run("falls back to DefaultElasticsearchPassword when no instance var", func(t *testing.T) {
		cfgWithDefault := &Config{DefaultElasticsearchPassword: "global-default-pw"}
		result := cfgWithDefault.GetElasticsearchPassword("no-env-set")
		if result != "global-default-pw" {
			t.Errorf("GetElasticsearchPassword fallback = %q, want %q", result, "global-default-pw")
		}
	})

	t.Run("instance-specific var takes precedence over default", func(t *testing.T) {
		cfgWithDefault := &Config{DefaultElasticsearchPassword: "global-default-pw"}
		setEnvForTest(t, "ELASTICSEARCH_PASSWORD_OVERRIDE_TEST", "instance-specific-pw")
		result := cfgWithDefault.GetElasticsearchPassword("override-test")
		if result != "instance-specific-pw" {
			t.Errorf("GetElasticsearchPassword precedence = %q, want %q", result, "instance-specific-pw")
		}
	})
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
			name:       "missing env var falls back to default secret key",
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

	t.Run("falls back to DefaultS3SecretKey when no instance var", func(t *testing.T) {
		cfgWithDefault := &Config{DefaultS3SecretKey: "global-s3-secret"}
		result := cfgWithDefault.GetS3SecretKey("no-env-set")
		if result != "global-s3-secret" {
			t.Errorf("GetS3SecretKey fallback = %q, want %q", result, "global-s3-secret")
		}
	})

	t.Run("instance-specific var takes precedence over default", func(t *testing.T) {
		cfgWithDefault := &Config{DefaultS3SecretKey: "global-s3-secret"}
		setEnvForTest(t, "S3_SECRETKEY_OVERRIDE_TEST", "instance-specific-secret")
		result := cfgWithDefault.GetS3SecretKey("override-test")
		if result != "instance-specific-secret" {
			t.Errorf("GetS3SecretKey precedence = %q, want %q", result, "instance-specific-secret")
		}
	})
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
