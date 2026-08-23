package config

import (
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Config holds the application configuration
type Config struct {
	// Service Configuration
	Port     int
	LogLevel string
	DataDir  string

	// Default Configuration
	DefaultSchedule       string
	DefaultSuccessRetention int
	DefaultFailureRetention int

	// Backup Polling Configuration
	DefaultBackupPollInterval int // in seconds
	DefaultBackupMaxAttempts  int

	// Default Elasticsearch
	DefaultElasticsearchEndpoint           string
	DefaultElasticsearchUsername           string
	DefaultElasticsearchSnapshotRepository string
	DefaultElasticsearchSnapshotNamePrefix string

	// Default S3
	DefaultS3Endpoint     string
	DefaultS3AccessKey    string
	DefaultS3SecretKey    string
	DefaultS3Bucket       string
	DefaultS3Region       string
	DefaultS3Prefix       string
	DefaultS3UsePathStyle bool

	// Default Elasticsearch Password (falls back when no instance-specific var is set)
	DefaultElasticsearchPassword string

	// Reconciliation (orphaned backup detection)
	ReconcileEnabled            bool
	ReconcileTimeoutSeconds     int
	ReconcileGracePeriodMinutes int
	ReconcileStaleAfterMinutes  int

	// Alert Configuration
	AlertWebhookURL          string
	AlertEnableBackupFailed  bool
	AlertEnableCleanupFailed bool
	AlertEnableStuckBackup   bool
	AlertEnableCircuitOpen   bool
	AlertEnableSchedulerError       bool
	AlertEnableExporterResumeFailed bool

	// Backup Stuck Detection
	BackupStuckTimeoutMinutes int // 0 = disabled

	// Exporter Pause/Resume Configuration
	ExporterPauseMaxRetries int // Maximum retries for pause/resume requests
	ExporterPauseRetryDelay int // Delay between retries in seconds

	// Base Path for serving behind a reverse proxy (e.g. "/backup")
	BasePath string

	// secretProvider resolves credentials entered through the UI. Optional.
	secretProvider SecretProvider
}

// SecretProvider resolves per-instance credentials that were entered through
// the UI rather than supplied as environment variables.
type SecretProvider interface {
	ElasticsearchPassword(camundaInstanceID string) string
	S3SecretKey(camundaInstanceID string) string
}

// SetSecretProvider registers the provider consulted by GetElasticsearchPassword
// and GetS3SecretKey when no instance-specific environment variable is set.
func (c *Config) SetSecretProvider(p SecretProvider) {
	c.secretProvider = p
}

// Load loads configuration from environment variables with defaults
func Load() (*Config, error) {
	cfg := &Config{
		// Service Configuration
		Port:     getEnvAsInt("PORT", 8080),
		LogLevel: getEnv("LOG_LEVEL", "info"),
		DataDir:  getEnv("DATA_DIR", "/data"),

		// Defaults
		DefaultSchedule:       getEnv("DEFAULT_SCHEDULE", "0 2 * * *"),
		DefaultSuccessRetention: getEnvAsInt("DEFAULT_SUCCESS_RETENTION", 7),
		DefaultFailureRetention: getEnvAsInt("DEFAULT_FAILURE_RETENTION", 7),

		// Backup Polling Configuration
		DefaultBackupPollInterval: getEnvAsInt("DEFAULT_BACKUP_POLL_INTERVAL", 5),
		DefaultBackupMaxAttempts:  getEnvAsInt("DEFAULT_BACKUP_MAX_ATTEMPTS", 120),

		// Default Elasticsearch
		DefaultElasticsearchEndpoint:           getEnv("DEFAULT_ELASTICSEARCH_ENDPOINT", ""),
		DefaultElasticsearchUsername:           getEnv("DEFAULT_ELASTICSEARCH_USERNAME", ""),
		DefaultElasticsearchSnapshotRepository: getEnv("DEFAULT_ELASTICSEARCH_SNAPSHOT_REPOSITORY", "camunda-backup"),
		DefaultElasticsearchSnapshotNamePrefix: getEnv("DEFAULT_ELASTICSEARCH_SNAPSHOT_NAME_PREFIX", ""),

		// Default S3
		DefaultS3Endpoint:     getEnv("DEFAULT_S3_ENDPOINT", ""),
		DefaultS3AccessKey:    getEnv("DEFAULT_S3_ACCESSKEY", ""),
		DefaultS3SecretKey:    getEnv("DEFAULT_S3_SECRETKEY", ""),
		DefaultS3Bucket:       getEnv("DEFAULT_S3_BUCKET", "camunda-backups"),
		DefaultS3Region:       getEnv("DEFAULT_S3_REGION", "us-east-1"),
		DefaultS3Prefix:       getEnv("DEFAULT_S3_PREFIX", ""),
		DefaultS3UsePathStyle: getEnv("DEFAULT_S3_USE_PATH_STYLE", "true") == "true",

		// Default Elasticsearch Password
		DefaultElasticsearchPassword: getEnv("DEFAULT_ELASTICSEARCH_PASSWORD", ""),

		// Reconciliation. The grace period must comfortably exceed a backup's
		// polling window, or a sweep fired right after a backup would report
		// artifacts that are still being written.
		ReconcileEnabled:            getEnv("RECONCILE_ENABLED", "true") == "true",
		ReconcileTimeoutSeconds:     getEnvAsInt("RECONCILE_TIMEOUT_SECONDS", 120),
		ReconcileGracePeriodMinutes: getEnvAsInt("RECONCILE_GRACE_PERIOD_MINUTES", 15),
		ReconcileStaleAfterMinutes:  getEnvAsInt("RECONCILE_STALE_AFTER_MINUTES", 30),

		// Alert Configuration
		AlertWebhookURL:           getEnv("ALERT_WEBHOOK_URL", ""),
		AlertEnableBackupFailed:   getEnv("ALERT_ENABLE_BACKUP_FAILED", "true") == "true",
		AlertEnableCleanupFailed:  getEnv("ALERT_ENABLE_CLEANUP_FAILED", "true") == "true",
		AlertEnableStuckBackup:    getEnv("ALERT_ENABLE_STUCK_BACKUP", "true") == "true",
		AlertEnableCircuitOpen:    getEnv("ALERT_ENABLE_CIRCUIT_OPEN", "true") == "true",
		AlertEnableSchedulerError:       getEnv("ALERT_ENABLE_SCHEDULER_ERROR", "true") == "true",
		AlertEnableExporterResumeFailed: getEnv("ALERT_ENABLE_EXPORTER_RESUME_FAILED", "true") == "true",

		// Backup Stuck Detection (default: 120 minutes = 2 hours)
		BackupStuckTimeoutMinutes: getEnvAsInt("BACKUP_STUCK_TIMEOUT_MINUTES", 120),

		// Exporter Pause/Resume
		ExporterPauseMaxRetries: getEnvAsInt("EXPORTER_PAUSE_MAX_RETRIES", 5),
		ExporterPauseRetryDelay: getEnvAsInt("EXPORTER_PAUSE_RETRY_DELAY", 3),

		// Base Path
		BasePath: normalizeBasePath(getEnv("BASE_PATH", "/")),
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Port <= 0 || c.Port > 65535 {
		return utils.ErrInvalidConfiguration
	}

	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[c.LogLevel] {
		return utils.ErrInvalidConfiguration
	}

	if c.DefaultSuccessRetention < 0 {
		return utils.ErrInvalidConfiguration
	}

	if c.DefaultFailureRetention < 0 {
		return utils.ErrInvalidConfiguration
	}

	if c.DefaultBackupPollInterval <= 0 {
		return utils.ErrInvalidConfiguration
	}

	if c.DefaultBackupMaxAttempts <= 0 {
		return utils.ErrInvalidConfiguration
	}

	if c.ExporterPauseMaxRetries < 0 {
		return utils.ErrInvalidConfiguration
	}

	if c.ExporterPauseRetryDelay < 0 {
		return utils.ErrInvalidConfiguration
	}

	if !validBasePath(c.BasePath) {
		return utils.ErrInvalidConfiguration
	}

	return nil
}

var basePathPattern = regexp.MustCompile(`^(/[a-zA-Z0-9_-]+)+$`)

// validBasePath returns true for "/" or any path matching /seg1/seg2/...
// with only alphanumeric, hyphen, and underscore segments.
func validBasePath(p string) bool {
	return p == "" || p == "/" || basePathPattern.MatchString(p)
}

// getEnv retrieves an environment variable or returns a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvAsInt retrieves an environment variable as an integer or returns a default value
func getEnvAsInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// normalizeBasePath ensures the base path starts with "/" and has no trailing slash.
// Examples: "" -> "/", "/" -> "/", "/backup" -> "/backup", "/backup/" -> "/backup"
func normalizeBasePath(p string) string {
	if p == "" || p == "/" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimRight(p, "/")
}

// NormalizeForEnvVar converts a Camunda instance ID into a valid environment
// variable suffix: uppercase and hyphens replaced with underscores.
// Example: "my-cluster" -> "MY_CLUSTER"
func NormalizeForEnvVar(camundaInstanceID string) string {
	return strings.ToUpper(strings.ReplaceAll(camundaInstanceID, "-", "_"))
}

// GetElasticsearchPassword retrieves Elasticsearch password for a specific Camunda instance.
// Priority: instance-specific env var > UI-stored secret > DefaultElasticsearchPassword.
func (c *Config) GetElasticsearchPassword(camundaInstanceID string) string {
	if pw := os.Getenv("ELASTICSEARCH_PASSWORD_" + NormalizeForEnvVar(camundaInstanceID)); pw != "" {
		return pw
	}
	if c.secretProvider != nil {
		if pw := c.secretProvider.ElasticsearchPassword(camundaInstanceID); pw != "" {
			return pw
		}
	}
	return c.DefaultElasticsearchPassword
}

// GetElasticsearchSnapshotRepository retrieves the snapshot repository name for a Camunda instance.
// Priority: instance-specific env var > instanceValue (UI-configured) > global default.
func (c *Config) GetElasticsearchSnapshotRepository(camundaInstanceID string, instanceValue string) string {
	if repo := os.Getenv("ELASTICSEARCH_SNAPSHOT_REPOSITORY_" + NormalizeForEnvVar(camundaInstanceID)); repo != "" {
		return repo
	}
	if instanceValue != "" {
		return instanceValue
	}
	return c.DefaultElasticsearchSnapshotRepository
}

// GetElasticsearchSnapshotNamePrefix retrieves the snapshot name prefix for a Camunda instance.
// First checks for instance-specific env var, then falls back to default.
func (c *Config) GetElasticsearchSnapshotNamePrefix(camundaInstanceID string) string {
	if prefix := os.Getenv("ELASTICSEARCH_SNAPSHOT_NAME_PREFIX_" + NormalizeForEnvVar(camundaInstanceID)); prefix != "" {
		return prefix
	}
	return c.DefaultElasticsearchSnapshotNamePrefix
}

// GetS3SecretKey retrieves S3 secret key for a specific Camunda instance.
// Priority: instance-specific env var > UI-stored secret > DefaultS3SecretKey.
func (c *Config) GetS3SecretKey(camundaInstanceID string) string {
	if sk := os.Getenv("S3_SECRETKEY_" + NormalizeForEnvVar(camundaInstanceID)); sk != "" {
		return sk
	}
	if c.secretProvider != nil {
		if sk := c.secretProvider.S3SecretKey(camundaInstanceID); sk != "" {
			return sk
		}
	}
	return c.DefaultS3SecretKey
}
