package config

import (
	"os"
	"strconv"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Config holds the application configuration
type Config struct {
	// Service Configuration
	Port     int
	LogLevel string
	DataDir  string

	// Default Configuration
	DefaultSchedule         string
	DefaultRetentionCount   int
	DefaultSuccessHistory   int
	DefaultFailureHistory   int

	// Default Elasticsearch
	DefaultElasticsearchEndpoint  string
	DefaultElasticsearchUsername  string

	// Default S3
	DefaultS3Endpoint string
	DefaultS3AccessKey string
}

// Load loads configuration from environment variables with defaults
func Load() (*Config, error) {
	cfg := &Config{
		// Service Configuration
		Port:     getEnvAsInt("PORT", 8080),
		LogLevel: getEnv("LOG_LEVEL", "info"),
		DataDir:  getEnv("DATA_DIR", "/data"),

		// Defaults
		DefaultSchedule:         getEnv("DEFAULT_SCHEDULE", "0 2 * * *"),
		DefaultRetentionCount:   getEnvAsInt("DEFAULT_RETENTION_COUNT", 7),
		DefaultSuccessHistory:   getEnvAsInt("DEFAULT_SUCCESS_HISTORY", 30),
		DefaultFailureHistory:   getEnvAsInt("DEFAULT_FAILURE_HISTORY", 30),

		// Default Elasticsearch
		DefaultElasticsearchEndpoint: getEnv("DEFAULT_ELASTICSEARCH_ENDPOINT", ""),
		DefaultElasticsearchUsername: getEnv("DEFAULT_ELASTICSEARCH_USERNAME", ""),

		// Default S3
		DefaultS3Endpoint: getEnv("DEFAULT_S3_ENDPOINT", ""),
		DefaultS3AccessKey: getEnv("DEFAULT_S3_ACCESSKEY", ""),
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

	if c.DefaultRetentionCount < 0 {
		return utils.ErrInvalidConfiguration
	}

	if c.DefaultSuccessHistory < 0 {
		return utils.ErrInvalidConfiguration
	}

	if c.DefaultFailureHistory < 0 {
		return utils.ErrInvalidConfiguration
	}

	return nil
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

// GetElasticsearchPassword retrieves Elasticsearch password for a specific Camunda instance
func (c *Config) GetElasticsearchPassword(camundaInstanceID string) string {
	return os.Getenv("ELASTICSEARCH_PASSWORD_" + camundaInstanceID)
}

// GetS3SecretKey retrieves S3 secret key for a specific Camunda instance
func (c *Config) GetS3SecretKey(camundaInstanceID string) string {
	return os.Getenv("S3_SECRETKEY_" + camundaInstanceID)
}