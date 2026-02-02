package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	backuptypes "github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

const (
	// DefaultRetryAttempts is the default number of retry attempts for S3 operations
	DefaultRetryAttempts = 3
	// DefaultRetryDelay is the default delay between retry attempts
	DefaultRetryDelay = 1 * time.Second
	// MaxRetryDelay is the maximum delay between retry attempts
	MaxRetryDelay = 30 * time.Second

	// S3 path constants
	latestBackupIDFile = "latest-backup-id.txt"
	historyDir         = "history"
	incompleteDir      = "incomplete"
	orphanedDir        = "orphaned"
)

// S3Config holds configuration for S3 storage
type S3Config struct {
	Endpoint     string
	AccessKey    string
	SecretKey    string
	Bucket       string
	Prefix       string
	Region       string
	UsePathStyle bool
}

// S3Client implements the S3Storage interface with real AWS SDK operations
type S3Client struct {
	client      *s3.Client
	bucket      string
	prefix      string
	logger      *utils.Logger
	retryConfig RetryConfig
	mutex       sync.RWMutex
}

// RetryConfig holds retry configuration
type RetryConfig struct {
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// NewS3Client creates a new S3 client with the given configuration
func NewS3Client(cfg S3Config, logger *utils.Logger) (*S3Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("S3 endpoint is required")
	}
	if cfg.AccessKey == "" {
		return nil, fmt.Errorf("S3 access key is required")
	}
	if cfg.SecretKey == "" {
		return nil, fmt.Errorf("S3 secret key is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}

	// Set default region if not specified
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	// Create custom endpoint resolver for MinIO/custom S3 endpoints
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               cfg.Endpoint,
			HostnameImmutable: true,
			SigningRegion:     cfg.Region,
		}, nil
	})

	// Load AWS configuration with static credentials
	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			cfg.AccessKey,
			cfg.SecretKey,
			"",
		)),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with configurable path-style addressing
	// Path-style is required for MinIO and some S3-compatible storage systems
	// Default to true for backward compatibility with MinIO setups
	usePathStyle := cfg.UsePathStyle
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = usePathStyle
	})

	return &S3Client{
		client: client,
		bucket: cfg.Bucket,
		prefix: cfg.Prefix,
		logger: logger,
		retryConfig: RetryConfig{
			MaxAttempts:  DefaultRetryAttempts,
			InitialDelay: DefaultRetryDelay,
			MaxDelay:     MaxRetryDelay,
		},
	}, nil
}

// SetRetryConfig allows customization of retry behavior.
// This method is safe to call concurrently with S3 operations.
func (c *S3Client) SetRetryConfig(cfg RetryConfig) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.retryConfig = cfg
}

// withRetry executes an operation with retry logic
func (c *S3Client) withRetry(ctx context.Context, operation string, fn func() error) error {
	c.mutex.RLock()
	retryConfig := c.retryConfig
	c.mutex.RUnlock()

	var lastErr error
	delay := retryConfig.InitialDelay

	for attempt := 1; attempt <= retryConfig.MaxAttempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			c.logger.Warn("S3 operation '%s' failed (attempt %d/%d): %v",
				operation, attempt, c.retryConfig.MaxAttempts, err)

			if attempt < retryConfig.MaxAttempts {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
					// Exponential backoff with max delay
					delay *= 2
					if delay > retryConfig.MaxDelay {
						delay = retryConfig.MaxDelay
					}
				}
			}
		} else {
			return nil
		}
	}

	return fmt.Errorf("%s failed after %d attempts: %w", operation, retryConfig.MaxAttempts, lastErr)
}

// buildKey constructs an S3 key with the configured prefix
func (c *S3Client) buildKey(parts ...string) string {
	allParts := make([]string, 0, len(parts)+1)
	if c.prefix != "" {
		allParts = append(allParts, c.prefix)
	}
	allParts = append(allParts, parts...)
	return path.Join(allParts...)
}

// getDatePath returns a date-based path (YYYY/MM/DD) for the given time
func getDatePath(t time.Time) string {
	return t.Format("2006/01/02")
}

// StoreLatestBackupID stores the latest backup ID for a Camunda instance
func (c *S3Client) StoreLatestBackupID(camundaInstanceID, backupID string) error {
	ctx := context.Background()
	key := c.buildKey(camundaInstanceID, latestBackupIDFile)

	return c.withRetry(ctx, "StoreLatestBackupID", func() error {
		_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(c.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader([]byte(backupID)),
			ContentType: aws.String("text/plain"),
		})
		if err != nil {
			return fmt.Errorf("failed to store latest backup ID: %w", err)
		}
		c.logger.Debug("Stored latest backup ID for %s: %s", camundaInstanceID, backupID)
		return nil
	})
}

// GetLatestBackupID retrieves the latest backup ID for a Camunda instance
func (c *S3Client) GetLatestBackupID(camundaInstanceID string) (string, error) {
	ctx := context.Background()
	key := c.buildKey(camundaInstanceID, latestBackupIDFile)

	c.mutex.RLock()
	retryConfig := c.retryConfig
	c.mutex.RUnlock()

	result, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if the object doesn't exist
		var nsk *types.NoSuchKey
		if ok := isNoSuchKey(err, &nsk); ok {
			return "", utils.ErrBackupNotFound
		}
		// For other errors, apply retry logic
		var lastErr error
		for attempt := 1; attempt <= retryConfig.MaxAttempts; attempt++ {
			result, err = c.client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(c.bucket),
				Key:    aws.String(key),
			})
			if err == nil {
				break
			}
			if ok := isNoSuchKey(err, &nsk); ok {
				return "", utils.ErrBackupNotFound
			}
			lastErr = err
			c.logger.Warn("S3 operation 'GetLatestBackupID' failed (attempt %d/%d): %v",
				attempt, retryConfig.MaxAttempts, err)
			if attempt < retryConfig.MaxAttempts {
				time.Sleep(retryConfig.InitialDelay * time.Duration(1<<(attempt-1)))
			}
		}
		if err != nil {
			return "", fmt.Errorf("failed to get latest backup ID: %w", lastErr)
		}
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read latest backup ID: %w", err)
	}

	return strings.TrimSpace(string(data)), nil
}

// StoreBackupHistory stores a backup history entry
func (c *S3Client) StoreBackupHistory(history *models.BackupHistory) error {
	ctx := context.Background()

	// Determine the directory based on status
	var dir string
	switch history.Status {
	case backuptypes.BackupStatusIncomplete:
		dir = incompleteDir
	default:
		dir = historyDir
	}

	key := c.buildKey(
		history.CamundaInstanceID,
		dir,
		getDatePath(history.StartTime),
		history.BackupID+".json",
	)

	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize backup history: %w", err)
	}

	return c.withRetry(ctx, "StoreBackupHistory", func() error {
		_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(c.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			return fmt.Errorf("failed to store backup history: %w", err)
		}
		c.logger.Debug("Stored backup history for %s: %s (status: %s)",
			history.CamundaInstanceID, history.BackupID, history.Status)
		return nil
	})
}

// GetBackupHistory retrieves a backup history entry
func (c *S3Client) GetBackupHistory(camundaInstanceID, backupID string) (*models.BackupHistory, error) {
	ctx := context.Background()

	// Try to find the backup in different directories
	dirs := []string{historyDir, incompleteDir, orphanedDir}

	for _, dir := range dirs {
		history, err := c.findBackupInDir(ctx, camundaInstanceID, backupID, dir)
		if err == nil {
			return history, nil
		}
		// Continue searching if not found in this directory
	}

	return nil, utils.ErrBackupNotFound
}

// findBackupInDir searches for a backup in a specific directory
func (c *S3Client) findBackupInDir(ctx context.Context, camundaInstanceID, backupID, dir string) (*models.BackupHistory, error) {
	prefix := c.buildKey(camundaInstanceID, dir)

	// List objects with the prefix to find the backup
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix + "/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			// Check if this is the backup we're looking for
			if strings.HasSuffix(key, "/"+backupID+".json") {
				return c.getBackupHistoryByKey(ctx, key)
			}
		}
	}

	return nil, utils.ErrBackupNotFound
}

// getBackupHistoryByKey retrieves a backup history by its S3 key
func (c *S3Client) getBackupHistoryByKey(ctx context.Context, key string) (*models.BackupHistory, error) {
	result, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get backup history: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read backup history: %w", err)
	}

	var history models.BackupHistory
	if err := json.Unmarshal(data, &history); err != nil {
		return nil, fmt.Errorf("failed to deserialize backup history: %w", err)
	}

	return &history, nil
}

// ListBackupHistory lists backup history entries for a Camunda instance
func (c *S3Client) ListBackupHistory(camundaInstanceID string, status backuptypes.BackupStatus) ([]*models.BackupHistory, error) {
	ctx := context.Background()

	// Determine which directory to list based on status
	var dir string
	switch status {
	case backuptypes.BackupStatusIncomplete:
		dir = incompleteDir
	default:
		dir = historyDir
	}

	return c.listBackupsInDir(ctx, camundaInstanceID, dir, status)
}

// listBackupsInDir lists all backups in a specific directory
func (c *S3Client) listBackupsInDir(ctx context.Context, camundaInstanceID, dir string, filterStatus backuptypes.BackupStatus) ([]*models.BackupHistory, error) {
	prefix := c.buildKey(camundaInstanceID, dir)

	var backups []*models.BackupHistory

	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix + "/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			// Only process JSON files
			if !strings.HasSuffix(key, ".json") {
				continue
			}

			history, err := c.getBackupHistoryByKey(ctx, key)
			if err != nil {
				c.logger.Warn("Failed to get backup history for key %s: %v", key, err)
				continue
			}

			// Filter by status if specified
			if filterStatus == "" || history.Status == filterStatus {
				backups = append(backups, history)
			}
		}
	}

	// Sort by start time (newest first)
	sort.Slice(backups, func(i, j int) bool {
		return backups[i].StartTime.After(backups[j].StartTime)
	})

	return backups, nil
}

// UpdateBackupStatus updates the status of a backup
func (c *S3Client) UpdateBackupStatus(camundaInstanceID, backupID string, status backuptypes.BackupStatus) error {
	ctx := context.Background()

	// Get the current backup history
	history, err := c.GetBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		return err
	}

	// Delete the old entry
	if err := c.deleteBackupFromAllDirs(ctx, camundaInstanceID, backupID); err != nil {
		c.logger.Warn("Failed to delete old backup entry: %v", err)
	}

	// Update status
	history.Status = status
	now := time.Now()
	history.EndTime = &now
	duration := int(now.Sub(history.StartTime).Seconds())
	history.DurationSeconds = &duration

	// Store in the appropriate directory based on new status
	return c.StoreBackupHistory(history)
}

// deleteBackupFromAllDirs attempts to delete a backup from all directories
func (c *S3Client) deleteBackupFromAllDirs(ctx context.Context, camundaInstanceID, backupID string) error {
	dirs := []string{historyDir, incompleteDir, orphanedDir}

	for _, dir := range dirs {
		if err := c.deleteBackupFromDir(ctx, camundaInstanceID, backupID, dir); err == nil {
			return nil
		}
	}

	return utils.ErrBackupNotFound
}

// deleteBackupFromDir deletes a backup from a specific directory
func (c *S3Client) deleteBackupFromDir(ctx context.Context, camundaInstanceID, backupID, dir string) error {
	prefix := c.buildKey(camundaInstanceID, dir)

	// Find the exact key
	paginator := s3.NewListObjectsV2Paginator(c.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix + "/"),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if strings.HasSuffix(key, "/"+backupID+".json") {
				_, err := c.client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(c.bucket),
					Key:    aws.String(key),
				})
				if err != nil {
					return fmt.Errorf("failed to delete backup: %w", err)
				}
				c.logger.Debug("Deleted backup from %s: %s", dir, backupID)
				return nil
			}
		}
	}

	return utils.ErrBackupNotFound
}

// DeleteBackupHistory deletes a backup history entry
func (c *S3Client) DeleteBackupHistory(camundaInstanceID, backupID string) error {
	ctx := context.Background()
	return c.deleteBackupFromAllDirs(ctx, camundaInstanceID, backupID)
}

// MoveToOrphaned moves a backup to the orphaned directory
func (c *S3Client) MoveToOrphaned(camundaInstanceID, backupID string) error {
	ctx := context.Background()

	// Get the current backup history
	history, err := c.GetBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		return err
	}

	// Delete from current location
	if err := c.deleteBackupFromAllDirs(ctx, camundaInstanceID, backupID); err != nil {
		c.logger.Warn("Failed to delete old backup entry: %v", err)
	}

	// Store in orphaned directory
	key := c.buildKey(
		camundaInstanceID,
		orphanedDir,
		getDatePath(history.StartTime),
		history.BackupID+".json",
	)

	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize backup history: %w", err)
	}

	return c.withRetry(ctx, "MoveToOrphaned", func() error {
		_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(c.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			return fmt.Errorf("failed to move backup to orphaned: %w", err)
		}
		c.logger.Debug("Moved backup to orphaned for %s: %s", camundaInstanceID, backupID)
		return nil
	})
}

// MoveToIncomplete moves a backup to the incomplete directory
func (c *S3Client) MoveToIncomplete(camundaInstanceID, backupID string) error {
	ctx := context.Background()

	// Get the current backup history
	history, err := c.GetBackupHistory(camundaInstanceID, backupID)
	if err != nil {
		return err
	}

	// Delete from current location
	if err := c.deleteBackupFromAllDirs(ctx, camundaInstanceID, backupID); err != nil {
		c.logger.Warn("Failed to delete old backup entry: %v", err)
	}

	// Update status
	history.Status = backuptypes.BackupStatusIncomplete

	// Store in incomplete directory
	key := c.buildKey(
		camundaInstanceID,
		incompleteDir,
		getDatePath(history.StartTime),
		history.BackupID+".json",
	)

	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize backup history: %w", err)
	}

	return c.withRetry(ctx, "MoveToIncomplete", func() error {
		_, err := c.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(c.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			return fmt.Errorf("failed to move backup to incomplete: %w", err)
		}
		c.logger.Debug("Moved backup to incomplete for %s: %s", camundaInstanceID, backupID)
		return nil
	})
}

// ListOrphanedBackups lists all orphaned backups for a Camunda instance
func (c *S3Client) ListOrphanedBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	ctx := context.Background()
	return c.listBackupsInDir(ctx, camundaInstanceID, orphanedDir, "")
}

// ListIncompleteBackups lists all incomplete backups for a Camunda instance
func (c *S3Client) ListIncompleteBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	ctx := context.Background()
	return c.listBackupsInDir(ctx, camundaInstanceID, incompleteDir, "")
}

// ListAllBackups lists all backups regardless of status
func (c *S3Client) ListAllBackups(camundaInstanceID string) ([]*models.BackupHistory, error) {
	ctx := context.Background()

	var allBackups []*models.BackupHistory

	// List from all directories
	dirs := []string{historyDir, incompleteDir, orphanedDir}
	for _, dir := range dirs {
		backups, err := c.listBackupsInDir(ctx, camundaInstanceID, dir, "")
		if err != nil {
			c.logger.Warn("Failed to list backups from %s: %v", dir, err)
			continue
		}
		allBackups = append(allBackups, backups...)
	}

	// Sort by start time (newest first)
	sort.Slice(allBackups, func(i, j int) bool {
		return allBackups[i].StartTime.After(allBackups[j].StartTime)
	})

	return allBackups, nil
}

// HealthCheck verifies S3 connectivity
func (c *S3Client) HealthCheck() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		return fmt.Errorf("S3 health check failed: %w", err)
	}

	return nil
}

// isNoSuchKey checks if the error is a NoSuchKey error
func isNoSuchKey(err error, target **types.NoSuchKey) bool {
	if err == nil {
		return false
	}
	// Check for both NoSuchKey and general "not found" errors
	if strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "404") {
		return true
	}
	return false
}
