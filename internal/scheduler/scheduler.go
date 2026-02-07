package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// BackupExecutor defines the interface for executing backups
type BackupExecutor interface {
	ExecuteScheduledBackup(ctx context.Context, instance *models.CamundaInstance) error
}

// InstanceProvider defines the interface for retrieving Camunda instances
type InstanceProvider interface {
	GetInstance(id string) (*models.CamundaInstance, error)
	ListInstances() ([]models.CamundaInstance, error)
}

// Job represents a scheduled backup job
type Job struct {
	ID                string
	CamundaInstanceID string
	Schedule          string // Cron expression
	Enabled           bool
	LastRun           *time.Time
	NextRun           *time.Time
	Running           bool
}

// Scheduler manages cron-based backup scheduling for Camunda instances
type Scheduler struct {
	executor         BackupExecutor
	instanceProvider InstanceProvider
	logger           *utils.Logger

	// Job management
	jobs      map[string]*Job // keyed by CamundaInstanceID
	jobsMutex sync.RWMutex

	// Concurrency control - prevents concurrent backups (scheduled or manual)
	backupMutex  sync.Mutex
	activeBackup *string // Currently running backup instance ID, nil if none

	// Scheduler state
	running      bool
	runningMutex sync.RWMutex
	stopChan     chan struct{}
	wg           sync.WaitGroup

	// Configuration
	tickInterval    time.Duration // How often to check for due jobs
	shutdownTimeout time.Duration // Max time to wait for graceful shutdown
}

// Config holds scheduler configuration
type Config struct {
	TickInterval    time.Duration
	ShutdownTimeout time.Duration
}

// DefaultConfig returns default scheduler configuration
func DefaultConfig() Config {
	return Config{
		TickInterval:    time.Minute,
		ShutdownTimeout: 5 * time.Minute,
	}
}

// NewScheduler creates a new scheduler instance
func NewScheduler(
	executor BackupExecutor,
	instanceProvider InstanceProvider,
	logger *utils.Logger,
	cfg Config,
) *Scheduler {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = time.Minute
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 5 * time.Minute
	}

	return &Scheduler{
		executor:         executor,
		instanceProvider: instanceProvider,
		logger:           logger,
		jobs:             make(map[string]*Job),
		stopChan:         make(chan struct{}),
		tickInterval:     cfg.TickInterval,
		shutdownTimeout:  cfg.ShutdownTimeout,
	}
}

// Start begins the scheduler's main loop
func (s *Scheduler) Start(ctx context.Context) error {
	s.runningMutex.Lock()
	if s.running {
		s.runningMutex.Unlock()
		return fmt.Errorf("scheduler is already running")
	}
	s.running = true
	s.stopChan = make(chan struct{})
	s.runningMutex.Unlock()

	s.logger.Info("Scheduler starting...")

	// Load initial jobs from configured instances
	if err := s.loadJobsFromInstances(); err != nil {
		s.logger.Warn("Failed to load initial jobs: %v", err)
	}

	// Start the main scheduling loop
	s.wg.Add(1)
	go s.run(ctx)

	s.logger.Info("Scheduler started successfully")
	return nil
}

// Stop gracefully stops the scheduler
func (s *Scheduler) Stop(ctx context.Context) error {
	s.runningMutex.Lock()
	if !s.running {
		s.runningMutex.Unlock()
		return nil
	}
	s.running = false
	s.runningMutex.Unlock()

	s.logger.Info("Scheduler stopping...")

	// Signal the main loop to stop
	close(s.stopChan)

	// Create a timeout context for shutdown
	shutdownCtx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
	defer cancel()

	// Wait for goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("Scheduler stopped gracefully")
		return nil
	case <-shutdownCtx.Done():
		s.logger.Warn("Scheduler shutdown timed out, some jobs may still be running")
		return fmt.Errorf("scheduler shutdown timed out")
	}
}

// run is the main scheduling loop
func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Scheduler context cancelled")
			return
		case <-s.stopChan:
			s.logger.Info("Scheduler stop signal received")
			return
		case <-ticker.C:
			s.checkAndExecuteDueJobs(ctx)
		}
	}
}

// checkAndExecuteDueJobs checks for and executes any jobs that are due
func (s *Scheduler) checkAndExecuteDueJobs(ctx context.Context) {
	s.jobsMutex.RLock()
	dueJobs := make([]*Job, 0)
	now := time.Now()

	for _, job := range s.jobs {
		if job.Enabled && !job.Running && job.NextRun != nil && now.After(*job.NextRun) {
			dueJobs = append(dueJobs, job)
		}
	}
	s.jobsMutex.RUnlock()

	// Execute due jobs
	for _, job := range dueJobs {
		select {
		case <-ctx.Done():
			return
		case <-s.stopChan:
			return
		default:
			s.executeJob(ctx, job)
		}
	}
}

// executeJob executes a single scheduled job
func (s *Scheduler) executeJob(ctx context.Context, job *Job) {
	// Try to acquire the backup lock
	if !s.tryAcquireBackupLock(job.CamundaInstanceID) {
		s.logger.Debug("Skipping job for instance %s: another backup is in progress", job.CamundaInstanceID)
		return
	}

	// Mark job as running
	s.jobsMutex.Lock()
	job.Running = true
	s.jobsMutex.Unlock()

	// Execute in goroutine to not block the scheduler loop
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.releaseBackupLock()
		defer func() {
			s.jobsMutex.Lock()
			job.Running = false
			now := time.Now()
			job.LastRun = &now
			// Calculate next run time
			if nextRun, err := s.calculateNextRun(job.Schedule); err == nil {
				job.NextRun = nextRun
			}
			s.jobsMutex.Unlock()
		}()

		// Get the instance
		instance, err := s.instanceProvider.GetInstance(job.CamundaInstanceID)
		if err != nil {
			s.logger.Error("Failed to get instance %s for scheduled backup: %v", job.CamundaInstanceID, err)
			return
		}

		// Check if instance is still enabled
		if !instance.Enabled {
			s.logger.Debug("Skipping scheduled backup for disabled instance: %s", instance.Name)
			return
		}

		s.logger.Info("Executing scheduled backup for instance: %s", instance.Name)

		// Execute the backup
		if err := s.executor.ExecuteScheduledBackup(ctx, instance); err != nil {
			s.logger.Error("Scheduled backup failed for instance %s: %v", instance.Name, err)
		} else {
			s.logger.Info("Scheduled backup completed for instance: %s", instance.Name)
		}
	}()
}

// RegisterJob registers a new scheduled job for a Camunda instance
func (s *Scheduler) RegisterJob(instanceID, schedule string, enabled bool) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	// Calculate next run time
	nextRun, err := s.calculateNextRun(schedule)
	if err != nil {
		return fmt.Errorf("invalid schedule expression: %w", err)
	}

	job := &Job{
		ID:                instanceID,
		CamundaInstanceID: instanceID,
		Schedule:          schedule,
		Enabled:           enabled,
		NextRun:           nextRun,
		Running:           false,
	}

	s.jobs[instanceID] = job
	s.logger.Info("Registered job for instance %s with schedule: %s (next run: %v)", instanceID, schedule, nextRun)
	return nil
}

// DeregisterJob removes a scheduled job
func (s *Scheduler) DeregisterJob(instanceID string) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	job, exists := s.jobs[instanceID]
	if !exists {
		return fmt.Errorf("job not found for instance: %s", instanceID)
	}

	if job.Running {
		return fmt.Errorf("cannot deregister job while it is running: %s", instanceID)
	}

	delete(s.jobs, instanceID)
	s.logger.Info("Deregistered job for instance: %s", instanceID)
	return nil
}

// UpdateJob updates an existing job's schedule or enabled state
func (s *Scheduler) UpdateJob(instanceID, schedule string, enabled bool) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	job, exists := s.jobs[instanceID]
	if !exists {
		// Create new job if it doesn't exist
		nextRun, err := s.calculateNextRun(schedule)
		if err != nil {
			return fmt.Errorf("invalid schedule expression: %w", err)
		}
		s.jobs[instanceID] = &Job{
			ID:                instanceID,
			CamundaInstanceID: instanceID,
			Schedule:          schedule,
			Enabled:           enabled,
			NextRun:           nextRun,
			Running:           false,
		}
		s.logger.Info("Created job for instance %s with schedule: %s", instanceID, schedule)
		return nil
	}

	// Update existing job
	if job.Schedule != schedule {
		nextRun, err := s.calculateNextRun(schedule)
		if err != nil {
			return fmt.Errorf("invalid schedule expression: %w", err)
		}
		job.Schedule = schedule
		job.NextRun = nextRun
	}
	job.Enabled = enabled

	s.logger.Info("Updated job for instance %s: schedule=%s, enabled=%v", instanceID, schedule, enabled)
	return nil
}

// EnableJob enables a scheduled job
func (s *Scheduler) EnableJob(instanceID string) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	job, exists := s.jobs[instanceID]
	if !exists {
		return fmt.Errorf("job not found for instance: %s", instanceID)
	}

	job.Enabled = true
	// Recalculate next run time
	if nextRun, err := s.calculateNextRun(job.Schedule); err == nil {
		job.NextRun = nextRun
	}

	s.logger.Info("Enabled job for instance: %s", instanceID)
	return nil
}

// DisableJob disables a scheduled job
func (s *Scheduler) DisableJob(instanceID string) error {
	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	job, exists := s.jobs[instanceID]
	if !exists {
		return fmt.Errorf("job not found for instance: %s", instanceID)
	}

	job.Enabled = false
	s.logger.Info("Disabled job for instance: %s", instanceID)
	return nil
}

// GetJob returns information about a job
func (s *Scheduler) GetJob(instanceID string) (*Job, error) {
	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()

	job, exists := s.jobs[instanceID]
	if !exists {
		return nil, fmt.Errorf("job not found for instance: %s", instanceID)
	}

	// Return a copy to avoid race conditions
	jobCopy := *job
	return &jobCopy, nil
}

// ListJobs returns all registered jobs
func (s *Scheduler) ListJobs() []*Job {
	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()

	jobs := make([]*Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobCopy := *job
		jobs = append(jobs, &jobCopy)
	}
	return jobs
}

// tryAcquireBackupLock attempts to acquire the backup lock for an instance
// Returns true if the lock was acquired, false if another backup is in progress
func (s *Scheduler) tryAcquireBackupLock(instanceID string) bool {
	s.backupMutex.Lock()
	defer s.backupMutex.Unlock()

	if s.activeBackup != nil {
		return false
	}

	s.activeBackup = &instanceID
	return true
}

// releaseBackupLock releases the backup lock
func (s *Scheduler) releaseBackupLock() {
	s.backupMutex.Lock()
	defer s.backupMutex.Unlock()
	s.activeBackup = nil
}

// TryAcquireManualBackupLock attempts to acquire the backup lock for a manual backup
// This is called by the API layer to prevent concurrent manual and scheduled backups
func (s *Scheduler) TryAcquireManualBackupLock(instanceID string) bool {
	return s.tryAcquireBackupLock(instanceID)
}

// ReleaseManualBackupLock releases the backup lock after a manual backup
func (s *Scheduler) ReleaseManualBackupLock() {
	s.releaseBackupLock()
}

// IsBackupInProgress returns true if a backup is currently in progress
func (s *Scheduler) IsBackupInProgress() bool {
	s.backupMutex.Lock()
	defer s.backupMutex.Unlock()
	return s.activeBackup != nil
}

// GetActiveBackupInstance returns the instance ID of the currently running backup, or empty if none
func (s *Scheduler) GetActiveBackupInstance() string {
	s.backupMutex.Lock()
	defer s.backupMutex.Unlock()
	if s.activeBackup != nil {
		return *s.activeBackup
	}
	return ""
}

// loadJobsFromInstances loads jobs from all configured Camunda instances
func (s *Scheduler) loadJobsFromInstances() error {
	instances, err := s.instanceProvider.ListInstances()
	if err != nil {
		return fmt.Errorf("failed to list instances: %w", err)
	}

	for _, instance := range instances {
		if err := s.RegisterJob(instance.ID, instance.Schedule, instance.Enabled); err != nil {
			s.logger.Warn("Failed to register job for instance %s: %v", instance.ID, err)
		}
	}

	s.logger.Info("Loaded %d jobs from configured instances", len(instances))
	return nil
}

// ReloadJobs reloads all jobs from configured instances
func (s *Scheduler) ReloadJobs() error {
	// Get current running jobs to preserve their state
	s.jobsMutex.Lock()
	runningJobs := make(map[string]bool)
	for id, job := range s.jobs {
		if job.Running {
			runningJobs[id] = true
		}
	}
	s.jobsMutex.Unlock()

	// Load instances
	instances, err := s.instanceProvider.ListInstances()
	if err != nil {
		return fmt.Errorf("failed to list instances: %w", err)
	}

	s.jobsMutex.Lock()
	defer s.jobsMutex.Unlock()

	// Create new jobs map
	newJobs := make(map[string]*Job)

	for _, instance := range instances {
		nextRun, err := s.calculateNextRun(instance.Schedule)
		if err != nil {
			s.logger.Warn("Invalid schedule for instance %s: %v", instance.ID, err)
			continue
		}

		// Preserve running state if job was running
		running := runningJobs[instance.ID]

		// Preserve last run time if job existed
		var lastRun *time.Time
		if oldJob, exists := s.jobs[instance.ID]; exists {
			lastRun = oldJob.LastRun
		}

		newJobs[instance.ID] = &Job{
			ID:                instance.ID,
			CamundaInstanceID: instance.ID,
			Schedule:          instance.Schedule,
			Enabled:           instance.Enabled,
			LastRun:           lastRun,
			NextRun:           nextRun,
			Running:           running,
		}
	}

	s.jobs = newJobs
	s.logger.Info("Reloaded %d jobs from configured instances", len(newJobs))
	return nil
}

// HealthCheck returns the health status of the scheduler
func (s *Scheduler) HealthCheck() HealthStatus {
	s.runningMutex.RLock()
	running := s.running
	s.runningMutex.RUnlock()

	s.jobsMutex.RLock()
	totalJobs := len(s.jobs)
	enabledJobs := 0
	runningJobs := 0
	for _, job := range s.jobs {
		if job.Enabled {
			enabledJobs++
		}
		if job.Running {
			runningJobs++
		}
	}
	s.jobsMutex.RUnlock()

	return HealthStatus{
		Running:      running,
		TotalJobs:    totalJobs,
		EnabledJobs:  enabledJobs,
		RunningJobs:  runningJobs,
		BackupActive: s.IsBackupInProgress(),
	}
}

// HealthStatus represents the scheduler's health status
type HealthStatus struct {
	Running      bool `json:"running"`
	TotalJobs    int  `json:"total_jobs"`
	EnabledJobs  int  `json:"enabled_jobs"`
	RunningJobs  int  `json:"running_jobs"`
	BackupActive bool `json:"backup_active"`
}

// IsRunning returns whether the scheduler is currently running
func (s *Scheduler) IsRunning() bool {
	s.runningMutex.RLock()
	defer s.runningMutex.RUnlock()
	return s.running
}
