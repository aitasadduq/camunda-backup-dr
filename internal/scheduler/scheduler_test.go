package scheduler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// MockBackupExecutor implements BackupExecutor for testing
type MockBackupExecutor struct {
	ExecutedBackups []string
	mutex           sync.Mutex
	delay           time.Duration
	shouldFail      bool
	executionCount  atomic.Int32
}

func NewMockBackupExecutor() *MockBackupExecutor {
	return &MockBackupExecutor{
		ExecutedBackups: make([]string, 0),
	}
}

func (m *MockBackupExecutor) ExecuteScheduledBackup(ctx context.Context, instance *models.CamundaInstance) error {
	m.executionCount.Add(1)
	if m.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(m.delay):
		}
	}
	m.mutex.Lock()
	m.ExecutedBackups = append(m.ExecutedBackups, instance.ID)
	m.mutex.Unlock()
	if m.shouldFail {
		return context.DeadlineExceeded
	}
	return nil
}

func (m *MockBackupExecutor) GetExecutedBackups() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	result := make([]string, len(m.ExecutedBackups))
	copy(result, m.ExecutedBackups)
	return result
}

// MockInstanceProvider implements InstanceProvider for testing
type MockInstanceProvider struct {
	instances map[string]*models.CamundaInstance
	mutex     sync.RWMutex
}

func NewMockInstanceProvider() *MockInstanceProvider {
	return &MockInstanceProvider{
		instances: make(map[string]*models.CamundaInstance),
	}
}

func (m *MockInstanceProvider) AddInstance(instance *models.CamundaInstance) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.instances[instance.ID] = instance
}

func (m *MockInstanceProvider) GetInstance(id string) (*models.CamundaInstance, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	if instance, ok := m.instances[id]; ok {
		return instance, nil
	}
	return nil, utils.ErrCamundaInstanceNotFound
}

func (m *MockInstanceProvider) ListInstances() ([]models.CamundaInstance, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	instances := make([]models.CamundaInstance, 0, len(m.instances))
	for _, instance := range m.instances {
		instances = append(instances, *instance)
	}
	return instances, nil
}

func createTestInstance(id, name, schedule string, enabled bool) *models.CamundaInstance {
	return &models.CamundaInstance{
		ID:       id,
		Name:     name,
		BaseURL:  "http://localhost:8080",
		Enabled:  enabled,
		Schedule: schedule,
	}
}

func TestScheduler_StartStop(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	ctx := context.Background()

	// Test start
	if err := scheduler.Start(ctx); err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	if !scheduler.IsRunning() {
		t.Error("Scheduler should be running after Start()")
	}

	// Test double start (should error)
	if err := scheduler.Start(ctx); err == nil {
		t.Error("Expected error when starting already running scheduler")
	}

	// Test stop
	if err := scheduler.Stop(ctx); err != nil {
		t.Fatalf("Failed to stop scheduler: %v", err)
	}

	if scheduler.IsRunning() {
		t.Error("Scheduler should not be running after Stop()")
	}

	// Test double stop (should not error)
	if err := scheduler.Stop(ctx); err != nil {
		t.Errorf("Unexpected error stopping already stopped scheduler: %v", err)
	}
}

func TestScheduler_RegisterDeregisterJob(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	// Register a job
	err := scheduler.RegisterJob("instance-1", "0 2 * * *", true)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	// Verify job exists
	job, err := scheduler.GetJob("instance-1")
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}
	if job.Schedule != "0 2 * * *" {
		t.Errorf("Expected schedule '0 2 * * *', got '%s'", job.Schedule)
	}
	if !job.Enabled {
		t.Error("Job should be enabled")
	}

	// Deregister the job
	err = scheduler.DeregisterJob("instance-1")
	if err != nil {
		t.Fatalf("Failed to deregister job: %v", err)
	}

	// Verify job no longer exists
	_, err = scheduler.GetJob("instance-1")
	if err == nil {
		t.Error("Expected error when getting deregistered job")
	}

	// Test deregister non-existent job
	err = scheduler.DeregisterJob("non-existent")
	if err == nil {
		t.Error("Expected error when deregistering non-existent job")
	}
}

func TestScheduler_UpdateJob(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	// Register a job
	err := scheduler.RegisterJob("instance-1", "0 2 * * *", true)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	// Update the job
	err = scheduler.UpdateJob("instance-1", "0 3 * * *", false)
	if err != nil {
		t.Fatalf("Failed to update job: %v", err)
	}

	// Verify changes
	job, err := scheduler.GetJob("instance-1")
	if err != nil {
		t.Fatalf("Failed to get job: %v", err)
	}
	if job.Schedule != "0 3 * * *" {
		t.Errorf("Expected schedule '0 3 * * *', got '%s'", job.Schedule)
	}
	if job.Enabled {
		t.Error("Job should be disabled")
	}

	// Update non-existent job (should create it)
	err = scheduler.UpdateJob("instance-2", "0 4 * * *", true)
	if err != nil {
		t.Fatalf("Failed to update/create job: %v", err)
	}

	job, err = scheduler.GetJob("instance-2")
	if err != nil {
		t.Fatalf("Failed to get newly created job: %v", err)
	}
	if job.Schedule != "0 4 * * *" {
		t.Errorf("Expected schedule '0 4 * * *', got '%s'", job.Schedule)
	}
}

func TestScheduler_EnableDisableJob(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	// Register a job
	err := scheduler.RegisterJob("instance-1", "0 2 * * *", true)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	// Disable the job
	err = scheduler.DisableJob("instance-1")
	if err != nil {
		t.Fatalf("Failed to disable job: %v", err)
	}

	job, _ := scheduler.GetJob("instance-1")
	if job.Enabled {
		t.Error("Job should be disabled")
	}

	// Enable the job
	err = scheduler.EnableJob("instance-1")
	if err != nil {
		t.Fatalf("Failed to enable job: %v", err)
	}

	job, _ = scheduler.GetJob("instance-1")
	if !job.Enabled {
		t.Error("Job should be enabled")
	}

	// Test enable/disable non-existent job
	err = scheduler.EnableJob("non-existent")
	if err == nil {
		t.Error("Expected error when enabling non-existent job")
	}

	err = scheduler.DisableJob("non-existent")
	if err == nil {
		t.Error("Expected error when disabling non-existent job")
	}
}

func TestScheduler_ListJobs(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	// Register multiple jobs
	scheduler.RegisterJob("instance-1", "0 1 * * *", true)
	scheduler.RegisterJob("instance-2", "0 2 * * *", false)
	scheduler.RegisterJob("instance-3", "0 3 * * *", true)

	jobs := scheduler.ListJobs()
	if len(jobs) != 3 {
		t.Errorf("Expected 3 jobs, got %d", len(jobs))
	}
}

func TestScheduler_ConcurrencyControl(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	// Try to acquire lock for manual backup
	if !scheduler.TryAcquireManualBackupLock("instance-1") {
		t.Error("Should be able to acquire lock when no backup is running")
	}

	if !scheduler.IsBackupInProgress() {
		t.Error("Backup should be in progress after acquiring lock")
	}

	if scheduler.GetActiveBackupInstance() != "instance-1" {
		t.Error("Active backup instance should be instance-1")
	}

	// Try to acquire another lock (should fail)
	if scheduler.TryAcquireManualBackupLock("instance-2") {
		t.Error("Should not be able to acquire lock when backup is in progress")
	}

	// Release the lock
	scheduler.ReleaseManualBackupLock()

	if scheduler.IsBackupInProgress() {
		t.Error("Backup should not be in progress after releasing lock")
	}

	// Should be able to acquire lock again
	if !scheduler.TryAcquireManualBackupLock("instance-2") {
		t.Error("Should be able to acquire lock after release")
	}
	scheduler.ReleaseManualBackupLock()
}

func TestScheduler_HealthCheck(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	// Register some jobs
	scheduler.RegisterJob("instance-1", "0 1 * * *", true)
	scheduler.RegisterJob("instance-2", "0 2 * * *", false)
	scheduler.RegisterJob("instance-3", "0 3 * * *", true)

	health := scheduler.HealthCheck()

	if health.Running {
		t.Error("Scheduler should not be running before Start()")
	}

	if health.TotalJobs != 3 {
		t.Errorf("Expected 3 total jobs, got %d", health.TotalJobs)
	}

	if health.EnabledJobs != 2 {
		t.Errorf("Expected 2 enabled jobs, got %d", health.EnabledJobs)
	}

	if health.RunningJobs != 0 {
		t.Errorf("Expected 0 running jobs, got %d", health.RunningJobs)
	}

	if health.BackupActive {
		t.Error("No backup should be active")
	}

	// Start scheduler
	ctx := context.Background()
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	health = scheduler.HealthCheck()
	if !health.Running {
		t.Error("Scheduler should be running after Start()")
	}
}

func TestScheduler_LoadJobsFromInstances(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	// Add instances
	provider.AddInstance(createTestInstance("instance-1", "Instance 1", "0 1 * * *", true))
	provider.AddInstance(createTestInstance("instance-2", "Instance 2", "0 2 * * *", false))

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	ctx := context.Background()
	if err := scheduler.Start(ctx); err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}
	defer scheduler.Stop(ctx)

	// Verify jobs were loaded
	jobs := scheduler.ListJobs()
	if len(jobs) != 2 {
		t.Errorf("Expected 2 jobs, got %d", len(jobs))
	}
}

func TestScheduler_ReloadJobs(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	// Add initial instance
	provider.AddInstance(createTestInstance("instance-1", "Instance 1", "0 1 * * *", true))

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	ctx := context.Background()
	scheduler.Start(ctx)
	defer scheduler.Stop(ctx)

	// Add another instance
	provider.AddInstance(createTestInstance("instance-2", "Instance 2", "0 2 * * *", true))

	// Reload jobs
	if err := scheduler.ReloadJobs(); err != nil {
		t.Fatalf("Failed to reload jobs: %v", err)
	}

	// Verify new job exists
	jobs := scheduler.ListJobs()
	if len(jobs) != 2 {
		t.Errorf("Expected 2 jobs after reload, got %d", len(jobs))
	}
}

func TestScheduler_InvalidSchedule(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	scheduler := NewScheduler(executor, provider, logger, DefaultConfig())

	// Try to register with invalid schedule
	err := scheduler.RegisterJob("instance-1", "invalid", true)
	if err == nil {
		t.Error("Expected error for invalid cron expression")
	}

	// Try to update with invalid schedule
	scheduler.RegisterJob("instance-2", "0 2 * * *", true)
	err = scheduler.UpdateJob("instance-2", "also-invalid", true)
	if err == nil {
		t.Error("Expected error for invalid cron expression on update")
	}
}

func TestScheduler_GracefulShutdownWithRunningJob(t *testing.T) {
	jobDuration := 500 * time.Millisecond
	executor := NewMockBackupExecutor()
	executor.delay = jobDuration

	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("instance-1", "Instance 1", "0 0 * * *", true))

	logger := utils.NewLogger("error")

	cfg := Config{
		TickInterval:    50 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
	}
	scheduler := NewScheduler(executor, provider, logger, cfg)

	ctx := context.Background()
	err := scheduler.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}

	// Register the job and force NextRun into the past so it triggers immediately
	err = scheduler.RegisterJob("instance-1", "0 0 * * *", true)
	if err != nil {
		t.Fatalf("Failed to register job: %v", err)
	}

	// Manually set NextRun to the past to trigger the job on next tick
	scheduler.jobsMutex.Lock()
	pastTime := time.Now().Add(-1 * time.Hour)
	scheduler.jobs["instance-1"].NextRun = &pastTime
	scheduler.jobsMutex.Unlock()

	// Wait for the job to start running (scheduler will pick it up on next tick)
	var jobStarted bool
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if executor.executionCount.Load() > 0 {
			jobStarted = true
			break
		}
	}

	if !jobStarted {
		t.Fatal("Job did not start within expected time")
	}

	// Verify job is running
	job, _ := scheduler.GetJob("instance-1")
	if job == nil || !job.Running {
		t.Fatal("Expected job to be in running state")
	}

	// Stop should wait for the running job to complete
	start := time.Now()
	err = scheduler.Stop(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Unexpected error during shutdown: %v", err)
	}

	// Stop should have blocked until the job completed (or close to it)
	// The job takes 500ms, and we called Stop shortly after it started,
	// so Stop should have waited at least 200ms (accounting for timing variance)
	minExpectedWait := jobDuration / 3
	if elapsed < minExpectedWait {
		t.Errorf("Stop returned too quickly (%v), expected to wait at least %v for running job", elapsed, minExpectedWait)
	}

	// Verify the job completed (was added to ExecutedBackups)
	executedBackups := executor.GetExecutedBackups()
	if len(executedBackups) != 1 || executedBackups[0] != "instance-1" {
		t.Errorf("Expected job to complete, got executed backups: %v", executedBackups)
	}

	t.Logf("Shutdown took %v (job duration: %v)", elapsed, jobDuration)
}
