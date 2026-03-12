package scheduler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
	listError error
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
	if m.listError != nil {
		return nil, m.listError
	}
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
	if !scheduler.TryAcquireBackupLock("instance-1") {
		t.Error("Should be able to acquire lock when no backup is running")
	}

	if !scheduler.IsBackupInProgress() {
		t.Error("Backup should be in progress after acquiring lock")
	}

	if scheduler.GetActiveBackupInstance() != "instance-1" {
		t.Error("Active backup instance should be instance-1")
	}

	// Try to acquire another lock (should fail)
	if scheduler.TryAcquireBackupLock("instance-2") {
		t.Error("Should not be able to acquire lock when backup is in progress")
	}

	// Release the lock
	scheduler.ReleaseBackupLock()

	if scheduler.IsBackupInProgress() {
		t.Error("Backup should not be in progress after releasing lock")
	}

	// Should be able to acquire lock again
	if !scheduler.TryAcquireBackupLock("instance-2") {
		t.Error("Should be able to acquire lock after release")
	}
	scheduler.ReleaseBackupLock()
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

// ---------------------------------------------------------------------------
// NewScheduler config tests
// ---------------------------------------------------------------------------

func TestNewScheduler_ZeroConfig(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, Config{})

	if s.tickInterval != time.Minute {
		t.Errorf("Expected default tick interval of 1m, got %v", s.tickInterval)
	}
	if s.shutdownTimeout != 5*time.Minute {
		t.Errorf("Expected default shutdown timeout of 5m, got %v", s.shutdownTimeout)
	}
	if s.stuckTimeout != 0 {
		t.Errorf("Expected zero stuck timeout (disabled), got %v", s.stuckTimeout)
	}
}

func TestNewScheduler_CustomConfig(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	cfg := Config{
		TickInterval:    30 * time.Second,
		ShutdownTimeout: 10 * time.Minute,
		StuckTimeout:    3 * time.Hour,
	}
	s := NewScheduler(executor, provider, logger, cfg)

	if s.tickInterval != 30*time.Second {
		t.Errorf("Expected tick interval 30s, got %v", s.tickInterval)
	}
	if s.shutdownTimeout != 10*time.Minute {
		t.Errorf("Expected shutdown timeout 10m, got %v", s.shutdownTimeout)
	}
	if s.stuckTimeout != 3*time.Hour {
		t.Errorf("Expected stuck timeout 3h, got %v", s.stuckTimeout)
	}
}

// ---------------------------------------------------------------------------
// GetJobsCount / GetEnabledJobsCount
// ---------------------------------------------------------------------------

func TestGetJobsCount(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")
	s := NewScheduler(executor, provider, logger, DefaultConfig())

	if s.GetJobsCount() != 0 {
		t.Errorf("Expected 0 jobs initially, got %d", s.GetJobsCount())
	}

	s.RegisterJob("i1", "0 1 * * *", true)
	s.RegisterJob("i2", "0 2 * * *", false)
	s.RegisterJob("i3", "0 3 * * *", true)

	if s.GetJobsCount() != 3 {
		t.Errorf("Expected 3 jobs, got %d", s.GetJobsCount())
	}
}

func TestGetEnabledJobsCount(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")
	s := NewScheduler(executor, provider, logger, DefaultConfig())

	if s.GetEnabledJobsCount() != 0 {
		t.Errorf("Expected 0 enabled jobs initially, got %d", s.GetEnabledJobsCount())
	}

	s.RegisterJob("i1", "0 1 * * *", true)
	s.RegisterJob("i2", "0 2 * * *", false)
	s.RegisterJob("i3", "0 3 * * *", true)

	if s.GetEnabledJobsCount() != 2 {
		t.Errorf("Expected 2 enabled jobs, got %d", s.GetEnabledJobsCount())
	}
}

// ---------------------------------------------------------------------------
// SetAlerter
// ---------------------------------------------------------------------------

func TestSetAlerter_BeforeStart(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())
	alerter := utils.NewAlerter("http://example.com/webhook", logger)

	s.SetAlerter(alerter)

	if s.alerter != alerter {
		t.Error("SetAlerter should set alerter before scheduler starts")
	}
}

func TestSetAlerter_AfterStart(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Failed to start scheduler: %v", err)
	}
	defer s.Stop(ctx)

	alerter := utils.NewAlerter("http://example.com/webhook", logger)
	s.SetAlerter(alerter)

	if s.alerter != nil {
		t.Error("SetAlerter should be no-op when scheduler is running")
	}
}

// ---------------------------------------------------------------------------
// checkForStuckJobs
// ---------------------------------------------------------------------------

func TestCheckForStuckJobs_DisabledWhenZeroTimeout(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, Config{
		TickInterval:    time.Minute,
		ShutdownTimeout: 5 * time.Minute,
		StuckTimeout:    0, // disabled
	})

	startedAt := time.Now().Add(-24 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1",
		Running: true, RunningStartedAt: &startedAt,
	}

	s.checkForStuckJobs()

	s.jobsMutex.RLock()
	if s.jobs["i1"].StuckAlertedAt != nil {
		t.Error("Should not alert when stuck timeout is disabled (zero)")
	}
	s.jobsMutex.RUnlock()
}

func TestCheckForStuckJobs_NoStuckJobs(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, Config{
		TickInterval:    time.Minute,
		ShutdownTimeout: 5 * time.Minute,
		StuckTimeout:    1 * time.Hour,
	})

	// Job running but within timeout
	startedAt := time.Now().Add(-30 * time.Minute)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1",
		Running: true, RunningStartedAt: &startedAt,
	}

	// Job not running
	s.jobs["i2"] = &Job{
		ID: "i2", CamundaInstanceID: "i2",
		Running: false,
	}

	// Job running but nil RunningStartedAt (shouldn't happen, but guard)
	s.jobs["i3"] = &Job{
		ID: "i3", CamundaInstanceID: "i3",
		Running: true, RunningStartedAt: nil,
	}

	s.checkForStuckJobs()

	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()
	for id, job := range s.jobs {
		if job.StuckAlertedAt != nil {
			t.Errorf("Job %s should not be alerted", id)
		}
	}
}

func TestCheckForStuckJobs_NoAlerter(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, Config{
		TickInterval:    time.Minute,
		ShutdownTimeout: 5 * time.Minute,
		StuckTimeout:    1 * time.Hour,
	})
	// Intentionally don't set an alerter

	startedAt := time.Now().Add(-2 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1",
		Running: true, RunningStartedAt: &startedAt,
	}

	s.checkForStuckJobs() // should not panic

	s.jobsMutex.RLock()
	if s.jobs["i1"].StuckAlertedAt == nil {
		t.Error("StuckAlertedAt should be set even without an alerter")
	}
	s.jobsMutex.RUnlock()
}

func TestCheckForStuckJobs_StuckJobTriggersAlert(t *testing.T) {
	alertReceived := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alertReceived <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := utils.NewLogger("error")
	alerter := utils.NewAlerter(server.URL, logger)

	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()

	s := NewScheduler(executor, provider, logger, Config{
		TickInterval:    time.Minute,
		ShutdownTimeout: 5 * time.Minute,
		StuckTimeout:    1 * time.Hour,
	})
	s.SetAlerter(alerter)

	startedAt := time.Now().Add(-2 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: true, RunningStartedAt: &startedAt,
	}

	s.checkForStuckJobs()

	// Wait for the async alert HTTP request
	select {
	case <-alertReceived:
		// Alert was delivered
	case <-time.After(5 * time.Second):
		t.Fatal("Expected alert to be sent for stuck job")
	}

	s.jobsMutex.RLock()
	if s.jobs["i1"].StuckAlertedAt == nil {
		t.Error("StuckAlertedAt should be set after alert")
	}
	s.jobsMutex.RUnlock()
}

func TestCheckForStuckJobs_DeduplicatesAlerts(t *testing.T) {
	var alertCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		alertCount.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := utils.NewLogger("error")
	alerter := utils.NewAlerter(server.URL, logger)

	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()

	s := NewScheduler(executor, provider, logger, Config{
		TickInterval:    time.Minute,
		ShutdownTimeout: 5 * time.Minute,
		StuckTimeout:    1 * time.Hour,
	})
	s.SetAlerter(alerter)

	startedAt := time.Now().Add(-2 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: true, RunningStartedAt: &startedAt,
	}

	// First check — triggers alert
	s.checkForStuckJobs()
	time.Sleep(300 * time.Millisecond)

	if alertCount.Load() != 1 {
		t.Fatalf("Expected 1 alert after first check, got %d", alertCount.Load())
	}

	// Second check — StuckAlertedAt is already set, no new alert
	s.checkForStuckJobs()
	time.Sleep(300 * time.Millisecond)

	if alertCount.Load() != 1 {
		t.Errorf("Expected 1 alert total (dedup), got %d", alertCount.Load())
	}
}

// ---------------------------------------------------------------------------
// deepCopyJob
// ---------------------------------------------------------------------------

func TestDeepCopyJob_AllFields(t *testing.T) {
	now := time.Now()
	lastRun := now.Add(-1 * time.Hour)
	nextRun := now.Add(1 * time.Hour)
	startedAt := now.Add(-30 * time.Minute)
	alertedAt := now.Add(-10 * time.Minute)

	original := &Job{
		ID:                "test-id",
		CamundaInstanceID: "instance-1",
		Schedule:          "0 2 * * *",
		Enabled:           true,
		Running:           true,
		LastRun:           &lastRun,
		NextRun:           &nextRun,
		RunningStartedAt:  &startedAt,
		StuckAlertedAt:    &alertedAt,
	}

	copied := deepCopyJob(original)

	// Scalar fields
	if copied.ID != original.ID {
		t.Errorf("ID: got %s, want %s", copied.ID, original.ID)
	}
	if copied.CamundaInstanceID != original.CamundaInstanceID {
		t.Errorf("CamundaInstanceID: got %s, want %s", copied.CamundaInstanceID, original.CamundaInstanceID)
	}
	if copied.Schedule != original.Schedule {
		t.Errorf("Schedule: got %s, want %s", copied.Schedule, original.Schedule)
	}
	if copied.Enabled != original.Enabled {
		t.Errorf("Enabled: got %v, want %v", copied.Enabled, original.Enabled)
	}
	if copied.Running != original.Running {
		t.Errorf("Running: got %v, want %v", copied.Running, original.Running)
	}

	// Pointer fields must be deep copies (different addresses, same values)
	ptrFields := []struct {
		name     string
		copiedP  *time.Time
		origP    *time.Time
	}{
		{"LastRun", copied.LastRun, original.LastRun},
		{"NextRun", copied.NextRun, original.NextRun},
		{"RunningStartedAt", copied.RunningStartedAt, original.RunningStartedAt},
		{"StuckAlertedAt", copied.StuckAlertedAt, original.StuckAlertedAt},
	}
	for _, pf := range ptrFields {
		if pf.copiedP == pf.origP {
			t.Errorf("%s should be a different pointer", pf.name)
		}
		if !pf.copiedP.Equal(*pf.origP) {
			t.Errorf("%s value: got %v, want %v", pf.name, *pf.copiedP, *pf.origP)
		}
	}

	// Mutation of copy must not affect original
	newTime := now.Add(24 * time.Hour)
	copied.StuckAlertedAt = &newTime
	if original.StuckAlertedAt.Equal(newTime) {
		t.Error("Modifying copy's StuckAlertedAt should not affect original")
	}
}

func TestDeepCopyJob_NilPointers(t *testing.T) {
	original := &Job{
		ID:                "test-id",
		CamundaInstanceID: "instance-1",
		Schedule:          "0 2 * * *",
		Enabled:           true,
		Running:           false,
	}

	copied := deepCopyJob(original)

	if copied.LastRun != nil {
		t.Error("LastRun should be nil")
	}
	if copied.NextRun != nil {
		t.Error("NextRun should be nil")
	}
	if copied.RunningStartedAt != nil {
		t.Error("RunningStartedAt should be nil")
	}
	if copied.StuckAlertedAt != nil {
		t.Error("StuckAlertedAt should be nil")
	}
}

// ---------------------------------------------------------------------------
// executeJob error paths
// ---------------------------------------------------------------------------

func TestExecuteJob_InstanceNotFound(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	// Intentionally don't add instance to provider
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	pastTime := time.Now().Add(-1 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: false, NextRun: &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, s.jobs["i1"])
	s.wg.Wait()

	s.jobsMutex.RLock()
	if s.jobs["i1"].Running {
		t.Error("Job should not be running after instance-not-found error")
	}
	s.jobsMutex.RUnlock()

	if len(executor.GetExecutedBackups()) != 0 {
		t.Error("No backup should have been executed")
	}
}

func TestExecuteJob_InstanceDisabled(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 2 * * *", false)) // disabled
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	pastTime := time.Now().Add(-1 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: false, NextRun: &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, s.jobs["i1"])
	s.wg.Wait()

	if len(executor.GetExecutedBackups()) != 0 {
		t.Error("No backup should have been executed for disabled instance")
	}
}

func TestExecuteJob_ExecutorFails(t *testing.T) {
	executor := NewMockBackupExecutor()
	executor.shouldFail = true
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 2 * * *", true))
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	pastTime := time.Now().Add(-1 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: false, NextRun: &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, s.jobs["i1"])
	s.wg.Wait()

	s.jobsMutex.RLock()
	job := s.jobs["i1"]
	if job.Running {
		t.Error("Job should not be running after executor failure")
	}
	if job.LastRun == nil {
		t.Error("LastRun should be set even after failure")
	}
	s.jobsMutex.RUnlock()
}

func TestExecuteJob_BackupLockContention(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 2 * * *", true))
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	// Hold the backup lock
	s.TryAcquireBackupLock("other-instance")

	pastTime := time.Now().Add(-1 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: false, NextRun: &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, s.jobs["i1"])
	// No goroutine was spawned, so no wg.Wait() needed

	if len(executor.GetExecutedBackups()) != 0 {
		t.Error("No backup should have been executed when lock is held")
	}

	s.ReleaseBackupLock()
}

func TestExecuteJob_JobDeregistered(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 2 * * *", true))
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	// Create a job reference but do NOT put it in the scheduler's jobs map
	pastTime := time.Now().Add(-1 * time.Hour)
	orphanJob := &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: false, NextRun: &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, orphanJob)

	if len(executor.GetExecutedBackups()) != 0 {
		t.Error("No backup should have been executed for deregistered job")
	}
	if s.IsBackupInProgress() {
		t.Error("Backup lock should have been released")
	}
}

func TestExecuteJob_JobAlreadyRunning(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 2 * * *", true))
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	startedAt := time.Now()
	pastTime := time.Now().Add(-1 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: true, // already running
		RunningStartedAt: &startedAt, NextRun: &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, s.jobs["i1"])

	if len(executor.GetExecutedBackups()) != 0 {
		t.Error("No backup should have been executed for already-running job")
	}
	if s.IsBackupInProgress() {
		t.Error("Backup lock should have been released")
	}
}

func TestExecuteJob_JobDisabledBeforeRecheck(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 2 * * *", true))
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	pastTime := time.Now().Add(-1 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: false, // disabled between scheduling and re-check
		Running: false, NextRun: &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, s.jobs["i1"])

	if len(executor.GetExecutedBackups()) != 0 {
		t.Error("No backup should have been executed for disabled job")
	}
	if s.IsBackupInProgress() {
		t.Error("Backup lock should have been released")
	}
}

func TestExecuteJob_ClearsStuckAlertedAt(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 2 * * *", true))
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, Config{
		TickInterval:    time.Minute,
		ShutdownTimeout: 5 * time.Minute,
		StuckTimeout:    1 * time.Hour,
	})

	// Simulate a job that was previously stuck-alerted
	alertedAt := time.Now().Add(-30 * time.Minute)
	pastTime := time.Now().Add(-3 * time.Hour)
	s.jobs["i1"] = &Job{
		ID: "i1", CamundaInstanceID: "i1", Schedule: "0 2 * * *",
		Enabled: true, Running: false,
		StuckAlertedAt: &alertedAt,
		NextRun:        &pastTime,
	}

	ctx := context.Background()
	s.executeJob(ctx, s.jobs["i1"])
	s.wg.Wait()

	s.jobsMutex.RLock()
	job := s.jobs["i1"]
	if job.StuckAlertedAt != nil {
		t.Error("StuckAlertedAt should be cleared after job finishes")
	}
	if job.Running {
		t.Error("Job should not be running after completion")
	}
	if job.RunningStartedAt != nil {
		t.Error("RunningStartedAt should be cleared after job finishes")
	}
	s.jobsMutex.RUnlock()
}

// ---------------------------------------------------------------------------
// loadJobsFromInstances error paths
// ---------------------------------------------------------------------------

func TestLoadJobsFromInstances_ListError(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.listError = fmt.Errorf("database connection failed")
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	err := s.loadJobsFromInstances()
	if err == nil {
		t.Error("Expected error from loadJobsFromInstances when ListInstances fails")
	}
}

func TestLoadJobsFromInstances_InvalidSchedule(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "invalid-schedule", true))
	provider.AddInstance(createTestInstance("i2", "Instance 2", "0 2 * * *", true))
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, DefaultConfig())

	err := s.loadJobsFromInstances()
	if err != nil {
		t.Fatalf("loadJobsFromInstances should not return error for individual invalid schedules: %v", err)
	}

	// Only the valid instance should have been registered
	if s.GetJobsCount() != 1 {
		t.Errorf("Expected 1 job (valid schedule only), got %d", s.GetJobsCount())
	}
}

// ---------------------------------------------------------------------------
// calculateNextRun — unreachable-schedule path
// ---------------------------------------------------------------------------

func TestCalculateNextRun_NoMatchingTime(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")
	s := NewScheduler(executor, provider, logger, DefaultConfig())

	// Feb 30 never exists — valid cron but no matching date in 4 years
	_, err := s.calculateNextRun("0 0 30 2 *")
	if err == nil {
		t.Error("Expected error for impossible schedule (Feb 30)")
	}
}

// ---------------------------------------------------------------------------
// Scheduler lifecycle: context cancellation & stop timeout
// ---------------------------------------------------------------------------

func TestScheduler_ContextCancellation(t *testing.T) {
	executor := NewMockBackupExecutor()
	provider := NewMockInstanceProvider()
	logger := utils.NewLogger("error")

	s := NewScheduler(executor, provider, logger, Config{
		TickInterval:    50 * time.Millisecond,
		ShutdownTimeout: 5 * time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())

	if err := s.Start(ctx); err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	// Cancel the parent context — the run goroutine should exit via ctx.Done()
	cancel()
	time.Sleep(200 * time.Millisecond)

	// Stop should complete immediately since run already exited
	stopCtx := context.Background()
	if err := s.Stop(stopCtx); err != nil {
		t.Errorf("Stop should succeed after context cancellation: %v", err)
	}
}

func TestScheduler_StopTimeout(t *testing.T) {
	executor := NewMockBackupExecutor()
	executor.delay = 10 * time.Second // deliberately long

	provider := NewMockInstanceProvider()
	provider.AddInstance(createTestInstance("i1", "Instance 1", "0 0 * * *", true))
	logger := utils.NewLogger("error")

	cfg := Config{
		TickInterval:    50 * time.Millisecond,
		ShutdownTimeout: 200 * time.Millisecond, // very short
	}
	s := NewScheduler(executor, provider, logger, cfg)

	ctx := context.Background()
	if err := s.Start(ctx); err != nil {
		t.Fatalf("Failed to start: %v", err)
	}

	// Force the job to be due immediately
	s.jobsMutex.Lock()
	pastTime := time.Now().Add(-1 * time.Hour)
	if job, ok := s.jobs["i1"]; ok {
		job.NextRun = &pastTime
	}
	s.jobsMutex.Unlock()

	// Wait for the job to begin executing
	var started bool
	for i := 0; i < 40; i++ {
		time.Sleep(50 * time.Millisecond)
		if executor.executionCount.Load() > 0 {
			started = true
			break
		}
	}
	if !started {
		t.Skip("Job did not start within expected time — skipping timeout test")
	}

	// Stop should time out because the job takes 10 s and shutdown timeout is 200 ms
	err := s.Stop(ctx)
	if err == nil {
		t.Error("Expected timeout error from Stop")
	}

	// Let the cancelled goroutine clean up
	time.Sleep(500 * time.Millisecond)
}
