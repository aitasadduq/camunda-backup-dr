//go:build e2e

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/orchestrator"
	"github.com/aitasadduq/camunda-backup-dr/internal/retention"
	"github.com/aitasadduq/camunda-backup-dr/internal/scheduler"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// ---------------------------------------------------------------------------
// Mock Camunda/ES component servers
// ---------------------------------------------------------------------------

// componentServer wraps an httptest.Server that simulates a Camunda component or
// Elasticsearch. The caller configures it with desired trigger/poll behaviour.
type componentServer struct {
	server *httptest.Server

	// triggerStatus is the HTTP status code returned by the POST /backup trigger.
	triggerStatus int

	// pollResponses is a queue of JSON bodies returned by GET /backup/{id}.
	// The server pops the first entry on each poll; after exhaustion it
	// returns the last entry forever.
	pollResponses []string
	pollIndex     int
	mu            sync.Mutex
}

// newComponentServer creates a mock Camunda component (Zeebe, Operate, etc.)
// that responds to trigger (POST) and poll (GET) requests.
func newComponentServer(triggerStatus int, pollResponses []string) *componentServer {
	cs := &componentServer{
		triggerStatus: triggerStatus,
		pollResponses: pollResponses,
	}

	cs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			// Trigger backup
			w.WriteHeader(cs.triggerStatus)
			w.Write([]byte(`{"message":"accepted"}`))
		case http.MethodGet:
			// Poll status
			cs.mu.Lock()
			idx := cs.pollIndex
			if idx < len(cs.pollResponses)-1 {
				cs.pollIndex++
			}
			body := cs.pollResponses[idx]
			cs.mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(body))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	return cs
}

func (cs *componentServer) URL() string  { return cs.server.URL }
func (cs *componentServer) Close()       { cs.server.Close() }

// newESServer creates a mock Elasticsearch server that responds to snapshot
// create (PUT) and status (GET) requests.
func newESServer(createStatus int, snapshotStates []string) *componentServer {
	cs := &componentServer{
		triggerStatus: createStatus,
		pollResponses: snapshotStates, // reuse field for ES states
	}

	cs.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			// Create snapshot
			w.WriteHeader(cs.triggerStatus)
			w.Write([]byte(`{"accepted":true}`))
		case http.MethodGet:
			// Get snapshot status
			cs.mu.Lock()
			idx := cs.pollIndex
			if idx < len(cs.pollResponses)-1 {
				cs.pollIndex++
			}
			state := cs.pollResponses[idx]
			cs.mu.Unlock()
			resp := fmt.Sprintf(`{"snapshots":[{"snapshot":"test","state":"%s"}]}`, state)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(resp))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))

	return cs
}

// ---------------------------------------------------------------------------
// Test environment setup
// ---------------------------------------------------------------------------

// testEnv holds all components wired together for an E2E test.
type testEnv struct {
	t                *testing.T
	router           *Router
	server           *httptest.Server // the API server under test
	fileStorage      *storage.FileStorageImpl
	s3Storage        *storage.S3StorageImpl
	camundaManager   *camunda.Manager
	orchestratorImpl *orchestrator.Orchestrator
	sched            *scheduler.Scheduler
	retentionMgr     *retention.Manager
	logger           *utils.Logger
	cfg              *config.Config
	dataDir          string

	// Component mock servers — kept for deferred cleanup.
	mockServers []*componentServer
}

func (env *testEnv) cleanup() {
	for _, s := range env.mockServers {
		s.Close()
	}
	if env.sched != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		env.sched.Stop(ctx)
	}
}

// setupTestEnv creates a fully wired test environment. callers must defer env.cleanup().
// componentMocks lets the caller provide pre-configured mock servers for each component.
// If nil, default "always succeed" mocks are used.
func setupTestEnv(t *testing.T, opts ...envOption) *testEnv {
	t.Helper()

	o := defaultEnvOpts()
	for _, fn := range opts {
		fn(o)
	}

	logger := utils.NewLogger("debug")

	// Create temp data directory for file storage
	dataDir := t.TempDir()

	cfg := &config.Config{
		Port:                                   0, // not used directly
		LogLevel:                               "debug",
		DataDir:                                dataDir,
		DefaultSchedule:                        "0 2 * * *",
		DefaultRetentionCount:                  7,
		DefaultSuccessHistory:                  30,
		DefaultFailureHistory:                  30,
		DefaultBackupPollInterval:              1, // 1 second — fast for tests
		DefaultBackupMaxAttempts:               10,
		DefaultElasticsearchSnapshotRepository: "test-repo",
	}

	fileStorage, err := storage.NewFileStorage(dataDir, cfg, logger)
	if err != nil {
		t.Fatalf("NewFileStorage: %v", err)
	}

	s3Storage, err := storage.NewS3Storage("http://mock", "mock", "mock", "test-bucket", "", logger)
	if err != nil {
		t.Fatalf("NewS3Storage: %v", err)
	}

	camundaManager := camunda.NewManager(fileStorage, logger)

	httpClientCfg := camunda.HTTPClientConfig{
		Timeout:       10 * time.Second,
		MaxRetries:    0, // no retries in tests — fail fast
		RetryDelay:    100 * time.Millisecond,
		MaxRetryDelay: 1 * time.Second,
	}
	httpClient := camunda.NewHTTPClient(httpClientCfg, logger)

	pollInterval := time.Duration(cfg.DefaultBackupPollInterval) * time.Second
	if o.pollInterval > 0 {
		pollInterval = o.pollInterval
	}
	maxAttempts := cfg.DefaultBackupMaxAttempts
	if o.maxPollAttempts > 0 {
		maxAttempts = o.maxPollAttempts
	}

	orch := orchestrator.NewOrchestrator(
		fileStorage,
		s3Storage,
		httpClient,
		cfg,
		logger,
		pollInterval,
		maxAttempts,
	)

	retentionMgr := retention.NewManager(s3Storage, fileStorage, logger)

	// Wire retention to orchestrator
	orch.SetRetentionFunc(func(camundaInstanceID string, retentionCount int) {
		retentionMgr.ApplyRetention(camundaInstanceID, retentionCount)
	})

	// Create scheduler with very fast tick for tests
	schedCfg := scheduler.Config{
		TickInterval:    200 * time.Millisecond,
		ShutdownTimeout: 5 * time.Second,
		StuckTimeout:    0, // disabled
	}

	backupExecutor := &backupExecutorAdapter{orchestrator: orch}
	sched := scheduler.NewScheduler(backupExecutor, camundaManager, logger, schedCfg)

	env := &testEnv{
		t:                t,
		fileStorage:      fileStorage,
		s3Storage:        s3Storage,
		camundaManager:   camundaManager,
		orchestratorImpl: orch,
		retentionMgr:     retentionMgr,
		sched:            sched,
		logger:           logger,
		cfg:              cfg,
		dataDir:          dataDir,
	}

	// Build handler stack with full middleware
	handlers := NewHandlers(camundaManager, orch, s3Storage, sched, retentionMgr, fileStorage, logger)
	router := NewRouter(handlers, nil)

	middlewareChain := ChainMiddleware(
		RecoveryMiddleware(logger),
		LoggingMiddleware(logger),
		CORSMiddleware(),
		CSRFMiddleware(),
		ContentTypeMiddleware(),
	)

	env.router = router
	env.server = httptest.NewServer(middlewareChain(router))

	return env
}

// backupExecutorAdapter is identical to the one in main.go — adapts orchestrator
// to the scheduler.BackupExecutor interface.
type backupExecutorAdapter struct {
	orchestrator *orchestrator.Orchestrator
}

func (a *backupExecutorAdapter) ExecuteScheduledBackup(ctx context.Context, instance *models.CamundaInstance) error {
	req := orchestrator.BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeScheduled,
		BackupReason:    "Scheduled backup",
	}
	_, err := a.orchestrator.ExecuteBackup(ctx, req)
	return err
}

// envOption configures the test environment.
type envOption func(*envOpts)

type envOpts struct {
	pollInterval    time.Duration
	maxPollAttempts int
}

func defaultEnvOpts() *envOpts {
	return &envOpts{}
}

func withPollInterval(d time.Duration) envOption {
	return func(o *envOpts) { o.pollInterval = d }
}

func withMaxPollAttempts(n int) envOption {
	return func(o *envOpts) { o.maxPollAttempts = n }
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

// apiRequest builds and performs an HTTP request against the test API server.
// State-changing methods automatically include the CSRF header.
func (env *testEnv) apiRequest(method, path string, body interface{}) *http.Response {
	env.t.Helper()

	var bodyReader io.Reader
	if body != nil {
		jsonBytes, err := json.Marshal(body)
		if err != nil {
			env.t.Fatalf("json.Marshal: %v", err)
		}
		bodyReader = bytes.NewReader(jsonBytes)
	}

	req, err := http.NewRequest(method, env.server.URL+path, bodyReader)
	if err != nil {
		env.t.Fatalf("http.NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	// Add CSRF header for state-changing methods
	if method != http.MethodGet && method != http.MethodHead && method != http.MethodOptions {
		req.Header.Set("X-Requested-With", "XMLHttpRequest")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		env.t.Fatalf("HTTP %s %s failed: %v", method, path, err)
	}
	return resp
}

// readJSON reads a JSON response body into the given target.
func readJSON(t *testing.T, resp *http.Response, target interface{}) {
	t.Helper()
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatalf("json.Unmarshal: %v (body: %s)", err, string(data))
	}
}

// readBody reads the response body as a string.
func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return string(data)
}

// createTestInstance creates a Camunda instance via the API and returns it.
func (env *testEnv) createTestInstance(id string, componentEndpoints map[string]string) {
	env.t.Helper()

	instance := map[string]interface{}{
		"id":                      id,
		"name":                    "Test Instance " + id,
		"base_url":                "http://test-camunda:8080",
		"enabled":                 true,
		"schedule":                "0 2 * * *",
		"retention_count":         3,
		"success_history_count":   30,
		"failure_history_count":   30,
		"s3_endpoint":             "http://mock-s3",
		"s3_accesskey":            "mock-access-key",
		"parallel_execution":      false,
		"elasticsearch_endpoint":  componentEndpoints["elasticsearch"],
		"elasticsearch_username":  "",
		"zeebe_backup_endpoint":   componentEndpoints["zeebe"],
		"operate_backup_endpoint": componentEndpoints["operate"],
		"tasklist_backup_endpoint": componentEndpoints["tasklist"],
		"optimize_backup_endpoint": "",
		"components": []map[string]interface{}{
			{"name": "zeebe", "enabled": componentEndpoints["zeebe"] != ""},
			{"name": "operate", "enabled": componentEndpoints["operate"] != ""},
			{"name": "tasklist", "enabled": componentEndpoints["tasklist"] != ""},
			{"name": "optimize", "enabled": false},
			{"name": "elasticsearch", "enabled": componentEndpoints["elasticsearch"] != ""},
		},
	}

	resp := env.apiRequest(http.MethodPost, "/api/camundas", instance)
	if resp.StatusCode != http.StatusCreated {
		body := readBody(env.t, resp)
		env.t.Fatalf("createTestInstance: expected 201, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()
}

// pollBackupHistory polls the backup history endpoint until a backup with
// the given status appears or the timeout elapses.
func (env *testEnv) pollBackupHistory(instanceID string, wantStatus types.BackupStatus, timeout time.Duration) []*models.BackupHistory {
	env.t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp := env.apiRequest(http.MethodGet, fmt.Sprintf("/api/camundas/%s/backups?status=%s", instanceID, wantStatus), nil)
		var history []*models.BackupHistory
		readJSON(env.t, resp, &history)
		if len(history) > 0 {
			return history
		}
		time.Sleep(300 * time.Millisecond)
	}

	env.t.Fatalf("pollBackupHistory: no backup with status %s found within %v", wantStatus, timeout)
	return nil
}

// pollBackupHistoryAll polls all backup history until at least one entry appears.
func (env *testEnv) pollBackupHistoryAll(instanceID string, timeout time.Duration) []*models.BackupHistory {
	env.t.Helper()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		resp := env.apiRequest(http.MethodGet, fmt.Sprintf("/api/camundas/%s/backups", instanceID), nil)
		var history []*models.BackupHistory
		readJSON(env.t, resp, &history)
		// Look for a terminal status (not RUNNING)
		for _, h := range history {
			if h.Status == types.BackupStatusCompleted || h.Status == types.BackupStatusFailed || h.Status == types.BackupStatusIncomplete {
				return history
			}
		}
		time.Sleep(300 * time.Millisecond)
	}

	env.t.Fatalf("pollBackupHistoryAll: no terminal backup found within %v", timeout)
	return nil
}

// ===========================================================================
// Test 1: Full backup workflow — happy path
// ===========================================================================

func TestE2E_FullBackupWorkflow(t *testing.T) {
	env := setupTestEnv(t, withPollInterval(100*time.Millisecond), withMaxPollAttempts(20))
	defer env.cleanup()
	defer env.server.Close()

	// Create mock Camunda component servers that succeed
	zeebe := newComponentServer(http.StatusOK, []string{
		`{"state":"IN_PROGRESS"}`,
		`{"state":"COMPLETED"}`,
	})
	defer zeebe.Close()

	operate := newComponentServer(http.StatusOK, []string{
		`{"state":"COMPLETED"}`,
	})
	defer operate.Close()

	tasklist := newComponentServer(http.StatusOK, []string{
		`{"state":"COMPLETED"}`,
	})
	defer tasklist.Close()

	es := newESServer(http.StatusOK, []string{
		"IN_PROGRESS",
		"SUCCESS",
	})
	defer es.Close()

	env.mockServers = append(env.mockServers, zeebe, operate, tasklist, es)

	// Step 1: Create Camunda instance via API
	env.createTestInstance("test-cluster", map[string]string{
		"zeebe":         zeebe.URL(),
		"operate":       operate.URL(),
		"tasklist":      tasklist.URL(),
		"elasticsearch": es.URL(),
	})

	// Step 2: Verify instance was created
	resp := env.apiRequest(http.MethodGet, "/api/camundas/test-cluster", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var instance models.CamundaInstance
	readJSON(t, resp, &instance)
	if instance.ID != "test-cluster" {
		t.Fatalf("expected ID 'test-cluster', got %q", instance.ID)
	}

	// Step 3: Trigger backup via API
	resp = env.apiRequest(http.MethodPost, "/api/camundas/test-cluster/backup", nil)
	if resp.StatusCode != http.StatusAccepted {
		body := readBody(t, resp)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, body)
	}
	var triggerResp BackupTriggerResponse
	readJSON(t, resp, &triggerResp)
	if triggerResp.Status != string(types.BackupStatusRunning) {
		t.Fatalf("expected RUNNING status, got %q", triggerResp.Status)
	}

	// Step 4: Poll for completion
	history := env.pollBackupHistory("test-cluster", types.BackupStatusCompleted, 30*time.Second)

	// Step 5: Verify backup appears in history with correct attributes
	if len(history) == 0 {
		t.Fatal("expected at least one completed backup")
	}
	backup := history[0]
	if backup.CamundaInstanceID != "test-cluster" {
		t.Errorf("expected instance ID 'test-cluster', got %q", backup.CamundaInstanceID)
	}
	if backup.Status != types.BackupStatusCompleted {
		t.Errorf("expected COMPLETED status, got %q", backup.Status)
	}
	if backup.TriggerType != types.TriggerTypeManual {
		t.Errorf("expected MANUAL trigger type, got %q", backup.TriggerType)
	}
	if backup.BackupStats.SuccessfulComponents == 0 {
		t.Error("expected at least one successful component")
	}
	if backup.DurationSeconds == nil {
		t.Error("expected duration to be set")
	}
}

// ===========================================================================
// Test 2: Backup with component failure
// ===========================================================================

func TestE2E_BackupWithComponentFailure(t *testing.T) {
	env := setupTestEnv(t, withPollInterval(100*time.Millisecond), withMaxPollAttempts(20))
	defer env.cleanup()
	defer env.server.Close()

	// Zeebe returns 200 on trigger but fails on poll
	zeebe := newComponentServer(http.StatusOK, []string{
		`{"state":"FAILED"}`,
	})
	defer zeebe.Close()

	// Operate succeeds
	operate := newComponentServer(http.StatusOK, []string{
		`{"state":"COMPLETED"}`,
	})
	defer operate.Close()

	// Tasklist trigger fails immediately with 500
	tasklist := newComponentServer(http.StatusInternalServerError, []string{})
	defer tasklist.Close()

	env.mockServers = append(env.mockServers, zeebe, operate, tasklist)

	// Create instance with only zeebe, operate, tasklist (no ES)
	env.createTestInstance("fail-cluster", map[string]string{
		"zeebe":         zeebe.URL(),
		"operate":       operate.URL(),
		"tasklist":      tasklist.URL(),
		"elasticsearch": "",
	})

	// Trigger backup
	resp := env.apiRequest(http.MethodPost, "/api/camundas/fail-cluster/backup", nil)
	if resp.StatusCode != http.StatusAccepted {
		body := readBody(t, resp)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// The backup has failing components so it should end up FAILED.
	// Note: the orchestrator moves FAILED backups to incomplete. The backup
	// history is first stored as FAILED then moved, so we check incomplete.
	history := env.pollBackupHistoryAll("fail-cluster", 30*time.Second)

	// Find the backup — it may be in FAILED or INCOMPLETE status
	var found *models.BackupHistory
	for _, h := range history {
		if h.Status == types.BackupStatusFailed || h.Status == types.BackupStatusIncomplete {
			found = h
			break
		}
	}
	if found == nil {
		t.Fatal("expected a FAILED or INCOMPLETE backup in history")
	}

	// Verify at least some component details exist
	if found.BackupStats.TotalComponents == 0 {
		t.Error("expected total components > 0")
	}

	// Verify at least one component failed
	hasFailed := false
	for _, comp := range found.Components {
		if comp.Status == types.ComponentStatusFailed {
			hasFailed = true
			break
		}
	}
	if !hasFailed {
		t.Error("expected at least one failed component")
	}
}

// ===========================================================================
// Test 3: Scheduler-triggered backup
// ===========================================================================

func TestE2E_SchedulerTriggeredBackup(t *testing.T) {
	env := setupTestEnv(t, withPollInterval(100*time.Millisecond), withMaxPollAttempts(20))
	defer env.cleanup()
	defer env.server.Close()

	// All components succeed immediately
	zeebe := newComponentServer(http.StatusOK, []string{`{"state":"COMPLETED"}`})
	defer zeebe.Close()
	operate := newComponentServer(http.StatusOK, []string{`{"state":"COMPLETED"}`})
	defer operate.Close()

	env.mockServers = append(env.mockServers, zeebe, operate)

	// Create instance directly via manager so we can control scheduling
	instanceID := "sched-cluster"
	env.createTestInstance(instanceID, map[string]string{
		"zeebe":         zeebe.URL(),
		"operate":       operate.URL(),
		"tasklist":      "",
		"elasticsearch": "",
	})

	// Start the scheduler
	ctx := context.Background()
	if err := env.sched.Start(ctx); err != nil {
		t.Fatalf("sched.Start: %v", err)
	}

	// Register a job with a schedule that's already past due (NextRun in the past)
	// We use a cron expression "* * * * *" (every minute) and manually set NextRun
	// to the past so the scheduler picks it up on its next tick.
	if err := env.sched.RegisterJob(instanceID, "* * * * *", true); err != nil {
		t.Fatalf("RegisterJob: %v", err)
	}

	// Wait for scheduler to execute the job. The scheduler tick interval is
	// 200ms and cron "* * * * *" means NextRun is at most 1 minute from now.
	// Since the scheduler checks every 200ms, the backup should start within
	// ~1 minute. We'll wait up to 90 seconds.
	history := env.pollBackupHistory(instanceID, types.BackupStatusCompleted, 90*time.Second)

	if len(history) == 0 {
		t.Fatal("expected at least one completed backup from scheduler")
	}

	// The trigger type should be SCHEDULED
	foundScheduled := false
	for _, h := range history {
		if h.TriggerType == types.TriggerTypeScheduled {
			foundScheduled = true
			break
		}
	}
	if !foundScheduled {
		t.Error("expected at least one backup with SCHEDULED trigger type")
	}
}

// ===========================================================================
// Test 4: Retention after backup
// ===========================================================================

func TestE2E_RetentionAfterBackup(t *testing.T) {
	env := setupTestEnv(t, withPollInterval(100*time.Millisecond), withMaxPollAttempts(20))
	defer env.cleanup()
	defer env.server.Close()

	// Set up mock servers that always succeed
	zeebe := newComponentServer(http.StatusOK, []string{`{"state":"COMPLETED"}`})
	defer zeebe.Close()

	env.mockServers = append(env.mockServers, zeebe)

	instanceID := "retention-cluster"

	// Create instance with retention_count = 2 (keep only 2 backups)
	instance := map[string]interface{}{
		"id":                      instanceID,
		"name":                    "Retention Test",
		"base_url":                "http://test:8080",
		"enabled":                 true,
		"schedule":                "0 2 * * *",
		"retention_count":         2,
		"success_history_count":   30,
		"failure_history_count":   30,
		"s3_endpoint":             "http://mock-s3",
		"s3_accesskey":            "mock-key",
		"parallel_execution":      false,
		"zeebe_backup_endpoint":   zeebe.URL(),
		"components": []map[string]interface{}{
			{"name": "zeebe", "enabled": true},
			{"name": "operate", "enabled": false},
			{"name": "tasklist", "enabled": false},
			{"name": "optimize", "enabled": false},
			{"name": "elasticsearch", "enabled": false},
		},
	}

	resp := env.apiRequest(http.MethodPost, "/api/camundas", instance)
	if resp.StatusCode != http.StatusCreated {
		body := readBody(t, resp)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Start scheduler so backup lock is managed
	ctx := context.Background()
	if err := env.sched.Start(ctx); err != nil {
		t.Fatalf("sched.Start: %v", err)
	}

	// Run 4 backups sequentially to exceed retention count (keep 2).
	// We must wait for each backup to fully complete AND the backup lock to
	// be released before triggering the next one.
	// NOTE: After each backup, retention runs asynchronously and may move
	// old completed backups to orphaned. So we track completion by checking
	// whether the orchestrator is idle and the scheduler lock is free.
	for i := 0; i < 4; i++ {
		// Reset poll index for mock server so each backup gets COMPLETED
		zeebe.mu.Lock()
		zeebe.pollIndex = 0
		zeebe.mu.Unlock()

		// Wait until the backup lock is free before triggering
		lockDeadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(lockDeadline) {
			if !env.sched.IsBackupInProgress() && !env.orchestratorImpl.IsBackupRunning() {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		resp := env.apiRequest(http.MethodPost, fmt.Sprintf("/api/camundas/%s/backup", instanceID), nil)
		if resp.StatusCode != http.StatusAccepted {
			body := readBody(t, resp)
			t.Fatalf("backup %d: expected 202, got %d: %s", i+1, resp.StatusCode, body)
		}
		resp.Body.Close()

		// Wait for this backup to complete: orchestrator idle + lock released.
		// We can't count completed backups because retention may have already
		// moved older ones to orphaned before we check.
		deadline := time.Now().Add(15 * time.Second)
		completed := false
		for time.Now().Before(deadline) {
			lockFree := !env.sched.IsBackupInProgress()
			orchIdle := !env.orchestratorImpl.IsBackupRunning()
			if lockFree && orchIdle {
				completed = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !completed {
			t.Fatalf("backup %d did not complete in time", i+1)
		}

		// Give retention goroutine time to finish before starting next backup
		time.Sleep(500 * time.Millisecond)

		// Sleep between backups to ensure unique backup IDs (timestamp-based)
		time.Sleep(1100 * time.Millisecond)
	}

	// Give retention goroutine a moment to run after the last backup
	time.Sleep(2 * time.Second)

	// Verify: only 2 completed backups remain in history
	completed, err := env.s3Storage.ListBackupHistory(instanceID, types.BackupStatusCompleted)
	if err != nil {
		t.Fatalf("ListBackupHistory: %v", err)
	}
	if len(completed) > 2 {
		t.Errorf("expected at most 2 completed backups after retention, got %d", len(completed))
	}

	// Verify: orphaned backups exist (excess were moved to orphaned)
	orphaned, err := env.s3Storage.ListOrphanedBackups(instanceID)
	if err != nil {
		t.Fatalf("ListOrphanedBackups: %v", err)
	}
	if len(orphaned) == 0 {
		t.Error("expected some orphaned backups after retention, got 0")
	}

	// Verify via API as well
	resp = env.apiRequest(http.MethodGet, fmt.Sprintf("/api/camundas/%s/backups/orphaned", instanceID), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var orphanedAPI []*models.BackupHistory
	readJSON(t, resp, &orphanedAPI)
	if len(orphanedAPI) == 0 {
		t.Error("expected orphaned backups via API, got 0")
	}
}

// ===========================================================================
// Test 5: Concurrent backup rejection
// ===========================================================================

func TestE2E_ConcurrentBackupRejection(t *testing.T) {
	env := setupTestEnv(t, withPollInterval(100*time.Millisecond), withMaxPollAttempts(50))
	defer env.cleanup()
	defer env.server.Close()

	// Create a slow component that stays IN_PROGRESS for many polls
	zeebe := newComponentServer(http.StatusOK, []string{
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"IN_PROGRESS"}`,
		`{"state":"COMPLETED"}`,
	})
	defer zeebe.Close()

	env.mockServers = append(env.mockServers, zeebe)

	instanceID := "concurrent-cluster"
	env.createTestInstance(instanceID, map[string]string{
		"zeebe":         zeebe.URL(),
		"operate":       "",
		"tasklist":      "",
		"elasticsearch": "",
	})

	// Start scheduler so backup lock is managed
	ctx := context.Background()
	if err := env.sched.Start(ctx); err != nil {
		t.Fatalf("sched.Start: %v", err)
	}

	// Trigger first backup (should succeed)
	resp1 := env.apiRequest(http.MethodPost, fmt.Sprintf("/api/camundas/%s/backup", instanceID), nil)
	if resp1.StatusCode != http.StatusAccepted {
		body := readBody(t, resp1)
		t.Fatalf("first backup: expected 202, got %d: %s", resp1.StatusCode, body)
	}
	resp1.Body.Close()

	// Give a moment for the first backup to start executing
	time.Sleep(200 * time.Millisecond)

	// Trigger second backup while first is still running — should be rejected
	resp2 := env.apiRequest(http.MethodPost, fmt.Sprintf("/api/camundas/%s/backup", instanceID), nil)
	body2 := readBody(t, resp2)

	if resp2.StatusCode != http.StatusConflict {
		t.Errorf("second backup: expected 409 Conflict, got %d: %s", resp2.StatusCode, body2)
	}

	// Verify the error message indicates backup in progress
	if !strings.Contains(body2, "backup") && !strings.Contains(body2, "progress") {
		t.Errorf("expected error message about backup in progress, got: %s", body2)
	}

	// Wait for first backup to eventually complete
	env.pollBackupHistory(instanceID, types.BackupStatusCompleted, 30*time.Second)
}

// ===========================================================================
// Test 6: API health and status endpoints work with full stack
// ===========================================================================

func TestE2E_HealthAndStatusEndpoints(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()
	defer env.server.Close()

	// Start scheduler so status reflects it
	ctx := context.Background()
	if err := env.sched.Start(ctx); err != nil {
		t.Fatalf("sched.Start: %v", err)
	}

	// Test healthz
	resp := env.apiRequest(http.MethodGet, "/healthz", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: expected 200, got %d", resp.StatusCode)
	}
	var healthResp HealthResponse
	readJSON(t, resp, &healthResp)
	if healthResp.Status != "healthy" {
		t.Errorf("expected healthy status, got %q", healthResp.Status)
	}

	// Test readyz
	resp = env.apiRequest(http.MethodGet, "/readyz", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz: expected 200, got %d", resp.StatusCode)
	}
	var readyResp HealthResponse
	readJSON(t, resp, &readyResp)
	if readyResp.Status != "ready" {
		t.Errorf("expected ready status, got %q", readyResp.Status)
	}
	if readyResp.Checks["scheduler"] != "running" {
		t.Errorf("expected scheduler running, got %q", readyResp.Checks["scheduler"])
	}

	// Test system status
	resp = env.apiRequest(http.MethodGet, "/api/status", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: expected 200, got %d", resp.StatusCode)
	}
	var statusResp SystemStatusResponse
	readJSON(t, resp, &statusResp)
	if statusResp.Status != "ok" {
		t.Errorf("expected ok status, got %q", statusResp.Status)
	}
	if !statusResp.Scheduler.Running {
		t.Error("expected scheduler to be running")
	}
}

// ===========================================================================
// Test 7: Full CRUD lifecycle for Camunda instances
// ===========================================================================

func TestE2E_CamundaInstanceCRUDLifecycle(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()
	defer env.server.Close()

	// Start scheduler for job registration
	ctx := context.Background()
	if err := env.sched.Start(ctx); err != nil {
		t.Fatalf("sched.Start: %v", err)
	}

	// CREATE
	env.createTestInstance("crud-cluster", map[string]string{
		"zeebe":         "http://fake-zeebe",
		"operate":       "",
		"tasklist":      "",
		"elasticsearch": "",
	})

	// READ
	resp := env.apiRequest(http.MethodGet, "/api/camundas/crud-cluster", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: expected 200, got %d", resp.StatusCode)
	}
	var inst models.CamundaInstance
	readJSON(t, resp, &inst)
	if inst.Name != "Test Instance crud-cluster" {
		t.Errorf("expected name 'Test Instance crud-cluster', got %q", inst.Name)
	}

	// LIST
	resp = env.apiRequest(http.MethodGet, "/api/camundas", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("LIST: expected 200, got %d", resp.StatusCode)
	}
	var instances []models.CamundaInstance
	readJSON(t, resp, &instances)
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}

	// UPDATE
	update := map[string]interface{}{
		"id":                "crud-cluster",
		"name":              "Updated Instance",
		"base_url":          "http://updated:8080",
		"enabled":           true,
		"schedule":          "0 3 * * *",
		"retention_count":   5,
		"success_history_count": 20,
		"failure_history_count": 20,
		"s3_endpoint":       "http://mock-s3",
		"s3_accesskey":      "mock",
		"zeebe_backup_endpoint": "http://fake-zeebe",
		"components": []map[string]interface{}{
			{"name": "zeebe", "enabled": true},
			{"name": "operate", "enabled": false},
			{"name": "tasklist", "enabled": false},
			{"name": "optimize", "enabled": false},
			{"name": "elasticsearch", "enabled": false},
		},
	}
	resp = env.apiRequest(http.MethodPut, "/api/camundas/crud-cluster", update)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("UPDATE: expected 200, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Verify update
	resp = env.apiRequest(http.MethodGet, "/api/camundas/crud-cluster", nil)
	readJSON(t, resp, &inst)
	if inst.Name != "Updated Instance" {
		t.Errorf("expected updated name, got %q", inst.Name)
	}

	// DISABLE
	resp = env.apiRequest(http.MethodPost, "/api/camundas/crud-cluster/disable", nil)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("DISABLE: expected 200, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	resp = env.apiRequest(http.MethodGet, "/api/camundas/crud-cluster", nil)
	readJSON(t, resp, &inst)
	if inst.Enabled {
		t.Error("expected instance to be disabled")
	}

	// ENABLE
	resp = env.apiRequest(http.MethodPost, "/api/camundas/crud-cluster/enable", nil)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("ENABLE: expected 200, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	resp = env.apiRequest(http.MethodGet, "/api/camundas/crud-cluster", nil)
	readJSON(t, resp, &inst)
	if !inst.Enabled {
		t.Error("expected instance to be enabled")
	}

	// DELETE
	resp = env.apiRequest(http.MethodDelete, "/api/camundas/crud-cluster", nil)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("DELETE: expected 200, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Verify deletion
	resp = env.apiRequest(http.MethodGet, "/api/camundas/crud-cluster", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected 404 after delete, got %d", resp.StatusCode)
	}
	resp.Body.Close()
}

// ===========================================================================
// Test 8: Backup log file access after backup
// ===========================================================================

func TestE2E_BackupLogFileAccess(t *testing.T) {
	env := setupTestEnv(t, withPollInterval(100*time.Millisecond), withMaxPollAttempts(20))
	defer env.cleanup()
	defer env.server.Close()

	zeebe := newComponentServer(http.StatusOK, []string{`{"state":"COMPLETED"}`})
	defer zeebe.Close()

	env.mockServers = append(env.mockServers, zeebe)

	instanceID := "log-cluster"
	env.createTestInstance(instanceID, map[string]string{
		"zeebe":         zeebe.URL(),
		"operate":       "",
		"tasklist":      "",
		"elasticsearch": "",
	})

	// Trigger backup
	resp := env.apiRequest(http.MethodPost, fmt.Sprintf("/api/camundas/%s/backup", instanceID), nil)
	if resp.StatusCode != http.StatusAccepted {
		body := readBody(t, resp)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Wait for completion
	history := env.pollBackupHistory(instanceID, types.BackupStatusCompleted, 30*time.Second)
	backupID := history[0].BackupID

	// Access the backup log file via API
	resp = env.apiRequest(http.MethodGet, fmt.Sprintf("/api/camundas/%s/backups/%s/logs", instanceID, backupID), nil)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("GET logs: expected 200, got %d: %s", resp.StatusCode, body)
	}

	logContent := readBody(t, resp)
	if logContent == "" {
		t.Error("expected non-empty log content")
	}
	// The log file should contain the backup ID
	if !strings.Contains(logContent, backupID) {
		t.Errorf("expected log to contain backup ID %q", backupID)
	}
}

// ===========================================================================
// Test 9: Parallel execution mode backup
// ===========================================================================

func TestE2E_ParallelExecutionBackup(t *testing.T) {
	env := setupTestEnv(t, withPollInterval(100*time.Millisecond), withMaxPollAttempts(20))
	defer env.cleanup()
	defer env.server.Close()

	// Track that both components are called concurrently
	var zeebeTriggered, operateTriggered atomic.Int32

	zeebeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			zeebeTriggered.Add(1)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"accepted":true}`))
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"state":"COMPLETED"}`))
		}
	}))
	defer zeebeSrv.Close()

	operateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			operateTriggered.Add(1)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"accepted":true}`))
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"state":"COMPLETED"}`))
		}
	}))
	defer operateSrv.Close()

	instanceID := "parallel-cluster"

	// Create instance with parallel_execution=true
	instance := map[string]interface{}{
		"id":                       instanceID,
		"name":                     "Parallel Test",
		"base_url":                 "http://test:8080",
		"enabled":                  true,
		"schedule":                 "0 2 * * *",
		"retention_count":          7,
		"success_history_count":    30,
		"failure_history_count":    30,
		"s3_endpoint":              "http://mock-s3",
		"s3_accesskey":             "mock-key",
		"parallel_execution":       true,
		"zeebe_backup_endpoint":    zeebeSrv.URL,
		"operate_backup_endpoint":  operateSrv.URL,
		"components": []map[string]interface{}{
			{"name": "zeebe", "enabled": true},
			{"name": "operate", "enabled": true},
			{"name": "tasklist", "enabled": false},
			{"name": "optimize", "enabled": false},
			{"name": "elasticsearch", "enabled": false},
		},
	}

	resp := env.apiRequest(http.MethodPost, "/api/camundas", instance)
	if resp.StatusCode != http.StatusCreated {
		body := readBody(t, resp)
		t.Fatalf("expected 201, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Trigger backup
	resp = env.apiRequest(http.MethodPost, fmt.Sprintf("/api/camundas/%s/backup", instanceID), nil)
	if resp.StatusCode != http.StatusAccepted {
		body := readBody(t, resp)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	// Wait for completion
	history := env.pollBackupHistory(instanceID, types.BackupStatusCompleted, 30*time.Second)

	// Verify both components were triggered
	if zeebeTriggered.Load() == 0 {
		t.Error("zeebe was never triggered")
	}
	if operateTriggered.Load() == 0 {
		t.Error("operate was never triggered")
	}

	// Verify backup metadata indicates parallel execution
	backup := history[0]
	if backup.Metadata.ExecutionMode != "parallel" {
		t.Errorf("expected parallel execution mode, got %q", backup.Metadata.ExecutionMode)
	}
}

// ===========================================================================
// Test 10: CSRF protection enforced
// ===========================================================================

func TestE2E_CSRFProtection(t *testing.T) {
	env := setupTestEnv(t)
	defer env.cleanup()
	defer env.server.Close()

	// Make a POST request WITHOUT the X-Requested-With header
	body := `{"id":"csrf-test","name":"CSRF Test","base_url":"http://test"}`
	req, _ := http.NewRequest(http.MethodPost, env.server.URL+"/api/camundas", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	// Deliberately NOT setting X-Requested-With

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden without CSRF header, got %d", resp.StatusCode)
	}

	respBody := readBody(t, resp)
	if !strings.Contains(respBody, "csrf") {
		t.Errorf("expected CSRF error message, got: %s", respBody)
	}
}
