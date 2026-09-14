package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
	"github.com/aitasadduq/camunda-backup-dr/pkg/types"
)

// recordingNotifier captures what the orchestrator asked to send.
type recordingNotifier struct {
	mutex    sync.Mutex
	requests []models.NotificationRequest
	messages []string
	err      error
}

func (r *recordingNotifier) Send(ctx context.Context, req models.NotificationRequest, message string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.requests = append(r.requests, req)
	r.messages = append(r.messages, message)
	return r.err
}

func (r *recordingNotifier) calls() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return len(r.requests)
}

// componentServer replies to component backup triggers and status polls.
func componentServer(t *testing.T, state string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		case http.MethodGet:
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": state})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func notifyingInstance(t *testing.T, state string) *models.CamundaInstance {
	t.Helper()
	server := componentServer(t, state)
	instance := setupTestInstance("test-instance", "Test Instance")
	instance.ZeebeBackupEndpoint = server.URL + "/zeebe/backup"
	instance.OperateBackupEndpoint = server.URL + "/operate/backup"
	instance.TasklistBackupEndpoint = server.URL + "/tasklist/backup"
	instance.Notifications = models.NotificationConfig{
		OnSuccess: models.NotificationRequest{
			Enabled:      true,
			Method:       "POST",
			URL:          "https://hooks.example.com:8443/success",
			MessageField: "text",
		},
		OnFailure: models.NotificationRequest{
			Enabled:      true,
			Method:       "POST",
			URL:          "https://hooks.example.com:8443/failure",
			MessageField: "text",
		},
	}
	return instance
}

// notifyingOrchestrator builds an orchestrator for these tests. A nil notifier
// leaves the feature unwired, which is the default deployment shape.
func notifyingOrchestrator(s3 storage.S3Storage, notifier Notifier) *Orchestrator {
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	o := NewOrchestrator(newMockFileStorage(), s3, httpClient,
		setupTestConfig(), utils.NewLogger("test"), 50*time.Millisecond, 3)
	if notifier != nil {
		o.SetNotifier(notifier)
	}
	return o
}

func executeNotifyingBackup(t *testing.T, o *Orchestrator, instance *models.CamundaInstance) *models.BackupExecution {
	t.Helper()
	execution, err := o.ExecuteBackup(context.Background(), BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test backup",
	})
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	return execution
}

func TestExecuteBackup_NotifiesOnSuccess(t *testing.T) {
	notifier := &recordingNotifier{}
	o := notifyingOrchestrator(newMockS3Storage(), notifier)
	instance := notifyingInstance(t, "COMPLETED")

	execution := executeNotifyingBackup(t, o, instance)

	if execution.Status != types.BackupStatusCompleted {
		t.Fatalf("Expected status COMPLETED, got: %s", execution.Status)
	}
	if notifier.calls() != 1 {
		t.Fatalf("Expected exactly 1 notification, got %d", notifier.calls())
	}
	if got := notifier.requests[0].URL; got != instance.Notifications.OnSuccess.URL {
		t.Errorf("Expected the success endpoint, got %s", got)
	}

	message := notifier.messages[0]
	for _, want := range []string{execution.BackupID, instance.Name, instance.ID, "completed successfully"} {
		if !strings.Contains(message, want) {
			t.Errorf("Expected message to contain %q, got: %s", want, message)
		}
	}
}

func TestExecuteBackup_NotifiesOnFailure(t *testing.T) {
	notifier := &recordingNotifier{}
	o := notifyingOrchestrator(newMockS3Storage(), notifier)
	instance := notifyingInstance(t, "FAILED")

	execution := executeNotifyingBackup(t, o, instance)

	if execution.Status != types.BackupStatusFailed {
		t.Fatalf("Expected status FAILED, got: %s", execution.Status)
	}
	if notifier.calls() != 1 {
		t.Fatalf("Expected exactly 1 notification, got %d", notifier.calls())
	}
	if got := notifier.requests[0].URL; got != instance.Notifications.OnFailure.URL {
		t.Errorf("Expected the failure endpoint, got %s", got)
	}

	message := notifier.messages[0]
	for _, want := range []string{execution.BackupID, string(types.BackupStatusFailed), "zeebe"} {
		if !strings.Contains(message, want) {
			t.Errorf("Expected message to contain %q, got: %s", want, message)
		}
	}
}

func TestExecuteBackup_NotifiesWhenBackupFailsBeforeComponentsRun(t *testing.T) {
	notifier := &recordingNotifier{}
	s3 := newFailingS3Storage(false, false)
	s3.failStoreLatestBackupID = true
	o := notifyingOrchestrator(s3, notifier)
	instance := notifyingInstance(t, "COMPLETED")

	execution, err := o.ExecuteBackup(context.Background(), BackupRequest{
		CamundaInstance: instance,
		TriggerType:     types.TriggerTypeManual,
		BackupReason:    "Test backup",
	})
	if err == nil {
		t.Fatal("Expected an error when the backup ID cannot be stored")
	}
	if execution.Status != types.BackupStatusFailed {
		t.Fatalf("Expected status FAILED, got: %s", execution.Status)
	}
	if notifier.calls() != 1 {
		t.Fatalf("Expected a failure notification for a backup that never started, got %d", notifier.calls())
	}
	if got := notifier.requests[0].URL; got != instance.Notifications.OnFailure.URL {
		t.Errorf("Expected the failure endpoint, got %s", got)
	}
}

func TestExecuteBackup_SkipsDisabledNotification(t *testing.T) {
	notifier := &recordingNotifier{}
	o := notifyingOrchestrator(newMockS3Storage(), notifier)
	instance := notifyingInstance(t, "COMPLETED")
	instance.Notifications.OnSuccess.Enabled = false

	executeNotifyingBackup(t, o, instance)

	if notifier.calls() != 0 {
		t.Errorf("Expected no notification when the success request is disabled, got %d", notifier.calls())
	}
}

func TestExecuteBackup_NotificationFailureDoesNotChangeResult(t *testing.T) {
	notifier := &recordingNotifier{err: fmt.Errorf("endpoint unreachable")}
	o := notifyingOrchestrator(newMockS3Storage(), notifier)
	instance := notifyingInstance(t, "COMPLETED")

	execution := executeNotifyingBackup(t, o, instance)

	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected a failed notification to leave the backup COMPLETED, got: %s", execution.Status)
	}
	if notifier.calls() != 1 {
		t.Errorf("Expected exactly 1 notification attempt, got %d", notifier.calls())
	}
}

func TestExecuteBackup_NoNotifierConfigured(t *testing.T) {
	o := notifyingOrchestrator(newMockS3Storage(), nil)
	instance := notifyingInstance(t, "COMPLETED")

	execution := executeNotifyingBackup(t, o, instance)

	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}
}

func TestExecuteBackup_NotifiesOnIncomplete(t *testing.T) {
	notifier := &recordingNotifier{}
	o := notifyingOrchestrator(newMockS3Storage(), notifier)
	instance := notifyingInstance(t, "COMPLETED")
	for i := range instance.Components {
		instance.Components[i].Enabled = false
	}

	execution := executeNotifyingBackup(t, o, instance)

	if execution.Status != types.BackupStatusIncomplete {
		t.Fatalf("Expected status INCOMPLETE, got: %s", execution.Status)
	}
	if notifier.calls() != 1 {
		t.Fatalf("Expected exactly 1 notification, got %d", notifier.calls())
	}
	if got := notifier.requests[0].URL; got != instance.Notifications.OnFailure.URL {
		t.Errorf("Expected the failure endpoint for an INCOMPLETE backup, got %s", got)
	}
	for _, want := range []string{string(types.BackupStatusIncomplete), "No components were executed"} {
		if !strings.Contains(notifier.messages[0], want) {
			t.Errorf("Expected message to contain %q, got: %s", want, notifier.messages[0])
		}
	}
}

func TestNotificationMessage(t *testing.T) {
	req := BackupRequest{CamundaInstance: setupTestInstance("inst-a", "Prod")}

	completed := models.NewBackupExecution("inst-a", "b-0")
	completed.Status = types.BackupStatusCompleted

	failing := models.NewBackupExecution("inst-a", "b-1")
	failing.Status = types.BackupStatusFailed
	for _, c := range []string{types.ComponentZeebe, types.ComponentTasklist, types.ComponentOperate} {
		failing.UpdateComponentStatus(c, types.ComponentStatusFailed)
	}
	failing.UpdateComponentStatus(types.ComponentOptimize, types.ComponentStatusCompleted)

	withReason := models.NewBackupExecution("inst-a", "b-2")
	withReason.Status = types.BackupStatusIncomplete
	withReason.ErrorMessage = "No components were executed"
	withReason.UpdateComponentStatus(types.ComponentZeebe, types.ComponentStatusFailed)

	bare := models.NewBackupExecution("inst-a", "b-3")
	bare.Status = types.BackupStatusFailed

	unfinished := models.NewBackupExecution("inst-a", "b-4")
	unfinished.Status = types.BackupStatusIncomplete
	unfinished.UpdateComponentStatus(types.ComponentZeebe, types.ComponentStatusCompleted)
	unfinished.UpdateComponentStatus(types.ComponentTasklist, types.ComponentStatusRunning)
	unfinished.UpdateComponentStatus(types.ComponentOperate, types.ComponentStatusPending)

	tests := []struct {
		name      string
		execution *models.BackupExecution
		want      string
	}{
		{"completed", completed, "Backup b-0 of Camunda instance Prod (inst-a) completed successfully."},
		{"failed components are sorted", failing, "Backup b-1 of Camunda instance Prod (inst-a) finished with status FAILED: failed components: operate, tasklist, zeebe"},
		{"recorded error wins over component list", withReason, "Backup b-2 of Camunda instance Prod (inst-a) finished with status INCOMPLETE: No components were executed"},
		{"nothing recorded", bare, "Backup b-3 of Camunda instance Prod (inst-a) finished with status FAILED: no reason recorded"},
		{"interrupted backup names what never finished", unfinished, "Backup b-4 of Camunda instance Prod (inst-a) finished with status INCOMPLETE: unfinished components: operate, tasklist"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := notificationMessage(req, tt.execution); got != tt.want {
				t.Errorf("got  %q\nwant %q", got, tt.want)
			}
		})
	}
}

func TestExecuteBackup_ResumeFailureIsFailedAndNotifiedAsFailure(t *testing.T) {
	// Components complete, pause succeeds, resume does not. The exporter is
	// left paused, so the backup is FAILED even though every component says
	// COMPLETED, and the failure endpoint is the one that hears about it.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/actuator/exporting/pause":
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 204})
		case r.URL.Path == "/actuator/exporting/resume":
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": 500})
		case r.Method == http.MethodPost:
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"message": "Backup triggered"})
		default:
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(map[string]string{"state": "COMPLETED"})
		}
	}))
	t.Cleanup(server.Close)

	notifier := &recordingNotifier{}
	s3 := newMockS3Storage()
	// Short retry budgets: the resume call is meant to fail, and every retry
	// tier (HTTP client and exporter loop) would otherwise back off for seconds.
	httpClient := camunda.NewHTTPClient(camunda.HTTPClientConfig{
		Timeout:       5 * time.Second,
		MaxRetries:    1,
		RetryDelay:    50 * time.Millisecond,
		MaxRetryDelay: 100 * time.Millisecond,
	}, utils.NewLogger("test"))
	cfg := setupTestConfig()
	cfg.ExporterPauseMaxRetries = 1
	cfg.ExporterPauseRetryDelay = 1
	o := NewOrchestrator(newMockFileStorage(), s3, httpClient, cfg, utils.NewLogger("test"), 50*time.Millisecond, 3)
	o.SetNotifier(notifier)

	instance := notifyingInstance(t, "COMPLETED")
	instance.ExportingEndpoint = server.URL + "/actuator/exporting"

	execution := executeNotifyingBackup(t, o, instance)

	if execution.Status != types.BackupStatusFailed {
		t.Fatalf("Expected a backup whose exporter would not resume to be FAILED, got: %s", execution.Status)
	}
	for comp, status := range execution.ComponentStatus {
		if status != types.ComponentStatusCompleted {
			t.Errorf("Expected component %s COMPLETED (the failure is the exporter, not a component), got %s", comp, status)
		}
	}
	if stored, _ := s3.GetBackupHistory(instance.ID, execution.BackupID); stored == nil || stored.Status != types.BackupStatusFailed {
		t.Errorf("Expected the stored record to say FAILED, got %+v", stored)
	}
	if notifier.calls() != 1 {
		t.Fatalf("Expected exactly 1 notification, got %d", notifier.calls())
	}
	if got := notifier.requests[0].URL; got != instance.Notifications.OnFailure.URL {
		t.Errorf("Expected the failure endpoint, got %s", got)
	}
	if !strings.Contains(notifier.messages[0], "exporter resume failed") {
		t.Errorf("Expected the message to say why, got: %s", notifier.messages[0])
	}
}

func TestExecuteBackup_LastBackupPersistedBeforeNotification(t *testing.T) {
	var order []string
	var mu sync.Mutex
	record := func(step string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, step)
	}

	var o *Orchestrator
	notifier := &orderingNotifier{onSend: func() {
		record("notify")
		if o.IsBackupRunning() {
			record("slot-still-held")
		}
	}}
	o = notifyingOrchestrator(newMockS3Storage(), notifier)
	o.SetLastBackupFunc(func(string, time.Time, string) { record("last-backup") })
	instance := notifyingInstance(t, "COMPLETED")

	executeNotifyingBackup(t, o, instance)

	// The webhook runs after the status is persisted and after the backup slot
	// is released, so a slow endpoint neither delays the UI nor makes the
	// post-backup sweep think a backup is still in flight.
	mu.Lock()
	defer mu.Unlock()
	if len(order) != 2 || order[0] != "last-backup" || order[1] != "notify" {
		t.Errorf("Expected last-backup persisted, then notify, with the slot released; got %v", order)
	}
}

// orderingNotifier records when it was called relative to other callbacks.
type orderingNotifier struct {
	onSend func()
}

func (n *orderingNotifier) Send(context.Context, models.NotificationRequest, string) error {
	n.onSend()
	return nil
}
