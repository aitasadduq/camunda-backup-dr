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

// failingBackupIDStorage fails the initial backup ID write, which fails a backup
// before any component runs.
type failingBackupIDStorage struct {
	*mockS3Storage
}

func (f *failingBackupIDStorage) StoreLatestBackupID(camundaInstanceID, backupID string) error {
	return fmt.Errorf("simulated S3 failure")
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

func notifyingOrchestrator(notifier Notifier) *Orchestrator {
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	o := NewOrchestrator(newMockFileStorage(), newMockS3Storage(), httpClient,
		setupTestConfig(), utils.NewLogger("test"), 50*time.Millisecond, 3)
	o.SetNotifier(notifier)
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
	o := notifyingOrchestrator(notifier)
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
	o := notifyingOrchestrator(notifier)
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
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	o := NewOrchestrator(newMockFileStorage(), &failingBackupIDStorage{newMockS3Storage()}, httpClient,
		setupTestConfig(), utils.NewLogger("test"), 50*time.Millisecond, 3)
	o.SetNotifier(notifier)
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
	o := notifyingOrchestrator(notifier)
	instance := notifyingInstance(t, "COMPLETED")
	instance.Notifications.OnSuccess.Enabled = false

	executeNotifyingBackup(t, o, instance)

	if notifier.calls() != 0 {
		t.Errorf("Expected no notification when the success request is disabled, got %d", notifier.calls())
	}
}

func TestExecuteBackup_NotificationFailureDoesNotChangeResult(t *testing.T) {
	notifier := &recordingNotifier{err: fmt.Errorf("endpoint unreachable")}
	o := notifyingOrchestrator(notifier)
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
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), utils.NewLogger("test"))
	o := NewOrchestrator(newMockFileStorage(), newMockS3Storage(), httpClient,
		setupTestConfig(), utils.NewLogger("test"), 50*time.Millisecond, 3)
	instance := notifyingInstance(t, "COMPLETED")

	execution := executeNotifyingBackup(t, o, instance)

	if execution.Status != types.BackupStatusCompleted {
		t.Errorf("Expected status COMPLETED, got: %s", execution.Status)
	}
}
