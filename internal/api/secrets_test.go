package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/config"
	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/secrets"
	"github.com/aitasadduq/camunda-backup-dr/internal/storage"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// secretTestEnv wires real file storage, a real Camunda manager and a real
// secret store behind the handlers, so the full persistence path is exercised.
type secretTestEnv struct {
	handlers *Handlers
	cfg      *config.Config
	store    *secrets.Store
	dataDir  string
}

func newSecretTestEnv(t *testing.T) *secretTestEnv {
	t.Helper()

	dataDir := t.TempDir()
	logger := utils.NewLogger("error")
	cfg := &config.Config{DataDir: dataDir}

	fileStorage, err := storage.NewFileStorage(dataDir, cfg, logger)
	if err != nil {
		t.Fatalf("NewFileStorage() error = %v", err)
	}
	store, err := secrets.NewStore(dataDir, logger)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	cfg.SetSecretProvider(store)

	manager := camunda.NewManager(fileStorage, logger)
	handlers := NewHandlers(manager, &mockOrchestrator{}, &mockHistoryProvider{}, &mockScheduler{running: true}, &mockRetentionManager{}, fileStorage, logger, cfg)
	handlers.SetSecretStore(store)

	return &secretTestEnv{handlers: handlers, cfg: cfg, store: store, dataDir: dataDir}
}

func (e *secretTestEnv) createInstance(t *testing.T, payload map[string]interface{}) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/camundas", bytes.NewReader(body))
	w := httptest.NewRecorder()
	e.handlers.CreateCamundaInstanceHandler(w, req)
	return w
}

func (e *secretTestEnv) updateInstance(t *testing.T, id string, payload map[string]interface{}) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}
	req := httptest.NewRequest(http.MethodPut, "/api/camundas/"+id, bytes.NewReader(body))
	w := httptest.NewRecorder()
	e.handlers.UpdateCamundaInstanceHandler(w, req)
	return w
}

func (e *secretTestEnv) getInstance(t *testing.T, id string) models.CamundaInstance {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/camundas/"+id, nil)
	w := httptest.NewRecorder()
	e.handlers.GetCamundaInstanceHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GetCamundaInstanceHandler status = %d, body = %s", w.Code, w.Body.String())
	}
	var instance models.CamundaInstance
	if err := json.Unmarshal(w.Body.Bytes(), &instance); err != nil {
		t.Fatalf("failed to unmarshal instance: %v", err)
	}
	return instance
}

func (e *secretTestEnv) listInstances(t *testing.T) []models.CamundaInstance {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/camundas", nil)
	w := httptest.NewRecorder()
	e.handlers.ListCamundaInstancesHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ListCamundaInstancesHandler status = %d, body = %s", w.Code, w.Body.String())
	}
	var instances []models.CamundaInstance
	if err := json.Unmarshal(w.Body.Bytes(), &instances); err != nil {
		t.Fatalf("failed to unmarshal instances: %v", err)
	}
	return instances
}

func (e *secretTestEnv) deleteInstance(t *testing.T, id string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, "/api/camundas/"+id, nil)
	w := httptest.NewRecorder()
	e.handlers.DeleteCamundaInstanceHandler(w, req)
	return w
}

func (e *secretTestEnv) readFile(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(e.dataDir, name))
	if err != nil {
		t.Fatalf("failed to read %s: %v", name, err)
	}
	return string(data)
}

func instancePayload(id, name string) map[string]interface{} {
	return map[string]interface{}{
		"id":                     id,
		"name":                   name,
		"base_url":               "https://" + id + ".example.com",
		"enabled":                true,
		"schedule":               "0 2 * * *",
		"elasticsearch_endpoint": "https://es-" + id + ".example.com:9200",
		"elasticsearch_username": "elastic",
		"s3_endpoint":            "https://s3.example.com",
		"s3_accesskey":           "AKIA" + strings.ToUpper(strings.ReplaceAll(id, "-", "")),
		"components": []map[string]interface{}{
			{"name": "zeebe", "enabled": true},
			{"name": "elasticsearch", "enabled": true},
		},
	}
}

// setupTwoInstances creates camunda-a and camunda-b, each with its own
// UI-entered Elasticsearch password and S3 secret key.
func setupTwoInstances(t *testing.T) *secretTestEnv {
	t.Helper()
	env := newSecretTestEnv(t)

	a := instancePayload("camunda-a", "Camunda A")
	a["elasticsearch_password"] = "es-pw-a"
	a["s3_secret_key"] = "s3-key-a"
	if w := env.createInstance(t, a); w.Code != http.StatusCreated {
		t.Fatalf("create camunda-a status = %d, body = %s", w.Code, w.Body.String())
	}

	b := instancePayload("camunda-b", "Camunda B")
	b["elasticsearch_password"] = "es-pw-b"
	b["s3_secret_key"] = "s3-key-b"
	if w := env.createInstance(t, b); w.Code != http.StatusCreated {
		t.Fatalf("create camunda-b status = %d, body = %s", w.Code, w.Body.String())
	}

	return env
}

func TestCreateInstancesWithUISecrets(t *testing.T) {
	env := setupTwoInstances(t)

	// Each instance resolves its own credentials
	if got := env.cfg.GetElasticsearchPassword("camunda-a"); got != "es-pw-a" {
		t.Errorf("camunda-a ES password = %q, want %q", got, "es-pw-a")
	}
	if got := env.cfg.GetElasticsearchPassword("camunda-b"); got != "es-pw-b" {
		t.Errorf("camunda-b ES password = %q, want %q", got, "es-pw-b")
	}
	if got := env.cfg.GetS3SecretKey("camunda-a"); got != "s3-key-a" {
		t.Errorf("camunda-a S3 secret key = %q, want %q", got, "s3-key-a")
	}
	if got := env.cfg.GetS3SecretKey("camunda-b"); got != "s3-key-b" {
		t.Errorf("camunda-b S3 secret key = %q, want %q", got, "s3-key-b")
	}
}

func TestSecretsAreNeverWrittenToConfigJSON(t *testing.T) {
	env := setupTwoInstances(t)

	configJSON := env.readFile(t, "config.json")
	for _, secret := range []string{"es-pw-a", "es-pw-b", "s3-key-a", "s3-key-b"} {
		if strings.Contains(configJSON, secret) {
			t.Errorf("config.json leaked credential %q", secret)
		}
	}
	for _, field := range []string{"elasticsearch_password", "s3_secret_key"} {
		if strings.Contains(configJSON, `"`+field+`"`) {
			t.Errorf("config.json contains credential field %q", field)
		}
	}

	// The credentials live in the secrets file instead
	secretsJSON := env.readFile(t, "secrets.json")
	for _, secret := range []string{"es-pw-a", "es-pw-b", "s3-key-a", "s3-key-b"} {
		if !strings.Contains(secretsJSON, secret) {
			t.Errorf("secrets.json is missing credential %q", secret)
		}
	}
}

func TestAPIReportsSecretsAsSetWithoutRevealingThem(t *testing.T) {
	env := setupTwoInstances(t)

	for _, id := range []string{"camunda-a", "camunda-b"} {
		instance := env.getInstance(t, id)
		if !instance.ElasticsearchPasswordSet {
			t.Errorf("%s: expected elasticsearch_password_set to be true", id)
		}
		if !instance.BackupIDS3SecretKeySet {
			t.Errorf("%s: expected s3_secret_key_set to be true", id)
		}
		if instance.ElasticsearchPassword != nil || instance.BackupIDS3SecretKey != nil {
			t.Errorf("%s: credential values must never be returned", id)
		}
	}

	// The raw list response body must not contain any credential value
	req := httptest.NewRequest(http.MethodGet, "/api/camundas", nil)
	w := httptest.NewRecorder()
	env.handlers.ListCamundaInstancesHandler(w, req)
	body := w.Body.String()
	for _, secret := range []string{"es-pw-a", "es-pw-b", "s3-key-a", "s3-key-b"} {
		if strings.Contains(body, secret) {
			t.Errorf("list response leaked credential %q", secret)
		}
	}

	instances := env.listInstances(t)
	if len(instances) != 2 {
		t.Fatalf("expected 2 instances, got %d", len(instances))
	}
	for _, instance := range instances {
		if !instance.ElasticsearchPasswordSet || !instance.BackupIDS3SecretKeySet {
			t.Errorf("%s: expected both credentials to be reported as set", instance.ID)
		}
	}
}

func TestCreateResponseDoesNotLeakSecrets(t *testing.T) {
	env := newSecretTestEnv(t)

	payload := instancePayload("camunda-a", "Camunda A")
	payload["elasticsearch_password"] = "es-pw-a"
	payload["s3_secret_key"] = "s3-key-a"

	w := env.createInstance(t, payload)
	if w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if strings.Contains(body, "es-pw-a") || strings.Contains(body, "s3-key-a") {
		t.Errorf("create response leaked a credential: %s", body)
	}
	if !strings.Contains(body, `"elasticsearch_password_set":true`) {
		t.Errorf("create response should report the password as set: %s", body)
	}
}

func TestUpdateWithoutSecretFieldsLeavesThemUnchanged(t *testing.T) {
	env := setupTwoInstances(t)

	payload := instancePayload("camunda-a", "Camunda A Renamed")
	if w := env.updateInstance(t, "camunda-a", payload); w.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", w.Code, w.Body.String())
	}

	if got := env.getInstance(t, "camunda-a").Name; got != "Camunda A Renamed" {
		t.Errorf("name = %q, want %q", got, "Camunda A Renamed")
	}
	if got := env.cfg.GetElasticsearchPassword("camunda-a"); got != "es-pw-a" {
		t.Errorf("ES password = %q, want it unchanged (%q)", got, "es-pw-a")
	}
	if got := env.cfg.GetS3SecretKey("camunda-a"); got != "s3-key-a" {
		t.Errorf("S3 secret key = %q, want it unchanged (%q)", got, "s3-key-a")
	}
}

func TestUpdateReplacesSecretsForOneInstanceOnly(t *testing.T) {
	env := setupTwoInstances(t)

	payload := instancePayload("camunda-a", "Camunda A")
	payload["elasticsearch_password"] = "es-pw-a-rotated"
	payload["s3_secret_key"] = "s3-key-a-rotated"
	if w := env.updateInstance(t, "camunda-a", payload); w.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", w.Code, w.Body.String())
	}

	if got := env.cfg.GetElasticsearchPassword("camunda-a"); got != "es-pw-a-rotated" {
		t.Errorf("camunda-a ES password = %q, want %q", got, "es-pw-a-rotated")
	}
	if got := env.cfg.GetS3SecretKey("camunda-a"); got != "s3-key-a-rotated" {
		t.Errorf("camunda-a S3 secret key = %q, want %q", got, "s3-key-a-rotated")
	}
	if got := env.cfg.GetElasticsearchPassword("camunda-b"); got != "es-pw-b" {
		t.Errorf("camunda-b ES password = %q, want it untouched (%q)", got, "es-pw-b")
	}
	if got := env.cfg.GetS3SecretKey("camunda-b"); got != "s3-key-b" {
		t.Errorf("camunda-b S3 secret key = %q, want it untouched (%q)", got, "s3-key-b")
	}
}

func TestUpdateWithEmptySecretClearsIt(t *testing.T) {
	env := setupTwoInstances(t)

	payload := instancePayload("camunda-a", "Camunda A")
	payload["elasticsearch_password"] = ""
	if w := env.updateInstance(t, "camunda-a", payload); w.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", w.Code, w.Body.String())
	}

	instance := env.getInstance(t, "camunda-a")
	if instance.ElasticsearchPasswordSet {
		t.Error("expected elasticsearch_password_set to be false after clearing")
	}
	if !instance.BackupIDS3SecretKeySet {
		t.Error("clearing the password must not clear the S3 secret key")
	}
	if got := env.cfg.GetElasticsearchPassword("camunda-a"); got != "" {
		t.Errorf("ES password = %q, want it cleared", got)
	}
	if got := env.cfg.GetElasticsearchPassword("camunda-b"); got != "es-pw-b" {
		t.Errorf("camunda-b ES password = %q, want it untouched", got)
	}
}

func TestInstanceEnvVarStillOverridesUISecret(t *testing.T) {
	env := setupTwoInstances(t)

	t.Setenv("ELASTICSEARCH_PASSWORD_CAMUNDA_A", "env-pw-a")
	t.Setenv("S3_SECRETKEY_CAMUNDA_A", "env-key-a")

	if got := env.cfg.GetElasticsearchPassword("camunda-a"); got != "env-pw-a" {
		t.Errorf("camunda-a ES password = %q, want the env var value %q", got, "env-pw-a")
	}
	if got := env.cfg.GetS3SecretKey("camunda-a"); got != "env-key-a" {
		t.Errorf("camunda-a S3 secret key = %q, want the env var value %q", got, "env-key-a")
	}
	// The second instance keeps resolving its UI-entered credentials
	if got := env.cfg.GetElasticsearchPassword("camunda-b"); got != "es-pw-b" {
		t.Errorf("camunda-b ES password = %q, want %q", got, "es-pw-b")
	}
	if got := env.cfg.GetS3SecretKey("camunda-b"); got != "s3-key-b" {
		t.Errorf("camunda-b S3 secret key = %q, want %q", got, "s3-key-b")
	}
}

func TestDeleteInstanceRemovesOnlyItsSecrets(t *testing.T) {
	env := setupTwoInstances(t)

	if w := env.deleteInstance(t, "camunda-a"); w.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", w.Code, w.Body.String())
	}

	if env.store.HasElasticsearchPassword("camunda-a") || env.store.HasS3SecretKey("camunda-a") {
		t.Error("expected camunda-a secrets to be deleted")
	}
	if got := env.cfg.GetElasticsearchPassword("camunda-b"); got != "es-pw-b" {
		t.Errorf("camunda-b ES password = %q, want it untouched", got)
	}
	if got := env.cfg.GetS3SecretKey("camunda-b"); got != "s3-key-b" {
		t.Errorf("camunda-b S3 secret key = %q, want it untouched", got)
	}

	secretsJSON := env.readFile(t, "secrets.json")
	if strings.Contains(secretsJSON, "es-pw-a") || strings.Contains(secretsJSON, "s3-key-a") {
		t.Error("secrets.json still contains camunda-a credentials")
	}
}

func TestSecretsSurviveRestart(t *testing.T) {
	env := setupTwoInstances(t)

	logger := utils.NewLogger("error")
	reloaded, err := secrets.NewStore(env.dataDir, logger)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	cfg := &config.Config{DataDir: env.dataDir}
	cfg.SetSecretProvider(reloaded)

	if got := cfg.GetElasticsearchPassword("camunda-a"); got != "es-pw-a" {
		t.Errorf("camunda-a ES password after restart = %q, want %q", got, "es-pw-a")
	}
	if got := cfg.GetS3SecretKey("camunda-b"); got != "s3-key-b" {
		t.Errorf("camunda-b S3 secret key after restart = %q, want %q", got, "s3-key-b")
	}
}

func TestHandlersWorkWithoutSecretStore(t *testing.T) {
	env := newSecretTestEnv(t)
	env.handlers.SetSecretStore(nil)

	payload := instancePayload("camunda-a", "Camunda A")
	payload["elasticsearch_password"] = "es-pw-a"
	if w := env.createInstance(t, payload); w.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", w.Code, w.Body.String())
	}

	instance := env.getInstance(t, "camunda-a")
	if instance.ElasticsearchPasswordSet {
		t.Error("expected elasticsearch_password_set to be false without a secret store")
	}
	if w := env.updateInstance(t, "camunda-a", payload); w.Code != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", w.Code, w.Body.String())
	}
	if w := env.deleteInstance(t, "camunda-a"); w.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestUpdateOfMissingInstanceDoesNotStoreSecrets(t *testing.T) {
	env := setupTwoInstances(t)

	payload := instancePayload("camunda-missing", "Missing")
	payload["elasticsearch_password"] = "should-not-persist"
	if w := env.updateInstance(t, "camunda-missing", payload); w.Code != http.StatusNotFound {
		t.Fatalf("update status = %d, want %d", w.Code, http.StatusNotFound)
	}

	if env.store.HasElasticsearchPassword("camunda-missing") {
		t.Error("secrets must not be stored for an instance that does not exist")
	}
}
