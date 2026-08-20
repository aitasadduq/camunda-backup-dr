package secrets

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func newTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewStore(dir, utils.NewLogger("error"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store, dir
}

func TestNewStoreWithoutExistingFile(t *testing.T) {
	store, _ := newTestStore(t)

	if got := store.ElasticsearchPassword("camunda-a"); got != "" {
		t.Errorf("expected empty password, got %q", got)
	}
	if store.HasS3SecretKey("camunda-a") {
		t.Error("expected no stored S3 secret key")
	}
}

func TestNewStoreCreatesDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	if _, err := NewStore(dir, utils.NewLogger("error")); err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("expected data dir to be created: %v", err)
	}
}

func TestSetAndGetSecrets(t *testing.T) {
	store, _ := newTestStore(t)

	if err := store.SetElasticsearchPassword("camunda-a", "es-pw"); err != nil {
		t.Fatalf("SetElasticsearchPassword() error = %v", err)
	}
	if err := store.SetS3SecretKey("camunda-a", "s3-key"); err != nil {
		t.Fatalf("SetS3SecretKey() error = %v", err)
	}

	if got := store.ElasticsearchPassword("camunda-a"); got != "es-pw" {
		t.Errorf("ElasticsearchPassword() = %q, want %q", got, "es-pw")
	}
	if got := store.S3SecretKey("camunda-a"); got != "s3-key" {
		t.Errorf("S3SecretKey() = %q, want %q", got, "s3-key")
	}
	if !store.HasElasticsearchPassword("camunda-a") || !store.HasS3SecretKey("camunda-a") {
		t.Error("expected both secrets to be reported as set")
	}
}

func TestSecretsAreIsolatedPerInstance(t *testing.T) {
	store, _ := newTestStore(t)

	if err := store.SetElasticsearchPassword("camunda-a", "pw-a"); err != nil {
		t.Fatalf("SetElasticsearchPassword(a) error = %v", err)
	}
	if err := store.SetElasticsearchPassword("camunda-b", "pw-b"); err != nil {
		t.Fatalf("SetElasticsearchPassword(b) error = %v", err)
	}
	if err := store.SetS3SecretKey("camunda-b", "key-b"); err != nil {
		t.Fatalf("SetS3SecretKey(b) error = %v", err)
	}

	if got := store.ElasticsearchPassword("camunda-a"); got != "pw-a" {
		t.Errorf("instance a password = %q, want %q", got, "pw-a")
	}
	if got := store.ElasticsearchPassword("camunda-b"); got != "pw-b" {
		t.Errorf("instance b password = %q, want %q", got, "pw-b")
	}
	if store.HasS3SecretKey("camunda-a") {
		t.Error("instance a must not inherit instance b's S3 secret key")
	}
	if got := store.S3SecretKey("camunda-b"); got != "key-b" {
		t.Errorf("instance b secret key = %q, want %q", got, "key-b")
	}
	if store.HasElasticsearchPassword("camunda-c") {
		t.Error("unknown instance must not report a stored password")
	}
}

func TestSecretsPersistAcrossReload(t *testing.T) {
	store, dir := newTestStore(t)

	if err := store.SetElasticsearchPassword("camunda-a", "pw-a"); err != nil {
		t.Fatalf("SetElasticsearchPassword() error = %v", err)
	}
	if err := store.SetS3SecretKey("camunda-b", "key-b"); err != nil {
		t.Fatalf("SetS3SecretKey() error = %v", err)
	}

	reloaded, err := NewStore(dir, utils.NewLogger("error"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}

	if got := reloaded.ElasticsearchPassword("camunda-a"); got != "pw-a" {
		t.Errorf("reloaded password = %q, want %q", got, "pw-a")
	}
	if got := reloaded.S3SecretKey("camunda-b"); got != "key-b" {
		t.Errorf("reloaded secret key = %q, want %q", got, "key-b")
	}
}

func TestSetEmptyValueClearsSecret(t *testing.T) {
	store, dir := newTestStore(t)

	if err := store.SetElasticsearchPassword("camunda-a", "pw-a"); err != nil {
		t.Fatalf("SetElasticsearchPassword() error = %v", err)
	}
	if err := store.SetS3SecretKey("camunda-a", "key-a"); err != nil {
		t.Fatalf("SetS3SecretKey() error = %v", err)
	}

	if err := store.SetElasticsearchPassword("camunda-a", ""); err != nil {
		t.Fatalf("clear password error = %v", err)
	}

	if store.HasElasticsearchPassword("camunda-a") {
		t.Error("expected password to be cleared")
	}
	if got := store.S3SecretKey("camunda-a"); got != "key-a" {
		t.Errorf("clearing the password must not affect the secret key, got %q", got)
	}

	// Clearing the last secret removes the instance from the file entirely
	if err := store.SetS3SecretKey("camunda-a", ""); err != nil {
		t.Fatalf("clear secret key error = %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, secretsFileName))
	if err != nil {
		t.Fatalf("failed to read secrets file: %v", err)
	}
	var stored map[string]instanceSecrets
	if err := json.Unmarshal(data, &stored); err != nil {
		t.Fatalf("failed to unmarshal secrets file: %v", err)
	}
	if _, ok := stored["camunda-a"]; ok {
		t.Error("expected instance entry to be removed once all secrets are cleared")
	}
}

func TestDeleteInstance(t *testing.T) {
	store, _ := newTestStore(t)

	if err := store.SetElasticsearchPassword("camunda-a", "pw-a"); err != nil {
		t.Fatalf("SetElasticsearchPassword(a) error = %v", err)
	}
	if err := store.SetElasticsearchPassword("camunda-b", "pw-b"); err != nil {
		t.Fatalf("SetElasticsearchPassword(b) error = %v", err)
	}

	if err := store.DeleteInstance("camunda-a"); err != nil {
		t.Fatalf("DeleteInstance() error = %v", err)
	}

	if store.HasElasticsearchPassword("camunda-a") {
		t.Error("expected instance a secrets to be deleted")
	}
	if got := store.ElasticsearchPassword("camunda-b"); got != "pw-b" {
		t.Errorf("instance b password = %q, want %q", got, "pw-b")
	}

	// Deleting an unknown instance is a no-op
	if err := store.DeleteInstance("camunda-unknown"); err != nil {
		t.Errorf("DeleteInstance(unknown) error = %v, want nil", err)
	}
}

func TestSecretsFileIsNotWorldReadable(t *testing.T) {
	store, dir := newTestStore(t)

	if err := store.SetElasticsearchPassword("camunda-a", "pw-a"); err != nil {
		t.Fatalf("SetElasticsearchPassword() error = %v", err)
	}

	info, err := os.Stat(filepath.Join(dir, secretsFileName))
	if err != nil {
		t.Fatalf("failed to stat secrets file: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("secrets file mode = %o, want 0600", perm)
	}
}

func TestSetRequiresInstanceID(t *testing.T) {
	store, _ := newTestStore(t)

	if err := store.SetElasticsearchPassword("", "pw"); err == nil {
		t.Error("expected an error when the instance ID is empty")
	}
	if err := store.SetS3SecretKey("", "key"); err == nil {
		t.Error("expected an error when the instance ID is empty")
	}
}

func TestNewStoreRejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, secretsFileName), []byte("{not json"), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	_, err := NewStore(dir, utils.NewLogger("error"))
	if err == nil {
		t.Fatal("expected an error for a corrupt secrets file")
	}
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("unexpected error = %v", err)
	}
}

func TestNewStoreAcceptsEmptyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, secretsFileName), nil, 0600); err != nil {
		t.Fatalf("failed to write empty file: %v", err)
	}

	store, err := NewStore(dir, utils.NewLogger("error"))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	if store.HasElasticsearchPassword("camunda-a") {
		t.Error("expected no secrets in an empty file")
	}
}

func TestConcurrentAccess(t *testing.T) {
	store, _ := newTestStore(t)

	var wg sync.WaitGroup
	for _, id := range []string{"camunda-a", "camunda-b"} {
		wg.Add(2)
		go func(id string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if err := store.SetElasticsearchPassword(id, "pw-"+id); err != nil {
					t.Errorf("SetElasticsearchPassword() error = %v", err)
					return
				}
			}
		}(id)
		go func(id string) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = store.ElasticsearchPassword(id)
			}
		}(id)
	}
	wg.Wait()

	if got := store.ElasticsearchPassword("camunda-a"); got != "pw-camunda-a" {
		t.Errorf("instance a password = %q, want %q", got, "pw-camunda-a")
	}
	if got := store.ElasticsearchPassword("camunda-b"); got != "pw-camunda-b" {
		t.Errorf("instance b password = %q, want %q", got, "pw-camunda-b")
	}
}
