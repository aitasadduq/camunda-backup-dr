// Package secrets persists per-instance credentials entered through the UI.
//
// Values live in a dedicated file (secrets.json) with 0600 permissions,
// separate from config.json, and are never returned by the API — callers can
// only ask whether a credential is set.
package secrets

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

const secretsFileName = "secrets.json"

// instanceSecrets holds the credentials stored for a single Camunda instance.
type instanceSecrets struct {
	ElasticsearchPassword string `json:"elasticsearch_password,omitempty"`
	S3SecretKey           string `json:"s3_secret_key,omitempty"`
}

func (s instanceSecrets) empty() bool {
	return s.ElasticsearchPassword == "" && s.S3SecretKey == ""
}

// Store is a concurrency-safe, file-backed credential store.
type Store struct {
	path   string
	logger *utils.Logger

	mutex sync.RWMutex
	data  map[string]instanceSecrets
}

// NewStore opens (or creates) the secrets file inside dataDir.
func NewStore(dataDir string, logger *utils.Logger) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	store := &Store{
		path:   filepath.Join(dataDir, secretsFileName),
		logger: logger,
		data:   make(map[string]instanceSecrets),
	}

	if err := store.load(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read secrets file: %w", err)
	}
	if len(data) == 0 {
		return nil
	}

	var stored map[string]instanceSecrets
	if err := json.Unmarshal(data, &stored); err != nil {
		return fmt.Errorf("failed to unmarshal secrets file: %w", err)
	}
	if stored != nil {
		s.data = stored
	}
	return nil
}

// save writes the store to disk atomically. Callers must hold the write lock.
func (s *Store) save() error {
	data, err := json.MarshalIndent(s.data, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal secrets: %w", err)
	}

	tempPath := s.path + ".tmp"
	if err := os.WriteFile(tempPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write secrets file: %w", err)
	}
	if err := os.Rename(tempPath, s.path); err != nil {
		if rmErr := os.Remove(tempPath); rmErr != nil {
			s.logger.Warn("Failed to remove temporary secrets file %s: %v", tempPath, rmErr)
		}
		return fmt.Errorf("failed to rename secrets file: %w", err)
	}
	return nil
}

func (s *Store) get(instanceID string) instanceSecrets {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.data[instanceID]
}

// set applies mutate to the instance's secrets and persists the result.
// An instance whose secrets all become empty is removed entirely.
func (s *Store) set(instanceID string, mutate func(*instanceSecrets)) error {
	if instanceID == "" {
		return fmt.Errorf("instance ID is required")
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	current := s.data[instanceID]
	mutate(&current)

	if current.empty() {
		delete(s.data, instanceID)
	} else {
		s.data[instanceID] = current
	}

	return s.save()
}

// ElasticsearchPassword returns the stored Elasticsearch password, or "" if unset.
func (s *Store) ElasticsearchPassword(instanceID string) string {
	return s.get(instanceID).ElasticsearchPassword
}

// S3SecretKey returns the stored S3 secret key, or "" if unset.
func (s *Store) S3SecretKey(instanceID string) string {
	return s.get(instanceID).S3SecretKey
}

// HasElasticsearchPassword reports whether an Elasticsearch password is stored.
func (s *Store) HasElasticsearchPassword(instanceID string) bool {
	return s.ElasticsearchPassword(instanceID) != ""
}

// HasS3SecretKey reports whether an S3 secret key is stored.
func (s *Store) HasS3SecretKey(instanceID string) bool {
	return s.S3SecretKey(instanceID) != ""
}

// SetElasticsearchPassword stores the password. An empty value clears it.
func (s *Store) SetElasticsearchPassword(instanceID, value string) error {
	return s.set(instanceID, func(sec *instanceSecrets) {
		sec.ElasticsearchPassword = value
	})
}

// SetS3SecretKey stores the secret key. An empty value clears it.
func (s *Store) SetS3SecretKey(instanceID, value string) error {
	return s.set(instanceID, func(sec *instanceSecrets) {
		sec.S3SecretKey = value
	})
}

// DeleteInstance removes all secrets stored for an instance.
func (s *Store) DeleteInstance(instanceID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.data[instanceID]; !ok {
		return nil
	}
	delete(s.data, instanceID)
	return s.save()
}
