//go:build integration
// +build integration

package elasticsearch

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/camunda"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

// Integration test configuration
// These tests require a running Elasticsearch instance
const (
	testESEndpoint   = "http://localhost:9200"
	testESUsername   = "elastic"
	testESPassword   = "localelastic12345"
	testESRepository = "camunda-backup"
)

// TestIntegration_CreateESSnapshots tests: "Can create ES snapshots"
func TestIntegration_CreateESSnapshots(t *testing.T) {
	client := setupIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snapshotName := generateTestSnapshotName("create")

	// Create snapshot
	err := client.CreateSnapshot(ctx, testESRepository, snapshotName)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	t.Logf("Successfully created snapshot: %s", snapshotName)

	// Wait for snapshot to complete and clean up
	defer cleanupSnapshot(t, client, testESRepository, snapshotName)

	// Verify snapshot exists by checking status
	state, err := client.GetSnapshotStatus(ctx, testESRepository, snapshotName)
	if err != nil {
		t.Fatalf("Failed to get snapshot status after creation: %v", err)
	}

	t.Logf("Snapshot state after creation: %s", state)

	// Snapshot should be either IN_PROGRESS or SUCCESS
	if state != SnapshotStateInProgress && state != SnapshotStateSuccess {
		t.Errorf("Expected snapshot state IN_PROGRESS or SUCCESS, got: %s", state)
	}
}

// TestIntegration_CheckSnapshotStatus tests: "Can check snapshot status"
func TestIntegration_CheckSnapshotStatus(t *testing.T) {
	client := setupIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	snapshotName := generateTestSnapshotName("status")

	// Create a snapshot first
	err := client.CreateSnapshot(ctx, testESRepository, snapshotName)
	if err != nil {
		t.Fatalf("Failed to create snapshot for status test: %v", err)
	}
	defer cleanupSnapshot(t, client, testESRepository, snapshotName)

	// Poll for status changes
	var lastState SnapshotState
	maxAttempts := 30
	for i := 0; i < maxAttempts; i++ {
		state, err := client.GetSnapshotStatus(ctx, testESRepository, snapshotName)
		if err != nil {
			t.Fatalf("Failed to check snapshot status on attempt %d: %v", i+1, err)
		}

		if state != lastState {
			t.Logf("Snapshot status changed: %s -> %s (attempt %d)", lastState, state, i+1)
			lastState = state
		}

		// If snapshot completed or failed, we're done
		if state == SnapshotStateSuccess {
			t.Logf("Snapshot completed successfully")
			return
		}
		if state == SnapshotStateFailed {
			t.Logf("Snapshot failed (this is still a valid status check)")
			return
		}

		time.Sleep(2 * time.Second)
	}

	t.Logf("Snapshot still in progress after %d attempts, final state: %s", maxAttempts, lastState)
}

// TestIntegration_AuthenticationWorks tests: "Authentication works correctly"
func TestIntegration_AuthenticationWorks(t *testing.T) {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)

	// Test with correct credentials
	t.Run("ValidCredentials", func(t *testing.T) {
		client := NewClient(testESEndpoint, testESUsername, testESPassword, httpClient, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		snapshotName := generateTestSnapshotName("auth-valid")

		err := client.CreateSnapshot(ctx, testESRepository, snapshotName)
		if err != nil {
			t.Fatalf("Authentication with valid credentials failed: %v", err)
		}
		defer cleanupSnapshot(t, client, testESRepository, snapshotName)

		t.Log("Authentication with valid credentials succeeded")
	})

	// Test with incorrect credentials
	t.Run("InvalidCredentials", func(t *testing.T) {
		client := NewClient(testESEndpoint, "elastic", "wrongpassword", httpClient, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		snapshotName := generateTestSnapshotName("auth-invalid")

		err := client.CreateSnapshot(ctx, testESRepository, snapshotName)
		if err == nil {
			// If it somehow succeeded, clean up
			cleanupSnapshot(t, client, testESRepository, snapshotName)
			t.Fatal("Expected authentication to fail with invalid credentials")
		}

		t.Logf("Authentication correctly failed with invalid credentials: %v", err)
	})

	// Test with no credentials (should fail if ES requires auth)
	t.Run("NoCredentials", func(t *testing.T) {
		client := NewClient(testESEndpoint, "", "", httpClient, logger)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		snapshotName := generateTestSnapshotName("auth-none")

		err := client.CreateSnapshot(ctx, testESRepository, snapshotName)
		if err == nil {
			// If ES allows anonymous access, clean up
			cleanupSnapshot(t, client, testESRepository, snapshotName)
			t.Log("ES allows anonymous access (this may be intentional)")
		} else {
			t.Logf("Authentication correctly required credentials: %v", err)
		}
	})
}

// TestIntegration_ErrorHandling tests: "Error handling prevents crashes"
func TestIntegration_ErrorHandling(t *testing.T) {
	client := setupIntegrationClient(t)

	// Test non-existent repository
	t.Run("NonExistentRepository", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := client.CreateSnapshot(ctx, "non-existent-repo-12345", "test-snapshot")
		if err == nil {
			t.Error("Expected error for non-existent repository")
		} else {
			t.Logf("Correctly received error for non-existent repository: %v", err)
		}
	})

	// Test getting status of non-existent snapshot
	t.Run("NonExistentSnapshot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.GetSnapshotStatus(ctx, testESRepository, "non-existent-snapshot-12345")
		if err == nil {
			t.Error("Expected error for non-existent snapshot")
		} else {
			t.Logf("Correctly received error for non-existent snapshot: %v", err)
		}
	})

	// Test deleting non-existent snapshot
	t.Run("DeleteNonExistentSnapshot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := client.DeleteSnapshot(ctx, testESRepository, "non-existent-snapshot-12345")
		// Note: ES may return success for deleting non-existent snapshots
		if err != nil {
			t.Logf("Delete non-existent snapshot returned error (acceptable): %v", err)
		} else {
			t.Log("Delete non-existent snapshot returned success (ES behavior)")
		}
	})

	// Test with empty parameters
	t.Run("EmptyParameters", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := client.CreateSnapshot(ctx, "", "test-snapshot")
		if err == nil {
			t.Error("Expected error for empty repository")
		}

		err = client.CreateSnapshot(ctx, testESRepository, "")
		if err == nil {
			t.Error("Expected error for empty snapshot name")
		}

		_, err = client.GetSnapshotStatus(ctx, "", "test-snapshot")
		if err == nil {
			t.Error("Expected error for empty repository in GetSnapshotStatus")
		}

		t.Log("Empty parameter validation working correctly")
	})

	// Test context cancellation
	t.Run("ContextCancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		err := client.CreateSnapshot(ctx, testESRepository, "test-snapshot")
		if err == nil {
			t.Error("Expected error for cancelled context")
		} else {
			t.Logf("Correctly handled cancelled context: %v", err)
		}
	})

	// Test unreachable endpoint
	t.Run("UnreachableEndpoint", func(t *testing.T) {
		logger := utils.NewLogger("debug")
		httpConfig := camunda.DefaultHTTPClientConfig()
		httpConfig.Timeout = 5 * time.Second // Short timeout for this test
		httpClient := camunda.NewHTTPClient(httpConfig, logger)

		unreachableClient := NewClient("http://localhost:59999", testESUsername, testESPassword, httpClient, logger)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := unreachableClient.CreateSnapshot(ctx, testESRepository, "test-snapshot")
		if err == nil {
			t.Error("Expected error for unreachable endpoint")
		} else {
			t.Logf("Correctly handled unreachable endpoint: %v", err)
		}
	})
}

// TestIntegration_FullSnapshotLifecycle tests the complete snapshot lifecycle
func TestIntegration_FullSnapshotLifecycle(t *testing.T) {
	client := setupIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	snapshotName := generateTestSnapshotName("lifecycle")

	// Step 1: Create snapshot
	t.Log("Step 1: Creating snapshot...")
	err := client.CreateSnapshot(ctx, testESRepository, snapshotName)
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}
	t.Logf("Snapshot '%s' creation initiated", snapshotName)

	// Step 2: Poll until completion
	t.Log("Step 2: Polling for completion...")
	finalState, err := waitForSnapshotCompletion(ctx, client, testESRepository, snapshotName, t)
	if err != nil {
		t.Fatalf("Error while waiting for snapshot completion: %v", err)
	}
	t.Logf("Snapshot completed with state: %s", finalState)

	// Step 3: Verify final state
	t.Log("Step 3: Verifying final state...")
	state, err := client.GetSnapshotStatus(ctx, testESRepository, snapshotName)
	if err != nil {
		t.Fatalf("Failed to verify final state: %v", err)
	}
	if state != SnapshotStateSuccess && state != SnapshotStateFailed && state != SnapshotStatePartial {
		t.Errorf("Expected terminal state, got: %s", state)
	}
	t.Logf("Verified snapshot state: %s", state)

	// Step 4: Delete snapshot
	t.Log("Step 4: Deleting snapshot...")
	err = client.DeleteSnapshot(ctx, testESRepository, snapshotName)
	if err != nil {
		t.Fatalf("Failed to delete snapshot: %v", err)
	}
	t.Log("Snapshot deleted successfully")

	// Step 5: Verify deletion
	t.Log("Step 5: Verifying deletion...")
	_, err = client.GetSnapshotStatus(ctx, testESRepository, snapshotName)
	if err == nil {
		t.Error("Expected error when getting status of deleted snapshot")
	} else {
		t.Log("Confirmed snapshot no longer exists")
	}

	t.Log("Full snapshot lifecycle test completed successfully!")
}

// TestIntegration_ConcurrentSnapshots tests handling of concurrent snapshot requests
func TestIntegration_ConcurrentSnapshots(t *testing.T) {
	client := setupIntegrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	snapshot1 := generateTestSnapshotName("concurrent-1")
	snapshot2 := generateTestSnapshotName("concurrent-2")

	// Create first snapshot
	err := client.CreateSnapshot(ctx, testESRepository, snapshot1)
	if err != nil {
		t.Fatalf("Failed to create first snapshot: %v", err)
	}
	defer cleanupSnapshot(t, client, testESRepository, snapshot1)
	t.Logf("Created first snapshot: %s", snapshot1)

	// Try to create second snapshot while first is in progress
	// ES may reject this or queue it depending on configuration
	err = client.CreateSnapshot(ctx, testESRepository, snapshot2)
	if err != nil {
		t.Logf("Second concurrent snapshot rejected (expected ES behavior): %v", err)
	} else {
		defer cleanupSnapshot(t, client, testESRepository, snapshot2)
		t.Log("Second concurrent snapshot accepted (ES may be queueing)")
	}

	// Wait for first snapshot to complete
	_, err = waitForSnapshotCompletion(ctx, client, testESRepository, snapshot1, t)
	if err != nil {
		t.Logf("Warning: First snapshot did not complete: %v", err)
	}
}

// Helper functions

func setupIntegrationClient(t *testing.T) *Client {
	logger := utils.NewLogger("debug")
	httpClient := camunda.NewHTTPClient(camunda.DefaultHTTPClientConfig(), logger)
	client := NewClient(testESEndpoint, testESUsername, testESPassword, httpClient, logger)

	// Verify connection by checking if repository exists
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.GetSnapshotStatus(ctx, testESRepository, "non-existent-test")
	// We expect an error (snapshot not found), but the connection should work
	if err != nil && !strings.Contains(err.Error(), "not found") && !strings.Contains(err.Error(), "404") && !strings.Contains(err.Error(), "missing") {
		// Check if it's an auth or connection error
		if strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "unauthorized") {
			t.Fatalf("Authentication failed - check credentials: %v", err)
		}
		if strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("Cannot connect to Elasticsearch at %s: %v", testESEndpoint, err)
		}
	}

	return client
}

func generateTestSnapshotName(prefix string) string {
	return fmt.Sprintf("integration-test-%s-%d", prefix, time.Now().UnixNano())
}

func cleanupSnapshot(t *testing.T, client *Client, repo, snapshot string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for snapshot to complete before deleting
	waitForSnapshotCompletion(ctx, client, repo, snapshot, t)

	err := client.DeleteSnapshot(ctx, repo, snapshot)
	if err != nil {
		t.Logf("Warning: Failed to cleanup snapshot %s: %v", snapshot, err)
	} else {
		t.Logf("Cleaned up snapshot: %s", snapshot)
	}
}

func waitForSnapshotCompletion(ctx context.Context, client *Client, repo, snapshot string, t *testing.T) (SnapshotState, error) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return SnapshotStateUnknown, ctx.Err()
		case <-ticker.C:
			state, err := client.GetSnapshotStatus(ctx, repo, snapshot)
			if err != nil {
				return SnapshotStateUnknown, err
			}

			switch state {
			case SnapshotStateSuccess, SnapshotStateFailed, SnapshotStatePartial:
				return state, nil
			case SnapshotStateInProgress:
				t.Logf("Snapshot %s still in progress...", snapshot)
				continue
			default:
				t.Logf("Snapshot %s in state: %s", snapshot, state)
				continue
			}
		}
	}
}
