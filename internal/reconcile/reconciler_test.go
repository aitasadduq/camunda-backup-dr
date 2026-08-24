package reconcile

import (
	"context"
	"errors"
	"testing"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
)

// Reports are persisted to S3 and served over the API, so an endpoint carrying
// credentials must not survive into either.
func TestStripUserinfo(t *testing.T) {
	cases := map[string]string{
		"https://user:pass@operate:8080/actuator/backups": "https://operate:8080/actuator/backups",
		"https://user@operate:8080/actuator/backups":      "https://operate:8080/actuator/backups",
		"http://operate:8080/actuator/backups":            "http://operate:8080/actuator/backups",
		"":                                                "",
	}
	for in, want := range cases {
		if got := stripUserinfo(in); got != want {
			t.Errorf("stripUserinfo(%q) = %q, want %q", in, got, want)
		}
	}
}

// A sweep fans out to every component and holds the whole record set in memory,
// so a second concurrent sweep must be refused rather than doubling the load.
func TestReconcileRefusesConcurrentSweep(t *testing.T) {
	r := &Reconciler{}
	if !r.sweepRunning.CompareAndSwap(false, true) {
		t.Fatal("expected the guard to start unclaimed")
	}
	defer r.sweepRunning.Store(false)

	_, err := r.Reconcile(context.Background(), &models.CamundaInstance{ID: "prod"})
	if !errors.Is(err, ErrSweepInProgress) {
		t.Errorf("err = %v, want ErrSweepInProgress", err)
	}
}

func TestIsBackupRunningDefaultsToFalse(t *testing.T) {
	r := &Reconciler{}
	if r.isBackupRunning() {
		t.Error("with no check registered the sweep must assume no backup is running")
	}
	r.SetBackupRunningFunc(func() bool { return true })
	if !r.isBackupRunning() {
		t.Error("registered check was not consulted")
	}
}
