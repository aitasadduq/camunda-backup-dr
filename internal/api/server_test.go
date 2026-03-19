package api

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/aitasadduq/camunda-backup-dr/internal/models"
	"github.com/aitasadduq/camunda-backup-dr/internal/utils"
)

func TestNewServer(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}

	s := NewServer(0, cm, orch, hist, sched, ret, lfr, logger, nil, nil)

	if s == nil {
		t.Fatal("expected non-nil server")
	}
	if s.handlers == nil {
		t.Error("expected non-nil handlers")
	}
	if s.router == nil {
		t.Error("expected non-nil router")
	}
	if s.logger != logger {
		t.Error("expected logger to be set")
	}
	if s.server == nil {
		t.Error("expected non-nil http.Server")
	}
}

func TestServer_StartAndShutdown(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}

	// Use port 0 to let the OS assign a free port
	s := NewServer(0, cm, orch, hist, sched, ret, lfr, logger, nil, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	}()

	// The listener should have been created
	if s.listener == nil {
		t.Fatal("expected listener to be set after Start")
	}

	// Make a request to verify the server is serving.
	// Extract the chosen port and connect via 127.0.0.1 to avoid using
	// wildcard addresses like "[::]:port" or "0.0.0.0:port", which are
	// not valid connect targets on some platforms.
	tcpAddr, ok := s.listener.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener address is not *net.TCPAddr: %T", s.listener.Addr())
	}
	resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/healthz", tcpAddr.Port))
	if err != nil {
		t.Fatalf("failed to reach server: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 from /healthz, got %d", resp.StatusCode)
	}
}

func TestServer_Shutdown_NilContext(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}

	s := NewServer(0, cm, orch, hist, sched, ret, lfr, logger, nil, nil)

	if err := s.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Shutdown with nil context (should create internal timeout context)
	if err := s.Shutdown(nil); err != nil {
		t.Errorf("expected graceful shutdown with nil context, got: %v", err)
	}
}

func TestServer_GetPort(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}

	s := NewServer(9876, cm, orch, hist, sched, ret, lfr, logger, nil, nil)

	if s.GetPort() != 9876 {
		t.Errorf("expected port 9876, got %d", s.GetPort())
	}
}

func TestServer_Start_PortConflict(t *testing.T) {
	logger := utils.NewLogger("error")
	cm := &mockCamundaManager{instances: []models.CamundaInstance{}}
	orch := &mockOrchestrator{}
	hist := &mockHistoryProvider{history: []*models.BackupHistory{}}
	sched := &mockScheduler{running: true}
	ret := &mockRetentionManager{}
	lfr := &mockLogFileReader{logs: make(map[string]string)}

	// Bind a port to create a conflict
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to bind port: %v", err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port

	s := NewServer(port, cm, orch, hist, sched, ret, lfr, logger, nil, nil)

	if err := s.Start(); err == nil {
		t.Error("expected error when starting on occupied port")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.Shutdown(ctx)
	}
}
