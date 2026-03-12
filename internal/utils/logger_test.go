package utils

import (
	"bytes"
	"strings"
	"testing"
)

// helper: create a Logger and redirect all its internal writers to buffers
// so we can capture and inspect log output in tests.
func newTestLogger(level string) (*Logger, *bytes.Buffer, *bytes.Buffer) {
	l := NewLogger(level)
	stdoutBuf := &bytes.Buffer{}
	stderrBuf := &bytes.Buffer{}
	l.infoLogger.SetOutput(stdoutBuf)
	l.warnLogger.SetOutput(stdoutBuf)
	l.debugLogger.SetOutput(stdoutBuf)
	l.errorLogger.SetOutput(stderrBuf)
	return l, stdoutBuf, stderrBuf
}

// ---------------------------------------------------------------------------
// NewLogger
// ---------------------------------------------------------------------------

func TestNewLogger_DebugLevel(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("debug")

	l.Debug("hello %s", "world")

	out := stdoutBuf.String()
	if !strings.Contains(out, "DEBUG") {
		t.Errorf("debug level logger should produce DEBUG output, got: %q", out)
	}
	if !strings.Contains(out, "hello world") {
		t.Errorf("expected message in output, got: %q", out)
	}
}

func TestNewLogger_InfoLevel_SuppressesDebug(t *testing.T) {
	// Do NOT redirect debugLogger — it should write to io.Discard at info level.
	l := NewLogger("info")
	buf := &bytes.Buffer{}
	l.infoLogger.SetOutput(buf)

	l.Debug("should not appear")
	l.Info("visible")

	out := buf.String()
	if strings.Contains(out, "should not appear") {
		t.Error("debug messages should be discarded at info level")
	}
	if !strings.Contains(out, "visible") {
		t.Error("info messages should still work")
	}
}

func TestNewLogger_ReturnsNonNil(t *testing.T) {
	l := NewLogger("info")
	if l == nil {
		t.Fatal("NewLogger should return non-nil logger")
	}
	if l.infoLogger == nil || l.warnLogger == nil || l.errorLogger == nil || l.debugLogger == nil {
		t.Fatal("all internal loggers should be initialized")
	}
}

// ---------------------------------------------------------------------------
// Logger.Info / Warn / Error / Debug
// ---------------------------------------------------------------------------

func TestLogger_Info(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")

	l.Info("backup started for %s", "prod-cluster")

	out := stdoutBuf.String()
	if !strings.Contains(out, "INFO") {
		t.Errorf("expected INFO prefix, got: %q", out)
	}
	if !strings.Contains(out, "backup started for prod-cluster") {
		t.Errorf("expected formatted message, got: %q", out)
	}
}

func TestLogger_Warn(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")

	l.Warn("retrying attempt %d", 3)

	out := stdoutBuf.String()
	if !strings.Contains(out, "WARN") {
		t.Errorf("expected WARN prefix, got: %q", out)
	}
	if !strings.Contains(out, "retrying attempt 3") {
		t.Errorf("expected formatted message, got: %q", out)
	}
}

func TestLogger_Error(t *testing.T) {
	l, _, stderrBuf := newTestLogger("info")

	l.Error("connection failed: %s", "timeout")

	out := stderrBuf.String()
	if !strings.Contains(out, "ERROR") {
		t.Errorf("expected ERROR prefix, got: %q", out)
	}
	if !strings.Contains(out, "connection failed: timeout") {
		t.Errorf("expected formatted message, got: %q", out)
	}
}

func TestLogger_Debug_Enabled(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("debug")

	l.Debug("detailed info: %v", map[string]int{"a": 1})

	out := stdoutBuf.String()
	if !strings.Contains(out, "DEBUG") {
		t.Errorf("expected DEBUG prefix, got: %q", out)
	}
	if !strings.Contains(out, "detailed info:") {
		t.Errorf("expected formatted message, got: %q", out)
	}
}

func TestLogger_Debug_Disabled(t *testing.T) {
	// Debug logger at info level writes to io.Discard — do not override it.
	l := NewLogger("info")
	buf := &bytes.Buffer{}
	l.infoLogger.SetOutput(buf)

	l.Debug("this should be discarded")
	l.Info("marker")

	out := buf.String()
	if strings.Contains(out, "this should be discarded") {
		t.Error("debug should be discarded at info level")
	}
	if !strings.Contains(out, "marker") {
		t.Error("info should still be written")
	}
}

func TestLogger_Info_NoArgs(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")

	l.Info("simple message")

	if !strings.Contains(stdoutBuf.String(), "simple message") {
		t.Errorf("expected plain message, got: %q", stdoutBuf.String())
	}
}

// ---------------------------------------------------------------------------
// Logger.WithBackupID → BackupLogger
// ---------------------------------------------------------------------------

func TestLogger_WithBackupID(t *testing.T) {
	l := NewLogger("info")
	bl := l.WithBackupID("backup-42")

	if bl == nil {
		t.Fatal("WithBackupID should return non-nil BackupLogger")
	}
	if bl.backupID != "backup-42" {
		t.Errorf("backupID = %q, want %q", bl.backupID, "backup-42")
	}
	if bl.logger != l {
		t.Error("BackupLogger should reference the parent Logger")
	}
}

func TestBackupLogger_Info(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")
	bl := l.WithBackupID("bk-001")

	bl.Info("snapshot created for %s", "zeebe")

	out := stdoutBuf.String()
	if !strings.Contains(out, "[BACKUP_ID: bk-001]") {
		t.Errorf("expected backup ID prefix, got: %q", out)
	}
	if !strings.Contains(out, "snapshot created for zeebe") {
		t.Errorf("expected formatted message, got: %q", out)
	}
	if !strings.Contains(out, "INFO") {
		t.Errorf("expected INFO prefix, got: %q", out)
	}
}

func TestBackupLogger_Warn(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")
	bl := l.WithBackupID("bk-002")

	bl.Warn("slow upload for %s", "operate")

	out := stdoutBuf.String()
	if !strings.Contains(out, "[BACKUP_ID: bk-002]") {
		t.Errorf("expected backup ID prefix, got: %q", out)
	}
	if !strings.Contains(out, "slow upload for operate") {
		t.Errorf("expected formatted message, got: %q", out)
	}
	if !strings.Contains(out, "WARN") {
		t.Errorf("expected WARN prefix, got: %q", out)
	}
}

func TestBackupLogger_Error(t *testing.T) {
	l, _, stderrBuf := newTestLogger("info")
	bl := l.WithBackupID("bk-003")

	bl.Error("upload failed: %s", "network error")

	out := stderrBuf.String()
	if !strings.Contains(out, "[BACKUP_ID: bk-003]") {
		t.Errorf("expected backup ID prefix, got: %q", out)
	}
	if !strings.Contains(out, "upload failed: network error") {
		t.Errorf("expected formatted message, got: %q", out)
	}
	if !strings.Contains(out, "ERROR") {
		t.Errorf("expected ERROR prefix, got: %q", out)
	}
}

func TestBackupLogger_Debug(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("debug")
	bl := l.WithBackupID("bk-004")

	bl.Debug("chunk %d uploaded", 5)

	out := stdoutBuf.String()
	if !strings.Contains(out, "[BACKUP_ID: bk-004]") {
		t.Errorf("expected backup ID prefix, got: %q", out)
	}
	if !strings.Contains(out, "chunk 5 uploaded") {
		t.Errorf("expected formatted message, got: %q", out)
	}
	if !strings.Contains(out, "DEBUG") {
		t.Errorf("expected DEBUG prefix, got: %q", out)
	}
}

func TestBackupLogger_Debug_Suppressed(t *testing.T) {
	// Debug logger at info level writes to io.Discard — do not override it.
	l := NewLogger("info")
	buf := &bytes.Buffer{}
	l.infoLogger.SetOutput(buf)
	bl := l.WithBackupID("bk-005")

	bl.Debug("should not appear")
	bl.Info("marker")

	out := buf.String()
	if strings.Contains(out, "should not appear") {
		t.Error("debug should be suppressed at info level")
	}
	if !strings.Contains(out, "marker") {
		t.Error("info should still be written")
	}
}

// ---------------------------------------------------------------------------
// Logger.WithContext → ContextLogger
// ---------------------------------------------------------------------------

func TestLogger_WithContext_AllFields(t *testing.T) {
	l := NewLogger("info")
	cl := l.WithContext("backup", "zeebe", "prod-1")

	if cl == nil {
		t.Fatal("WithContext should return non-nil ContextLogger")
	}
	if !strings.Contains(cl.prefix, "op=backup") {
		t.Errorf("prefix should contain op=backup, got: %q", cl.prefix)
	}
	if !strings.Contains(cl.prefix, "component=zeebe") {
		t.Errorf("prefix should contain component=zeebe, got: %q", cl.prefix)
	}
	if !strings.Contains(cl.prefix, "instance=prod-1") {
		t.Errorf("prefix should contain instance=prod-1, got: %q", cl.prefix)
	}
	if !strings.HasPrefix(cl.prefix, "[") || !strings.HasSuffix(cl.prefix, "] ") {
		t.Errorf("prefix should be bracketed, got: %q", cl.prefix)
	}
}

func TestLogger_WithContext_OperationOnly(t *testing.T) {
	l := NewLogger("info")
	cl := l.WithContext("restore", "", "")

	if cl.prefix != "[op=restore] " {
		t.Errorf("prefix = %q, want %q", cl.prefix, "[op=restore] ")
	}
}

func TestLogger_WithContext_ComponentOnly(t *testing.T) {
	l := NewLogger("info")
	cl := l.WithContext("", "elasticsearch", "")

	if cl.prefix != "[component=elasticsearch] " {
		t.Errorf("prefix = %q, want %q", cl.prefix, "[component=elasticsearch] ")
	}
}

func TestLogger_WithContext_InstanceOnly(t *testing.T) {
	l := NewLogger("info")
	cl := l.WithContext("", "", "dev-cluster")

	if cl.prefix != "[instance=dev-cluster] " {
		t.Errorf("prefix = %q, want %q", cl.prefix, "[instance=dev-cluster] ")
	}
}

func TestLogger_WithContext_Empty(t *testing.T) {
	l := NewLogger("info")
	cl := l.WithContext("", "", "")

	if cl.prefix != "" {
		t.Errorf("empty context should produce empty prefix, got: %q", cl.prefix)
	}
}

func TestLogger_WithContext_TwoFields(t *testing.T) {
	l := NewLogger("info")
	cl := l.WithContext("backup", "", "staging")

	want := "[op=backup instance=staging] "
	if cl.prefix != want {
		t.Errorf("prefix = %q, want %q", cl.prefix, want)
	}
}

func TestContextLogger_Info(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")
	cl := l.WithContext("backup", "zeebe", "prod")

	cl.Info("starting snapshot")

	out := stdoutBuf.String()
	if !strings.Contains(out, "INFO") {
		t.Errorf("expected INFO prefix, got: %q", out)
	}
	if !strings.Contains(out, "[op=backup component=zeebe instance=prod]") {
		t.Errorf("expected context prefix, got: %q", out)
	}
	if !strings.Contains(out, "starting snapshot") {
		t.Errorf("expected message, got: %q", out)
	}
}

func TestContextLogger_Warn(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")
	cl := l.WithContext("restore", "operate", "")

	cl.Warn("slow response from %s", "API")

	out := stdoutBuf.String()
	if !strings.Contains(out, "WARN") {
		t.Errorf("expected WARN prefix, got: %q", out)
	}
	if !strings.Contains(out, "[op=restore component=operate]") {
		t.Errorf("expected context prefix, got: %q", out)
	}
	if !strings.Contains(out, "slow response from API") {
		t.Errorf("expected formatted message, got: %q", out)
	}
}

func TestContextLogger_Error(t *testing.T) {
	l, _, stderrBuf := newTestLogger("info")
	cl := l.WithContext("backup", "elasticsearch", "prod")

	cl.Error("index creation failed: %s", "permission denied")

	out := stderrBuf.String()
	if !strings.Contains(out, "ERROR") {
		t.Errorf("expected ERROR prefix, got: %q", out)
	}
	if !strings.Contains(out, "[op=backup component=elasticsearch instance=prod]") {
		t.Errorf("expected context prefix, got: %q", out)
	}
	if !strings.Contains(out, "index creation failed: permission denied") {
		t.Errorf("expected formatted message, got: %q", out)
	}
}

func TestContextLogger_Debug(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("debug")
	cl := l.WithContext("backup", "zeebe", "dev")

	cl.Debug("partition %d ready", 3)

	out := stdoutBuf.String()
	if !strings.Contains(out, "DEBUG") {
		t.Errorf("expected DEBUG prefix, got: %q", out)
	}
	if !strings.Contains(out, "[op=backup component=zeebe instance=dev]") {
		t.Errorf("expected context prefix, got: %q", out)
	}
	if !strings.Contains(out, "partition 3 ready") {
		t.Errorf("expected formatted message, got: %q", out)
	}
}

func TestContextLogger_Debug_Suppressed(t *testing.T) {
	// Debug logger at info level writes to io.Discard — do not override it.
	l := NewLogger("info")
	buf := &bytes.Buffer{}
	l.infoLogger.SetOutput(buf)
	cl := l.WithContext("backup", "zeebe", "prod")

	cl.Debug("should not appear")
	cl.Info("marker")

	out := buf.String()
	if strings.Contains(out, "should not appear") {
		t.Error("debug should be suppressed at info level")
	}
	if !strings.Contains(out, "marker") {
		t.Error("info should still be written")
	}
}

func TestContextLogger_EmptyContext_Info(t *testing.T) {
	l, stdoutBuf, _ := newTestLogger("info")
	cl := l.WithContext("", "", "")

	cl.Info("no context message")

	out := stdoutBuf.String()
	if !strings.Contains(out, "no context message") {
		t.Errorf("expected message, got: %q", out)
	}
	// Should not contain brackets when context is empty
	if strings.Contains(out, "[") {
		t.Errorf("empty context should produce no brackets, got: %q", out)
	}
}
