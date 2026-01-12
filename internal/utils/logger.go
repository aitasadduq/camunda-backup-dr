package utils

import (
	"io"
	"log"
	"os"
)

// Logger wraps the standard logger for structured logging
type Logger struct {
	infoLogger  *log.Logger
	errorLogger *log.Logger
	debugLogger *log.Logger
}

// NewLogger creates a new logger instance
func NewLogger(logLevel string) *Logger {
	infoLogger := log.New(os.Stdout, "INFO  ", log.Ldate|log.Ltime|log.Lshortfile)
	errorLogger := log.New(os.Stderr, "ERROR ", log.Ldate|log.Ltime|log.Lshortfile)

	var debugWriter io.Writer = os.Stdout
	// Set debug logger output based on log level
	if logLevel != "debug" {
		debugWriter = io.Discard
	}
	debugLogger := log.New(debugWriter, "DEBUG ", log.Ldate|log.Ltime|log.Lshortfile)

	return &Logger{
		infoLogger:  infoLogger,
		errorLogger: errorLogger,
		debugLogger: debugLogger,
	}
}

// Info logs an info message
func (l *Logger) Info(format string, v ...interface{}) {
	l.infoLogger.Printf(format, v...)
}

// Error logs an error message
func (l *Logger) Error(format string, v ...interface{}) {
	l.errorLogger.Printf(format, v...)
}

// Debug logs a debug message
func (l *Logger) Debug(format string, v ...interface{}) {
	l.debugLogger.Printf(format, v...)
}

// WithBackupID adds a backup ID prefix to the log message
func (l *Logger) WithBackupID(backupID string) *BackupLogger {
	return &BackupLogger{
		logger:   l,
		backupID: backupID,
	}
}

// BackupLogger is a logger with backup ID context
type BackupLogger struct {
	logger   *Logger
	backupID string
}

// Info logs an info message with backup ID
func (bl *BackupLogger) Info(format string, v ...interface{}) {
	bl.logger.Info("[BACKUP_ID: %s] "+format, append([]interface{}{bl.backupID}, v...)...)
}

// Error logs an error message with backup ID
func (bl *BackupLogger) Error(format string, v ...interface{}) {
	bl.logger.Error("[BACKUP_ID: %s] "+format, append([]interface{}{bl.backupID}, v...)...)
}

// Debug logs a debug message with backup ID
func (bl *BackupLogger) Debug(format string, v ...interface{}) {
	bl.logger.Debug("[BACKUP_ID: %s] "+format, append([]interface{}{bl.backupID}, v...)...)
}
