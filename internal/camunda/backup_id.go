package camunda

import (
	"fmt"
	"time"
)

// GenerateBackupID generates a unique backup ID based on timestamp
// Format: YYYYMMDDHHMMSS
func GenerateBackupID() string {
	now := time.Now()
	return now.Format("20060102150405")
}

// GenerateBackupIDWithTimestamp generates a backup ID from a specific timestamp
func GenerateBackupIDWithTimestamp(timestamp time.Time) string {
	return timestamp.Format("20060102150405")
}

// ParseBackupIDTimestamp attempts to parse a backup ID back to a timestamp
// Returns error if the backup ID format is invalid
func ParseBackupIDTimestamp(backupID string) (time.Time, error) {
	return time.Parse("20060102150405", backupID)
}

// IsBackupIDShaped reports whether a string has the YYYYMMDDHHMMSS shape. It is
// a cheap shape check for classifying arbitrary artifact names; callers that
// need a real timestamp use ParseBackupIDTimestamp.
func IsBackupIDShaped(s string) bool {
	if len(s) != 14 {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// ValidateBackupID validates that a backup ID has the correct format
func ValidateBackupID(backupID string) error {
	_, err := ParseBackupIDTimestamp(backupID)
	if err != nil {
		return fmt.Errorf("invalid backup ID format: %w", err)
	}
	return nil
}
