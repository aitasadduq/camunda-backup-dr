package scheduler

import (
	"testing"
	"time"
)

func TestParseCronExpression_Valid(t *testing.T) {
	tests := []struct {
		name string
		expr string
	}{
		{"every minute", "* * * * *"},
		{"daily at 2am", "0 2 * * *"},
		{"every hour", "0 * * * *"},
		{"weekdays at 9am", "0 9 * * 1-5"},
		{"every 5 minutes", "*/5 * * * *"},
		{"monthly first day", "0 0 1 * *"},
		{"specific time", "30 14 * * *"},
		{"multiple hours", "0 9,12,17 * * *"},
		{"range of days", "0 0 1-15 * *"},
		{"weekend", "0 10 * * 0,6"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cron, err := ParseCronExpression(tt.expr)
			if err != nil {
				t.Errorf("ParseCronExpression(%q) error = %v", tt.expr, err)
				return
			}
			if cron == nil {
				t.Errorf("ParseCronExpression(%q) returned nil", tt.expr)
			}
		})
	}
}

func TestParseCronExpression_Invalid(t *testing.T) {
	tests := []struct {
		name string
		expr string
	}{
		{"empty", ""},
		{"too few fields", "* * *"},
		{"too many fields", "* * * * * *"},
		{"invalid minute", "60 * * * *"},
		{"invalid hour", "* 25 * * *"},
		{"invalid day", "* * 32 * *"},
		{"invalid month", "* * * 13 *"},
		{"invalid weekday", "* * * * 8"},
		{"non-numeric", "a * * * *"},
		{"invalid range", "5-2 * * * *"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseCronExpression(tt.expr)
			if err == nil {
				t.Errorf("ParseCronExpression(%q) expected error, got nil", tt.expr)
			}
		})
	}
}

func TestCronExpression_NextTime(t *testing.T) {
	// Test at a known time: 2024-01-15 10:30:00 (Monday)
	baseTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name     string
		expr     string
		expected time.Time
	}{
		{
			name:     "next minute",
			expr:     "* * * * *",
			expected: time.Date(2024, 1, 15, 10, 31, 0, 0, time.UTC),
		},
		{
			name:     "daily at 2am (next day)",
			expr:     "0 2 * * *",
			expected: time.Date(2024, 1, 16, 2, 0, 0, 0, time.UTC),
		},
		{
			name:     "every hour on the hour",
			expr:     "0 * * * *",
			expected: time.Date(2024, 1, 15, 11, 0, 0, 0, time.UTC),
		},
		{
			name:     "at 14:30 same day",
			expr:     "30 14 * * *",
			expected: time.Date(2024, 1, 15, 14, 30, 0, 0, time.UTC),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cron, err := ParseCronExpression(tt.expr)
			if err != nil {
				t.Fatalf("Failed to parse expression: %v", err)
			}

			next := cron.NextTime(baseTime)
			if !next.Equal(tt.expected) {
				t.Errorf("NextTime() = %v, expected %v", next, tt.expected)
			}
		})
	}
}

func TestCronExpression_Matches(t *testing.T) {
	tests := []struct {
		name    string
		expr    string
		time    time.Time
		matches bool
	}{
		{
			name:    "wildcard matches any time",
			expr:    "* * * * *",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			matches: true,
		},
		{
			name:    "specific minute matches",
			expr:    "30 * * * *",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			matches: true,
		},
		{
			name:    "specific minute does not match",
			expr:    "0 * * * *",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			matches: false,
		},
		{
			name:    "specific hour matches",
			expr:    "30 10 * * *",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			matches: true,
		},
		{
			name:    "specific hour does not match",
			expr:    "30 11 * * *",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			matches: false,
		},
		{
			name:    "weekday matches Monday (1)",
			expr:    "30 10 * * 1",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), // Monday
			matches: true,
		},
		{
			name:    "weekday range matches",
			expr:    "30 10 * * 1-5",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), // Monday
			matches: true,
		},
		{
			name:    "weekend does not match Monday",
			expr:    "30 10 * * 0,6",
			time:    time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), // Monday
			matches: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cron, err := ParseCronExpression(tt.expr)
			if err != nil {
				t.Fatalf("Failed to parse expression: %v", err)
			}

			if cron.matches(tt.time) != tt.matches {
				t.Errorf("matches(%v) = %v, expected %v", tt.time, !tt.matches, tt.matches)
			}
		})
	}
}

func TestParseField_Step(t *testing.T) {
	// Test */5 for minutes (should produce 0, 5, 10, 15, ...)
	field, err := parseField("*/5", 0, 59)
	if err != nil {
		t.Fatalf("Failed to parse */5: %v", err)
	}

	expectedValues := []int{0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}
	for _, v := range expectedValues {
		if !field.values[v] {
			t.Errorf("Expected value %d to be present", v)
		}
	}

	// Values not in step should not be present
	unexpectedValues := []int{1, 2, 3, 4, 6, 7, 8, 9}
	for _, v := range unexpectedValues {
		if field.values[v] {
			t.Errorf("Did not expect value %d to be present", v)
		}
	}
}

func TestParseField_Range(t *testing.T) {
	// Test 1-5 for weekday
	field, err := parseField("1-5", 0, 6)
	if err != nil {
		t.Fatalf("Failed to parse 1-5: %v", err)
	}

	expectedValues := []int{1, 2, 3, 4, 5}
	for _, v := range expectedValues {
		if !field.values[v] {
			t.Errorf("Expected value %d to be present", v)
		}
	}

	// 0 and 6 should not be present
	if field.values[0] {
		t.Error("Did not expect value 0 to be present")
	}
	if field.values[6] {
		t.Error("Did not expect value 6 to be present")
	}
}

func TestParseField_List(t *testing.T) {
	// Test 9,12,17 for hours
	field, err := parseField("9,12,17", 0, 23)
	if err != nil {
		t.Fatalf("Failed to parse 9,12,17: %v", err)
	}

	if len(field.values) != 3 {
		t.Errorf("Expected 3 values, got %d", len(field.values))
	}

	for _, v := range []int{9, 12, 17} {
		if !field.values[v] {
			t.Errorf("Expected value %d to be present", v)
		}
	}
}

func TestValidateCronExpression(t *testing.T) {
	// Valid expression
	if err := ValidateCronExpression("0 2 * * *"); err != nil {
		t.Errorf("Expected valid expression, got error: %v", err)
	}

	// Invalid expression
	if err := ValidateCronExpression("invalid"); err == nil {
		t.Error("Expected error for invalid expression")
	}
}

func TestCronExpression_RealWorldSchedules(t *testing.T) {
	// Test common backup schedules
	schedules := []string{
		"0 2 * * *",    // Daily at 2 AM
		"0 */6 * * *",  // Every 6 hours
		"0 0 * * 0",    // Weekly on Sunday at midnight
		"0 3 1 * *",    // Monthly on the 1st at 3 AM
		"30 4 * * 1-5", // Weekdays at 4:30 AM
		"0 0 1,15 * *", // 1st and 15th of each month
	}

	for _, schedule := range schedules {
		t.Run(schedule, func(t *testing.T) {
			cron, err := ParseCronExpression(schedule)
			if err != nil {
				t.Errorf("Failed to parse %q: %v", schedule, err)
				return
			}

			// Verify we can calculate next run time
			now := time.Now()
			next := cron.NextTime(now)
			if next.Before(now) || next.Equal(now) {
				t.Errorf("Next run time %v should be after now %v", next, now)
			}
		})
	}
}
