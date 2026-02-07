package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// CronField represents a single field in a cron expression
type CronField struct {
	min, max int
	values   map[int]bool
}

// CronExpression represents a parsed cron expression
type CronExpression struct {
	minute     CronField
	hour       CronField
	dayOfMonth CronField
	month      CronField
	dayOfWeek  CronField
}

// ParseCronExpression parses a standard cron expression (5 fields: minute hour day month weekday)
func ParseCronExpression(expr string) (*CronExpression, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, fmt.Errorf("invalid cron expression: expected 5 fields, got %d", len(fields))
	}

	cron := &CronExpression{}
	var err error

	// Parse minute (0-59)
	cron.minute, err = parseField(fields[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("invalid minute field: %w", err)
	}

	// Parse hour (0-23)
	cron.hour, err = parseField(fields[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("invalid hour field: %w", err)
	}

	// Parse day of month (1-31)
	cron.dayOfMonth, err = parseField(fields[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("invalid day of month field: %w", err)
	}

	// Parse month (1-12)
	cron.month, err = parseField(fields[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("invalid month field: %w", err)
	}

	// Parse day of week (0-6, 0=Sunday)
	cron.dayOfWeek, err = parseField(fields[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("invalid day of week field: %w", err)
	}

	return cron, nil
}

// parseField parses a single cron field
func parseField(field string, min, max int) (CronField, error) {
	cf := CronField{
		min:    min,
		max:    max,
		values: make(map[int]bool),
	}

	// Handle wildcard
	if field == "*" {
		for i := min; i <= max; i++ {
			cf.values[i] = true
		}
		return cf, nil
	}

	// Handle comma-separated values
	parts := strings.Split(field, ",")
	for _, part := range parts {
		// Handle step values (e.g., */5 or 1-10/2)
		if strings.Contains(part, "/") {
			stepParts := strings.Split(part, "/")
			if len(stepParts) != 2 {
				return cf, fmt.Errorf("invalid step format: %s", part)
			}
			step, err := strconv.Atoi(stepParts[1])
			if err != nil || step <= 0 {
				return cf, fmt.Errorf("invalid step value: %s", stepParts[1])
			}

			// Get range for step
			var rangeMin, rangeMax int
			if stepParts[0] == "*" {
				rangeMin, rangeMax = min, max
			} else if strings.Contains(stepParts[0], "-") {
				rangeParts := strings.Split(stepParts[0], "-")
				if len(rangeParts) != 2 {
					return cf, fmt.Errorf("invalid range format: %s", stepParts[0])
				}
				var err error
				rangeMin, err = strconv.Atoi(rangeParts[0])
				if err != nil {
					return cf, fmt.Errorf("invalid range start: %s", rangeParts[0])
				}
				rangeMax, err = strconv.Atoi(rangeParts[1])
				if err != nil {
					return cf, fmt.Errorf("invalid range end: %s", rangeParts[1])
				}
			} else {
				var err error
				rangeMin, err = strconv.Atoi(stepParts[0])
				if err != nil {
					return cf, fmt.Errorf("invalid step start value: %s", stepParts[0])
				}
				rangeMax = max
			}

			for i := rangeMin; i <= rangeMax; i += step {
				if i >= min && i <= max {
					cf.values[i] = true
				}
			}
			continue
		}

		// Handle ranges (e.g., 1-5)
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			if len(rangeParts) != 2 {
				return cf, fmt.Errorf("invalid range format: %s", part)
			}
			rangeMin, err := strconv.Atoi(rangeParts[0])
			if err != nil {
				return cf, fmt.Errorf("invalid range start: %s", rangeParts[0])
			}
			rangeMax, err := strconv.Atoi(rangeParts[1])
			if err != nil {
				return cf, fmt.Errorf("invalid range end: %s", rangeParts[1])
			}
			if rangeMin > rangeMax {
				return cf, fmt.Errorf("invalid range: start > end")
			}
			for i := rangeMin; i <= rangeMax; i++ {
				if i >= min && i <= max {
					cf.values[i] = true
				}
			}
			continue
		}

		// Handle single value
		val, err := strconv.Atoi(part)
		if err != nil {
			return cf, fmt.Errorf("invalid value: %s", part)
		}
		if val < min || val > max {
			return cf, fmt.Errorf("value %d out of range [%d, %d]", val, min, max)
		}
		cf.values[val] = true
	}

	if len(cf.values) == 0 {
		return cf, fmt.Errorf("no valid values")
	}

	return cf, nil
}

// NextTime calculates the next time the cron expression will trigger after the given time.
// Returns the next matching time and true if found, or zero time and false if no match
// is found within a 4-year search window (to account for leap years like Feb 29).
func (c *CronExpression) NextTime(after time.Time) (time.Time, bool) {
	// Start from the next minute
	t := after.Add(time.Minute).Truncate(time.Minute)

	// Search up to 4 years to handle leap year edge cases (e.g., Feb 29)
	// 4 years = 1461 days (including one leap year)
	maxIterations := 1461 * 24 * 60 // 4 years of minutes
	for i := 0; i < maxIterations; i++ {
		// Check if all fields match
		if c.matches(t) {
			return t, true
		}

		// Advance time
		t = t.Add(time.Minute)
	}

	// No match found within the search window
	return time.Time{}, false
}

// matches checks if the given time matches the cron expression
func (c *CronExpression) matches(t time.Time) bool {
	// Check minute
	if !c.minute.values[t.Minute()] {
		return false
	}

	// Check hour
	if !c.hour.values[t.Hour()] {
		return false
	}

	// Check day of month
	if !c.dayOfMonth.values[t.Day()] {
		return false
	}

	// Check month
	if !c.month.values[int(t.Month())] {
		return false
	}

	// Check day of week (0=Sunday)
	if !c.dayOfWeek.values[int(t.Weekday())] {
		return false
	}

	return true
}

// calculateNextRun calculates the next run time for a cron schedule
func (s *Scheduler) calculateNextRun(schedule string) (*time.Time, error) {
	cron, err := ParseCronExpression(schedule)
	if err != nil {
		return nil, err
	}

	nextRun, found := cron.NextTime(time.Now())
	if !found {
		return nil, fmt.Errorf("no matching time found for schedule %q within 4-year window", schedule)
	}
	return &nextRun, nil
}

// ValidateCronExpression validates a cron expression without scheduling
func ValidateCronExpression(expr string) error {
	_, err := ParseCronExpression(expr)
	return err
}
