package reconcile

import (
	"errors"
	"fmt"
	"time"
)

// Window limits a sweep to the backups whose ID falls on a range of dates.
//
// It is a statement about backup IDs, not wall-clock time: an ID is a
// YYYYMMDDHHMMSS string, and a backup belongs to the window when the YYYYMMDD
// part lies in [From, To]. The controller stamps IDs in UTC; an external job
// that stamps local time is matched by what its IDs say, whatever zone that is.
//
// A nil *Window means "all time", which is how a sweep has always behaved.
//
// What a windowed sweep concludes is about the window only. A backup outside
// it is not assessed: it is neither present nor missing, and nothing about it
// may be reported or acted on.
type Window struct {
	// From and To are inclusive calendar dates, YYYY-MM-DD.
	From string `json:"from"`
	To   string `json:"to"`
}

// maxWindowDays bounds how many days one sweep may span. A day is one Zeebe
// query, so the bound is also a bound on the work a request can start.
const maxWindowDays = 366

const (
	dateLayout   = "2006-01-02"
	idDateLayout = "20060102"
)

// ErrInvalidWindow is returned for a range that is malformed or unreasonable.
var ErrInvalidWindow = errors.New("invalid date range")

// NewWindow validates a range. Both bounds empty means all time (nil, nil);
// giving only one is an error, because a half-open range has no days to
// enumerate.
func NewWindow(from, to string) (*Window, error) {
	if from == "" && to == "" {
		return nil, nil
	}
	if from == "" || to == "" {
		return nil, fmt.Errorf("%w: both from and to are required, or neither for all time", ErrInvalidWindow)
	}
	f, err := time.Parse(dateLayout, from)
	if err != nil {
		return nil, fmt.Errorf("%w: from %q is not a YYYY-MM-DD date", ErrInvalidWindow, from)
	}
	t, err := time.Parse(dateLayout, to)
	if err != nil {
		return nil, fmt.Errorf("%w: to %q is not a YYYY-MM-DD date", ErrInvalidWindow, to)
	}
	if t.Before(f) {
		return nil, fmt.Errorf("%w: from %s is after to %s", ErrInvalidWindow, from, to)
	}
	if days := int(t.Sub(f).Hours()/24) + 1; days > maxWindowDays {
		return nil, fmt.Errorf("%w: %d days is more than the %d-day limit", ErrInvalidWindow, days, maxWindowDays)
	}
	return &Window{From: from, To: to}, nil
}

// LastDays is the window of the last n days of backup IDs, ending tomorrow (UTC).
// Tomorrow is included because a job running ahead of UTC stamps IDs with the
// next date for part of every night.
func LastDays(n int, now time.Time) *Window {
	if n <= 0 {
		return nil
	}
	if n > maxWindowDays {
		n = maxWindowDays
	}
	to := now.UTC().AddDate(0, 0, 1)
	from := to.AddDate(0, 0, -(n - 1))
	return &Window{From: from.Format(dateLayout), To: to.Format(dateLayout)}
}

// Contains reports whether a backup ID is dated inside the window. A nil window
// contains every ID. An ID that does not start with a valid date cannot be
// placed, so a window excludes it.
func (w *Window) Contains(backupID string) bool {
	if w == nil {
		return true
	}
	if len(backupID) < len(idDateLayout) {
		return false
	}
	day, err := time.Parse(idDateLayout, backupID[:len(idDateLayout)])
	if err != nil {
		return false
	}
	f, errF := time.Parse(dateLayout, w.From)
	t, errT := time.Parse(dateLayout, w.To)
	if errF != nil || errT != nil {
		return false
	}
	return !day.Before(f) && !day.After(t)
}

// Days lists every date in the window as YYYYMMDD, the prefix of an ID that
// falls on it.
func (w *Window) Days() []string {
	if w == nil {
		return nil
	}
	f, errF := time.Parse(dateLayout, w.From)
	t, errT := time.Parse(dateLayout, w.To)
	if errF != nil || errT != nil {
		return nil
	}
	var days []string
	for d := f; !d.After(t); d = d.AddDate(0, 0, 1) {
		days = append(days, d.Format(idDateLayout))
	}
	return days
}

// String is the range as people read it, for logs and messages.
func (w *Window) String() string {
	if w == nil {
		return "all time"
	}
	return w.From + " to " + w.To
}
