package reconcile

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestNewWindow(t *testing.T) {
	tests := []struct {
		name     string
		from, to string
		want     *Window
		wantErr  bool
	}{
		{name: "neither means all time", want: nil},
		{name: "a range", from: "2026-11-01", to: "2026-11-17", want: &Window{From: "2026-11-01", To: "2026-11-17"}},
		{name: "a single day", from: "2026-11-17", to: "2026-11-17", want: &Window{From: "2026-11-17", To: "2026-11-17"}},
		{name: "only from is refused", from: "2026-11-01", wantErr: true},
		{name: "only to is refused", to: "2026-11-01", wantErr: true},
		{name: "from after to", from: "2026-11-17", to: "2026-11-01", wantErr: true},
		{name: "not a date", from: "yesterday", to: "2026-11-01", wantErr: true},
		{name: "not a real date", from: "2026-02-30", to: "2026-03-01", wantErr: true},
		{name: "wrong layout", from: "20261101", to: "20261117", wantErr: true},
		{name: "366 days is allowed", from: "2026-01-01", to: "2026-12-31", want: &Window{From: "2026-01-01", To: "2026-12-31"}},
		{name: "more than 366 days is refused", from: "2025-01-01", to: "2026-12-31", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewWindow(tt.from, tt.to)
			if tt.wantErr {
				if !errors.Is(err, ErrInvalidWindow) {
					t.Fatalf("expected ErrInvalidWindow, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestWindowContains(t *testing.T) {
	w := &Window{From: "2026-11-10", To: "2026-11-12"}
	tests := map[string]bool{
		"20261110000000": true, // first moment of the first day
		"20261112235959": true, // last moment of the last day
		"20261111103001": true,
		"20261109235959": false,
		"20261113000000": false,
		"short":          false, // cannot be placed in a date
		"2026111":        false,
		"abcdefgh010101": false,
		"":               false,
	}
	for id, want := range tests {
		if got := w.Contains(id); got != want {
			t.Errorf("Contains(%q) = %v, want %v", id, got, want)
		}
	}

	var all *Window
	for _, id := range []string{"20261110000000", "short", ""} {
		if !all.Contains(id) {
			t.Errorf("a nil window must contain %q", id)
		}
	}
}

func TestWindowDays(t *testing.T) {
	w := &Window{From: "2026-11-29", To: "2026-12-02"}
	want := []string{"20261129", "20261130", "20261201", "20261202"}
	if got := w.Days(); !reflect.DeepEqual(got, want) {
		t.Errorf("Days() = %v, want %v (must cross a month boundary)", got, want)
	}
	var all *Window
	if all.Days() != nil {
		t.Error("a nil window has no days to enumerate")
	}
}

// The window ends tomorrow: a job running ahead of UTC stamps tomorrow's date on
// IDs for part of every night, and a window ending today would not see them.
func TestLastDaysEndsTomorrowUTC(t *testing.T) {
	now := time.Date(2026, 11, 17, 21, 30, 0, 0, time.UTC)
	got := LastDays(7, now)
	if got.From != "2026-11-12" || got.To != "2026-11-18" {
		t.Errorf("LastDays(7) = %+v, want 2026-11-12..2026-11-18 (7 days)", got)
	}
	if !got.Contains("20261118013000") {
		t.Error("an ID dated tomorrow must be inside the window")
	}
	if LastDays(0, now) != nil || LastDays(-1, now) != nil {
		t.Error("zero or negative days means all time")
	}
	if w := LastDays(100000, now); len(w.Days()) > maxWindowDays {
		t.Errorf("window not capped: %d days", len(w.Days()))
	}
}
