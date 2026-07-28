package model

import (
	"testing"
	"time"
)

func newIntervalRecurrence(seconds int) *Recurrence {
	return &Recurrence{
		Cadence: CadenceInterval,
		Seconds: &seconds,
	}
}
func newDailyRecurrence() *Recurrence {
	return &Recurrence{
		Cadence: CadenceDaily,
	}
}
func newWeeklyRecurrence(day Weekday) *Recurrence {
	return &Recurrence{
		Cadence:   CadenceWeekly,
		DayOfWeek: &day,
	}
}
func newMonthlyRecurrence(dayOfMonth int) *Recurrence {
	return &Recurrence{
		Cadence:    CadenceMonthly,
		DayOfMonth: &dayOfMonth,
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromEqualsFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt
	expected := firstRunAt
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromOnLaterSlot(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(2 * 3600 * time.Second)
	expected := from
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromBetweenSlots(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(90 * time.Minute)
	expected := firstRunAt.Add(7200 * time.Second)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Interval_FromBeforeFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(-3600 * time.Second)
	expected := firstRunAt
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromEqualsFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt
	expected := firstRunAt.Add(3600 * time.Second)
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromOnLaterSlot(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(2 * 3600 * time.Second)
	expected := from.Add(3600 * time.Second)
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromBetweenSlots(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(90 * time.Minute)
	expected := firstRunAt.Add(7200 * time.Second)
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunAfter_Interval_FromBeforeFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(-3600 * time.Second)
	expected := firstRunAt
	got, err := ComputeNextRunAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeLatestRunOnOrBefore_Interval_FromBetweenSlots(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(90 * time.Minute)
	expected := firstRunAt.Add(3600 * time.Second)
	got, err := ComputeLatestRunOnOrBefore(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeLatestRunOnOrBefore_Interval_FromBeforeFirstRunAt(t *testing.T) {
	rec := newIntervalRecurrence(3600)
	firstRunAt := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(-3600 * time.Second)
	got, err := ComputeLatestRunOnOrBefore(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", *got)
	}
}

func TestComputeNextRunOnOrAfter_Weekly_FromBetweenSlots(t *testing.T) {
	rec := newWeeklyRecurrence(WeekdayMonday)
	firstRunAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(10 * 24 * time.Hour)
	expected := firstRunAt.Add(14 * 24 * time.Hour)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeLatestRunOnOrBefore_Weekly_FromBetweenSlots(t *testing.T) {
	rec := newWeeklyRecurrence(WeekdayMonday)
	firstRunAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(10 * 24 * time.Hour)
	expected := firstRunAt.Add(7 * 24 * time.Hour)
	got, err := ComputeLatestRunOnOrBefore(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Monthly_HappyPath(t *testing.T) {
	rec := newMonthlyRecurrence(15)
	firstRunAt := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	from := time.Date(2025, 2, 10, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 2, 15, 12, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Monthly_ClampFebruary(t *testing.T) {
	rec := newMonthlyRecurrence(31)
	firstRunAt := time.Date(2025, 1, 31, 12, 0, 0, 0, time.UTC)
	from := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 2, 28, 12, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Monthly_WalkingAnniversary(t *testing.T) {
	rec := newMonthlyRecurrence(31)
	firstRunAt := time.Date(2025, 1, 31, 12, 0, 0, 0, time.UTC)
	from := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	expected := time.Date(2025, 3, 31, 12, 0, 0, 0, time.UTC)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}

func TestComputeNextRunOnOrAfter_Daily_FromBetweenSlots(t *testing.T) {
	rec := newDailyRecurrence()
	firstRunAt := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	from := firstRunAt.Add(36 * time.Hour)
	expected := firstRunAt.Add(48 * time.Hour)
	got, err := ComputeNextRunOnOrAfter(rec, "UTC", firstRunAt, from)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if !got.Equal(expected) {
		t.Errorf("got %v, want %v", got, expected)
	}
}
