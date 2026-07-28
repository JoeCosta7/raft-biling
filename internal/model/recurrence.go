package model

import (
	"math"
	"time"

	"raft-biling/internal/apperr"
)

// (comment describing what it does)
func computeIntervalOnOrAfter(firstRunAt time.Time, seconds int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) || from.Equal(firstRunAt) {
		return &firstRunAt, nil
	}
	timeDifference := from.Sub(firstRunAt)
	N := math.Ceil(timeDifference.Seconds() / float64(seconds))
	offset := time.Duration(int(N)*seconds) * time.Second
	result := firstRunAt.Add(offset)
	return &result, nil
}

func computeDailyOnOrAfter(firstRunAt time.Time, from time.Time) (*time.Time, error) {
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	if from.Before(firstRunAt) {
		return &firstRunAt, nil
	}
	candidate := time.Date(from.Year(), from.Month(), from.Day(), hour, minute, second, 0, time.UTC)
	if candidate.Before(from) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	return &candidate, nil
}

func computeWeeklyOnOrAfter(firstRunAt time.Time, from time.Time) (*time.Time, error) {
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	if from.Before(firstRunAt) {
		return &firstRunAt, nil
	}
	targetWeekday := firstRunAt.Weekday()
	daysUntilTarget := (int(targetWeekday) - int(from.Weekday()) + 7) % 7
	candidate := time.Date(from.Year(), from.Month(), from.Day()+daysUntilTarget, hour, minute, second, 0, time.UTC)
	if candidate.Before(from) {
		candidate = candidate.AddDate(0, 0, 7)
	}
	return &candidate, nil
}

func computeMonthlyOnOrAfter(firstRunAt time.Time, dayOfMonth int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return &firstRunAt, nil
	}
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	year, month := from.Year(), from.Month()
	for i := 0; i < 120; i++ {
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
		actualDay := min(dayOfMonth, lastDay)
		candidate := time.Date(year, month, actualDay, hour, minute, second, 0, time.UTC)
		if !candidate.Before(from) {
			return &candidate, nil
		}
		next := time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
		year, month = next.Year(), next.Month()
	}
	return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "day_of_month", Message: "computeMonthlyOnOrAfter: no slot found within 120 months"}
}

// Creating schedule initially or Resuming catch_up_all with no prior exec
func ComputeNextRunOnOrAfter(recurrence *Recurrence, timezone string, firstRunAt, from time.Time) (*time.Time, error) {
	if recurrence == nil {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "recurrence", Message: "recurrence: no recurrence, cannot compute"}
	}
	//TODO fix and add more timezones
	if timezone != "UTC" {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "timezone", Message: "timezone: no timezone functionality yet"}
	}
	switch recurrence.Cadence {
	case CadenceInterval:
		return computeIntervalOnOrAfter(firstRunAt, *recurrence.Seconds, from)
	case CadenceDaily:
		return computeDailyOnOrAfter(firstRunAt, from)
	case CadenceWeekly:
		return computeWeeklyOnOrAfter(firstRunAt, from)
	case CadenceMonthly:
		return computeMonthlyOnOrAfter(firstRunAt, *recurrence.DayOfMonth, from)
	default:
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "cadence", Message: "recurrence.cadence: unknown cadence"}
	}
}

func computeIntervalAfter(firstRunAt time.Time, seconds int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return &firstRunAt, nil
	}
	timeDifference := from.Sub(firstRunAt)
	N := math.Floor(timeDifference.Seconds()/float64(seconds)) + 1
	offset := time.Duration(int(N)*seconds) * time.Second
	result := firstRunAt.Add(offset)
	return &result, nil
}

func computeDailyAfter(firstRunAt time.Time, from time.Time) (*time.Time, error) {
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	if from.Before(firstRunAt) {
		return &firstRunAt, nil
	}
	candidate := time.Date(from.Year(), from.Month(), from.Day(), hour, minute, second, 0, time.UTC)
	if !candidate.After(from) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	return &candidate, nil
}

func computeWeeklyAfter(firstRunAt time.Time, from time.Time) (*time.Time, error) {
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	if from.Before(firstRunAt) {
		return &firstRunAt, nil
	}
	candidate := time.Date(from.Year(), from.Month(), from.Day(), hour, minute, second, 0, time.UTC)
	if !candidate.After(from) {
		candidate = candidate.AddDate(0, 0, 7)
	}
	return &candidate, nil
}

func computeMonthlyAfter(firstRunAt time.Time, dayOfMonth int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return &firstRunAt, nil
	}
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	year, month := from.Year(), from.Month()
	for i := 0; i < 120; i++ {
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
		actualDay := min(dayOfMonth, lastDay)
		candidate := time.Date(year, month, actualDay, hour, minute, second, 0, time.UTC)
		if candidate.After(from) {
			return &candidate, nil
		}
		next := time.Date(year, month+1, 1, 0, 0, 0, 0, time.UTC)
		year, month = next.Year(), next.Month()
	}
	return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "day_of_month", Message: "computeMonthlyAfter: no slot found within 120 months"}
}

func ComputeNextRunAfter(recurrence *Recurrence, timezone string, firstRunAt, from time.Time) (*time.Time, error) {
	if recurrence == nil {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "recurrence", Message: "recurrence: no recurrence, cannot compute"}
	}
	//TODO fix and add more timezones
	if timezone != "UTC" {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "timezone", Message: "timezone: no timezone functionality yet"}
	}
	switch recurrence.Cadence {
	case CadenceInterval:
		return computeIntervalAfter(firstRunAt, *recurrence.Seconds, from)
	case CadenceDaily:
		return computeDailyAfter(firstRunAt, from)
	case CadenceWeekly:
		return computeWeeklyAfter(firstRunAt, from)
	case CadenceMonthly:
		return computeMonthlyAfter(firstRunAt, *recurrence.DayOfMonth, from)
	default:
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "cadence", Message: "recurrence.cadence: unknown cadence"}
	}
}

func computeIntervalLatestOnOrBefore(firstRunAt time.Time, seconds int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return nil, nil
	}
	timeDifference := from.Sub(firstRunAt)
	N := math.Floor(timeDifference.Seconds() / float64(seconds))
	offset := time.Duration(int(N)*seconds) * time.Second
	result := firstRunAt.Add(offset)
	return &result, nil
}

func computeDailyLatestOnOrBefore(firstRunAt time.Time, from time.Time) (*time.Time, error) {
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	if from.Before(firstRunAt) {
		return nil, nil
	}
	candidate := time.Date(from.Year(), from.Month(), from.Day(), hour, minute, second, 0, time.UTC)
	if candidate.After(from) {
		candidate = candidate.AddDate(0, 0, -1)
	}
	return &candidate, nil
}

func computeWeeklyLatestOnOrBefore(firstRunAt time.Time, from time.Time) (*time.Time, error) {
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	if from.Before(firstRunAt) {
		return nil, nil
	}
	targetWeekday := firstRunAt.Weekday()
	daysBackToTarget := (int(from.Weekday()) - int(targetWeekday) + 7) % 7
	candidate := time.Date(from.Year(), from.Month(), from.Day()-daysBackToTarget, hour, minute, second, 0, time.UTC)

	if candidate.After(from) {
		candidate = candidate.AddDate(0, 0, -7)
	}
	return &candidate, nil
}

func computeMonthlyLatestOnOrBefore(firstRunAt time.Time, dayOfMonth int, from time.Time) (*time.Time, error) {
	if from.Before(firstRunAt) {
		return nil, nil
	}
	hour, minute, second := firstRunAt.Hour(), firstRunAt.Minute(), firstRunAt.Second()
	year, month := from.Year(), from.Month()
	for i := 0; i < 120; i++ {
		lastDay := time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
		actualDay := min(dayOfMonth, lastDay)
		candidate := time.Date(year, month, actualDay, hour, minute, second, 0, time.UTC)
		if !candidate.After(from) {
			return &candidate, nil
		}
		prev := time.Date(year, month-1, 1, 0, 0, 0, 0, time.UTC)
		year, month = prev.Year(), prev.Month()
	}
	return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "day_of_month", Message: "computeMonthlyLatestOnOrBefore: no slot found within 120 months"}
}

// Catch up latest only
func ComputeLatestRunOnOrBefore(recurrence *Recurrence, timezone string, firstRunAt, from time.Time) (*time.Time, error) {
	if recurrence == nil {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "recurrence", Message: "recurrence: no recurrence, cannot compute"}
	}
	//TODO fix and add more timezones
	if timezone != "UTC" {
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "timezone", Message: "timezone: no timezone functionality yet"}
	}
	switch recurrence.Cadence {
	case CadenceInterval:
		return computeIntervalLatestOnOrBefore(firstRunAt, *recurrence.Seconds, from)
	case CadenceDaily:
		return computeDailyLatestOnOrBefore(firstRunAt, from)
	case CadenceWeekly:
		return computeWeeklyLatestOnOrBefore(firstRunAt, from)
	case CadenceMonthly:
		return computeMonthlyLatestOnOrBefore(firstRunAt, *recurrence.DayOfMonth, from)
	default:
		return nil, &apperr.CommandError{Kind: apperr.KindValidation, Field: "cadence", Message: "recurrence.cadence: unknown cadence"}
	}
}
