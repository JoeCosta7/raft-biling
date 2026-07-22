package model

import (
	"encoding/json"
	"time"
)

type ScheduleType string

const (
	ScheduleTypeOnce      ScheduleType = "once"
	ScheduleTypeRecurring ScheduleType = "recurring"
)

type ScheduleStatus string

const (
	ScheduleStatusActive   ScheduleStatus = "active"
	ScheduleStatusPaused   ScheduleStatus = "paused"
	ScheduleStatusCanceled ScheduleStatus = "canceled"
)

type CatchUpPolicy string

const (
	CatchUpAll        CatchUpPolicy = "catch_up_all"
	SkipMissed        CatchUpPolicy = "skip_missed"
	CatchUpLatestOnly CatchUpPolicy = "catch_up_latest_only"
)

type Cadence string

const (
	CadenceMonthly  Cadence = "monthly"
	CadenceWeekly   Cadence = "weekly"
	CadenceDaily    Cadence = "daily"
	CadenceInterval Cadence = "interval"
)

type Weekday string

const (
	WeekdayMonday    Weekday = "monday"
	WeekdayTuesday   Weekday = "tuesday"
	WeekdayWednesday Weekday = "wednesday"
	WeekdayThursday  Weekday = "thursday"
	WeekdayFriday    Weekday = "friday"
	WeekdaySaturday  Weekday = "saturday"
	WeekdaySunday    Weekday = "sunday"
)

type Recurrence struct {
	Cadence    Cadence  `json:"cadence"`
	DayOfMonth *int     `json:"day_of_month,omitempty"`
	DayOfWeek  *Weekday `json:"day_of_week,omitempty"`
	Seconds    *int     `json:"seconds,omitempty"`
}

type RetryBackoff struct {
	Initial    time.Duration `json:"initial"`
	Multiplier float64       `json:"multiplier"`
	Max        time.Duration `json:"max"`
}

type Schedule struct {
	SchemaVersion    int               `json:"schema_version"`
	ID               string            `json:"id"`
	TenantID         string            `json:"tenant_id"`
	CreatedAt        time.Time         `json:"created_at"`
	UpdatedAt        time.Time         `json:"updated_at"`
	CreatedBy        string            `json:"created_by,omitempty"`
	CallbackURL      string            `json:"callback_url"`
	Payload          json.RawMessage   `json:"payload"`
	Headers          map[string]string `json:"headers,omitempty"`
	ScheduleType     ScheduleType      `json:"schedule_type"`
	FirstRunAt       time.Time         `json:"first_run_at"`
	Recurrence       *Recurrence       `json:"recurrence"`
	Timezone         string            `json:"timezone"`
	MaxAttempts      int               `json:"max_attempts"`
	RetryBackoff     RetryBackoff      `json:"retry_backoff"`
	ExecutionTimeout time.Duration     `json:"execution_timeout"`
	CatchUpPolicy    CatchUpPolicy     `json:"catch_up_policy"`
	Status           ScheduleStatus    `json:"status"`
	NextRunAt        time.Time         `json:"next_run_at"`
}
