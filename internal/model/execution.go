package model

import (
	"time"
)

func DeriveIdempotencyKey(scheduleID string, scheduledFor time.Time) string {
	return scheduleID + ":" + scheduledFor.Format(idempotencyKeyTimeFormat)
}

const idempotencyKeyTimeFormat = time.RFC3339Nano

var CurrentExecutionSchemaVersion = 1

type ExecutionStatus string

const (
	ExecutionStatusPending          ExecutionStatus = "pending"
	ExecutionStatusInFlight         ExecutionStatus = "in_flight"
	ExecutionStatusSucceeded        ExecutionStatus = "succeeded"
	ExecutionStatusFailedTerminal   ExecutionStatus = "failed_terminal"
	ExecutionStatusFailedMaxRetries ExecutionStatus = "failed_max_retries"
)

type Execution struct {
	SchemaVersion  int             `json:"schema_version"`
	ID             string          `json:"id"`
	TenantID       string          `json:"tenant_id"`
	ScheduleID     string          `json:"schedule_id"`
	ScheduledFor   time.Time       `json:"scheduled_for"`
	IdempotencyKey string          `json:"idempotency_key"`
	OwnerNodeID    string          `json:"owner_node_id,omitempty"`
	ClaimedAt      *time.Time      `json:"claimed_at,omitempty"`
	Status         ExecutionStatus `json:"status"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	AttemptCount   int             `json:"attempt_count"`
	LastAttemptID  string          `json:"last_attempt_id"`
	LastRetryAt    *time.Time      `json:"last_retry_at,omitempty"`
	FinalOutcome   string          `json:"final_outcome,omitempty"`
}
