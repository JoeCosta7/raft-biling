package model

import "time"

type AttemptOutcome string

const (
	OutcomeSuccess         AttemptOutcome = "success"
	OutcomeRetry           AttemptOutcome = "retry"
	OutcomeTerminalFailure AttemptOutcome = "terminal_failure"
)

type Attempt struct {
	SchemaVersion       int               `json:"schema_version"`
	ID                  string            `json:"id"`
	TenantID            string            `json:"tenant_id"`
	ExecutionID         string            `json:"execution_id"`
	NodeID              string            `json:"node_id"`
	AttemptNumber       int               `json:"attempt_number"`
	StartedAt           *time.Time        `json:"started_at,omitempty"`
	CompletedAt         *time.Time        `json:"completed_at,omitempty"`
	RequestURL          string            `json:"request_url"`
	RequestHeaders      map[string]string `json:"request_headers,omitempty"`
	RequestBodyHash     string            `json:"request_body_hash"`
	ResponseStatus      int               `json:"response_status"`
	ResponseBodyExcerpt string            `json:"response_body_excerpt,omitempty"`
	ResponseHeaders     map[string]string `json:"response_headers,omitempty"`
	Outcome             AttemptOutcome    `json:"outcome"`
}
