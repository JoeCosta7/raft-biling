package command

import (
	"errors"
	"fmt"
	"raft-biling/internal/model"
	"raft-biling/internal/storage"
	"time"
)

func IsTerminal(s model.ExecutionStatus) bool {
	switch s {
	case model.ExecutionStatusSucceeded, model.ExecutionStatusFailedTerminal, model.ExecutionStatusFailedMaxRetries:
		return true
	default:
		return false
	}
}

var errFoundCollision = errors.New("collision found")

type ClaimExecutionCommand struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	ScheduleID   string    `json:"schedule_id"`
	ScheduledFor time.Time `json:"scheduled_for"`
	OwnerNodeID  string    `json:"owner_node_id,omitempty"`
}

type AdoptExecutionCommand struct {
	TenantID      string `json:"tenant_id"`
	ExecutionID   string `json:"execution_id"`
	PreviousOwner string `json:"previous_owner"`
	NewOwner      string `json:"new_owner"`
}

type RecordAttemptCommand struct {
	TenantID            string               `json:"tenant_id"`
	ExecutionID         string               `json:"execution_id"`
	ID                  string               `json:"id"`
	NodeID              string               `json:"node_id"`
	StartedAt           *time.Time           `json:"started_at,omitempty"`
	CompletedAt         *time.Time           `json:"completed_at,omitempty"`
	Outcome             model.AttemptOutcome `json:"outcome"`
	RetryAt             *time.Time           `json:"retry_at,omitempty"`
	RequestURL          string               `json:"request_url"`
	RequestHeaders      map[string]string    `json:"request_headers,omitempty"`
	RequestBodyHash     string               `json:"request_body_hash"`
	ResponseStatus      int                  `json:"response_status"`
	ResponseBodyExcerpt string               `json:"response_body_excerpt,omitempty"`
	ResponseHeaders     map[string]string    `json:"response_headers,omitempty"`
}

type CompleteExecutionCommand struct {
	TenantID     string                `json:"tenant_id"`
	ExecutionID  string                `json:"execution_id"`
	FinalStatus  model.ExecutionStatus `json:"final_status"`
	FinalOutcome string                `json:"final_outcome"`
	NextRunAt    *time.Time            `json:"next_run_at,omitempty"`
}

type FailExecutionTimeoutCommand struct {
	TenantID     string `json:"tenant_id"`
	ExecutionID  string `json:"execution_id"`
	FinalOutcome string `json:"final_outcome"`
}

// Claim Execution
// Record Attempt
func ApplyClaimExecution(tx storage.Tx, cmd ClaimExecutionCommand, proposedAt time.Time) (*model.Execution, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ScheduleID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "schedule_id", Message: "schedule_id is missing"}
	}
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	if cmd.ScheduledFor.IsZero() {
		return nil, &CommandError{Kind: KindValidation, Field: "scheduled_for", Message: "scheduled_for is zero"}
	}
	if cmd.OwnerNodeID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "owner_node_id", Message: "owner_node_id is missing"}
	}
	var existing *model.Execution
	err := tx.ListExecutionsBySchedule(cmd.TenantID, cmd.ScheduleID, func(ex *model.Execution) error {
		if ex.ScheduledFor.Equal(cmd.ScheduledFor) {
			existing = ex
			return errFoundCollision
		}
		return nil
	})
	if err != nil && !errors.Is(err, errFoundCollision) {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("list executions failed: %v", err)}
	}
	if existing != nil {
		return nil, &CommandError{
			Kind:    KindConflict,
			Field:   "scheduled_for",
			Message: "an execution already exists for this schedule at scheduled_for",
			Details: map[string]any{"existing_execution_id": existing.ID},
		}
	}
	ex := &model.Execution{
		SchemaVersion:  model.CurrentExecutionSchemaVersion,
		ID:             cmd.ID,
		TenantID:       cmd.TenantID,
		ScheduleID:     cmd.ScheduleID,
		ScheduledFor:   cmd.ScheduledFor,
		IdempotencyKey: model.DeriveIdempotencyKey(cmd.ScheduleID, cmd.ScheduledFor),
		OwnerNodeID:    cmd.OwnerNodeID,
		ClaimedAt:      &proposedAt,
		Status:         model.ExecutionStatusInFlight,
		StartedAt:      nil,
		CompletedAt:    nil,
		AttemptCount:   0,
		LastAttemptID:  "",
		FinalOutcome:   "",
	}
	if err := tx.PutExecution(ex); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return ex, nil
}

func ApplyAdoptExecution(tx storage.Tx, cmd AdoptExecutionCommand, proposedAt time.Time) (*model.Execution, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ExecutionID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "execution_id", Message: "execution_id is missing"}
	}
	if cmd.PreviousOwner == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "current_owner", Message: "current_owner is missing"}
	}
	if cmd.NewOwner == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "new_owner", Message: "new_owner is missing"}
	}
	if cmd.NewOwner == cmd.PreviousOwner {
		return nil, &CommandError{Kind: KindValidation, Field: "new_owner", Message: "new_owner is the same as current owner"}
	}
	exec, err := tx.GetExecution(cmd.TenantID, cmd.ExecutionID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("get execution failed: %v", err)}
	}
	if exec == nil {
		return nil, &CommandError{Kind: KindNotFound, Message: "no execution exists"}
	}
	if exec.Status != model.ExecutionStatusInFlight {
		return nil, &CommandError{Kind: KindConflict, Field: "status", Message: "status must be in flight"}
	}
	if exec.OwnerNodeID != cmd.PreviousOwner {
		return nil, &CommandError{Kind: KindConflict, Field: "owner_node_id", Message: "owner node mismatch"}
	}
	updated := *exec
	updated.OwnerNodeID = cmd.NewOwner
	if err := tx.PutExecution(&updated); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return &updated, nil
}

func ApplyRecordAttempt(tx storage.Tx, cmd RecordAttemptCommand, proposedAt time.Time) (*model.Attempt, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ExecutionID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "execution_id", Message: "execution_id is missing"}
	}
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	if cmd.NodeID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "node_id", Message: "node_id is missing"}
	}
	if cmd.Outcome == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "outcome", Message: "outcome is missing"}
	}
	if cmd.StartedAt == nil {
		return nil, &CommandError{Kind: KindValidation, Field: "started_at", Message: "started_at is missing"}
	}
	if cmd.StartedAt.IsZero() {
		return nil, &CommandError{Kind: KindValidation, Field: "started_at", Message: "started_at is zero"}
	}
	if cmd.CompletedAt == nil {
		return nil, &CommandError{Kind: KindValidation, Field: "completed_at", Message: "completed_at is missing"}
	}
	if cmd.CompletedAt.IsZero() {
		return nil, &CommandError{Kind: KindValidation, Field: "completed_at", Message: "completed_at is zero"}
	}
	if cmd.RequestURL == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "request_url", Message: "request_url is missing"}
	}
	if cmd.Outcome == model.OutcomeSuccess && cmd.ResponseStatus == 0 {
		return nil, &CommandError{Kind: KindValidation, Field: "response_status", Message: "response_status is missing"}
	}
	exec, err := tx.GetExecution(cmd.TenantID, cmd.ExecutionID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("get execution failed: %v", err)}
	}
	if exec == nil {
		return nil, &CommandError{Kind: KindNotFound, Message: "no execution exists"}
	}
	if exec.Status != model.ExecutionStatusInFlight {
		return nil, &CommandError{Kind: KindConflict, Field: "status", Message: "status must be in flight"}
	}
	if exec.OwnerNodeID != cmd.NodeID {
		return nil, &CommandError{Kind: KindConflict, Field: "owner_node_id", Message: "owner node mismatch"}
	}
	attempt := model.Attempt{
		SchemaVersion:       model.CurrentAttemptSchemaVersion,
		ID:                  cmd.ID,
		TenantID:            cmd.TenantID,
		ExecutionID:         cmd.ExecutionID,
		NodeID:              cmd.NodeID,
		AttemptNumber:       exec.AttemptCount + 1,
		StartedAt:           cmd.StartedAt,
		CompletedAt:         cmd.CompletedAt,
		RequestURL:          cmd.RequestURL,
		RequestHeaders:      cmd.RequestHeaders,
		RequestBodyHash:     cmd.RequestBodyHash,
		ResponseStatus:      cmd.ResponseStatus,
		ResponseBodyExcerpt: cmd.ResponseBodyExcerpt,
		ResponseHeaders:     cmd.ResponseHeaders,
		Outcome:             cmd.Outcome,
	}
	if cmd.RetryAt != nil {
		attempt.RetryAt = *cmd.RetryAt
	}
	if err := tx.PutAttempt(&attempt); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	updated := *exec
	updated.AttemptCount = exec.AttemptCount + 1
	updated.LastAttemptID = attempt.ID
	updated.LastRetryAt = cmd.RetryAt
	if err := tx.PutExecution(&updated); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return &attempt, nil

}

func ApplyCompleteExecution(tx storage.Tx, cmd CompleteExecutionCommand, proposedAt time.Time) (*model.Execution, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ExecutionID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "execution_id", Message: "execution_id is missing"}
	}
	if cmd.FinalStatus == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "final_status", Message: "final_status is missing"}
	}
	if cmd.FinalOutcome == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "final_outcome", Message: "final_outcome is missing"}
	}
	if !IsTerminal(cmd.FinalStatus) {
		return nil, &CommandError{Kind: KindValidation, Field: "status", Message: "final_status must be terminal"}
	}
	exec, err := tx.GetExecution(cmd.TenantID, cmd.ExecutionID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("get execution failed: %v", err)}
	}
	if exec == nil {
		return nil, &CommandError{Kind: KindNotFound, Message: "no execution exists"}
	}
	if IsTerminal(exec.Status) {
		return nil, &CommandError{Kind: KindConflict, Field: "status", Message: "execution is already terminal"}
	}
	updated := *exec
	updated.Status = cmd.FinalStatus
	updated.FinalOutcome = cmd.FinalOutcome
	updated.CompletedAt = &proposedAt
	updated.OwnerNodeID = ""
	if err := tx.PutExecution(&updated); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	schedule, err := tx.GetSchedule(cmd.TenantID, exec.ScheduleID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("get schedule failed: %v", err)}
	}
	if schedule == nil {
		return nil, &CommandError{Kind: KindNotFound, Message: "no schedule exists"}
	}
	newSchedule := *schedule
	if newSchedule.ScheduleType == model.ScheduleTypeRecurring {
		if newSchedule.Status == model.ScheduleStatusActive {
			newSchedule.NextRunAt, err = model.ComputeNextRunAfter(newSchedule.Recurrence, "UTC", newSchedule.FirstRunAt, exec.ScheduledFor)
			if err != nil {
				return nil, &CommandError{Kind: KindValidation, Field: "recurrence", Message: "next run at could now be found"}
			}
		}
	} else {
		newSchedule.Status = model.ScheduleStatusCompleted
		newSchedule.NextRunAt = nil
	}
	if err := tx.PutSchedule(&newSchedule); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return &updated, nil
}

func ApplyFailExecutionTimeout(tx storage.Tx, cmd FailExecutionTimeoutCommand, proposedAt time.Time) (*model.Execution, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ExecutionID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "execution_id", Message: "execution_id is missing"}
	}
	if cmd.FinalOutcome == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "final_outcome", Message: "final_outcome is missing"}
	}
	exec, err := tx.GetExecution(cmd.TenantID, cmd.ExecutionID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("get execution failed: %v", err)}
	}
	if exec == nil {
		return nil, &CommandError{Kind: KindNotFound, Message: "no execution exists"}
	}
	if IsTerminal(exec.Status) {
		return nil, &CommandError{Kind: KindConflict, Field: "status", Message: "execution is already terminal"}
	}
	updated := *exec
	updated.Status = model.ExecutionStatusFailedMaxRetries
	updated.FinalOutcome = cmd.FinalOutcome
	updated.CompletedAt = &proposedAt
	updated.OwnerNodeID = ""
	if err := tx.PutExecution(&updated); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	schedule, err := tx.GetSchedule(cmd.TenantID, exec.ScheduleID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("get schedule failed: %v", err)}
	}
	if schedule == nil {
		return nil, &CommandError{Kind: KindNotFound, Message: "no schedule exists"}
	}
	newSchedule := *schedule
	if newSchedule.ScheduleType == model.ScheduleTypeRecurring {
		if newSchedule.Status == model.ScheduleStatusActive {
			newSchedule.NextRunAt, err = model.ComputeNextRunAfter(newSchedule.Recurrence, "UTC", newSchedule.FirstRunAt, exec.ScheduledFor)
			if err != nil {
				return nil, &CommandError{Kind: KindValidation, Field: "recurrence", Message: "next run at could not be found"}
			}
		}
	} else {
		newSchedule.Status = model.ScheduleStatusCompleted
		newSchedule.NextRunAt = nil
	}
	if err := tx.PutSchedule(&newSchedule); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return &updated, nil
}
