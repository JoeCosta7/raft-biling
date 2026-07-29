package command

import (
	"errors"
	"fmt"
	"raft-biling/internal/model"
	"raft-biling/storage"
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
		SchemaVersion:  model.CurrentScheduleSchemaVersion,
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
