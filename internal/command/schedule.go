package command

import (
	"encoding/json"
	"fmt"
	"raft-biling/internal/model"
	"raft-biling/storage"
	"time"
)

type CreateScheduleCommand struct {
	ID               string              `json:"id"`
	TenantID         string              `json:"tenant_id"`
	CreatedBy        string              `json:"created_by,omitempty"`
	CallbackURL      string              `json:"callback_url"`
	Payload          json.RawMessage     `json:"payload"`
	Headers          map[string]string   `json:"headers,omitempty"`
	ScheduleType     model.ScheduleType  `json:"schedule_type"`
	FirstRunAt       time.Time           `json:"first_run_at"`
	Recurrence       *model.Recurrence   `json:"recurrence,omitempty"`
	Timezone         string              `json:"timezone"`
	MaxAttempts      int                 `json:"max_attempts"`
	RetryBackoff     model.RetryBackoff  `json:"retry_backoff"`
	ExecutionTimeout time.Duration       `json:"execution_timeout"`
	CatchUpPolicy    model.CatchUpPolicy `json:"catch_up_policy"`
}

func ApplyCreateSchedule(tx storage.Tx, cmd CreateScheduleCommand, proposedAt time.Time) (*model.Schedule, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	existing, err := tx.GetSchedule(cmd.TenantID, cmd.ID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("read failed: %v", err)}
	}
	if existing != nil {
		return nil, &CommandError{Kind: KindConflict, Field: "id", Message: "schedule with this ID already exists"}
	}
	var nextRun *time.Time
	if cmd.ScheduleType == model.ScheduleTypeOnce {
		//one time schedule
		nextRun = &cmd.FirstRunAt
	} else {
		var nr *time.Time
		nr, err = model.ComputeNextRun(cmd.Recurrence, cmd.Timezone, cmd.FirstRunAt)
		if err != nil {
			return nil, &CommandError{Kind: KindValidation, Field: "recurrence", Message: err.Error()}
		}
		nextRun = nr
	}
	schedule := model.Schedule{
		SchemaVersion:    model.CurrentScheduleSchemaVersion,
		ID:               cmd.ID,
		TenantID:         cmd.TenantID,
		CreatedAt:        proposedAt,
		UpdatedAt:        proposedAt,
		CreatedBy:        cmd.CreatedBy,
		CallbackURL:      cmd.CallbackURL,
		Payload:          cmd.Payload,
		Headers:          cmd.Headers,
		ScheduleType:     cmd.ScheduleType,
		FirstRunAt:       cmd.FirstRunAt,
		Recurrence:       cmd.Recurrence,
		Timezone:         cmd.Timezone,
		MaxAttempts:      cmd.MaxAttempts,
		RetryBackoff:     cmd.RetryBackoff,
		ExecutionTimeout: cmd.ExecutionTimeout,
		CatchUpPolicy:    cmd.CatchUpPolicy,
		Status:           model.ScheduleStatusActive,
		NextRunAt:        nextRun,
	}
	if err := tx.PutSchedule(&schedule); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("write failed: %v", err)}
	}
	return &schedule, nil
}
