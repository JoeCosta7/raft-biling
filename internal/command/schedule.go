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

type UpdateScheduleCommand struct {
	TenantID         string
	ID               string
	CallbackURL      *string
	Headers          *map[string]string
	Payload          *json.RawMessage
	RetryBackoff     *model.RetryBackoff
	MaxAttempts      *int
	ExecutionTimeout *time.Duration
}

type PauseScheduleCommand struct {
	TenantID string
	ID       string
}

type CancelScheduleCommand struct {
	TenantID string
	ID       string
}

type ResumeScheduleCommand struct {
	TenantID string
	ID       string
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
		nr, err = model.ComputeNextRunOnOrAfter(cmd.Recurrence, cmd.Timezone, cmd.FirstRunAt, cmd.FirstRunAt)
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

func ApplyUpdateSchedule(tx storage.Tx, cmd UpdateScheduleCommand, proposedAt time.Time) (*model.Schedule, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	existing, err := tx.GetSchedule(cmd.TenantID, cmd.ID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("Getting prexisiting row failed: %v", err)}
	}
	if existing == nil {
		return nil, &CommandError{Kind: KindNotFound, Field: "id", Message: "schedule not found"}
	}
	if existing.Status == model.ScheduleStatusCanceled {
		return nil, &CommandError{Kind: KindConflict, Field: "status", Message: "you cannot modify a canceled schedule"}
	}
	updated := *existing
	if cmd.CallbackURL != nil {
		updated.CallbackURL = *cmd.CallbackURL
	}
	if cmd.Headers != nil {
		updated.Headers = *cmd.Headers
	}
	if cmd.Payload != nil {
		updated.Payload = *cmd.Payload
	}
	if cmd.RetryBackoff != nil {
		updated.RetryBackoff = *cmd.RetryBackoff
	}
	if cmd.MaxAttempts != nil {
		updated.MaxAttempts = *cmd.MaxAttempts
	}
	if cmd.ExecutionTimeout != nil {
		updated.ExecutionTimeout = *cmd.ExecutionTimeout
	}
	updated.UpdatedAt = proposedAt
	if err := tx.PutSchedule(&updated); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("update failed: %v", err)}
	}
	return &updated, nil
}

func ApplyPauseSchedule(tx storage.Tx, cmd PauseScheduleCommand, proposedAt time.Time) (*model.Schedule, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	existing, err := tx.GetSchedule(cmd.TenantID, cmd.ID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("Getting preexisiting row failed: %v", err)}
	}
	if existing == nil {
		return nil, &CommandError{Kind: KindNotFound, Field: "id", Message: "schedule not found"}
	}
	if existing.Status == model.ScheduleStatusCanceled {
		return nil, &CommandError{Kind: KindConflict, Field: "status", Message: "you cannot pause a canceled schedule"}
	}
	if existing.Status == model.ScheduleStatusCompleted {
		return nil, &CommandError{Kind: KindConflict, Field: "status", Message: "you cannot pause a completed schedule"}
	}
	updated := *existing
	updated.Status = model.ScheduleStatusPaused
	updated.NextRunAt = nil
	updated.UpdatedAt = proposedAt
	if err := tx.PutSchedule(&updated); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("pause: put schedule failed: %v", err)}
	}
	return &updated, nil
}

func ApplyCancelSchedule(tx storage.Tx, cmd CancelScheduleCommand, proposedAt time.Time) (*model.Schedule, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	existing, err := tx.GetSchedule(cmd.TenantID, cmd.ID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("Getting preexisiting row failed: %v", err)}
	}
	if existing == nil {
		return nil, &CommandError{Kind: KindNotFound, Field: "id", Message: "schedule not found"}
	}
	updated := *existing
	updated.Status = model.ScheduleStatusCanceled
	updated.NextRunAt = nil
	updated.UpdatedAt = proposedAt
	if err := tx.PutSchedule(&updated); err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("schedule canceled: %v", err)}
	}
	return &updated, nil

}

func ApplyResumeSchedule(tx storage.Tx, cmd ResumeScheduleCommand, proposedAt time.Time) (*model.Schedule, error) {
	if cmd.TenantID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "tenant_id", Message: "tenant_id is missing"}
	}
	if cmd.ID == "" {
		return nil, &CommandError{Kind: KindValidation, Field: "id", Message: "id is missing"}
	}
	existing, err := tx.GetSchedule(cmd.TenantID, cmd.ID)
	if err != nil {
		return nil, &CommandError{Kind: KindStorage, Message: fmt.Sprintf("Getting preexisiting row failed: %v", err)}
	}
	if existing == nil {
		return nil, &CommandError{Kind: KindNotFound, Field: "id", Message: "schedule not found"}
	}
}
