package command

import (
	"encoding/json"
	"fmt"
	"time"

	"raft-biling/internal/apperr"
	"raft-biling/internal/storage"
)

type LogEntry struct {
	Type       string          `json:"type"`
	ProposedAt time.Time       `json:"proposed_at"`
	Payload    json.RawMessage `json:"payload"`
}

type CommandError = apperr.CommandError

const (
	KindValidation = apperr.KindValidation
	KindConflict   = apperr.KindConflict
	KindNotFound   = apperr.KindNotFound
	KindStorage    = apperr.KindStorage
)

func Dispatch(tx storage.Tx, cmdType string, payload json.RawMessage, proposedAt time.Time) (any, error) {
	switch cmdType {
	case "create_tenant":
		var cmd CreateTenantCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyCreateTenant(tx, cmd, proposedAt)
	case "create_schedule":
		var cmd CreateScheduleCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyCreateSchedule(tx, cmd, proposedAt)
	case "update_schedule":
		var cmd UpdateScheduleCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyUpdateSchedule(tx, cmd, proposedAt)
	case "pause_schedule":
		var cmd PauseScheduleCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyPauseSchedule(tx, cmd, proposedAt)
	case "cancel_schedule":
		var cmd CancelScheduleCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyCancelSchedule(tx, cmd, proposedAt)
	case "resume_schedule":
		var cmd ResumeScheduleCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyResumeSchedule(tx, cmd, proposedAt)
	case "claim_execution":
		var cmd ClaimExecutionCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyClaimExecution(tx, cmd, proposedAt)
	case "adopt_execution":
		var cmd AdoptExecutionCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyAdoptExecution(tx, cmd, proposedAt)
	case "record_attempt":
		var cmd RecordAttemptCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyRecordAttempt(tx, cmd, proposedAt)
	case "complete_execution":
		var cmd CompleteExecutionCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyCompleteExecution(tx, cmd, proposedAt)
	case "fail_execution_timeout":
		var cmd FailExecutionTimeoutCommand
		if err := json.Unmarshal(payload, &cmd); err != nil {
			return nil, err
		}
		return ApplyFailExecutionTimeout(tx, cmd, proposedAt)
	default:
		return nil, fmt.Errorf("unknown log entry type: %q", cmdType)
	}
}
