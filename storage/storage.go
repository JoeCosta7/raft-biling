package storage

import (
	"github.com/JoeCosta7/raft-biling/model"
)

// bbolt api
type Storage interface {
	Update(func(tx Tx) error) error
	View(func(tx Tx) error) error
	Close() error
}

type Tx interface {
	GetSchedule(tenantID, id string) (*model.Schedule, error)
	PutSchedule(s *model.Schedule) error

	GetExecution(tenantID, id string) (*model.Execution, error)
	PutExecution(ex *model.Execution) error

	GetAttempt(tenantID, id string) (*model.Attempt, error)
	PutAttempt(a *model.Attempt) error

	ListExecutionsBySchedule(tenantID, scheduleID string, fn func(*model.Execution) error) error
	ListExecutionsByStatus(tenantID string, status model.ExecutionStatus, fn func(*model.Execution) error) error
	ListAttemptsByExecution(tenantID, executionID string, fn func(*model.Attempt) error) error
}

//User facing
//Create Schedule, check whether a schedule with the tenant_id, & id already exists, read a schedule by tenant_id, id and write a schedule
//Update Schedule, check whether the schedule acutally exists, but get covers that and then update it, which put covers
//Pause Schedule, get to check if schedule exists, then use Put to set status to paused
//Resume Schedule, get to check if schedule exists, then use Put to set status to active
//Cancel Schedule, get to check if schedule exists, then use Put to set status to canceled
//Internal Commands
//Claim Exectuion, check whether exectuion already exists, create if not already existing, set owner
//Transfer Execuiotn, use put to set new owner and update claimed_at
//RecordAttempt appends Attempt row, calls put execution to updates parent's attempt count and last_attempt_id. Maybe need another function for proposer verification
//CompleteExecution, use put to update Execution & clear owner node id. Also put to schedule to advance next_run_at.
//FailExecutionTimeout, use put to update execution failed with timeout reasoning
