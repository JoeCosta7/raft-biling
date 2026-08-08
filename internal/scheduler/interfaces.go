package scheduler

import (
	"context"
	"raft-biling/internal/model"
	"time"
)

type Reader interface {
	ListTenants() ([]*model.Tenant, error)
	ListExecutionsByStatus(tenantID string, status model.ExecutionStatus) ([]*model.Execution, error)
	ListAttemptsByExecution(tenantID, executionID string) ([]*model.Attempt, error)
}

type Proposer interface {
	Propose(cmdType string, cmd any, timeout time.Duration) (any, error)
	ID() string
	TransferLeadership() error
}

type Runner interface {
	Run(ctx context.Context) error
}
