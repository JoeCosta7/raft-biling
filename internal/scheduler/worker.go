package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"raft-biling/internal/command"
	"raft-biling/internal/model"
	"runtime/debug"
	"time"
)

type Bucket string

const (
	BucketNoAttempts      Bucket = "no_attempts"
	BucketSuccess         Bucket = "success"
	BucketTerminalFailure Bucket = "terminal_failure"
	BucketRetryElapsed    Bucket = "retry_elapsed"
	BucketRetryPending    Bucket = "retry_pending"
)

// proposeTimeout bounds how long a single recovery-driven Propose call waits.
const proposeTimeout = 5 * time.Second

func classifyOne(exec model.Execution, attempts []*model.Attempt, now time.Time) (Bucket, error) {
	if len(attempts) == 0 {
		return BucketNoAttempts, nil
	}
	switch attempts[len(attempts)-1].Outcome {
	case model.OutcomeSuccess:
		return BucketSuccess, nil
	case model.OutcomeTerminalFailure:
		return BucketTerminalFailure, nil
	case model.OutcomeRetry:
		if !now.Before(attempts[len(attempts)-1].RetryAt) {
			return BucketRetryElapsed, nil
		}
		return BucketRetryPending, nil
	default:
		return "", fmt.Errorf("unknown outcome value: %v", attempts[len(attempts)-1].Outcome)
	}
}

type Worker struct {
	reader   Reader
	proposer Proposer
	nodeID   string
	logger   *slog.Logger
}

func NewWorker(reader Reader, proposer Proposer, logger *slog.Logger) *Worker {
	return &Worker{
		reader:   reader,
		proposer: proposer,
		nodeID:   proposer.ID(),
		logger:   logger,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if err := w.runRecover(ctx); err != nil {
		return err
	}
	if err := w.steadyState(ctx); err != nil {
		return err
	}
	return nil
}

func (w *Worker) runRecover(ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recover: panic during recovery: %v\n%s", r, debug.Stack())
		}
	}()

	tenants, err := w.reader.ListTenants()
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}

	for _, tenant := range tenants {
		if err := ctx.Err(); err != nil {
			return err
		}
		execs, err := w.reader.ListExecutionsByStatus(tenant.ID, model.ExecutionStatusInFlight)
		if err != nil {
			return fmt.Errorf("list in-flight for tenant %s: %w", tenant.ID, err)
		}

		for _, exec := range execs {
			if err := ctx.Err(); err != nil {
				return err
			}
			attempts, err := w.reader.ListAttemptsByExecution(tenant.ID, exec.ID)
			if err != nil {
				return fmt.Errorf("read attempts for execution %s: %w", exec.ID, err)
			}

			bucket, err := classifyOne(*exec, attempts, time.Now())
			if err != nil {
				return fmt.Errorf("classify for execution %s: %w", exec.ID, err)
			}

			if err := w.adoptOrComplete(ctx, *tenant, *exec, bucket); err != nil {
				return fmt.Errorf("adopt or complete for execution %s: %w", exec.ID, err)
			}
		}
	}
	return nil
}

func (w *Worker) adoptOrComplete(ctx context.Context, tenant model.Tenant, exec model.Execution, bucket Bucket) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch bucket {
	case BucketNoAttempts, BucketRetryElapsed, BucketRetryPending:
		cmd := command.AdoptExecutionCommand{
			TenantID:      tenant.ID,
			ExecutionID:   exec.ID,
			PreviousOwner: exec.OwnerNodeID,
			NewOwner:      w.nodeID,
		}
		if _, err := w.proposer.Propose("adopt_execution", cmd, proposeTimeout); err != nil {
			return fmt.Errorf("propose adopt for execution %s: %w", exec.ID, err)
		}
		return nil
	case BucketSuccess:
		cmd := command.CompleteExecutionCommand{
			TenantID:     tenant.ID,
			ExecutionID:  exec.ID,
			FinalStatus:  model.ExecutionStatusSucceeded,
			FinalOutcome: string(model.OutcomeSuccess),
		}
		if _, err := w.proposer.Propose("complete_execution", cmd, proposeTimeout); err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", exec.ID, err)
		}
		return nil
	case BucketTerminalFailure:
		cmd := command.CompleteExecutionCommand{
			TenantID:     tenant.ID,
			ExecutionID:  exec.ID,
			FinalStatus:  model.ExecutionStatusFailedTerminal,
			FinalOutcome: string(model.OutcomeTerminalFailure),
		}
		if _, err := w.proposer.Propose("complete_execution", cmd, proposeTimeout); err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", exec.ID, err)
		}
		return nil
	default:
		return fmt.Errorf("adoptOrComplete: unhandled bucket %q", bucket)
	}
}

func (w *Worker) steadyState(ctx context.Context) error {
	//TODO ticker value needs to be figured out
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			//Tick body for each step
		}
	}
}
