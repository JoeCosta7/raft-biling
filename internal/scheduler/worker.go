package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"raft-biling/internal/command"
	"raft-biling/internal/model"
	"runtime/debug"
	"sync"
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

type inFlightTaskKind string

const (
	KindRetry   inFlightTaskKind = "retry"
	KindTimeout inFlightTaskKind = "timeout"
)

type inFlightTask struct {
	exec     model.Execution
	schedule *model.Schedule
	wg       *sync.WaitGroup
	kind     inFlightTaskKind
	ctx      context.Context
	proposer Proposer
}

type freshFireTask struct {
	schedule     *model.Schedule
	scheduledFor time.Time
	nodeID       string
	wg           *sync.WaitGroup
	ctx          context.Context
	proposer     Proposer
}

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
	taskCh   chan any
}

func NewWorker(reader Reader, proposer Proposer, logger *slog.Logger) *Worker {
	return &Worker{
		reader:   reader,
		proposer: proposer,
		nodeID:   proposer.ID(),
		logger:   logger,
		taskCh:   make(chan any, 256),
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
			if err := w.runTick(ctx); err != nil {
				return fmt.Errorf("run tick: %w", err)
			}
		}
	}
}

func (w *Worker) runTick(ctx context.Context) error {
	var wg sync.WaitGroup
	tenants, err := w.reader.ListTenants()
	if err != nil {
		return fmt.Errorf("list tenants: %w", err)
	}
	for _, tenant := range tenants {
		if err := ctx.Err(); err != nil {
			return err
		}
		//Schedule-scan
		//TODO Implement ListSchedulesDue
		schedules, err := w.reader.ListSchedulesDue(tenant.ID)
		if err != nil {
			w.logger.Warn("schedule scan failed", "tenant", tenant.ID, "err", err)
			continue
		}
		for _, schedule := range schedules {
			if err := ctx.Err(); err != nil {
				return err
			}
			if schedule.NextRunAt == nil {
				w.logger.Error("due schedule has nil NextRunAt — index invariant violated",
					"tenant", tenant.ID, "schedule", schedule.ID)
				continue
			}
			scheduledFor := *schedule.NextRunAt
			dispatch := freshFireTask{
				schedule:     &schedule,
				scheduledFor: scheduledFor,
				nodeID:       w.nodeID,
				wg:           &wg,
				ctx:          ctx,
				proposer:     w.proposer,
			}
			wg.Add(1)
			w.taskCh <- dispatch
		}
		//in-flight-scan
		executions, err := w.reader.ListExecutionsByStatus(tenant.ID, model.ExecutionStatusInFlight)
		if err != nil {
			w.logger.Warn("list executions by status for tenant", "tenant", tenant.ID, "err", err)
			continue
		}
		for _, exec := range executions {
			if err := ctx.Err(); err != nil {
				return err
			}
			if exec.LastRetryAt != nil && !exec.LastRetryAt.After(time.Now()) {
				schedule, err := w.reader.GetSchedule(tenant.ID, exec.ScheduleID)
				if err != nil {
					w.logger.Warn("parent schedule load failed",
						"tenant", tenant.ID, "execution", exec.ID, "schedule", exec.ScheduleID, "err", err)
					continue // skip this Execution; try again next tick
				}
				executeRetry := inFlightTask{
					exec:     *exec,
					schedule: &schedule,
					wg:       &wg,
					kind:     KindRetry,
					ctx:      ctx,
					proposer: w.proposer,
				}
				wg.Add(1)
				w.taskCh <- executeRetry
			} else if exec.ClaimedAt != nil && time.Since(*exec.ClaimedAt) > timeoutThreshold {
				executeTimeout := inFlightTask{
					exec:     *exec,
					schedule: nil, // schedule is not needed for timeout tasks
					wg:       &wg,
					kind:     KindTimeout,
					ctx:      ctx,
					proposer: w.proposer,
				}
				wg.Add(1)
				w.taskCh <- executeTimeout
			}
		}
	}
	wg.Wait()
	return nil
}

// 1. Create WaitGroup for this tick
// 2. List tenants
// 3. For each tenant:
//    a. Schedule-scan
//       - for each due Schedule:
//         - construct dispatchTask (isRetry=false)
//         - wg.Add(1)
//         - send task to w.taskCh
//    b. In-flight scan (merged retry + timeout)
//       - for each in-flight Execution:
//         - check last_retry_at <= now → retry task
//         - check now - claimed_at > threshold → timeout task
//         - construct dispatchTask, wg.Add(1), send to w.taskCh
// 4. Wait on WaitGroup
// 5. Return
