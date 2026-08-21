package scheduler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"raft-biling/internal/command"
	"raft-biling/internal/model"
	"runtime/debug"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
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

// TODO timeoutThreshold value needs to be figured out
const timeoutThreshold = 5 * time.Minute

// TODO httpClientTimeout value needs to be figured out
const httpClientTimeout = 30 * time.Second

// responseExcerptLimit bounds how much of a callback response body is retained on an Attempt.
const responseExcerptLimit = 4096

type inFlightTaskKind string

const (
	KindRetry   inFlightTaskKind = "retry"
	KindTimeout inFlightTaskKind = "timeout"
)

type inFlightTask struct {
	exec       model.Execution
	schedule   *model.Schedule
	wg         *sync.WaitGroup
	kind       inFlightTaskKind
	ctx        context.Context
	proposer   Proposer
	httpClient *http.Client
	logger     *slog.Logger
}

type freshFireTask struct {
	schedule   *model.Schedule
	exec       model.Execution
	nodeID     string
	wg         *sync.WaitGroup
	ctx        context.Context
	proposer   Proposer
	httpClient *http.Client
	logger     *slog.Logger
}

func asCommandError(result any) *command.CommandError {
	cmdErr, _ := result.(*command.CommandError)
	return cmdErr
}

// 2XX = success
func classifyAttemptOutcome(status int, doErr error, attemptNumber, maxAttempts int) model.AttemptOutcome {
	if doErr == nil && status >= 200 && status < 300 {
		return model.OutcomeSuccess
	}
	if maxAttempts > 0 && attemptNumber >= maxAttempts {
		return model.OutcomeTerminalFailure
	}
	return model.OutcomeRetry
}

func flattenHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, v := range h {
		if len(v) > 0 {
			out[k] = v[0]
		}
	}
	return out
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
	reader     Reader
	proposer   Proposer
	nodeID     string
	logger     *slog.Logger
	taskCh     chan any
	httpClient *http.Client
}

func NewWorker(reader Reader, proposer Proposer, logger *slog.Logger) *Worker {
	return &Worker{
		reader:     reader,
		proposer:   proposer,
		nodeID:     proposer.ID(),
		logger:     logger,
		taskCh:     make(chan any, 256),
		httpClient: &http.Client{Timeout: httpClientTimeout},
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

func (t *freshFireTask) run(ctx context.Context) error {
	execID := t.exec.ID
	if err := ctx.Err(); err != nil {
		return err
	}
	attemptID := ulid.Make().String()
	startedAt := time.Now()
	body := t.schedule.Payload
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.schedule.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request for execution %s: %w", execID, err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range t.schedule.Headers {
		req.Header.Set(k, v)
	}
	bodyHash := sha256.Sum256(body)
	resp, doErr := t.httpClient.Do(req)
	completedAt := time.Now()
	var respStatus int
	var respHeaders map[string]string
	var respExcerpt string
	if resp != nil {
		defer resp.Body.Close()
		respStatus = resp.StatusCode
		respHeaders = flattenHeaders(resp.Header)

		excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, responseExcerptLimit))
		respExcerpt = string(excerpt)
		if readErr != nil {
			t.logger.Warn("failed to fully read callback response body",
				"execution", execID, "err", readErr)
		}
	}
	const attemptNumber = 1
	outcome := classifyAttemptOutcome(respStatus, doErr, attemptNumber, t.schedule.MaxAttempts)
	var retryAt *time.Time
	if outcome == model.OutcomeRetry {
		at := completedAt.Add(t.schedule.RetryBackoff.DelayForAttempt(attemptNumber))
		retryAt = &at
	}
	recordCmd := command.RecordAttemptCommand{
		TenantID:            t.schedule.TenantID,
		ExecutionID:         execID,
		ID:                  attemptID,
		NodeID:              t.nodeID,
		StartedAt:           &startedAt,
		CompletedAt:         &completedAt,
		Outcome:             outcome,
		RequestURL:          t.schedule.CallbackURL,
		RequestHeaders:      t.schedule.Headers,
		RequestBodyHash:     hex.EncodeToString(bodyHash[:]),
		ResponseStatus:      respStatus,
		ResponseBodyExcerpt: respExcerpt,
		ResponseHeaders:     respHeaders,
		RetryAt:             retryAt,
	}

	result, err := t.proposer.Propose("record_attempt", recordCmd, proposeTimeout)
	if err != nil {
		return fmt.Errorf("propose record attempt for execution %s: %w", execID, err)
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		return fmt.Errorf("record attempt rejected for execution %s: %w", execID, cmdErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if outcome != model.OutcomeRetry {
		finalStatus := model.ExecutionStatusSucceeded
		if outcome == model.OutcomeTerminalFailure {
			finalStatus = model.ExecutionStatusFailedTerminal
		}
		completeCmd := command.CompleteExecutionCommand{
			TenantID:     t.schedule.TenantID,
			ExecutionID:  execID,
			FinalStatus:  finalStatus,
			FinalOutcome: string(outcome),
		}
		result, err := t.proposer.Propose("complete_execution", completeCmd, proposeTimeout)
		if err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", execID, err)
		}
		if cmdErr := asCommandError(result); cmdErr != nil {
			return fmt.Errorf("complete execution rejected for execution %s: %w", execID, cmdErr)
		}
	}

	return nil
}

func (t *inFlightTask) run(ctx context.Context) {
	defer t.wg.Done()

	execID := t.exec.ID

	if err := ctx.Err(); err != nil {
		t.logger.Warn("in-flight task not started: context already cancelled",
			"kind", t.kind, "execution", execID, "err", err)
		return
	}

	var err error
	switch t.kind {
	case KindRetry:
		err = t.runRetry(ctx, execID)
	case KindTimeout:
		err = t.runTimeout(execID)
	default:
		t.logger.Error("inFlightTask: unhandled kind", "kind", t.kind, "execution", execID)
		return
	}
	if err != nil {
		t.logger.Error("in-flight task failed", "kind", t.kind, "execution", execID, "err", err)
	}
}

func (t *inFlightTask) runTimeout(execID string) error {
	cmd := command.FailExecutionTimeoutCommand{
		TenantID:     t.exec.TenantID,
		ExecutionID:  execID,
		FinalOutcome: model.FinalOutcomeTimedOut,
	}
	result, err := t.proposer.Propose("fail_execution_timeout", cmd, proposeTimeout)
	if err != nil {
		return fmt.Errorf("propose fail execution timeout for execution %s: %w", execID, err)
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		if cmdErr.Kind == command.KindConflict {
			t.logger.Info("execution already terminal, timeout not applied",
				"execution", execID, "err", cmdErr)
			return nil
		}
		return fmt.Errorf("fail execution timeout rejected for execution %s: %w", execID, cmdErr)
	}
	return nil
}

func (t *inFlightTask) runRetry(ctx context.Context, execID string) error {
	attemptID := ulid.Make().String()
	startedAt := time.Now()
	body := t.schedule.Payload
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.schedule.CallbackURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request for execution %s: %w", execID, err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range t.schedule.Headers {
		req.Header.Set(k, v)
	}
	bodyHash := sha256.Sum256(body)
	resp, doErr := t.httpClient.Do(req)
	completedAt := time.Now()
	var respStatus int
	var respHeaders map[string]string
	var respExcerpt string
	if resp != nil {
		defer resp.Body.Close()
		respStatus = resp.StatusCode
		respHeaders = flattenHeaders(resp.Header)

		excerpt, readErr := io.ReadAll(io.LimitReader(resp.Body, responseExcerptLimit))
		respExcerpt = string(excerpt)
		if readErr != nil {
			t.logger.Warn("failed to fully read callback response body",
				"execution", execID, "err", readErr)
		}
	}

	attemptNumber := t.exec.AttemptCount + 1
	outcome := classifyAttemptOutcome(respStatus, doErr, attemptNumber, t.schedule.MaxAttempts)
	var retryAt *time.Time
	if outcome == model.OutcomeRetry {
		at := completedAt.Add(t.schedule.RetryBackoff.DelayForAttempt(attemptNumber))
		retryAt = &at
	}
	recordCmd := command.RecordAttemptCommand{
		TenantID:            t.schedule.TenantID,
		ExecutionID:         execID,
		ID:                  attemptID,
		NodeID:              t.proposer.ID(),
		StartedAt:           &startedAt,
		CompletedAt:         &completedAt,
		Outcome:             outcome,
		RequestURL:          t.schedule.CallbackURL,
		RequestHeaders:      t.schedule.Headers,
		RequestBodyHash:     hex.EncodeToString(bodyHash[:]),
		ResponseStatus:      respStatus,
		ResponseBodyExcerpt: respExcerpt,
		ResponseHeaders:     respHeaders,
		RetryAt:             retryAt,
	}

	result, err := t.proposer.Propose("record_attempt", recordCmd, proposeTimeout)
	if err != nil {
		return fmt.Errorf("propose record attempt for execution %s: %w", execID, err)
	}
	if cmdErr := asCommandError(result); cmdErr != nil {
		return fmt.Errorf("record attempt rejected for execution %s: %w", execID, cmdErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if outcome != model.OutcomeRetry {
		finalStatus := model.ExecutionStatusSucceeded
		if outcome == model.OutcomeTerminalFailure {
			finalStatus = model.ExecutionStatusFailedTerminal
		}
		completeCmd := command.CompleteExecutionCommand{
			TenantID:     t.schedule.TenantID,
			ExecutionID:  execID,
			FinalStatus:  finalStatus,
			FinalOutcome: string(outcome),
		}
		result, err := t.proposer.Propose("complete_execution", completeCmd, proposeTimeout)
		if err != nil {
			return fmt.Errorf("propose complete for execution %s: %w", execID, err)
		}
		if cmdErr := asCommandError(result); cmdErr != nil {
			return fmt.Errorf("complete execution rejected for execution %s: %w", execID, cmdErr)
		}
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
			// Claim here, before the task is constructed, so the (possibly slow) HTTP
			// dispatch in freshFireTask.run never holds up a Raft round-trip, and a
			// losing race against another node's scan is resolved cheaply — as a
			// rejected claim — rather than after paying for an HTTP call.
			claimCmd := command.ClaimExecutionCommand{
				ID:           ulid.Make().String(),
				TenantID:     tenant.ID,
				ScheduleID:   schedule.ID,
				ScheduledFor: scheduledFor,
				OwnerNodeID:  w.nodeID,
			}
			result, err := w.proposer.Propose("claim_execution", claimCmd, proposeTimeout)
			if err != nil {
				w.logger.Warn("propose claim execution failed",
					"tenant", tenant.ID, "schedule", schedule.ID, "execution", claimCmd.ID, "err", err)
				continue
			}
			if cmdErr := asCommandError(result); cmdErr != nil {
				if cmdErr.Kind == command.KindConflict {
					// Another node's scan already claimed this occurrence — expected
					// under concurrent scanning, not a failure.
					w.logger.Info("execution already claimed for schedule occurrence",
						"tenant", tenant.ID, "schedule", schedule.ID, "scheduled_for", scheduledFor)
				} else {
					w.logger.Warn("claim execution rejected",
						"tenant", tenant.ID, "schedule", schedule.ID, "execution", claimCmd.ID, "err", cmdErr)
				}
				continue
			}
			claimedExec, ok := result.(*model.Execution)
			if !ok || claimedExec == nil {
				w.logger.Error("claim execution returned unexpected result",
					"tenant", tenant.ID, "schedule", schedule.ID)
				continue
			}
			dispatch := freshFireTask{
				schedule:   schedule,
				exec:       *claimedExec,
				nodeID:     w.nodeID,
				wg:         &wg,
				ctx:        ctx,
				proposer:   w.proposer,
				httpClient: w.httpClient,
				logger:     w.logger,
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
					exec:       *exec,
					schedule:   schedule,
					wg:         &wg,
					kind:       KindRetry,
					ctx:        ctx,
					proposer:   w.proposer,
					httpClient: w.httpClient,
					logger:     w.logger,
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
					// httpClient not needed for timeout tasks — no HTTP call on this path
					logger: w.logger,
				}
				wg.Add(1)
				w.taskCh <- executeTimeout
			}
		}
	}
	wg.Wait()
	return nil
}
