package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"raft-biling/internal/command"
	"raft-biling/internal/model"
)

type fakeReader struct {
	tenants        []*model.Tenant
	execsByTenant  map[string][]*model.Execution
	attemptsByExec map[string][]*model.Attempt

	listTenantsErr    error
	listExecutionsErr map[string]error
	listAttemptsErr   map[string]error
}

func (f *fakeReader) ListTenants() ([]*model.Tenant, error) {
	if f.listTenantsErr != nil {
		return nil, f.listTenantsErr
	}
	return f.tenants, nil
}

func (f *fakeReader) ListExecutionsByStatus(tenantID string, status model.ExecutionStatus) ([]*model.Execution, error) {
	if err := f.listExecutionsErr[tenantID]; err != nil {
		return nil, err
	}
	return f.execsByTenant[tenantID], nil
}

func (f *fakeReader) ListAttemptsByExecution(tenantID, executionID string) ([]*model.Attempt, error) {
	if err := f.listAttemptsErr[executionID]; err != nil {
		return nil, err
	}
	return f.attemptsByExec[executionID], nil
}

func (f *fakeReader) ListSchedulesDue(tenantID string) ([]*model.Schedule, error) {
	return nil, nil
}

func (f *fakeReader) GetSchedule(tenantID, id string) (*model.Schedule, error) {
	return nil, nil
}

type proposal struct {
	cmdType string
	cmd     any
	timeout time.Duration
}

type fakeProposer struct {
	calls []proposal
	err   error
	id    string
}

func (f *fakeProposer) Propose(cmdType string, cmd any, timeout time.Duration) (any, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.calls = append(f.calls, proposal{cmdType: cmdType, cmd: cmd, timeout: timeout})
	return nil, nil
}

func (f *fakeProposer) ID() string { return f.id }

func (f *fakeProposer) TransferLeadership() error { return nil }

func TestClassifyOne_EmptyAttempts(t *testing.T) {
	bucket, err := classifyOne(model.Execution{}, nil, time.Now())
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketNoAttempts {
		t.Errorf("bucket: got %q, want %q", bucket, BucketNoAttempts)
	}
}

func TestClassifyOne_LastAttemptSuccess(t *testing.T) {
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeSuccess},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, time.Now())
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketSuccess {
		t.Errorf("bucket: got %q, want %q", bucket, BucketSuccess)
	}
}

func TestClassifyOne_LastAttemptTerminalFailure(t *testing.T) {
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeTerminalFailure},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, time.Now())
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketTerminalFailure {
		t.Errorf("bucket: got %q, want %q", bucket, BucketTerminalFailure)
	}
}

func TestClassifyOne_RetryElapsed(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt.Add(1 * time.Hour) // well past RetryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryElapsed {
		t.Errorf("bucket: got %q, want %q", bucket, BucketRetryElapsed)
	}
}

func TestClassifyOne_RetryPending(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt.Add(-1 * time.Hour) // well before RetryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryPending {
		t.Errorf("bucket: got %q, want %q", bucket, BucketRetryPending)
	}
}

func TestClassifyOne_RetryBoundary(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryElapsed {
		t.Errorf("bucket: got %q, want %q (now == RetryAt must count as elapsed)", bucket, BucketRetryElapsed)
	}
}

func TestClassifyOne_UnknownOutcome(t *testing.T) {
	attempts := []*model.Attempt{
		{Outcome: model.AttemptOutcome("bogus_outcome")},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, time.Now())
	if err == nil {
		t.Fatal("classifyOne: expected error for unknown outcome, got nil")
	}
	if bucket != "" {
		t.Errorf("bucket: got %q, want empty on error", bucket)
	}
	if !strings.Contains(err.Error(), "bogus_outcome") {
		t.Errorf("error message: got %q, want it to mention the outcome value", err.Error())
	}
}

func TestClassifyOne_MultipleAttempts_LastMatters(t *testing.T) {
	retryAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	now := retryAt.Add(1 * time.Hour) // well past RetryAt
	attempts := []*model.Attempt{
		{Outcome: model.OutcomeSuccess},
		{Outcome: model.OutcomeRetry, RetryAt: retryAt},
	}
	bucket, err := classifyOne(model.Execution{}, attempts, now)
	if err != nil {
		t.Fatalf("classifyOne: unexpected error: %v", err)
	}
	if bucket != BucketRetryElapsed {
		t.Errorf("bucket: got %q, want %q (should classify off the last attempt, not the first)", bucket, BucketRetryElapsed)
	}
}

func TestRunRecover_Buckets(t *testing.T) {
	const (
		tenantID   = "tenant-1"
		execID     = "exec-1"
		nodeID     = "node-self"
		otherOwner = "node-other"
	)
	now := time.Now()

	tests := []struct {
		name        string
		ownerNodeID string
		attempts    []*model.Attempt
		wantCmdType string
		wantCmd     any
	}{
		{
			name:        "no attempts",
			ownerNodeID: otherOwner,
			attempts:    nil,
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: otherOwner,
				NewOwner:      nodeID,
			},
		},
		{
			name:        "retry elapsed",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeRetry, RetryAt: now.Add(-1 * time.Hour)},
			},
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: otherOwner,
				NewOwner:      nodeID,
			},
		},
		{
			name:        "retry pending",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeRetry, RetryAt: now.Add(1 * time.Hour)},
			},
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: otherOwner,
				NewOwner:      nodeID,
			},
		},
		{
			name:        "last attempt success",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeSuccess},
			},
			wantCmdType: "complete_execution",
			wantCmd: command.CompleteExecutionCommand{
				TenantID:     tenantID,
				ExecutionID:  execID,
				FinalStatus:  model.ExecutionStatusSucceeded,
				FinalOutcome: string(model.OutcomeSuccess),
			},
		},
		{
			name:        "last attempt terminal failure",
			ownerNodeID: otherOwner,
			attempts: []*model.Attempt{
				{Outcome: model.OutcomeTerminalFailure},
			},
			wantCmdType: "complete_execution",
			wantCmd: command.CompleteExecutionCommand{
				TenantID:     tenantID,
				ExecutionID:  execID,
				FinalStatus:  model.ExecutionStatusFailedTerminal,
				FinalOutcome: string(model.OutcomeTerminalFailure),
			},
		},
		{
			name:        "already own it",
			ownerNodeID: nodeID,
			attempts:    nil,
			wantCmdType: "adopt_execution",
			wantCmd: command.AdoptExecutionCommand{
				TenantID:      tenantID,
				ExecutionID:   execID,
				PreviousOwner: nodeID,
				NewOwner:      nodeID,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakeReader{
				tenants: []*model.Tenant{{ID: tenantID}},
				execsByTenant: map[string][]*model.Execution{
					tenantID: {{ID: execID, TenantID: tenantID, OwnerNodeID: tc.ownerNodeID, Status: model.ExecutionStatusInFlight}},
				},
				attemptsByExec: map[string][]*model.Attempt{
					execID: tc.attempts,
				},
			}
			proposer := &fakeProposer{id: nodeID}
			w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

			if err := w.runRecover(context.Background()); err != nil {
				t.Fatalf("runRecover: unexpected error: %v", err)
			}

			if len(proposer.calls) != 1 {
				t.Fatalf("calls: got %d, want 1: %+v", len(proposer.calls), proposer.calls)
			}
			got := proposer.calls[0]
			if got.cmdType != tc.wantCmdType {
				t.Errorf("cmdType: got %q, want %q", got.cmdType, tc.wantCmdType)
			}
			if !reflect.DeepEqual(got.cmd, tc.wantCmd) {
				t.Errorf("cmd: got %+v, want %+v", got.cmd, tc.wantCmd)
			}
		})
	}
}

func TestRunRecover_ListTenantsError(t *testing.T) {
	reader := &fakeReader{listTenantsErr: errors.New("boom")}
	proposer := &fakeProposer{id: "node-self"}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "list tenants") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "list tenants")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0 — nothing should have been proposed", len(proposer.calls))
	}
}

func TestRunRecover_ListExecutionsError(t *testing.T) {
	const tenantID = "tenant-1"
	reader := &fakeReader{
		tenants:           []*model.Tenant{{ID: tenantID}},
		listExecutionsErr: map[string]error{tenantID: errors.New("boom")},
	}
	proposer := &fakeProposer{id: "node-self"}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "list in-flight") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "list in-flight")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_ListAttemptsError(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, Status: model.ExecutionStatusInFlight}},
		},
		listAttemptsErr: map[string]error{execID: errors.New("boom")},
	}
	proposer := &fakeProposer{id: "node-self"}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "read attempts") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "read attempts")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_ProposeError(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, OwnerNodeID: "node-other", Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{execID: nil}, // BucketNoAttempts -> adopt path
	}
	proposer := &fakeProposer{id: "node-self", err: errors.New("boom")}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "propose adopt") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "propose adopt")
	}
}

func TestRunRecover_ContextCancelled(t *testing.T) {
	const tenantID = "tenant-1"
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
	}
	proposer := &fakeProposer{id: "node-self"}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := w.runRecover(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runRecover: got %v, want context.Canceled", err)
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_ClassificationError(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{
			execID: {{Outcome: model.AttemptOutcome("bogus_outcome")}},
		},
	}
	proposer := &fakeProposer{id: "node-self"}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "classify") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "classify")
	}
	if len(proposer.calls) != 0 {
		t.Errorf("calls: got %d, want 0", len(proposer.calls))
	}
}

func TestRunRecover_PanicRecovery(t *testing.T) {
	const (
		tenantID = "tenant-1"
		execID   = "exec-1"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantID}},
		execsByTenant: map[string][]*model.Execution{
			tenantID: {{ID: execID, TenantID: tenantID, Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{
			execID: {nil}, // classifyOne dereferences the last element -> nil pointer panic
		},
	}
	proposer := &fakeProposer{id: "node-self"}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error from recovered panic, got nil")
	}
	if !strings.Contains(err.Error(), "recover: panic") {
		t.Errorf("error: got %q, want it to contain %q", err.Error(), "recover: panic")
	}
}

func TestRunRecover_MultiTenant_ErrorOnSecondTenant(t *testing.T) {
	const (
		tenantA = "tenant-a"
		tenantB = "tenant-b"
		execA   = "exec-a"
	)
	reader := &fakeReader{
		tenants: []*model.Tenant{{ID: tenantA}, {ID: tenantB}},
		execsByTenant: map[string][]*model.Execution{
			tenantA: {{ID: execA, TenantID: tenantA, OwnerNodeID: "node-other", Status: model.ExecutionStatusInFlight}},
		},
		attemptsByExec: map[string][]*model.Attempt{
			execA: nil, // BucketNoAttempts -> adopt
		},
		listExecutionsErr: map[string]error{
			tenantB: errors.New("boom"),
		},
	}
	proposer := &fakeProposer{id: "node-self"}
	w := NewWorker(reader, proposer, slog.New(slog.DiscardHandler))

	err := w.runRecover(context.Background())
	if err == nil {
		t.Fatal("runRecover: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "list in-flight") || !strings.Contains(err.Error(), tenantB) {
		t.Errorf("error: got %q, want it to mention tenant %q and the list in-flight failure", err.Error(), tenantB)
	}
	if len(proposer.calls) != 1 {
		t.Fatalf("calls: got %d, want 1 (tenant A should have been fully processed before tenant B failed)", len(proposer.calls))
	}
	if proposer.calls[0].cmdType != "adopt_execution" {
		t.Errorf("cmdType: got %q, want %q", proposer.calls[0].cmdType, "adopt_execution")
	}
}
