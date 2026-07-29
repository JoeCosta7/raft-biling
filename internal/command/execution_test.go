package command

import (
	"errors"
	"raft-biling/internal/model"
	"testing"
	"time"
)

func newTestClaimExecutionCommand(opts ...func(*ClaimExecutionCommand)) *ClaimExecutionCommand {
	cmd := &ClaimExecutionCommand{
		ID:           testExecutionID, // "exec_test_01" or similar
		TenantID:     testTenantID,    // "default"
		ScheduleID:   testScheduleID,  // "sched_test_01"
		ScheduledFor: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
		OwnerNodeID:  "node_a", // add a testOwnerNodeID const if you prefer
	}
	for _, o := range opts {
		o(cmd)
	}
	return cmd
}

func TestApplyClaimExecution_HappyPath(t *testing.T) {
	tx := newFakeTx()
	seedSchedule(tx, newTestSchedule())
	cmd := newTestClaimExecutionCommand()
	proposedAt := testTime
	got, err := ApplyClaimExecution(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyClaimExecution: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("ApplyClaimExecution: returned nil execution with nil error")
	}
	if got.ID != cmd.ID {
		t.Errorf("ID: got %q, want %q", got.ID, cmd.ID)
	}
	if got.TenantID != cmd.TenantID {
		t.Errorf("TenantID: got %q, want %q", got.TenantID, cmd.TenantID)
	}
	if got.ScheduleID != cmd.ScheduleID {
		t.Errorf("ScheduleID: got %q, want %q", got.ScheduleID, cmd.ScheduleID)
	}
	if !got.ScheduledFor.Equal(cmd.ScheduledFor) {
		t.Errorf("ScheduledFor: got %v, want %v", got.ScheduledFor, cmd.ScheduledFor)
	}
	if got.Status != model.ExecutionStatusInFlight {
		t.Errorf("Status: got %q, want %q", got.Status, model.ExecutionStatusInFlight)
	}
	if got.OwnerNodeID != cmd.OwnerNodeID {
		t.Errorf("OwnerNodeID: got %q, want %q", got.OwnerNodeID, cmd.OwnerNodeID)
	}
	if want := model.DeriveIdempotencyKey(cmd.ScheduleID, cmd.ScheduledFor); got.IdempotencyKey != want {
		t.Errorf("IdempotencyKey: got %q, want %q", got.IdempotencyKey, want)
	}
	if got.AttemptCount != 0 {
		t.Errorf("AttemptCount: got %d, want 0", got.AttemptCount)
	}
	if got.LastAttemptID != "" {
		t.Errorf("LastAttemptID: got %q, want empty", got.LastAttemptID)
	}
	if got.StartedAt != nil {
		t.Errorf("StartedAt: got %v, want nil", got.StartedAt)
	}
	if got.CompletedAt != nil {
		t.Errorf("CompletedAt: got %v, want nil", got.CompletedAt)
	}
	if got.FinalOutcome != "" {
		t.Errorf("FinalOutcome: got %q, want empty", got.FinalOutcome)
	}
	if got.SchemaVersion != 1 {
		t.Errorf("SchemaVersion: got %d, want 1", got.SchemaVersion)
	}
	if got.ClaimedAt == nil || !got.ClaimedAt.Equal(proposedAt) {
		t.Errorf("ClaimedAt: got %v, want %v", got.ClaimedAt, proposedAt)
	}
	stored, err := tx.GetExecution(cmd.TenantID, cmd.ID)
	if err != nil {
		t.Fatalf("GetExecution: unexpected error: %v", err)
	}
	if stored == nil {
		t.Fatal("GetExecution: returned nil, expected stored execution")
	}
	if stored.Status != model.ExecutionStatusInFlight {
		t.Errorf("stored.Status: got %q, want %q", stored.Status, model.ExecutionStatusInFlight)
	}
	if stored.IdempotencyKey != got.IdempotencyKey {
		t.Errorf("stored.IdempotencyKey: got %q, want %q", stored.IdempotencyKey, got.IdempotencyKey)
	}
	if stored.OwnerNodeID != got.OwnerNodeID {
		t.Errorf("stored.OwnerNodeID: got %q, want %q", stored.OwnerNodeID, got.OwnerNodeID)
	}
}

func TestApplyClaimExecution_Collision(t *testing.T) {
	tx := newFakeTx()
	existing := newTestExecution(func(ex *model.Execution) {
		ex.ID = "exec_existing_01"
	})
	seedExecution(tx, existing)
	cmd := newTestClaimExecutionCommand(func(c *ClaimExecutionCommand) {
		c.ID = "exec_new_01"
	})
	proposedAt := testTime
	got, err := ApplyClaimExecution(tx, *cmd, proposedAt)
	if got != nil {
		t.Errorf("execution: got %v, want nil", got)
	}
	if err == nil {
		t.Fatal("error: got nil, want *CommandError")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error type: got %T, want *CommandError", err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "scheduled_for" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "scheduled_for")
	}
	if cmdErr.Details["existing_execution_id"] != existing.ID {
		t.Errorf("Details[existing_execution_id]: got %v, want %q", cmdErr.Details["existing_execution_id"], existing.ID)
	}
	stored, err := tx.GetExecution(cmd.TenantID, cmd.ID)
	if err != nil {
		t.Fatalf("GetExecution: unexpected error: %v", err)
	}
	if stored != nil {
		t.Errorf("GetExecution(new_id): got %v, want nil (write should not have happened)", stored)
	}
	stored, err = tx.GetExecution(cmd.TenantID, existing.ID)
	if err != nil {
		t.Fatalf("GetExecution: unexpected error: %v", err)
	}
	if stored == nil {
		t.Fatal("existing execution disappeared")
	}
	if stored.OwnerNodeID != existing.OwnerNodeID {
		t.Errorf("existing.OwnerNodeID: got %q, want %q — existing was clobbered", stored.OwnerNodeID, existing.OwnerNodeID)
	}
}

func TestApplyClaimExecution_CollisionDifferentScheduledFor(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestClaimExecutionCommand(func(c *ClaimExecutionCommand) {
		c.ID = "exec_new_01"
	})
	existing := newTestExecution(func(ex *model.Execution) {
		ex.ID = "exec_existing_01"
		ex.ScheduleID = cmd.ScheduleID
		ex.ScheduledFor = cmd.ScheduledFor.Add(-30 * 24 * time.Hour) // a month earlier
	})
	seedExecution(tx, existing)
	proposedAt := testTime
	got, err := ApplyClaimExecution(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyClaimExecution: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("ApplyClaimExecution: returned nil execution with nil error")
	}
	if got.ID != cmd.ID {
		t.Errorf("ID: got %q, want %q", got.ID, cmd.ID)
	}

	storedNew, err := tx.GetExecution(cmd.TenantID, cmd.ID)
	if err != nil {
		t.Fatalf("GetExecution(new): unexpected error: %v", err)
	}
	if storedNew == nil {
		t.Fatal("GetExecution(new): got nil, want stored execution")
	}

	storedExisting, err := tx.GetExecution(cmd.TenantID, existing.ID)
	if err != nil {
		t.Fatalf("GetExecution(existing): unexpected error: %v", err)
	}
	if storedExisting == nil {
		t.Fatal("existing execution disappeared")
	}
	if !storedExisting.ScheduledFor.Equal(existing.ScheduledFor) {
		t.Errorf("existing.ScheduledFor changed: got %v, want %v", storedExisting.ScheduledFor, existing.ScheduledFor)
	}
}

func TestApplyClaimExecution_MultipleExistingNoCollision(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestClaimExecutionCommand(func(c *ClaimExecutionCommand) {
		c.ID = "exec_new_01"
	})
	past1 := newTestExecution(func(ex *model.Execution) {
		ex.ID = "exec_past_01"
		ex.ScheduleID = cmd.ScheduleID
		ex.ScheduledFor = cmd.ScheduledFor.Add(-90 * 24 * time.Hour)
	})
	past2 := newTestExecution(func(ex *model.Execution) {
		ex.ID = "exec_past_02"
		ex.ScheduleID = cmd.ScheduleID
		ex.ScheduledFor = cmd.ScheduledFor.Add(-60 * 24 * time.Hour)
	})
	past3 := newTestExecution(func(ex *model.Execution) {
		ex.ID = "exec_past_03"
		ex.ScheduleID = cmd.ScheduleID
		ex.ScheduledFor = cmd.ScheduledFor.Add(-30 * 24 * time.Hour)
	})
	seedExecution(tx, past1)
	seedExecution(tx, past2)
	seedExecution(tx, past3)
	proposedAt := testTime
	got, err := ApplyClaimExecution(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyClaimExecution: unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("ApplyClaimExecution: returned nil execution with nil error")
	}
	if got.ID != cmd.ID {
		t.Errorf("ID: got %q, want %q", got.ID, cmd.ID)
	}

	stored, err := tx.GetExecution(cmd.TenantID, cmd.ID)
	if err != nil {
		t.Fatalf("GetExecution: unexpected error: %v", err)
	}
	if stored == nil {
		t.Fatal("GetExecution: got nil, want stored execution")
	}

}
