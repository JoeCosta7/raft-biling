package storage

import (
	"encoding/json"
	"raft-biling/internal/model"
	"reflect"
	"testing"
	"time"
)

var testTime = time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

const (
	testTenantID    = "default"
	testScheduleID  = "sched_test_01"
	testExecutionID = "exec_test_01"
	testAttemptID   = "att_test_01"
)

func newTestStorage(t *testing.T) *BoltStorage {
	t.Helper()
	dir := t.TempDir()
	s, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func newTestSchedule() *model.Schedule {
	dayOfMonth := 15
	return &model.Schedule{
		SchemaVersion: 1,
		ID:            testScheduleID,
		TenantID:      testTenantID,
		CreatedAt:     testTime,
		UpdatedAt:     testTime,
		CreatedBy:     "test",
		CallbackURL:   "https://example.com/webhook",
		Payload:       json.RawMessage(`{"amount":100}`),
		Headers:       map[string]string{"X-Test": "hello"},
		ScheduleType:  model.ScheduleTypeRecurring,
		FirstRunAt:    testTime,
		Recurrence: &model.Recurrence{
			Cadence:    model.CadenceMonthly,
			DayOfMonth: &dayOfMonth,
		},
		Timezone:    "UTC",
		MaxAttempts: 5,
		RetryBackoff: model.RetryBackoff{
			Initial:    30 * time.Second,
			Multiplier: 2.0,
			Max:        1 * time.Hour,
		},
		ExecutionTimeout: 5 * time.Minute,
		CatchUpPolicy:    model.CatchUpAll,
		Status:           model.ScheduleStatusActive,
		NextRunAt:        testTime,
	}
}

func newTestExecution() *model.Execution {
	return &model.Execution{
		SchemaVersion:  1,
		ID:             testExecutionID,
		TenantID:       testTenantID,
		ScheduleID:     testScheduleID,
		ScheduledFor:   testTime,
		IdempotencyKey: "first",
		OwnerNodeID:    "node1",
		ClaimedAt:      &testTime,
		Status:         model.ExecutionStatusInFlight,
		StartedAt:      &testTime,
		CompletedAt:    &testTime,
		AttemptCount:   1,
		LastAttemptID:  "last",
		FinalOutcome:   "completed successfully",
	}
}

func newTestAttempt() *model.Attempt {
	return &model.Attempt{
		SchemaVersion:       1,
		ID:                  testAttemptID,
		TenantID:            testTenantID,
		ExecutionID:         testExecutionID,
		NodeID:              "node1",
		AttemptNumber:       1,
		StartedAt:           &testTime,
		CompletedAt:         &testTime,
		RequestURL:          "https://example.com/webhook",
		RequestHeaders:      map[string]string{"X-Test": "hello"},
		RequestBodyHash:     "sha256:abc123",
		ResponseStatus:      200,
		ResponseBodyExcerpt: "ok",
		ResponseHeaders:     map[string]string{"Content-Type": "application/json"},
		Outcome:             model.OutcomeSuccess,
	}
}
func TestStorage_ScheduleRoundTrip(t *testing.T) {
	s := newTestStorage(t)
	schedule := newTestSchedule()
	if err := s.Update(func(tx Tx) error { return tx.PutSchedule(schedule) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var got *model.Schedule
	err := s.View(func(tx Tx) error {
		result, err := tx.GetSchedule(schedule.TenantID, schedule.ID)
		if err != nil {
			return err
		}
		got = result
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if !reflect.DeepEqual(got, schedule) {
		t.Errorf("round-trip mismatch:\ngot:  %+v\nwant: %+v", got, schedule)
	}
}

func TestStorage_ExecutionRoundTrip(t *testing.T) {
	s := newTestStorage(t)
	execution := newTestExecution()
	if err := s.Update(func(tx Tx) error { return tx.PutExecution(execution) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var got *model.Execution
	err := s.View(func(tx Tx) error {
		result, err := tx.GetExecution(execution.TenantID, execution.ID)
		if err != nil {
			return err
		}
		got = result
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if !reflect.DeepEqual(got, execution) {
		t.Errorf("round-trip mismatch:\ngot:  %+v\nwant: %+v", got, execution)
	}

}

func TestStorage_AttemptRoundTrip(t *testing.T) {
	s := newTestStorage(t)
	attempt := newTestAttempt()
	if err := s.Update(func(tx Tx) error { return tx.PutAttempt(attempt) }); err != nil {
		t.Fatalf("Update: %v", err)
	}
	var got *model.Attempt
	err := s.View(func(tx Tx) error {
		result, err := tx.GetAttempt(attempt.TenantID, attempt.ID)
		if err != nil {
			return err
		}
		got = result
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
	if !reflect.DeepEqual(got, attempt) {
		t.Errorf("round-trip mismatch:\ngot:  %+v\nwant: %+v", got, attempt)
	}

}

func TestStorage_GetMissingReturnsNil(t *testing.T) {
	s := newTestStorage(t)
	err := s.View(func(tx Tx) error {
		gots, err := tx.GetSchedule(testTenantID, "does_not_exist")
		if err != nil {
			return err
		}
		if gots != nil {
			t.Errorf("GetSchedule: got %v, want nil", gots)
		}
		gotex, err := tx.GetExecution(testTenantID, "does_not_exist")
		if err != nil {
			return err
		}
		if gotex != nil {
			t.Errorf("GetExecution: got %v, want nil", gotex)
		}
		gotat, err := tx.GetAttempt(testTenantID, "does_not_exist")
		if err != nil {
			return err
		}
		if gotat != nil {
			t.Errorf("GetAttempt: got %v, want nil", gotat)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
}
