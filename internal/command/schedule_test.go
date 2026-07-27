package command

import (
	"encoding/json"
	"errors"
	"raft-biling/internal/model"
	"reflect"
	"testing"
	"time"
)

const (
	testTenantID    = "default"
	testScheduleID  = "sched_test_01"
	testExecutionID = "exec_test_01"
	testAttemptID   = "att_test_01"
)

var testTime = time.Date(2026, 7, 21, 12, 0, 0, 0, time.UTC)

func strPtr(s string) *string                    { return &s }
func intPtr(i int) *int                          { return &i }
func durationPtr(d time.Duration) *time.Duration { return &d }

func newTestCreateScheduleCommand(overrides ...func(c *CreateScheduleCommand)) *CreateScheduleCommand {
	dayOfMonth := 15
	sch := &CreateScheduleCommand{
		ID:           testScheduleID,
		TenantID:     testTenantID,
		CreatedBy:    "test",
		CallbackURL:  "https://example.com/webhook",
		Payload:      json.RawMessage(`{"amount":100}`),
		Headers:      map[string]string{"X-Test": "hello"},
		ScheduleType: model.ScheduleTypeRecurring,
		FirstRunAt:   testTime,
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
	}
	for _, o := range overrides {
		o(sch)
	}
	return sch
}

func newTestUpdateScheduleCommand(overrides ...func(*UpdateScheduleCommand)) *UpdateScheduleCommand {
	cmd := &UpdateScheduleCommand{
		TenantID:         testTenantID,
		ID:               testScheduleID,
		CallbackURL:      nil,
		Headers:          nil,
		Payload:          nil,
		RetryBackoff:     nil,
		MaxAttempts:      nil,
		ExecutionTimeout: nil,
	}
	for _, o := range overrides {
		o(cmd)
	}
	return cmd
}

func seedSchedule(f *fakeTx, s *model.Schedule) {
	f.schedules[s.TenantID+":"+s.ID] = s
}

func TestApplyCreateSchedule_OnceHappyPath(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.ScheduleType = model.ScheduleTypeOnce })
	proposedAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyCreateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyCreateSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyCreateSchedule: returned nil schedule with nil error")
	}
	if schedule.SchemaVersion != model.CurrentScheduleSchemaVersion {
		t.Errorf("SchemaVersion: got %d, want %d", schedule.SchemaVersion, model.CurrentScheduleSchemaVersion)
	}
	if !schedule.CreatedAt.Equal(proposedAt) {
		t.Errorf("CreatedAt: got %v, want %v", schedule.CreatedAt, proposedAt)
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", schedule.UpdatedAt, proposedAt)
	}
	if schedule.Status != model.ScheduleStatusActive {
		t.Errorf("Status: got %q, want %q", schedule.Status, model.ScheduleStatusActive)
	}
	if schedule.NextRunAt == nil {
		t.Errorf("NextRunAt: got nil, want non-nil")
	} else if !schedule.NextRunAt.Equal(cmd.FirstRunAt) {
		t.Errorf("NextRunAt: got %v, want %v", *schedule.NextRunAt, cmd.FirstRunAt)
	}
	key := cmd.TenantID + ":" + cmd.ID
	stored, ok := tx.schedules[key]
	if !ok {
		t.Fatalf("expected schedule persisted at key %q, but fake has no entry", key)
	}
	if stored.ID != cmd.ID {
		t.Errorf("stored.ID: got %q, want %q", stored.ID, cmd.ID)
	}

}

func ApplyCreateScheduleHappyPathRecurringPath(t testing.T) {
	//TODO once recurrence actually works

}

func TestApplyCreateSchedule_MissingID(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.ID = "" })
	schedule, err := ApplyCreateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyCreateSchedule_IDCollision(t *testing.T) {
	tx := newFakeTx()
	existing := &model.Schedule{
		TenantID: testTenantID,
		ID:       testScheduleID,
	}
	tx.schedules[testTenantID+":"+testScheduleID] = existing
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.ID = existing.ID })
	schedule, err := ApplyCreateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyCreateSchedule_MissingTenantID(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestCreateScheduleCommand(func(c *CreateScheduleCommand) { c.TenantID = "" })

	schedule, err := ApplyCreateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "tenant_id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "tenant_id")
	}
}

func TestApplyCreateSchedule_RecurrenceComputeFails(t *testing.T) {
	// TODO: once ComputeNextRun is implemented for real, this test needs
	// a genuinely-invalid recurrence config to trigger the error path.
	// For now, the stub errors on any non-nil recurrence, so the default
	// factory (recurring, non-nil recurrence) triggers the branch.
	//tx := newFakeTx()
	//cmd := newTestCreateScheduleCommand() // default is recurring, non-nil recurrence

	//schedule, err := ApplyCreateSchedule(tx, *cmd, time.Time{})

	// same assertion pattern as MissingTenantID:
	// schedule nil, err non-nil, unwraps to *CommandError,
	// Kind==KindValidation, Field=="recurrence"
}

func TestApplyUpdateSchedule_HappyPathOneField(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	newURL := "https://new.example.com/hook"
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.CallbackURL = &newURL })
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyUpdateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyUpdateSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyUpdateSchedule: returned nil schedule with nil error")
	}
	if schedule.CallbackURL != newURL {
		t.Errorf("CallbackURL: got %q, want %q", schedule.CallbackURL, newURL)
	}
	if !reflect.DeepEqual(schedule.Headers, preSeed.Headers) {
		t.Errorf("Headers: got %+v, want %+v", schedule.Headers, preSeed.Headers)
	}
	if !reflect.DeepEqual(schedule.Payload, preSeed.Payload) {
		t.Errorf("Payload: got %v, want %v", schedule.Payload, preSeed.Payload)
	}
	if schedule.RetryBackoff != preSeed.RetryBackoff {
		t.Errorf("RetryBackoff: got %+v, want %+v", schedule.RetryBackoff, preSeed.RetryBackoff)
	}
	if schedule.MaxAttempts != preSeed.MaxAttempts {
		t.Errorf("MaxAttempts: got %d, want %d", schedule.MaxAttempts, preSeed.MaxAttempts)
	}
	if schedule.ExecutionTimeout != preSeed.ExecutionTimeout {
		t.Errorf("ExecutionTimeout: got %v, want %v", schedule.ExecutionTimeout, preSeed.ExecutionTimeout)
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", schedule.UpdatedAt, proposedAt)
	}
	if !schedule.CreatedAt.Equal(preSeed.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", schedule.CreatedAt, preSeed.CreatedAt)
	}

	key := cmd.TenantID + ":" + cmd.ID
	stored, ok := tx.schedules[key]
	if !ok {
		t.Fatalf("expected schedule persisted at key %q, but fake has no entry", key)
	}
	if stored.CallbackURL != newURL {
		t.Errorf("stored.CallbackURL: got %q, want %q", stored.CallbackURL, newURL)
	}
}

func TestApplyUpdateSchedule_HappyPathNoFields(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand()
	proposedAt := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	schedule, err := ApplyUpdateSchedule(tx, *cmd, proposedAt)
	if err != nil {
		t.Fatalf("ApplyUpdateSchedule: unexpected error: %v", err)
	}
	if schedule == nil {
		t.Fatal("ApplyUpdateSchedule: returned nil schedule with nil error")
	}
	if schedule.CallbackURL != preSeed.CallbackURL {
		t.Errorf("CallbackURL: got %q, want %q", schedule.CallbackURL, preSeed.CallbackURL)
	}
	if !reflect.DeepEqual(schedule.Headers, preSeed.Headers) {
		t.Errorf("Headers: got %+v, want %+v", schedule.Headers, preSeed.Headers)
	}
	if !reflect.DeepEqual(schedule.Payload, preSeed.Payload) {
		t.Errorf("Payload: got %v, want %v", schedule.Payload, preSeed.Payload)
	}
	if schedule.RetryBackoff != preSeed.RetryBackoff {
		t.Errorf("RetryBackoff: got %+v, want %+v", schedule.RetryBackoff, preSeed.RetryBackoff)
	}
	if schedule.MaxAttempts != preSeed.MaxAttempts {
		t.Errorf("MaxAttempts: got %d, want %d", schedule.MaxAttempts, preSeed.MaxAttempts)
	}
	if schedule.ExecutionTimeout != preSeed.ExecutionTimeout {
		t.Errorf("ExecutionTimeout: got %v, want %v", schedule.ExecutionTimeout, preSeed.ExecutionTimeout)
	}
	if !schedule.UpdatedAt.Equal(proposedAt) {
		t.Errorf("UpdatedAt: got %v, want %v", schedule.UpdatedAt, proposedAt)
	}
	if !schedule.CreatedAt.Equal(preSeed.CreatedAt) {
		t.Errorf("CreatedAt: got %v, want %v", schedule.CreatedAt, preSeed.CreatedAt)
	}

	key := cmd.TenantID + ":" + cmd.ID
	stored, ok := tx.schedules[key]
	if !ok {
		t.Fatalf("expected schedule persisted at key %q, but fake has no entry", key)
	}
	if stored.CallbackURL != preSeed.CallbackURL {
		t.Errorf("stored.CallbackURL: got %q, want %q", stored.CallbackURL, preSeed.CallbackURL)
	}
}

func TestApplyUpdateSchedule_MissingTenantID(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.TenantID = "" })
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})

	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "tenant_id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "tenant_id")
	}
}

func TestApplyUpdateSchedule_MissingID(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule()
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand(func(c *UpdateScheduleCommand) { c.ID = "" })
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindValidation {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindValidation)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}

}

func TestApplyUpdateSchedule_NotFound(t *testing.T) {
	tx := newFakeTx()
	cmd := newTestUpdateScheduleCommand()
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindNotFound {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindNotFound)
	}
	if cmdErr.Field != "id" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "id")
	}
}

func TestApplyUpdateSchedule_CancelledSchedule(t *testing.T) {
	tx := newFakeTx()
	preSeed := newTestSchedule(func(s *model.Schedule) {
		s.Status = model.ScheduleStatusCanceled
	})
	seedSchedule(tx, preSeed)
	cmd := newTestUpdateScheduleCommand()
	schedule, err := ApplyUpdateSchedule(tx, *cmd, time.Time{})
	if schedule != nil {
		t.Errorf("expected nil schedule on error, got %+v", schedule)
	}
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var cmdErr *CommandError
	if !errors.As(err, &cmdErr) {
		t.Fatalf("error is not *CommandError: got type %T, value %v", err, err)
	}
	if cmdErr.Kind != KindConflict {
		t.Errorf("Kind: got %q, want %q", cmdErr.Kind, KindConflict)
	}
	if cmdErr.Field != "status" {
		t.Errorf("Field: got %q, want %q", cmdErr.Field, "status")
	}

}
