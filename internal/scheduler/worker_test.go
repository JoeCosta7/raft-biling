package scheduler

import (
	"strings"
	"testing"
	"time"

	"raft-biling/internal/model"
)

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
