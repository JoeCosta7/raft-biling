package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"raft-biling/internal/model"
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

func classifyOne(exec model.Execution, attempts []model.Attempt, now time.Time) (Bucket, error) {
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

func (w *Worker) runRecover(ctx context.Context) error {
	return nil
	//TODO: Implement recovery logic and figure out tick
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
