package scheduler

import (
	"context"
	"log/slog"
	"sync"
)

type Supervisor struct {
	newWorker func() Runner
	notifyCh  <-chan bool
	logger    *slog.Logger
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

func NewSupervisor(newWorker func() Runner, notifyCh <-chan bool, logger *slog.Logger) *Supervisor {
	return &Supervisor{
		newWorker: newWorker,
		notifyCh:  notifyCh,
		logger:    logger,
	}
}

func (sv *Supervisor) Run(ctx context.Context) error {
	ch := sv.notifyCh
	sv.logger.Info("Supervisor started")

	startWorker := func() {
		if sv.cancel != nil {
			sv.logger.Error("attempted to start the leader loop while leader loop is already running")
			return
		}
		workerCtx, cancel := context.WithCancel(ctx)
		sv.cancel = cancel
		worker := sv.newWorker()
		sv.wg.Add(1)
		go func() {
			defer sv.wg.Done()
			if err := worker.Run(workerCtx); err != nil {
				sv.logger.Error("worker exited with error", "error", err)
			}
		}()
	}
	stopWorker := func() {
		if sv.cancel == nil {
			sv.logger.Warn("attempted to stop the leader loop while leader loop is not running")
			return
		}
		sv.cancel()
		sv.wg.Wait()
		sv.cancel = nil
	}
	leaderStep := func(isLeader bool) {
		if isLeader {
			startWorker()
			return
		}
		stopWorker()
	}
	for {
		select {
		case <-ctx.Done():
			if sv.cancel != nil {
				sv.cancel()
				sv.wg.Wait()
			}
			return nil
		case isLeader := <-ch:
			leaderStep(isLeader)
		}
	}
}
