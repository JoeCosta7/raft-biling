package scheduler

import (
	"context"
	"raft-biling/internal/callback"
	"raft-biling/internal/raftnode"
	"raft-biling/internal/statemachine"
)

type Scheduler struct {
}

func New(rn *raftnode.RaftNode, sm *statemachine.StateMachine, cb *callback.Callback) *Scheduler {
	return &Scheduler{}
}

func (scheduler *Scheduler) Start(ctx context.Context) error    { return nil }
func (scheduler *Scheduler) Shutdown(ctx context.Context) error { return nil }
