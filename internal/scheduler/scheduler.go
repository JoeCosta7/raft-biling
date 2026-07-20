package scheduler

import (
	"raft-biling/internal/callback"
	"raft-biling/internal/raftnode"
	"raft-biling/internal/statemachine"
)

type Scheduler struct {
}

func New(rn *raftnode.RaftNode, sm *statemachine.StateMachine, cb *callback.Callback) *Scheduler {
	return &Scheduler{}
}
