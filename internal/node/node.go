package node

import (
	"context"
	"fmt"
	"raft-biling/internal/api"
	"raft-biling/internal/callback"
	"raft-biling/internal/config"
	"raft-biling/internal/raftnode"
	"raft-biling/internal/scheduler"
	"raft-biling/internal/statemachine"
)

type Node struct {
	cfg          *config.Config
	stateMachine *statemachine.StateMachine
	raftNode     *raftnode.RaftNode
	callback     *callback.Callback
	scheduler    *scheduler.Scheduler
	api          *api.API
}

func New(cfg *config.Config) (*Node, error) {
	sm, err := statemachine.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("statemachine: %w", err)
	}
	rn, err := raftnode.New(cfg, sm)
	if err != nil {
		return nil, fmt.Errorf("raftnode: %w", err)
	}
	cb := callback.New(cfg)
	sch := scheduler.New(rn, sm, cb)
	apiSrv := api.New(cfg, rn, sm)

	return &Node{cfg: cfg, stateMachine: sm, raftNode: rn, callback: cb,
		scheduler: sch, api: apiSrv}, nil
}

// the start phase. Not needed yet for the version you're about to
// write. Will kick off goroutines, open listeners, begin Raft
// participation. Stubs for now, real bodies come later.
func (n *Node) Start(ctx context.Context) error {
	return nil
}

//	the shutdown phase. Not needed yet either. Will walk subsystems
//
// in reverse order, respecting the context deadline.
func (n *Node) Shutdown(ctx context.Context) error {
	return nil
}
