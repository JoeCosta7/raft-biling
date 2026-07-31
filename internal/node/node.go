package node

import (
	"context"
	"errors"
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
	rn, err := raftnode.New(cfg, sm.Storage(), sm)
	if err != nil {
		return nil, fmt.Errorf("raftnode: %w", err)
	}
	cb := callback.New(cfg)
	sch := scheduler.New(rn, sm, cb)
	apiSrv := api.New(cfg, rn, sm)

	return &Node{cfg: cfg, stateMachine: sm, raftNode: rn, callback: cb,
		scheduler: sch, api: apiSrv}, nil
}

// Start walks subsystems in a specific order calling their Starts;
// Shutdown walks them in reverse calling their Shutdowns. Both respect the context.
func (n *Node) Start(ctx context.Context) error {
	n.stateMachine.Start(ctx)
	n.callback.Start(ctx)
	if err := n.api.Start(ctx); err != nil {
		return fmt.Errorf("api start: %w", err)
	}
	if err := n.raftNode.Start(ctx); err != nil {
		return fmt.Errorf("raftnode start: %w", err)
	}
	n.scheduler.Start(ctx)
	return nil
}

//	the shutdown phase. Not needed yet either. Will walk subsystems
//
// in reverse order, respecting the context deadline.
func (n *Node) Shutdown(ctx context.Context) error {
	var errs []error
	if err := n.scheduler.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("scheduler shutdown: %w", err))
	}
	if err := n.api.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("api shutdown: %w", err))
	}
	n.raftNode.Shutdown(ctx)
	n.callback.Shutdown(ctx)
	n.stateMachine.Shutdown(ctx)
	return errors.Join(errs...)
}
