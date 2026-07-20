package raftnode

import (
	"context"
	"raft-biling/internal/config"
	"raft-biling/internal/statemachine"
)

type RaftNode struct {
}

func New(cfg *config.Config, sm *statemachine.StateMachine) (*RaftNode, error) {
	return &RaftNode{}, nil
}

func (raftnode *RaftNode) Start(ctx context.Context) error    { return nil }
func (raftnode *RaftNode) Shutdown(ctx context.Context) error { return nil }
