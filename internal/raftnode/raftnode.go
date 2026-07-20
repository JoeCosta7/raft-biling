package raftnode

import (
	"raft-biling/internal/config"
	"raft-biling/internal/statemachine"
)

type RaftNode struct {
}

func New(cfg *config.Config, sm *statemachine.StateMachine) (*RaftNode, error) {
	return &RaftNode{}, nil
}
