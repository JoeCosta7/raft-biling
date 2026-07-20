package api

import (
	"raft-biling/internal/raftnode"
	"raft-biling/internal/statemachine"
	"raft-biling/internal/config"
)

type API struct{}

func New(cfg *config.Config, rn *raftnode.RaftNode, sm *statemachine.StateMachine) *API {
	return &API{}
}
