package api

import (
	"context"
	"raft-biling/internal/config"
	"raft-biling/internal/raftnode"
	"raft-biling/internal/statemachine"
)

type API struct{}

func New(cfg *config.Config, rn *raftnode.RaftNode, sm *statemachine.StateMachine) *API {
	return &API{}
}

func (api *API) Start(ctx context.Context) error    { return nil }
func (api *API) Shutdown(ctx context.Context) error { return nil }
