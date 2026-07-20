package statemachine

import (
	"context"
	"raft-biling/internal/config"
)

type StateMachine struct {
}

func New(cfg *config.Config) (*StateMachine, error) {
	return &StateMachine{}, nil
}

func (statemachine *StateMachine) Start(ctx context.Context) error    { return nil }
func (statemachine *StateMachine) Shutdown(ctx context.Context) error { return nil }
