package statemachine

import "raft-biling/internal/config"

type StateMachine struct {
}

func New(cfg *config.Config) (*StateMachine, error) {
	return &StateMachine{}, nil
}
