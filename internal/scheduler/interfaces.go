package scheduler

import (
	"context"
	"time"
)

type Reader interface {
	//will populate later
}

type Proposer interface {
	Propose(cmdType string, cmd any, timeout time.Duration) (any, error)
	ID() string
}

type Runner interface {
	Run(ctx context.Context) error
}
