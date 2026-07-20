package callback

import (
	"context"
	"raft-biling/internal/config"
)

type Callback struct {
}

func New(cfg *config.Config) *Callback {
	return &Callback{}
}

func (callback *Callback) Start(ctx context.Context) error    { return nil }
func (callback *Callback) Shutdown(ctx context.Context) error { return nil }
